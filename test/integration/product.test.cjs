const { test } = require('node:test');
const assert = require('node:assert/strict');
const { randomUUID } = require('node:crypto');
const { Database } = require('@ticketing/core');
const { Client } = require('pg');
const { fixture, request, until } = require('./support.cjs');

test(
  'готовый продукт: HTTP → SQL/outbox → RabbitMQ/Kafka → билеты и отчёты',
  { timeout: 240000 },
  async (t) => {
    const f = await fixture();
    t.after(async () => {
      if (t.signal.aborted) console.error(f.logTail());
      await f.cleanup();
    });
    let relay;
    try {
      f.start('sales');
      f.start('sales', { SALES_PORT: String(f.ports.second) });
      f.start('fulfillment');
      f.start('analytics');
      f.start('payment-mock');
      f.start('worker');
      await Promise.all([
        f.ready(f.urls.sales),
        f.ready(f.urls.second),
        f.ready(f.urls.fulfillment),
        f.ready(f.urls.analytics),
        f.ready(f.urls.paymentMock),
      ]);
      const buyer = await f.login(),
        organizer = await f.login(f.DEMO.organizer),
        otherBuyer = await f.login(f.DEMO.otherBuyer);
      const purchase = async (
        eventId,
        quantity = 1,
        key = randomUUID(),
        auth = buyer,
        base = f.urls.sales,
      ) => {
        const reserved = await request(base, '/orders', {
          method: 'POST',
          body: { eventId, quantity },
          auth,
          headers: { 'Idempotency-Key': key },
        });
        if (reserved.status !== 201) return reserved;
        const payment = await request(base, `/orders/${reserved.data.id}/payments`, { method: 'POST', auth });
        assert.equal(payment.status, 202, JSON.stringify(payment.data));
        let current;
        await until(async () => {
          current = await request(base, `/orders/${reserved.data.id}`, { auth });
          return current.data.status === 'confirmed';
        }, 'payment confirmed');
        return { ...reserved, data: current.data };
      };
      const createEvent = async (capacity, title = 'Проверка конкурентных продаж') => {
        const result = await request(f.urls.sales, '/events', {
          method: 'POST',
          auth: organizer,
          body: {
            title,
            venue: 'Тестовый зал',
            startsAt: '2027-12-25T15:00:00Z',
            capacity,
            unitPrice: 150000,
          },
        });
        assert.equal(result.status, 201, JSON.stringify(result.data));
        return result.data;
      };
      await t.test('Swagger, SQL JOIN и права пользователей', async () => {
        const document = await request(f.urls.sales, '/openapi.json');
        assert.equal(document.status, 200);
        assert.ok(document.data.paths['/orders']);
        assert.ok(document.data.components.securitySchemes.csrf);
        assert.equal((await request(f.urls.second, '/auth/me', { auth: buyer })).data.id, f.DEMO.buyer.id);
        const events = await request(f.urls.sales, '/organizer/events', { auth: organizer });
        assert.equal(events.status, 200);
        assert.ok(events.data.items.some((event) => event.id === f.DEMO.emptyEventId && event.sold === 0));
        const denied = await request(f.urls.sales, '/events', { method: 'POST', auth: buyer, body: {} });
        assert.equal(denied.status, 403);
        const csrf = await request(f.urls.sales, '/orders', {
          method: 'POST',
          body: { eventId: f.DEMO.eventId, quantity: 1 },
          headers: { Cookie: buyer.cookie, 'Idempotency-Key': randomUUID() },
        });
        assert.equal(csrf.status, 403);
        const otherOrganizer = await f.login(f.DEMO.otherOrganizer);
        assert.equal(
          (
            await request(f.urls.sales, `/events/${f.DEMO.eventId}`, {
              method: 'PATCH',
              auth: otherOrganizer,
              body: { title: 'Чужое', version: 1 },
            })
          ).status,
          404,
        );
        const wrongDatabase = new URL(f.connections.sales);
        wrongDatabase.pathname = new URL(f.connections.analytics).pathname;
        const unauthorized = new Client({ connectionString: wrongDatabase.toString() });
        try {
          await assert.rejects(unauthorized.connect(), (error) => error.code === '42501');
        } finally {
          await unauthorized.end();
        }
      });
      await t.test('последнее место и конкурирующие запросы с одним ключом', async () => {
        const event = await createEvent(1);
        const responses = await Promise.all(
          Array.from({ length: 20 }, (_, index) => purchase(event.id, 1, `last-${index}`)),
        );
        assert.equal(responses.filter((r) => r.status === 201).length, 1);
        assert.equal(responses.filter((r) => r.status === 409).length, 19);
        const key = randomUUID();
        const repeated = await Promise.all(Array.from({ length: 8 }, () => purchase(f.DEMO.eventId, 2, key)));
        assert.ok(
          repeated.every((r) => r.status === 201),
          JSON.stringify(repeated),
        );
        assert.equal(new Set(repeated.map((r) => r.data.id)).size, 1);
        const order = repeated[0].data;
        assert.deepEqual((await purchase(f.DEMO.eventId, 2, key, buyer, f.urls.second)).data, order);
        assert.equal((await purchase(f.DEMO.eventId, 3, key)).status, 409);
        assert.equal((await request(f.urls.sales, `/orders/${order.id}`, { auth: otherBuyer })).status, 404);
        assert.equal(
          (await request(f.urls.sales, `/orders/${order.id}/tickets`, { auth: otherBuyer })).status,
          404,
        );
        const mine = await request(f.urls.sales, '/orders', { auth: buyer });
        assert.ok(mine.data.items.some((item) => item.id === order.id));
        const pending = await request(f.urls.sales, `/orders/${order.id}/tickets`, { auth: buyer });
        assert.equal(pending.data.status, 'pending');
        const counts = await f.databases.sales.query(
          'SELECT count(*)::int AS count FROM outbox_deliveries WHERE status=$1',
          ['pending'],
        );
        assert.equal(counts.rows[0].count, 4);
      });
      await t.test('SQL rollback убирает заказ, остаток, ключ и outbox вместе', async () => {
        const before = (
          await f.databases.sales.query('SELECT available FROM events WHERE id=$1', [f.DEMO.eventId])
        ).rows[0].available;
        await f.databases.sales
          .query(`CREATE FUNCTION reject_delivery() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'test rollback'; END $$;
        CREATE TRIGGER force_rollback BEFORE INSERT ON orders FOR EACH ROW EXECUTE FUNCTION reject_delivery()`);
        const key = randomUUID();
        try {
          assert.equal((await purchase(f.DEMO.eventId, 1, key)).status, 500);
        } finally {
          await f.databases.sales.query(
            'DROP TRIGGER force_rollback ON orders; DROP FUNCTION reject_delivery()',
          );
        }
        assert.equal(
          (await f.databases.sales.query('SELECT available FROM events WHERE id=$1', [f.DEMO.eventId]))
            .rows[0].available,
          before,
        );
        assert.equal(
          (
            await f.databases.sales.query(
              'SELECT count(*)::int AS count FROM idempotency_keys WHERE key=$1',
              [key],
            )
          ).rows[0].count,
          0,
        );
        assert.equal((await purchase(f.DEMO.eventId, 1, key)).status, 201);
        const db = new Database(f.connections.sales, 'nested-transaction-test');
        try {
          await assert.rejects(
            db.run(async () => {
              await db.query("UPDATE events SET title='MUST ROLLBACK' WHERE id=$1", [f.DEMO.eventId]);
              try {
                await db.run(async () => {
                  throw new Error('nested failed');
                });
              } catch {}
            }),
            /rollback-only/,
          );
          await assert.rejects(
            db.run(async () => {
              try {
                await db.query('SELECT 1 / 0');
              } catch {}
              return 'must not report a committed transaction';
            }),
            /rollback-only/,
          );
        } finally {
          await db.close();
        }
        assert.notEqual(
          (await f.databases.sales.query('SELECT title FROM events WHERE id=$1', [f.DEMO.eventId])).rows[0]
            .title,
          'MUST ROLLBACK',
        );
      });
      await t.test('доставки outbox независимы; получатели догоняют после восстановления', async () => {
        relay = f.start('relay', { AMQP_URL: 'amqp://ticketing:ticketing-local@127.0.0.1:1' });
        await until(
          async () =>
            (
              await f.databases.sales.query(
                "SELECT count(*)::int AS count FROM outbox_deliveries WHERE destination='kafka' AND status<>'delivered'",
              )
            ).rows[0].count === 0,
          'Kafka delivered while Rabbit unavailable',
        );
        assert.ok(
          (
            await f.databases.sales.query(
              "SELECT count(*)::int AS count FROM outbox_deliveries WHERE destination='rabbitmq' AND status<>'delivered'",
            )
          ).rows[0].count > 0,
        );
        await f.stop(relay, 'SIGKILL');
        // An expired lease is recovered by a different process, with the same message identity.
        await f.databases.sales.query(
          "UPDATE outbox_deliveries SET lease_until=now()-interval '1 second', available_at=now() WHERE status<>'delivered'",
        );
        relay = f.start('relay');
        await until(
          async () =>
            (
              await f.databases.sales.query(
                "SELECT count(*)::int AS count FROM outbox_deliveries WHERE status<>'delivered'",
              )
            ).rows[0].count === 0,
          'both outbox destinations delivered',
        );
        const expected = (await f.databases.sales.query('SELECT sum(quantity)::int AS tickets FROM orders'))
          .rows[0].tickets;
        await until(
          async () =>
            (await f.databases.fulfillment.query('SELECT count(*)::int AS count FROM tickets')).rows[0]
              .count === expected,
          'all tickets issued',
        );
        await until(async () => {
          const reports = await request(f.urls.sales, '/reports/events', { auth: organizer });
          return (
            reports.status === 200 && reports.data.reduce((sum, row) => sum + row.tickets, 0) === expected
          );
        }, 'both-service HTTP reports');
        const orders = (await request(f.urls.sales, '/orders?limit=100', { auth: buyer })).data.items;
        for (const order of orders)
          await until(
            async () =>
              (await request(f.urls.sales, `/orders/${order.id}/tickets`, { auth: buyer })).data.status ===
              'completed',
            'notification and ticket completed',
          );
      });
      await t.test('недоступная Kafka не блокирует выпуск билетов через RabbitMQ', async () => {
        await f.stop(relay);
        relay = f.start('relay', { KAFKA_BROKERS: '127.0.0.1:1' });
        const created = await purchase(f.DEMO.eventId, 1);
        assert.equal(created.status, 201);
        await until(
          async () =>
            (
              await f.databases.sales.query(
                `SELECT d.status FROM outbox_deliveries d
        INNER JOIN outbox_messages m USING(message_id) WHERE m.order_id=$1 AND d.destination='rabbitmq'`,
                [created.data.id],
              )
            ).rows[0]?.status === 'delivered',
          'Rabbit delivered while Kafka unavailable',
        );
        assert.notEqual(
          (
            await f.databases.sales.query(
              `SELECT d.status FROM outbox_deliveries d
        INNER JOIN outbox_messages m USING(message_id) WHERE m.order_id=$1 AND d.destination='kafka'`,
              [created.data.id],
            )
          ).rows[0].status,
          'delivered',
        );
        await f.stop(relay, 'SIGKILL');
        await f.databases.sales.query(
          "UPDATE outbox_deliveries SET lease_until=now()-interval '1 second', available_at=now() WHERE status<>'delivered'",
        );
        relay = f.start('relay');
        const expected = (await f.databases.sales.query('SELECT sum(quantity)::int AS tickets FROM orders'))
          .rows[0].tickets;
        await until(
          async () =>
            (await request(f.urls.sales, '/reports/events', { auth: organizer })).data.reduce(
              (sum, row) => sum + row.tickets,
              0,
            ) === expected,
          'Kafka catches up after reconnect',
        );
        await until(
          async () =>
            (await request(f.urls.sales, `/orders/${created.data.id}/tickets`, { auth: buyer })).data
              .status === 'completed',
          'tickets completed with recovered relay',
        );
      });
      await t.test('повтор публикации после коммита не создаёт повторные эффекты', async () => {
        const before = await f.databases.fulfillment.query('SELECT count(*)::int AS count FROM tickets');
        const totals = (await request(f.urls.sales, '/reports/events', { auth: organizer })).data;
        await f.databases.sales.query(
          "UPDATE outbox_deliveries SET status='pending', available_at=now(), lease_owner=NULL, lease_until=NULL",
        );
        await until(
          async () =>
            (
              await f.databases.sales.query(
                "SELECT count(*)::int AS count FROM outbox_deliveries WHERE status<>'delivered'",
              )
            ).rows[0].count === 0,
          'republished duplicates',
        );
        await f.delay(1000);
        assert.equal(
          (await f.databases.fulfillment.query('SELECT count(*)::int AS count FROM tickets')).rows[0].count,
          before.rows[0].count,
        );
        assert.deepEqual((await request(f.urls.sales, '/reports/events', { auth: organizer })).data, totals);
        const popular = await request(f.urls.sales, '/reports/popular', { auth: organizer });
        assert.equal(popular.status, 200);
        assert.equal(popular.data[0].tickets, Math.max(...totals.map((row) => row.tickets)));
      });
      await t.test('replay при новых продажах сохраняет полный результат', async () => {
        const replaying = request(f.urls.analytics, '/internal/replay', {
          method: 'POST',
          body: { projection: 'all' },
          headers: { 'X-Internal-Token': f.settings.internalToken },
          timeout: 90000,
        });
        assert.equal((await purchase(f.DEMO.eventId, 3)).status, 201);
        const rebuilt = await replaying;
        assert.equal(rebuilt.status, 200, JSON.stringify(rebuilt.data));
        const expected = (
          await f.databases.sales.query(
            'SELECT sum(quantity)::int AS tickets, sum(total_price)::float8 AS revenue FROM orders',
          )
        ).rows[0];
        await until(
          async () => {
            const reports = (await request(f.urls.sales, '/reports/events', { auth: organizer })).data;
            return (
              Array.isArray(reports) &&
              reports.reduce((sum, row) => sum + row.tickets, 0) === expected.tickets
            );
          },
          'new generation consumed messages appended during replay',
          90000,
        );
        const daily = await request(f.urls.sales, '/reports/daily', { auth: organizer });
        assert.equal(daily.status, 200);
        await until(async () => {
          const rows = (await request(f.urls.sales, '/reports/daily', { auth: organizer })).data;
          return rows.at(-1)?.cumulativeRevenue === expected.revenue;
        }, 'daily cumulative revenue matches SQL source');
      });
      await t.test('кеш обновляется, выход отзывает общую сессию', async () => {
        await request(f.urls.sales, '/events');
        const result = await request(f.urls.sales, `/events/${f.DEMO.eventId}`, {
          method: 'PATCH',
          auth: organizer,
          body: { title: 'Обновлённое мероприятие', version: 1 },
        });
        assert.equal(result.status, 200);
        const catalog = await request(f.urls.second, '/events');
        assert.equal(
          catalog.data.items.find((event) => event.id === f.DEMO.eventId).title,
          'Обновлённое мероприятие',
        );
        assert.equal(
          (await request(f.urls.sales, '/auth/logout', { method: 'POST', auth: otherBuyer })).status,
          200,
        );
        assert.equal((await request(f.urls.second, '/auth/me', { auth: otherBuyer })).status, 401);
      });
    } catch (error) {
      console.error(f.logTail());
      throw error;
    }
  },
);
