const { test } = require('node:test');
const assert = require('node:assert/strict');
const { randomUUID, createHmac } = require('node:crypto');
const { fixture, request, until, delay } = require('./support.cjs');

test(
  'checkout lifecycle: reservations, provider failures, refunds, ordered revisions and CSV',
  { timeout: 300000 },
  async (t) => {
    const f = await fixture();
    t.after(() => f.cleanup());
    try {
      f.start('sales');
      f.start('sales', { SALES_PORT: String(f.ports.second) });
      f.start('payment-mock');
      f.start('fulfillment');
      f.start('analytics');
      await Promise.all(
        [f.urls.sales, f.urls.second, f.urls.paymentMock, f.urls.fulfillment, f.urls.analytics].map(f.ready),
      );
      const buyer = await f.login(),
        other = await f.login(f.DEMO.otherBuyer),
        organizer = await f.login(f.DEMO.organizer);
      let relay = f.start('relay');
      f.start('worker');
      f.start('worker');
      const get = (id) => request(f.urls.sales, `/orders/${id}`, { auth: buyer });
      const event = async (capacity = 10, title = 'Lifecycle') => {
        const r = await request(f.urls.sales, '/events', {
          method: 'POST',
          auth: organizer,
          body: { title, venue: 'Test', startsAt: '2035-01-01T12:00:00Z', capacity, unitPrice: 12000 },
        });
        assert.equal(r.status, 201, JSON.stringify(r.data));
        return r.data;
      };
      const reserve = async (e, key = randomUUID(), quantity = 1) => {
        const r = await request(f.urls.sales, '/orders', {
          method: 'POST',
          auth: buyer,
          headers: { 'Idempotency-Key': key },
          body: { eventId: e.id, quantity },
        });
        assert.equal(r.status, 201, JSON.stringify(r.data));
        return r.data;
      };
      const pay = async (o) => {
        const r = await request(f.urls.sales, `/orders/${o.id}/payments`, { method: 'POST', auth: buyer });
        assert.equal(r.status, 202, JSON.stringify(r.data));
        return r.data;
      };
      const waitState = async (id, status, payment) => {
        let current;
        await until(
          async () => {
            current = (await get(id)).data;
            return current.status === status && (!payment || current.payment?.status === payment);
          },
          `${status}/${payment ?? ''}`,
        );
        return current;
      };
      const scenario = async (o, mode, delayMs = 50) => {
        const r = await request(f.urls.paymentMock, `/internal/scenarios/${o.id}`, {
          method: 'POST',
          headers: { 'X-Internal-Token': f.settings.internalToken },
          body: { mode, delayMs },
        });
        assert.equal(r.status, 204);
      };
      const cancel = async (o) => {
        const r = await request(f.urls.sales, `/orders/${o.id}/cancel`, { method: 'POST', auth: buyer });
        assert.equal(r.status, 200, JSON.stringify(r.data));
        return r.data;
      };
      const available = async (e) =>
        (await f.databases.sales.query('SELECT available FROM events WHERE id=$1', [e.id])).rows[0].available;

      await t.test('cursor pagination and optimistic editing survive concurrent changes', async () => {
        const e = await event(20);
        for (let i = 0; i < 3; i++) await reserve(e);
        const first = await request(f.urls.sales, '/orders?limit=1', { auth: buyer });
        assert.equal(first.data.items.length, 1);
        assert.ok(first.data.nextCursor);
        const newlyCreated = await reserve(e);
        const second = await request(
          f.urls.sales,
          '/orders?limit=1&cursor=' + encodeURIComponent(first.data.nextCursor),
          { auth: buyer },
        );
        assert.equal(second.status, 200);
        assert.notEqual(second.data.items[0].id, first.data.items[0].id);
        assert.notEqual(second.data.items[0].id, newlyCreated.id);
        assert.equal(
          (
            await request(f.urls.sales, '/orders?cursor=' + encodeURIComponent(first.data.nextCursor), {
              auth: other,
            })
          ).status,
          400,
        );
        const catalogue = await request(f.urls.sales, '/events?limit=1');
        assert.ok(catalogue.data.nextCursor);
        await event();
        assert.equal(
          (await request(f.urls.sales, '/events?cursor=' + encodeURIComponent(catalogue.data.nextCursor)))
            .data.code,
          'CURSOR_STALE',
        );
        const patch = (title) =>
          request(f.urls.sales, `/events/${e.id}`, {
            method: 'PATCH',
            auth: organizer,
            body: { version: 1, title },
          });
        const results = await Promise.all([patch('first'), patch('second')]);
        assert.deepEqual(results.map((r) => r.status).sort(), [200, 409]);
        const changed = results.find((r) => r.status === 200).data;
        assert.equal(changed.version, 2);
        assert.equal(
          (
            await request(f.urls.sales, `/events/${e.id}`, {
              method: 'PATCH',
              auth: organizer,
              body: { version: 2, capacity: 1 },
            })
          ).status,
          409,
        );
      });

      await t.test('lost response and missing webhook are resolved without another charge', async () => {
        for (const mode of ['lost_response', 'no_webhook', 'duplicate_webhook']) {
          const e = await event(),
            o = await reserve(e);
          await scenario(o, mode);
          const responses = await Promise.all([pay(o), pay(o)]);
          assert.equal(responses[0].id, responses[1].id);
          const confirmed = await waitState(o.id, 'confirmed', 'succeeded');
          assert.equal(confirmed.revision, 2);
          const count = (
            await f.databases.paymentMock.query(
              'SELECT count(*)::integer AS n FROM provider_payments WHERE order_id=$1',
              [o.id],
            )
          ).rows[0].n;
          assert.equal(count, 1);
          await until(
            async () =>
              (await request(f.urls.sales, `/orders/${o.id}/tickets`, { auth: buyer })).data.status ===
              'completed',
            'tickets issued',
          );
        }
      });

      await t.test('decline and expiration release seats once with two workers', async () => {
        const e = await event(1),
          o = await reserve(e);
        await scenario(o, 'decline');
        await pay(o);
        await waitState(o.id, 'cancelled', 'failed');
        assert.equal(await available(e), 1);
        const second = await reserve(e);
        await f.databases.sales.query("UPDATE orders SET expires_at=now()-interval '1 second' WHERE id=$1", [
          second.id,
        ]);
        const attempted = await request(f.urls.sales, `/orders/${second.id}/payments`, {
          method: 'POST',
          auth: buyer,
        });
        assert.equal(attempted.status, 409);
        await waitState(second.id, 'expired');
        assert.equal(await available(e), 1);
        await delay(600);
        assert.equal(await available(e), 1);
      });

      await t.test('a capture after expiry is refunded without confirming or issuing tickets', async () => {
        const e = await event(1),
          o = await reserve(e);
        await scenario(o, 'success', 2000);
        await pay(o);
        await until(
          async () =>
            (
              await f.databases.paymentMock.query(
                'SELECT count(*)::integer AS n FROM provider_payments WHERE order_id=$1',
                [o.id],
              )
            ).rows[0].n === 1,
          'charge accepted',
        );
        await f.databases.sales.query("UPDATE orders SET expires_at=now()-interval '1 second' WHERE id=$1", [
          o.id,
        ]);
        const current = await waitState(o.id, 'expired', 'refunded');
        assert.equal(current.confirmedAt, null);
        assert.ok(current.refundedAt);
        assert.equal(await available(e), 1);
        assert.equal(
          (
            await f.databases.fulfillment.query(
              'SELECT count(*)::integer AS n FROM tickets WHERE order_id=$1',
              [o.id],
            )
          ).rows[0].n,
          0,
        );
      });

      let refunded;
      await t.test('full refund retries, retains cancelled state and corrects reports', async () => {
        const e = await event(1),
          o = await reserve(e);
        await scenario(o, 'refund_failure');
        await pay(o);
        await waitState(o.id, 'confirmed');
        await Promise.all([cancel(o), cancel(o)]);
        refunded = await waitState(o.id, 'cancelled', 'refunded');
        assert.equal(await available(e), 1);
        assert.equal(
          (
            await f.databases.sales.query(
              "SELECT count(*)::integer AS n FROM payment_operations x JOIN payments p ON p.id=x.payment_id WHERE p.order_id=$1 AND x.kind='refund'",
              [o.id],
            )
          ).rows[0].n,
          1,
        );
        await until(async () => {
          const r = (await request(f.urls.sales, '/reports/events', { auth: organizer })).data;
          const row = r.find((x) => x.eventId === e.id);
          return (
            row?.tickets === 0 && row.revenue === 0 && row.grossRevenue === 12000 && row.refunds === 12000
          );
        }, 'refund projection');
        await until(
          async () =>
            (await request(f.urls.sales, `/orders/${o.id}/tickets`, { auth: buyer })).data.status ===
            'cancelled',
          'tickets invalidated',
        );
      });

      await t.test('webhooks verify raw bytes, time, identity and duplicate content', async () => {
        const payment = (
          await request(f.urls.paymentMock, `/payments/${refunded.payment.id}`, {
            headers: { 'X-Internal-Token': f.settings.internalToken },
          })
        ).data;
        const eventId = randomUUID();
        const send = async (p, signatureOverride, time = String(Math.floor(Date.now() / 1000))) => {
          const body = JSON.stringify({ eventId, payment: p }),
            signature =
              signatureOverride ??
              createHmac('sha256', f.settings.webhookSecret)
                .update(time + '.' + body)
                .digest('hex');
          return fetch(f.urls.sales + '/webhooks/payments', {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
              'X-Webhook-Timestamp': time,
              'X-Webhook-Signature': signature,
            },
            body,
          });
        };
        let r = await send(payment, '0'.repeat(64));
        assert.equal(r.status, 401);
        await r.arrayBuffer();
        r = await send(payment);
        assert.equal(r.status, 204);
        r = await send(payment);
        assert.equal(r.status, 204);
        r = await send({ ...payment, version: payment.version + 1 });
        assert.equal(r.status, 409);
        await r.arrayBuffer();
        r = await send(payment, undefined, '1');
        assert.equal(r.status, 401);
        await r.arrayBuffer();
      });

      await t.test('reverse delivery and replay cannot revive a cancelled order', async () => {
        await f.stop(relay);
        const e = await event(1),
          o = await reserve(e);
        await pay(o);
        await waitState(o.id, 'confirmed');
        await cancel(o);
        await waitState(o.id, 'cancelled', 'refunded');
        const messages = (
          await f.databases.sales.query(
            'SELECT payload FROM outbox_messages WHERE order_id=$1 ORDER BY order_revision DESC',
            [o.id],
          )
        ).rows.map((r) => r.payload);
        assert.equal(messages.length, 3);
        const { Kafka, logLevel } = require('kafkajs'),
          amqp = require('amqplib');
        const producer = new Kafka({
          brokers: f.settings.kafkaBrokers,
          logLevel: logLevel.NOTHING,
        }).producer();
        await producer.connect();
        const connection = await amqp.connect(f.settings.amqpUrl),
          channel = await connection.createConfirmChannel();
        try {
          for (const m of [...messages, messages[0]]) {
            await producer.send({
              topic: f.env.KAFKA_TOPIC,
              messages: [{ key: m.order.eventId, value: JSON.stringify(m) }],
            });
            channel.publish(
              `${f.env.RESOURCE_PREFIX}.orders`,
              `${m.type}.v${m.version}`,
              Buffer.from(JSON.stringify(m)),
              { persistent: true },
            );
          }
          await channel.waitForConfirms();
        } finally {
          await producer.disconnect();
          await channel.close();
          await connection.close();
        }
        await until(async () => {
          const r = await request(f.urls.sales, `/orders/${o.id}/tickets`, { auth: buyer });
          return r.data.status === 'cancelled';
        }, 'reordered cancellation');
        assert.equal(
          (
            await f.databases.fulfillment.query(
              'SELECT count(*)::integer AS n FROM tickets WHERE order_id=$1 AND valid',
              [o.id],
            )
          ).rows[0].n,
          0,
        );
        relay = f.start('relay');
        await until(
          async () =>
            (
              await f.databases.sales.query(
                "SELECT count(*)::integer AS n FROM outbox_deliveries WHERE status<>'delivered'",
              )
            ).rows[0].n === 0,
          'outbox drained',
        );
        await until(async () => {
          const rows = (await request(f.urls.sales, '/reports/events', { auth: organizer })).data;
          return rows.find((r) => r.eventId === e.id)?.refunds === 12000;
        }, 'all lifecycle revisions consumed');
        const before = (await request(f.urls.sales, '/reports/events', { auth: organizer })).data;
        for (let i = 0; i < 2; i++) {
          const replay = await request(f.urls.analytics, '/internal/replay', {
            method: 'POST',
            headers: { 'X-Internal-Token': f.settings.internalToken },
            body: { projection: 'all' },
            timeout: 120000,
          });
          assert.equal(replay.status, 200, JSON.stringify(replay.data));
          assert.deepEqual(
            (await request(f.urls.sales, '/reports/events', { auth: organizer })).data,
            before,
          );
        }
      });

      await t.test(
        'CSV streams a stable generation across replay, scopes owners and closes cursors on abort',
        async () => {
          assert.equal((await request(f.urls.sales, '/reports/orders.csv', { auth: buyer })).status, 403);
          assert.equal(
            (await request(f.urls.sales, '/reports/orders.csv?from=bad', { auth: organizer })).status,
            400,
          );
          const normal = await request(f.urls.sales, '/reports/orders.csv', { auth: organizer });
          assert.equal(normal.status, 200);
          assert.match(normal.data, /refundedAmount/);
          assert.match(normal.data, new RegExp(refunded.id));
          const generation = (
            await f.databases.analytics.query(
              "SELECT generation FROM projection_active WHERE projection='events'",
            )
          ).rows[0].generation;
          const exportEvent = randomUUID(),
            owner = f.DEMO.organizer.id;
          const template = {
            order: { eventTitle: '=SUM(1,2)', quantity: 1, totalPrice: 12000 },
            state: { status: 'confirmed', cancelledAt: null, refundedAt: null },
          };
          await f.databases.analytics.query(
            `INSERT INTO projection_orders(projection,generation,order_id,organizer_id,event_id,confirmed_at,revision,snapshot) SELECT 'events',$1,gen_random_uuid(),$2,$3,'2030-01-01',1,$4::jsonb FROM generate_series(1,100000)`,
            [generation, owner, exportEvent, JSON.stringify(template)],
          );
          const url = f.urls.sales + `/reports/orders.csv?eventId=${exportEvent}`;
          const response = await fetch(url, {
            headers: { Cookie: organizer.cookie },
            signal: AbortSignal.timeout(60000),
          });
          assert.equal(response.status, 200);
          let lines = 0,
            bytes = 0,
            sawEscaped = false;
          for await (const chunk of response.body) {
            const text = Buffer.from(chunk).toString();
            lines += (text.match(/\n/g) || []).length;
            bytes += chunk.length;
            if (text.includes("'=SUM")) sawEscaped = true;
          }
          assert.ok(bytes > 1000000);
          assert.equal(lines, 100001);
          assert.ok(sawEscaped);
          const stop = new AbortController(),
            partial = await fetch(url, { headers: { Cookie: organizer.cookie }, signal: stop.signal });
          const reader = partial.body.getReader();
          await reader.read();
          await delay(50);
          stop.abort();
          await reader.cancel().catch(() => {});
          await until(
            async () =>
              (
                await f.databases.analytics.query(
                  "SELECT count(*)::integer AS n FROM pg_stat_activity WHERE datname=current_database() AND application_name='analytics-export' AND state LIKE 'idle in transaction%'",
                )
              ).rows[0].n === 0,
            'export transaction closed',
          );
          const snapshot = await fetch(url, {
            headers: { Cookie: organizer.cookie },
            signal: AbortSignal.timeout(120000),
          });
          assert.equal(snapshot.status, 200);
          const snapshotReader = snapshot.body.getReader();
          const initial = await snapshotReader.read();
          assert.equal(initial.done, false);
          const replay = await request(f.urls.analytics, '/internal/replay', {
            method: 'POST',
            headers: { 'X-Internal-Token': f.settings.internalToken },
            body: { projection: 'events' },
            timeout: 120000,
          });
          assert.equal(replay.status, 200, JSON.stringify(replay.data));
          let snapshotLines = Buffer.from(initial.value).toString().split('\n').length - 1;
          while (true) {
            const next = await snapshotReader.read();
            if (next.done) break;
            snapshotLines += Buffer.from(next.value).toString().split('\n').length - 1;
          }
          assert.equal(snapshotLines, 100001, 'in-flight export retains its original generation');
          const current = await request(f.urls.sales, `/reports/orders.csv?eventId=${exportEvent}`, {
            auth: organizer,
          });
          assert.equal(current.status, 200);
          assert.equal(current.data.split('\n').length - 1, 1, 'new export reads the rebuilt generation');
          await f.databases.analytics.query('DELETE FROM projection_orders WHERE event_id=$1', [exportEvent]);
        },
      );
    } catch (e) {
      console.error(f.logTail());
      throw e;
    }
  },
);
