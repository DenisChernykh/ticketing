const { test } = require('node:test');
const assert = require('node:assert/strict');
const { randomUUID } = require('node:crypto');
const { fixture, request } = require('./support.cjs');

test(
  'HTTP-границы Sales: права, CSRF, валидация до лимита и владение заказом',
  { timeout: 90000 },
  async (t) => {
    const f = await fixture();
    t.after(() => f.cleanup());
    try {
      // Только Sales: обращение к собственной выдаче должно явно сообщить, что Fulfillment недоступен.
      f.start('sales', {
        RATE_LIMIT_MAX: '1',
        RATE_LIMIT_WINDOW_SECONDS: '60',
        SESSION_TTL_SECONDS: '120',
        LOGIN_RATE_LIMIT_MAX: '1000',
      });
      await f.ready(f.urls.sales);
      const buyer = await f.login();
      const organizer = await f.login(f.DEMO.organizer);
      const otherBuyer = await f.login(f.DEMO.otherBuyer);
      const eventId = f.DEMO.eventId;
      const unknownOrder = randomUUID();
      const validOrder = { eventId, quantity: 1 };
      const validEvent = {
        title: 'Проверка HTTP-границ',
        venue: 'Тестовый зал',
        startsAt: '2027-12-25T15:00:00Z',
        capacity: 10,
        unitPrice: 50000,
      };
      const call = (path, options = {}) => request(f.urls.sales, path, options);
      const expectStatus = async (path, options, status, label = path) => {
        const result = await call(path, options);
        assert.equal(result.status, status, `${label}: ${JSON.stringify(result.data)}`);
        return result;
      };
      const purchase = (body, key, auth = buyer) =>
        call('/orders', {
          method: 'POST',
          auth,
          body,
          headers: key === undefined ? {} : { 'Idempotency-Key': key },
        });

      await t.test('публичный каталог открыт, все защищённые группы требуют сессию', async () => {
        const events = await expectStatus('/events', {}, 200);
        assert.ok(events.data.items.some((event) => event.id === eventId));
        const protectedRoutes = [
          ['/auth/me', {}],
          ['/auth/csrf', {}],
          ['/auth/logout', { method: 'POST' }],
          ['/organizer/events', {}],
          ['/events', { method: 'POST', body: validEvent }],
          [`/events/${eventId}`, { method: 'PATCH', body: { title: 'Чужое изменение' } }],
          ['/orders', {}],
          [`/orders/${unknownOrder}`, {}],
          [`/orders/${unknownOrder}/tickets`, {}],
          ['/orders', { method: 'POST', body: validOrder, headers: { 'Idempotency-Key': randomUUID() } }],
          ['/reports/events', {}],
          ['/reports/daily', {}],
          ['/reports/popular?limit=10', {}],
        ];
        for (const [path, options] of protectedRoutes) {
          await expectStatus(path, options, 401, `Без сессии: ${options.method ?? 'GET'} ${path}`);
        }
      });

      await t.test('покупатель и организатор не получают возможности чужой роли', async () => {
        const organizerOnly = [
          ['/organizer/events', {}],
          ['/events', { method: 'POST', body: validEvent }],
          [`/events/${eventId}`, { method: 'PATCH', body: { title: 'Изменение покупателем' } }],
          ['/reports/events', {}],
          ['/reports/daily', {}],
          ['/reports/popular?limit=10', {}],
        ];
        for (const [path, options] of organizerOnly) {
          await expectStatus(path, { ...options, auth: buyer }, 403, `Роль buyer: ${path}`);
        }
        const buyerOnly = [
          ['/orders', {}],
          [`/orders/${unknownOrder}`, {}],
          [`/orders/${unknownOrder}/tickets`, {}],
          ['/orders', { method: 'POST', body: validOrder, headers: { 'Idempotency-Key': randomUUID() } }],
        ];
        for (const [path, options] of buyerOnly) {
          await expectStatus(path, { ...options, auth: organizer }, 403, `Роль organizer: ${path}`);
        }
      });

      await t.test('изменения требуют правильный CSRF и допустимый Origin, включая logout', async () => {
        const mutations = [
          ['/events', { method: 'POST', body: validEvent }, organizer],
          [`/events/${eventId}`, { method: 'PATCH', body: { title: 'Изменение без CSRF' } }, organizer],
          [
            '/orders',
            { method: 'POST', body: validOrder, headers: { 'Idempotency-Key': randomUUID() } },
            buyer,
          ],
          ['/auth/logout', { method: 'POST' }, buyer],
        ];
        for (const [path, options, auth] of mutations) {
          await expectStatus(
            path,
            {
              ...options,
              headers: { ...options.headers, Cookie: auth.cookie },
            },
            403,
            `Нет CSRF: ${path}`,
          );
          await expectStatus(
            path,
            {
              ...options,
              auth,
              headers: { ...options.headers, 'X-CSRF-Token': 'incorrect-token' },
            },
            403,
            `Неверный CSRF: ${path}`,
          );
          await expectStatus(
            path,
            {
              ...options,
              auth,
              headers: { ...options.headers, Origin: 'https://another-origin.example.test' },
            },
            403,
            `Чужой Origin: ${path}`,
          );
        }
        await expectStatus(
          '/auth/login',
          {
            method: 'POST',
            body: { email: f.DEMO.buyer.email, password: f.DEMO.password },
            headers: { Origin: 'https://another-origin.example.test' },
          },
          403,
        );
        assert.equal(
          (await expectStatus('/auth/me', { auth: buyer }, 200)).data.id,
          f.DEMO.buyer.id,
          'Отклонённый logout не должен отзывать сессию',
        );
      });

      await t.test('некорректные поля входа отклоняются как неверный HTTP-контракт', async () => {
        const malformedLogins = [
          {},
          [],
          { password: f.DEMO.password },
          { email: 'not-an-email', password: f.DEMO.password },
          { email: f.DEMO.buyer.email, password: 123 },
          { email: f.DEMO.buyer.email, password: '' },
          { email: f.DEMO.buyer.email, password: f.DEMO.password, role: 'organizer' },
        ];
        for (const body of malformedLogins) {
          await expectStatus(
            '/auth/login',
            { method: 'POST', body },
            400,
            `Неверный login: ${JSON.stringify(body)}`,
          );
        }
      });

      await t.test('неверные покупки не расходуют единственный запрос и не изменяют данные', async () => {
        const before = (await f.databases.sales.query('SELECT available FROM events WHERE id=$1', [eventId]))
          .rows[0].available;
        const invalidOrders = [
          ['UUID', { eventId: 'not-a-uuid', quantity: 1 }, randomUUID()],
          ['отсутствующее quantity', { eventId }, randomUUID()],
          ['quantity=0', { eventId, quantity: 0 }, randomUUID()],
          ['quantity>100', { eventId, quantity: 101 }, randomUUID()],
          ['дробное quantity', { eventId, quantity: 1.5 }, randomUUID()],
          ['строковое quantity', { eventId, quantity: '1' }, randomUUID()],
          ['нет ключа', validOrder, undefined],
          ['пробел в ключе', validOrder, 'invalid key'],
          ['длинный ключ', validOrder, 'a'.repeat(129)],
          ['лишнее поле', { ...validOrder, buyerId: f.DEMO.otherBuyer.id }, randomUUID()],
        ];
        for (const [label, body, key] of invalidOrders) {
          const result = await purchase(body, key);
          assert.equal(result.status, 400, `${label}: ${JSON.stringify(result.data)}`);
        }
        const empty = (
          await f.databases.sales.query(`SELECT
        (SELECT count(*)::int FROM orders) AS orders,
        (SELECT count(*)::int FROM idempotency_keys) AS keys,
        (SELECT count(*)::int FROM outbox_messages) AS messages,
        (SELECT count(*)::int FROM outbox_deliveries) AS deliveries`)
        ).rows[0];
        assert.deepEqual(empty, { orders: 0, keys: 0, messages: 0, deliveries: 0 });
        assert.equal(
          (await f.databases.sales.query('SELECT available FROM events WHERE id=$1', [eventId])).rows[0]
            .available,
          before,
        );

        const key = randomUUID();
        const created = await purchase(validOrder, key);
        assert.equal(created.status, 201, `Первая корректная покупка: ${JSON.stringify(created.data)}`);
        const repeated = await purchase(validOrder, key);
        assert.equal(
          repeated.status,
          429,
          `Лимит сохраняется и для идемпотентного повтора: ${JSON.stringify(repeated.data)}`,
        );
        const persisted = (
          await f.databases.sales.query(`SELECT
        (SELECT count(*)::int FROM orders) AS orders,
        (SELECT count(*)::int FROM idempotency_keys) AS keys,
        (SELECT count(*)::int FROM outbox_messages) AS messages,
        (SELECT count(*)::int FROM outbox_deliveries) AS deliveries`)
        ).rows[0];
        assert.deepEqual(persisted, { orders: 1, keys: 1, messages: 0, deliveries: 0 });
        assert.equal(
          (await f.databases.sales.query('SELECT available FROM events WHERE id=$1', [eventId])).rows[0]
            .available,
          before - 1,
        );

        // Fulfillment не запущен: 404 для чужого заказа доказывает проверку владения до HTTP-вызова.
        const id = created.data.id;
        await expectStatus(`/orders/${id}`, { auth: otherBuyer }, 404);
        await expectStatus(`/orders/${id}/tickets`, { auth: otherBuyer }, 404);
        await expectStatus(`/orders/${id}`, { auth: buyer }, 200);
        await expectStatus(`/orders/${id}/tickets`, { auth: buyer }, 503);
      });
    } catch (error) {
      console.error(f.logTail());
      throw error;
    }
  },
);
