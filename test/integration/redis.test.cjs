const { test } = require('node:test');
const assert = require('node:assert/strict');
const { createServer, createConnection } = require('node:net');
const { once } = require('node:events');
const { randomUUID } = require('node:crypto');
const { fixture, request, until, delay } = require('./support.cjs');

// Only these API processes use the gate. Other tests and the shared Docker Redis stay online.
async function redisGate(target) {
  const upstream = new URL(target);
  assert.equal(upstream.protocol, 'redis:', 'The local Redis gate requires plain TCP');
  const sockets = new Set();
  const server = createServer((client) => {
    const remote = createConnection({ host: upstream.hostname, port: Number(upstream.port || 6379) });
    sockets.add(client);
    sockets.add(remote);
    client.on('error', () => remote.destroy());
    remote.on('error', () => client.destroy());
    client.on('close', () => {
      sockets.delete(client);
      remote.destroy();
    });
    remote.on('close', () => {
      sockets.delete(remote);
      client.destroy();
    });
    client.pipe(remote);
    remote.pipe(client);
  });
  let port;
  async function open() {
    server.listen(port ?? 0, '127.0.0.1');
    await once(server, 'listening');
    port = server.address().port;
  }
  async function close() {
    for (const socket of sockets) socket.destroy();
    if (server.listening)
      await new Promise((resolve, reject) => server.close((error) => (error ? reject(error) : resolve())));
  }
  await open();
  const url = new URL(target);
  url.hostname = '127.0.0.1';
  url.port = String(port);
  return { url: url.toString(), open, close };
}

test(
  'Redis: два API разделяют сессии, лимиты и кеш; отказ и восстановление',
  { timeout: 180000 },
  async (t) => {
    const f = await fixture();
    let gate;
    t.after(async () => {
      try {
        await f.cleanup();
      } finally {
        await gate?.close();
      }
    });
    try {
      gate = await redisGate(f.settings.redisUrl);
      Object.assign(f.env, {
        REDIS_URL: gate.url,
        RATE_LIMIT_MAX: '5',
        RATE_LIMIT_WINDOW_SECONDS: '2',
        SESSION_TTL_SECONDS: '5',
        CACHE_TTL_SECONDS: '2',
      });
      f.start('sales');
      let second = f.start('sales', { SALES_PORT: String(f.ports.second) });
      await Promise.all([f.ready(f.urls.sales), f.ready(f.urls.second)]);
      const purchase = (base, auth) =>
        request(base, '/orders', {
          method: 'POST',
          auth,
          headers: { 'Idempotency-Key': randomUUID() },
          body: { eventId: f.DEMO.eventId, quantity: 1 },
        });

      await t.test('сессия и её отзыв при logout видны обоим процессам', async () => {
        const buyer = await f.login();
        const shared = await request(f.urls.second, '/auth/me', { auth: buyer });
        assert.equal(shared.status, 200);
        assert.equal(shared.data.id, f.DEMO.buyer.id);
        assert.equal(
          (await request(f.urls.second, '/auth/logout', { method: 'POST', auth: buyer })).status,
          200,
        );
        const revoked = await Promise.all(
          [f.urls.sales, f.urls.second].map((url) => request(url, '/auth/me', { auth: buyer })),
        );
        assert.ok(revoked.every((response) => response.status === 401));
      });

      await t.test(
        'ровно пять из десяти конкурентных покупок проходят общий лимит; TTL открывает новое окно',
        async () => {
          const buyer = await f.login();
          const before = (await f.databases.sales.query('SELECT count(*)::integer AS count FROM orders'))
            .rows[0].count;
          const responses = await Promise.all(
            Array.from({ length: 10 }, (_, index) =>
              purchase(index % 2 ? f.urls.sales : f.urls.second, buyer),
            ),
          );
          assert.equal(
            responses.filter((result) => result.status === 201).length,
            5,
            JSON.stringify(responses),
          );
          assert.equal(
            responses.filter((result) => result.status === 429).length,
            5,
            JSON.stringify(responses),
          );
          assert.equal(
            (await f.databases.sales.query('SELECT count(*)::integer AS count FROM orders')).rows[0].count,
            before + 5,
          );
          assert.equal((await purchase(f.urls.second, buyer)).status, 429);
          await delay(2200);
          const afterExpiry = await purchase(f.urls.sales, buyer);
          assert.equal(afterExpiry.status, 201, JSON.stringify(afterExpiry.data));
          assert.equal(
            (await f.databases.sales.query('SELECT count(*)::integer AS count FROM orders')).rows[0].count,
            before + 6,
          );
        },
      );

      await t.test(
        'каталог использует общий кеш, TTL обновляет его, PATCH инвалидирует сразу на другом API',
        async () => {
          const first = await request(f.urls.sales, '/events');
          assert.equal(first.status, 200);
          const original = first.data.items.find((event) => event.id === f.DEMO.emptyEventId);
          assert.ok(original);
          const maintenanceTitle = `TTL ${randomUUID()}`;
          // Bypass normal invalidation deliberately, so the test can distinguish a cache hit from SQL.
          await f.databases.sales.query('UPDATE events SET title=$2 WHERE id=$1', [
            original.id,
            maintenanceTitle,
          ]);
          const cached = await request(f.urls.second, '/events');
          assert.equal(cached.data.items.find((event) => event.id === original.id).title, original.title);
          await until(
            async () => {
              const result = await request(f.urls.second, '/events');
              return (
                result.status === 200 &&
                result.data.items.find((event) => event.id === original.id).title === maintenanceTitle
              );
            },
            'catalogue refreshes after its TTL',
            5000,
          );
          const organizer = await f.login(f.DEMO.organizer);
          const updatedTitle = `PATCH ${randomUUID()}`;
          const update = await request(f.urls.sales, `/events/${original.id}`, {
            method: 'PATCH',
            auth: organizer,
            body: { title: updatedTitle, version: original.version },
          });
          assert.equal(update.status, 200, JSON.stringify(update.data));
          const invalidated = await request(f.urls.second, '/events');
          assert.equal(invalidated.data.items.find((event) => event.id === original.id).title, updatedTitle);
        },
      );

      await t.test('TTL сессии ограничивает её на всех API и не продлевается чтением /auth/me', async () => {
        await f.stop(second);
        second = f.start('sales', { SALES_PORT: String(f.ports.second), SESSION_TTL_SECONDS: '2' });
        await f.ready(f.urls.second);
        const shortSession = await f.login(f.DEMO.otherBuyer, f.urls.second);
        assert.equal((await request(f.urls.sales, '/auth/me', { auth: shortSession })).status, 200);
        await until(
          async () => (await request(f.urls.sales, '/auth/me', { auth: shortSession })).status === 401,
          'absolute session TTL, despite repeated authenticated reads',
          5000,
        );
        assert.equal((await request(f.urls.second, '/auth/me', { auth: shortSession })).status, 401);
        await f.stop(second);
        second = f.start('sales', { SALES_PORT: String(f.ports.second), SESSION_TTL_SECONDS: '30' });
        await f.ready(f.urls.second);
      });

      await t.test(
        'отказ Redis сохраняет публичный SQL-каталог, закрывает защищённые операции и восстанавливается',
        async () => {
          const buyer = await f.login(f.DEMO.otherBuyer, f.urls.second);
          const original = await request(f.urls.sales, '/events');
          assert.equal(original.status, 200);
          await gate.close();
          const outageTitle = `SQL fallback ${randomUUID()}`;
          await f.databases.sales.query('UPDATE events SET title=$2 WHERE id=$1', [
            f.DEMO.emptyEventId,
            outageTitle,
          ]);
          for (const base of [f.urls.sales, f.urls.second]) {
            const catalog = await request(base, '/events');
            assert.equal(catalog.status, 200, JSON.stringify(catalog.data));
            assert.equal(
              catalog.data.items.find((event) => event.id === f.DEMO.emptyEventId).title,
              outageTitle,
            );
            assert.equal((await request(base, '/auth/me', { auth: buyer })).status, 503);
            assert.equal((await purchase(base, buyer)).status, 503);
            const login = await request(base, '/auth/login', {
              method: 'POST',
              body: { email: f.DEMO.buyer.email, password: f.DEMO.password },
            });
            assert.equal(login.status, 503, 'a required login limit cannot fail open');
            assert.equal((await request(base, '/health/ready')).status, 503);
          }
          await gate.open();
          await Promise.all([f.ready(f.urls.sales), f.ready(f.urls.second)]);
          const restored = await request(f.urls.sales, '/auth/me', { auth: buyer });
          assert.equal(restored.status, 200, JSON.stringify(restored.data));
          assert.equal(restored.data.id, f.DEMO.otherBuyer.id);
          assert.equal((await purchase(f.urls.sales, buyer)).status, 201);
          assert.equal(
            (await request(f.urls.second, '/auth/logout', { method: 'POST', auth: buyer })).status,
            200,
          );
          assert.equal((await request(f.urls.sales, '/auth/me', { auth: buyer })).status, 401);
        },
      );
    } catch (error) {
      console.error(f.logTail());
      throw error;
    }
  },
);
