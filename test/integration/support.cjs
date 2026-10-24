const { spawn } = require('node:child_process');
const { once } = require('node:events');
const { createServer } = require('node:net');
const { randomUUID } = require('node:crypto');
const { resolve } = require('node:path');
const { Pool } = require('pg');
const { config } = require('@ticketing/core');
const { migrateAll } = require('../../scripts/migrate.cjs');
const { seed, DEMO } = require('../../scripts/seed.cjs');

const delay = (ms) => new Promise((done) => setTimeout(done, ms));
async function until(check, description, timeout = 45000) {
  const deadline = Date.now() + timeout;
  let last;
  while (Date.now() < deadline) {
    try {
      const result = await check();
      if (result) return result;
    } catch (error) {
      last = error;
    }
    await delay(100);
  }
  throw new Error(`Timed out: ${description}${last ? `; ${last.message}` : ''}`);
}
async function freePort() {
  const server = createServer();
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const port = server.address().port;
  await new Promise((done) => server.close(done));
  return port;
}
async function request(base, pathname, { method = 'GET', body, auth, headers = {}, timeout = 10000 } = {}) {
  const response = await fetch(`${base}${pathname}`, {
    method,
    signal: AbortSignal.timeout(timeout),
    headers: {
      ...(body !== undefined ? { 'Content-Type': 'application/json' } : {}),
      ...(auth ? { Cookie: auth.cookie, 'X-CSRF-Token': auth.csrfToken } : {}),
      ...headers,
    },
    ...(body !== undefined ? { body: JSON.stringify(body) } : {}),
  });
  const text = await response.text();
  let data;
  try {
    data = JSON.parse(text);
  } catch {
    data = text;
  }
  return { status: response.status, data, headers: response.headers };
}
async function fixture({ legacy = false } = {}) {
  const settings = config();
  const suffix = randomUUID().replaceAll('-', '').slice(0, 12);
  const prefix = `test_${suffix}`;
  const children = new Set();
  const ports = {
    sales: await freePort(),
    second: await freePort(),
    fulfillment: await freePort(),
    analytics: await freePort(),
    paymentMock: await freePort(),
  };
  const urls = Object.fromEntries(
    Object.entries(ports).map(([name, port]) => [name, `http://127.0.0.1:${port}`]),
  );
  const pg = new URL(settings.salesDatabaseUrl);
  if (!['127.0.0.1', 'localhost'].includes(pg.hostname))
    throw new Error('Integration tests require local PostgreSQL');
  const adminUrl = new URL(pg);
  adminUrl.username = 'ticketing_admin';
  adminUrl.password = 'ticketing-admin-local';
  adminUrl.pathname = '/postgres';
  const admin = new Pool({ connectionString: process.env.TEST_ADMIN_DATABASE_URL ?? adminUrl.toString() });
  const databases = {};
  const connections = {};
  const poolErrors = [];
  admin.on('error', (error) => poolErrors.push(error));
  for (const service of ['sales', 'fulfillment', 'analytics', 'paymentMock']) {
    const name = `ticketing_test_${suffix}_${service}`;
    await admin.query(
      `CREATE DATABASE "${name}" OWNER "${service === 'paymentMock' ? 'payment_mock' : service}"`,
    );
    await admin.query(`REVOKE CONNECT ON DATABASE "${name}" FROM PUBLIC`);
    await admin.query(
      `GRANT CONNECT ON DATABASE "${name}" TO "${service === 'paymentMock' ? 'payment_mock' : service}"`,
    );
    const url = new URL(settings[`${service}DatabaseUrl`]);
    url.pathname = `/${name}`;
    connections[service] = url.toString();
    databases[service] = new Pool({ connectionString: url.toString() });
    databases[service].on('error', (error) => poolErrors.push(error));
  }
  const env = {
    ...process.env,
    RESOURCE_PREFIX: prefix,
    KAFKA_TOPIC: `${prefix}.sales.v1`,
    SALES_DATABASE_URL: connections.sales,
    FULFILLMENT_DATABASE_URL: connections.fulfillment,
    ANALYTICS_DATABASE_URL: connections.analytics,
    PAYMENT_MOCK_DATABASE_URL: connections.paymentMock,
    PAYMENT_MOCK_PORT: String(ports.paymentMock),
    PAYMENT_URL: urls.paymentMock,
    PAYMENT_WEBHOOK_URL: urls.sales + '/webhooks/payments',
    OTEL_SDK_DISABLED: 'true',
    SALES_PORT: String(ports.sales),
    FULFILLMENT_PORT: String(ports.fulfillment),
    ANALYTICS_PORT: String(ports.analytics),
    FULFILLMENT_URL: urls.fulfillment,
    ANALYTICS_URL: urls.analytics,
    RATE_LIMIT_MAX: '1000',
    LOGIN_RATE_LIMIT_MAX: '1000',
    RATE_LIMIT_WINDOW_SECONDS: '60',
    SESSION_TTL_SECONDS: '3600',
    RABBIT_RETRY_MS: '150',
    CACHE_TTL_SECONDS: '2',
  };
  let logs = '';
  function start(name, extra = {}) {
    const entry =
      name === 'worker'
        ? 'sales/dist/bootstrap/worker.js'
        : name === 'relay'
          ? 'sales/dist/bootstrap/relay.js'
          : name === 'sales'
            ? 'sales/dist/bootstrap/api.js'
            : `${name}/dist/bootstrap/main.js`;
    const child = spawn(process.execPath, [resolve(__dirname, `../../apps/${entry}`)], {
      env: { ...env, ...extra },
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    children.add(child);
    for (const stream of [child.stdout, child.stderr])
      stream.on('data', (chunk) => {
        logs = (logs + `[${name}] ${chunk}`).slice(-100000);
      });
    child.on('exit', () => children.delete(child));
    return child;
  }
  async function stop(child, signal = 'SIGTERM') {
    if (!child || child.exitCode !== null || child.signalCode) return;
    const exited = once(child, 'exit');
    child.kill(signal);
    let timer;
    await Promise.race([
      exited,
      new Promise((done) => {
        timer = setTimeout(() => {
          child.kill('SIGKILL');
          done();
        }, 10000);
      }),
    ]);
    clearTimeout(timer);
    if (child.exitCode === null && !child.signalCode) await exited;
  }
  async function login(user = DEMO.buyer, base = urls.sales) {
    const result = await request(base, '/auth/login', {
      method: 'POST',
      body: { email: user.email, password: DEMO.password },
    });
    if (result.status !== 200) throw new Error(`Login ${user.email}: ${JSON.stringify(result.data)}`);
    return {
      cookie: result.headers.get('set-cookie').split(';')[0],
      csrfToken: result.data.csrfToken,
      user: result.data.user,
    };
  }
  async function ready(base) {
    return until(async () => (await request(base, '/health/ready')).status === 200, `ready ${base}`);
  }
  async function cleanup() {
    await Promise.all([...children].map((child) => stop(child)));
    // Only resources whose names were generated by this fixture are removed.
    const { Kafka, logLevel } = require('kafkajs');
    const kafkaAdmin = new Kafka({
      clientId: `${prefix}-cleanup`,
      brokers: settings.kafkaBrokers,
      logLevel: logLevel.NOTHING,
    }).admin();
    try {
      await kafkaAdmin.connect();
      const groups = (await kafkaAdmin.listGroups()).groups
        .filter((group) => group.groupId.startsWith(`${prefix}.`))
        .map((group) => group.groupId);
      if (groups.length) await kafkaAdmin.deleteGroups(groups);
      if ((await kafkaAdmin.listTopics()).includes(env.KAFKA_TOPIC)) {
        await kafkaAdmin.deleteTopics({ topics: [env.KAFKA_TOPIC], timeout: 10000 });
      }
    } catch (error) {
      console.error(`Kafka fixture cleanup: ${error.message}`);
    } finally {
      await kafkaAdmin.disconnect().catch(() => {});
    }
    const amqp = require('amqplib');
    let connection;
    try {
      connection = await amqp.connect(settings.amqpUrl);
      const channel = await connection.createChannel();
      for (const consumer of ['tickets', 'notifications'])
        for (const suffix of ['', '.retry', '.dead']) {
          await channel.deleteQueue(`${prefix}.${consumer}${suffix}`);
        }
      await channel.deleteExchange(`${prefix}.orders`);
      await channel.close();
    } catch (error) {
      console.error(`Rabbit fixture cleanup: ${error.message}`);
    } finally {
      await connection?.close().catch(() => {});
    }
    const { createClient } = require('redis');
    const redis = createClient({
      url: settings.redisUrl,
      socket: { reconnectStrategy: false, connectTimeout: 2000 },
    });
    redis.on('error', () => {});
    try {
      await redis.connect();
      const keys = await redis.keys(`${prefix}:*`);
      if (keys.length) await redis.del(keys);
    } finally {
      if (redis.isOpen) redis.destroy();
    }
    for (const pool of Object.values(databases)) await pool.end();
    for (const service of ['sales', 'fulfillment', 'analytics', 'paymentMock']) {
      const name = `ticketing_test_${suffix}_${service}`;
      // Process exit and pool.end() can precede PostgreSQL observing the final TCP FIN.
      // Wait for that acknowledgement instead of killing a closing client with DROP FORCE.
      await until(
        async () =>
          (await admin.query('SELECT count(*)::int AS count FROM pg_stat_activity WHERE datname=$1', [name]))
            .rows[0].count === 0,
        `connections closed for ${name}`,
        15000,
      );
      await admin.query(`DROP DATABASE "${name}"`);
    }
    await admin.end();
    if (poolErrors.length)
      throw new AggregateError(poolErrors, 'Unexpected PostgreSQL pool errors during integration');
  }
  try {
    if (legacy) {
      const { readFile } = require('node:fs/promises');
      const { createHash } = require('node:crypto');
      for (const [service, file] of [
        ['sales', '001_sales.sql'],
        ['fulfillment', '001_delivery.sql'],
        ['analytics', '001_analytics.sql'],
      ]) {
        const sql = await readFile(resolve(__dirname, `../../apps/${service}/migrations/${file}`), 'utf8');
        const client = await databases[service].connect();
        try {
          await client.query('BEGIN');
          await client.query(sql);
          await client.query(
            'CREATE TABLE schema_migrations(name text PRIMARY KEY,checksum text NOT NULL,applied_at timestamptz NOT NULL DEFAULT now())',
          );
          await client.query('INSERT INTO schema_migrations(name,checksum) VALUES($1,$2)', [
            file,
            createHash('sha256').update(sql).digest('hex'),
          ]);
          await client.query('COMMIT');
        } finally {
          client.release();
        }
      }
    } else {
      await migrateAll({
        ...settings,
        ...Object.fromEntries(
          Object.entries(connections).map(([key, value]) => [`${key}DatabaseUrl`, value]),
        ),
      });
    }
    await seed(connections.sales);
  } catch (error) {
    await cleanup();
    throw error;
  }
  return {
    settings,
    prefix,
    env,
    urls,
    ports,
    connections,
    databases,
    start,
    stop,
    ready,
    login,
    cleanup,
    logTail: () => logs,
    DEMO,
    request,
    until,
    delay,
  };
}
module.exports = { fixture, request, until, delay };
