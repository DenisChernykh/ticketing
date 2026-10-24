const { spawn } = require('node:child_process');
const { once } = require('node:events');
const { resolve } = require('node:path');
const { randomUUID } = require('node:crypto');
const { Pool } = require('pg');
const { config } = require('@ticketing/core');
const delay = (ms) => new Promise((r) => setTimeout(r, ms));
/** Offline, resumable migration. Only this maintenance program reads both owners' metadata. */
async function upgrade(settings = config()) {
  const sales = new Pool({ connectionString: settings.salesDatabaseUrl, application_name: 'state-upgrade' }),
    analytics = new Pool({
      connectionString: settings.analyticsDatabaseUrl,
      application_name: 'state-upgrade',
    });
  const children = [];
  let logs = '';
  let lock;
  async function stop(child) {
    if (child.exitCode !== null || child.signalCode) return;
    const done = once(child, 'exit');
    child.kill('SIGTERM');
    const timer = setTimeout(() => child.kill('SIGKILL'), 10000);
    try {
      await done;
    } finally {
      clearTimeout(timer);
    }
  }
  const start = (app, env) => {
    const child = spawn(process.execPath, [resolve(__dirname, `../apps/${app}/dist/bootstrap/main.js`)], {
      env: { ...process.env, OTEL_SDK_DISABLED: 'true', ...env },
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    children.push(child);
    for (const stream of [child.stdout, child.stderr])
      stream.on('data', (c) => (logs = (logs + c).slice(-12000)));
    return child;
  };
  async function ready(base, child) {
    const until = Date.now() + 30000;
    while (Date.now() < until) {
      if (child.exitCode !== null || child.signalCode)
        throw new Error('Maintenance service exited before becoming ready: ' + logs);
      try {
        const r = await fetch(base + '/health/live', { signal: AbortSignal.timeout(1000) });
        if (r.ok) return;
      } catch {}
      await delay(100);
    }
    throw new Error('Maintenance service did not start: ' + logs);
  }
  async function post(base, path, body) {
    const r = await fetch(base + path, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Internal-Token': settings.internalToken },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(150000),
    });
    if (!r.ok) throw new Error(await r.text());
    if (r.status !== 204) return r.json();
  }
  try {
    lock = await sales.connect();
    if (
      !(await lock.query("SELECT pg_try_advisory_lock(hashtext('ticketing-state-upgrade')) AS ok")).rows[0].ok
    )
      throw new Error('Another state upgrade is running');
    const old = (
      await sales.query(
        `SELECT o.id,o.total_price,o.confirmed_at,p.id AS payment_id FROM orders o LEFT JOIN payments p ON p.order_id=o.id WHERE o.revision=1 AND o.status='confirmed' AND (p.id IS NULL OR p.provider_version=0)`,
      )
    ).rows;
    const formats = (
      await analytics.query(
        `SELECT g.format_version FROM projection_active a JOIN projection_generations g USING(projection,generation)`,
      )
    ).rows;
    const needsReplay = formats.some((r) => r.format_version < 2);
    if (!old.length && !needsReplay) {
      console.log('State upgrade: already current');
      return;
    }
    const databases = [
      settings.salesDatabaseUrl,
      settings.fulfillmentDatabaseUrl,
      settings.analyticsDatabaseUrl,
      settings.paymentMockDatabaseUrl,
    ].map((url) => decodeURIComponent(new URL(url).pathname.slice(1)));
    const active = (
      await sales.query(
        `SELECT DISTINCT application_name FROM pg_stat_activity WHERE datname=ANY($1::text[]) AND application_name=ANY($2::text[])`,
        [
          databases,
          [
            'sales-api',
            'sales-worker',
            'sales-outbox-relay',
            'fulfillment',
            'ticketing-analytics',
            'analytics-export',
            'payment-mock',
          ],
        ],
      )
    ).rows;
    if (active.length)
      throw new Error(
        'Stop application processes before upgrade:state: ' +
          active.map((r) => r.application_name).join(', '),
      );
    if (old.length) {
      const child = start('payment-mock', {
        PAYMENT_MOCK_DATABASE_URL: settings.paymentMockDatabaseUrl,
        PAYMENT_MOCK_PORT: String(settings.paymentMockPort),
      });
      await ready(settings.paymentUrl, child);
      for (const o of old) {
        const id = o.payment_id ?? randomUUID();
        await sales.query(
          `INSERT INTO payments(id,order_id,amount,status) VALUES($1,$2,$3,'succeeded') ON CONFLICT(order_id) DO NOTHING`,
          [id, o.id, o.total_price],
        );
        const actual = (await sales.query('SELECT id FROM payments WHERE order_id=$1', [o.id])).rows[0].id;
        await post(settings.paymentUrl, '/internal/import', {
          id: actual,
          orderId: o.id,
          amount: Number(o.total_price),
          currency: 'RUB',
          capturedAt: new Date(o.confirmed_at).toISOString(),
        });
        await sales.query('UPDATE payments SET provider_version=2 WHERE id=$1 AND provider_version=0', [
          actual,
        ]);
      }
      await stop(child);
      console.log(`Imported ${old.length} legacy payments`);
    }
    if (needsReplay) {
      const child = start('analytics', {
        ANALYTICS_DATABASE_URL: settings.analyticsDatabaseUrl,
        ANALYTICS_PORT: String(settings.analyticsPort),
        KAFKA_TOPIC: settings.kafkaTopic,
        RESOURCE_PREFIX: settings.prefix,
        KAFKA_BROKERS: settings.kafkaBrokers.join(','),
      });
      await ready(settings.analyticsUrl, child);
      await post(settings.analyticsUrl, '/internal/replay', { projection: 'all' });
      await stop(child);
      console.log('Rebuilt both lifecycle projections');
    }
  } catch (e) {
    if (logs) console.error(logs);
    throw e;
  } finally {
    await Promise.allSettled(children.map(stop));
    if (lock) {
      await lock.query("SELECT pg_advisory_unlock(hashtext('ticketing-state-upgrade'))").catch(() => {});
      lock.release();
    }
    await sales.end();
    await analytics.end();
  }
}
module.exports = { upgrade };
if (require.main === module)
  upgrade().catch((e) => {
    console.error(e);
    process.exitCode = 1;
  });
