const assert = require('node:assert/strict');
const { randomUUID } = require('node:crypto');
const { readFile } = require('node:fs/promises');
const { createServer, connect } = require('node:net');
const { spawn, execFile } = require('node:child_process');
const { promisify } = require('node:util');
const { resolve } = require('node:path');
const { setTimeout: delay } = require('node:timers/promises');
const { Kafka, logLevel } = require('kafkajs');
const { config, Database, RedisConnection } = require('@ticketing/core');

async function eventually(check, label, timeout = 40_000) {
  const deadline = Date.now() + timeout;
  let last;
  while (Date.now() < deadline) {
    try {
      if (await check()) return;
    } catch (error) {
      last = error;
    }
    await delay(100);
  }
  throw new Error(`${label} timed out${last ? `: ${last.message}` : ''}`);
}

async function listen(server) {
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });
  return server.address().port;
}

async function freePort() {
  const server = createServer();
  const port = await listen(server);
  await new Promise((resolve) => server.close(resolve));
  return port;
}

async function main() {
  const environment = config();
  const prefix = `analytics_verify_${process.pid}_${randomUUID().slice(0, 8)}`;
  const schema = prefix;
  const topic = `${prefix}.sales.v1`;
  const owner = new Database(environment.analyticsDatabaseUrl, 'analytics-verification-owner');
  const connectionString = new URL(environment.analyticsDatabaseUrl);
  connectionString.searchParams.set('options', `-c search_path=${schema}`);
  const database = new Database(connectionString.toString(), 'analytics-verification');
  const redis = new RedisConnection(environment.redisUrl, `${prefix}-verify`);
  const kafka = new Kafka({
    clientId: `${prefix}-verify`,
    brokers: environment.kafkaBrokers,
    logLevel: logLevel.ERROR,
  });
  const admin = kafka.admin();
  const producer = kafka.producer();
  let child;
  let childLogs = '';
  let gateOpen = true;
  const sockets = new Set();
  const redisUpstream = new URL(environment.redisUrl);
  const gate = createServer((socket) => {
    if (!gateOpen) return socket.destroy();
    const upstream = connect(Number(redisUpstream.port || 6379), redisUpstream.hostname);
    for (const current of [socket, upstream]) {
      sockets.add(current);
      current.on('error', () => {
        socket.destroy();
        upstream.destroy();
      });
      current.once('close', () => sockets.delete(current));
    }
    socket.pipe(upstream).pipe(socket);
  });
  try {
    await owner.query(`CREATE SCHEMA "${schema}"`);
    assert.equal((await database.query('SELECT current_schema() AS schema')).rows[0].schema, schema);
    await database.query(await readFile(resolve(__dirname, '../migrations/001_analytics.sql'), 'utf8'));
    await database.query(await readFile(resolve(__dirname, '../migrations/002_lifecycle.sql'), 'utf8'));
    await admin.connect();
    await producer.connect();
    await redis.connect();
    const redisPort = await listen(gate);
    const port = await freePort();
    const base = `http://127.0.0.1:${port}`;
    const childEnvironment = {
      ...process.env,
      OTEL_SDK_DISABLED: 'true',
      RESOURCE_PREFIX: prefix,
      KAFKA_TOPIC: topic,
      ANALYTICS_DATABASE_URL: connectionString.toString(),
      ANALYTICS_PORT: String(port),
      ANALYTICS_URL: base,
      REDIS_URL: `redis://127.0.0.1:${redisPort}`,
    };
    child = spawn(process.execPath, [resolve(__dirname, '../dist/bootstrap/main.js')], {
      env: childEnvironment,
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    for (const stream of [child.stdout, child.stderr])
      stream.on('data', (data) => {
        childLogs = (childLogs + data).slice(-24_000);
      });
    const request = async (path, init = {}) => {
      const response = await fetch(`${base}${path}`, {
        ...init,
        headers: {
          'X-Internal-Token': environment.internalToken,
          'Content-Type': 'application/json',
          ...init.headers,
        },
        signal: AbortSignal.timeout(140_000),
      });
      const body = await response.json();
      if (!response.ok) throw new Error(`HTTP ${response.status}: ${JSON.stringify(body)}`);
      return body;
    };
    await eventually(async () => (await request('/health/workers')).ready, 'workers startup');

    // The empty topic has three empty partitions. No eachBatch callback is required to finish.
    const emptyReplay = await request('/internal/replay', {
      method: 'POST',
      body: JSON.stringify({ projection: 'all' }),
    });
    assert.equal(emptyReplay.length, 2);
    console.log('PASS replay_empty_partitions');

    const organizer = randomUUID();
    const otherOrganizer = randomUUID();
    const eventId = randomUUID();
    const secondEventId = randomUUID();
    const all = [];
    const sale = (event, organizerId = organizer, quantity = 1, day = '2026-09-20') => ({
      messageId: randomUUID(),
      type: 'order.confirmed',
      version: 1,
      occurredAt: `${day}T12:00:00.000Z`,
      order: {
        id: randomUUID(),
        buyerId: randomUUID(),
        organizerId,
        eventId: event,
        eventTitle: event === eventId ? 'Jazz concert' : 'Theatre',
        venue: 'Local hall',
        startsAt: '2026-10-01T16:00:00.000Z',
        quantity,
        unitPrice: 10000,
        totalPrice: quantity * 10000,
        currency: 'RUB',
      },
    });
    const publish = async (event, remember = true) => {
      if (remember) all.push(event);
      await producer.send({ topic, messages: [{ key: event.order.eventId, value: JSON.stringify(event) }] });
    };
    await publish(sale(eventId, organizer, 2, '2026-09-19'));
    await publish(sale(eventId, organizer, 3));
    await publish(sale(secondEventId, organizer, 1));
    await publish(sale(randomUUID(), otherOrganizer, 7));
    await publish(all[0], false);
    const reports = () => request(`/internal/reports/events?organizerId=${organizer}`);
    const expectedTickets = () =>
      all
        .filter((event) => event.order.organizerId === organizer)
        .reduce((sum, event) => sum + event.order.quantity, 0);
    await eventually(
      async () => (await reports()).reduce((sum, row) => sum + row.tickets, 0) === expectedTickets(),
      'event aggregates',
    );
    await eventually(
      async () =>
        (await request(`/internal/reports/daily?organizerId=${organizer}`)).reduce(
          (sum, row) => sum + row.tickets,
          0,
        ) === expectedTickets(),
      'daily aggregates',
    );
    const daily = await request(`/internal/reports/daily?organizerId=${organizer}`);
    assert.equal(daily.at(-1).cumulativeRevenue, expectedTickets() * 10000);
    assert.equal((await request(`/internal/reports/events?organizerId=${otherOrganizer}`))[0].tickets, 7);
    assert.equal(
      (
        await database.query(`SELECT count(*)::int AS count FROM message_receipts r
      INNER JOIN projection_active a USING (projection, generation)`)
      ).rows[0].count,
      all.length * 2,
    );
    console.log('PASS independent_groups_deduplication_organizer_isolation_window_reports');

    // The new generation replays the captured prefix while new sales continue to arrive.
    const before = (await database.query('SELECT * FROM projection_active ORDER BY projection')).rows;
    const replay = request('/internal/replay', {
      method: 'POST',
      body: JSON.stringify({ projection: 'all' }),
    });
    for (let index = 0; index < 15; index++) {
      await publish(sale(index % 2 ? eventId : secondEventId));
      await delay(30);
    }
    await replay;
    const after = (await database.query('SELECT * FROM projection_active ORDER BY projection')).rows;
    assert.notDeepEqual(after, before);
    await eventually(
      async () => (await reports()).reduce((sum, row) => sum + row.tickets, 0) === expectedTickets(),
      'live sales after replay',
    );
    await eventually(
      async () =>
        (await request(`/internal/reports/daily?organizerId=${organizer}`)).reduce(
          (sum, row) => sum + row.tickets,
          0,
        ) === expectedTickets(),
      'live daily after replay',
    );
    console.log('PASS replay_with_continuing_sales');

    const popularPath = `/internal/reports/popular?organizerId=${organizer}&limit=10`;
    const popular = await request(popularPath);
    const generation = after.find((row) => row.projection === 'events').generation;
    const rankingKey = `${prefix}:analytics:ranking:${generation}:${organizer}`;
    assert.equal(await redis.client.zCard(rankingKey), popular.length);
    await redis.client.del(rankingKey); // Partial cache loss is repaired, not returned as an empty ranking.
    assert.deepEqual(await request(popularPath), popular);
    gateOpen = false;
    for (const socket of sockets) socket.destroy();
    await delay(150);
    assert.deepEqual(await request(popularPath), popular);
    assert.equal((await request('/health/ready')).status, 'ready');
    gateOpen = true;
    await eventually(async () => {
      await request(popularPath);
      return (await redis.client.zCard(rankingKey)) === popular.length;
    }, 'Redis recovery');
    console.log('PASS_sorted_set_repair_and_redis_outage_sql_fallback');

    // Exercise the public CLI against the same process that owns the consumers.
    await promisify(execFile)(
      process.execPath,
      [resolve(__dirname, '../dist/bootstrap/replay.js'), 'daily'],
      { env: childEnvironment, timeout: 150_000 },
    );
    console.log('PASS replay_cli');

    const activeBeforeFailure = (
      await database.query("SELECT generation FROM projection_active WHERE projection = 'events'")
    ).rows[0].generation;
    const poison = await producer.send({
      topic,
      messages: [{ key: eventId, value: '{"type":"malformed"}' }],
    });
    await eventually(async () => !(await request('/health/workers')).ready, 'malformed event visibility');
    const committed = await admin.fetchOffsets({
      groupId: `${prefix}.analytics.events.${activeBeforeFailure}`,
      topics: [topic],
    });
    assert.equal(
      committed[0].partitions.find((partition) => partition.partition === poison[0].partition).offset,
      poison[0].baseOffset,
    );
    let replayFailed = false;
    try {
      await request('/internal/replay', { method: 'POST', body: JSON.stringify({ projection: 'events' }) });
    } catch (error) {
      replayFailed = /HTTP 503/.test(error.message);
    }
    assert.equal(replayFailed, true);
    assert.equal(
      (await database.query("SELECT generation FROM projection_active WHERE projection = 'events'")).rows[0]
        .generation,
      activeBeforeFailure,
    );
    assert.equal(
      (await reports()).reduce((sum, row) => sum + row.tickets, 0),
      expectedTickets(),
    );
    assert.ok(
      (await database.query("SELECT 1 FROM projection_generations WHERE status = 'failed'")).rowCount > 0,
    );
    console.log('PASS failed_replay_preserves_old_projection_and_poison_event_is_not_committed');
  } catch (error) {
    console.error(childLogs);
    throw error;
  } finally {
    if (child && child.exitCode === null) {
      child.kill('SIGTERM');
      await Promise.race([new Promise((resolve) => child.once('exit', resolve)), delay(15_000)]);
      if (child.exitCode === null) child.kill('SIGKILL');
    }
    for (const socket of sockets) socket.destroy();
    if (gate.listening) await new Promise((resolve) => gate.close(resolve));
    await producer.disconnect().catch(() => undefined);
    try {
      const { groups } = await admin.listGroups();
      const owned = groups
        .filter((group) => group.groupId.startsWith(`${prefix}.analytics.`))
        .map((group) => group.groupId);
      if (owned.length) await admin.deleteGroups(owned);
      await admin.deleteTopics({ topics: [topic] });
    } catch (error) {
      console.error('Owned Kafka cleanup:', error.message);
    }
    await admin.disconnect().catch(() => undefined);
    if (redis.client.isReady) {
      const keys = await redis.client.keys(`${prefix}:*`);
      if (keys.length) await redis.client.del(keys);
    }
    await redis.close();
    await database.close();
    await owner.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
    await owner.close();
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
