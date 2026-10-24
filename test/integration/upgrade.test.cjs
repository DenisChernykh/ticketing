const { test } = require('node:test');
const assert = require('node:assert/strict');
const { randomUUID, createHash } = require('node:crypto');
const { fixture, request, until } = require('./support.cjs');
const { migrateAll } = require('../../scripts/migrate.cjs');
const { upgrade } = require('../../scripts/upgrade-state.cjs');
const { Kafka, logLevel } = require('kafkajs');

test(
  'upgrade from 001 retains legacy purchases, tickets, replay totals and resumable payment import',
  { timeout: 120000 },
  async (t) => {
    const f = await fixture({ legacy: true });
    t.after(() => f.cleanup());
    const settings = {
      ...f.settings,
      ...Object.fromEntries(Object.entries(f.connections).map(([k, v]) => [k + 'DatabaseUrl', v])),
      paymentUrl: f.urls.paymentMock,
      paymentMockPort: f.ports.paymentMock,
      analyticsUrl: f.urls.analytics,
      analyticsPort: f.ports.analytics,
      kafkaTopic: f.env.KAFKA_TOPIC,
      prefix: f.env.RESOURCE_PREFIX,
    };
    const order = {
      id: randomUUID(),
      buyerId: f.DEMO.buyer.id,
      organizerId: f.DEMO.organizer.id,
      eventId: f.DEMO.eventId,
      eventTitle: 'Legacy purchase',
      venue: 'Legacy hall',
      startsAt: '2035-01-01T12:00:00.000Z',
      quantity: 1,
      unitPrice: 15000,
      totalPrice: 15000,
      currency: 'RUB',
    };
    const at = '2026-09-01T12:00:00.000Z',
      message = { messageId: randomUUID(), type: 'order.confirmed', version: 1, occurredAt: at, order };
    const hash = (v) => createHash('sha256').update(v).digest('hex');
    const snapshot = JSON.stringify([
      order.id,
      order.buyerId,
      order.organizerId,
      order.eventId,
      order.eventTitle,
      order.venue,
      order.startsAt,
      order.quantity,
      order.unitPrice,
      order.totalPrice,
      order.currency,
    ]);
    const sales = f.databases.sales,
      fulfillment = f.databases.fulfillment,
      analytics = f.databases.analytics;
    await sales.query(
      `INSERT INTO orders(id,buyer_id,event_id,organizer_id,quantity,unit_price,total_price,currency,event_title,venue,starts_at,created_at) VALUES($1,$2,$3,$4,1,15000,15000,'RUB',$5,$6,$7,$8)`,
      [
        order.id,
        order.buyerId,
        order.eventId,
        order.organizerId,
        order.eventTitle,
        order.venue,
        order.startsAt,
        at,
      ],
    );
    await sales.query('UPDATE events SET available=available-1 WHERE id=$1', [order.eventId]);
    await sales.query('INSERT INTO outbox_messages(message_id,order_id,payload) VALUES($1,$2,$3::jsonb)', [
      message.messageId,
      order.id,
      JSON.stringify(message),
    ]);
    await sales.query(
      "INSERT INTO outbox_deliveries(message_id,destination,status) VALUES($1,'rabbitmq','delivered'),($1,'kafka','delivered')",
      [message.messageId],
    );
    const ticketId = randomUUID();
    await fulfillment.query(
      'INSERT INTO fulfillment_orders(order_id,buyer_id,event_id,quantity,snapshot_hash) VALUES($1,$2,$3,1,$4)',
      [order.id, order.buyerId, order.eventId, hash(snapshot)],
    );
    await fulfillment.query(
      "INSERT INTO tickets(id,order_id,event_id,buyer_id,ordinal,code) VALUES($1,$2,$3,$4,1,'LEGACY-TICKET')",
      [ticketId, order.id, order.eventId, order.buyerId],
    );
    await fulfillment.query(
      "INSERT INTO notifications(id,order_id,buyer_id,message) VALUES($1,$2,$3,'Legacy notification')",
      [randomUUID(), order.id, order.buyerId],
    );
    for (const consumer of ['tickets', 'notifications'])
      await fulfillment.query(
        'INSERT INTO delivery_receipts(consumer,message_id,fingerprint,order_id) VALUES($1,$2,$3,$4)',
        [
          consumer,
          message.messageId,
          hash(JSON.stringify([message.messageId, message.type, message.version, at, snapshot])),
          order.id,
        ],
      );
    for (const projection of ['events', 'daily']) {
      const g = randomUUID();
      await analytics.query(
        "INSERT INTO projection_generations(projection,generation,status) VALUES($1,$2,'active')",
        [projection, g],
      );
      await analytics.query('INSERT INTO projection_active(projection,generation) VALUES($1,$2)', [
        projection,
        g,
      ]);
      if (projection === 'events')
        await analytics.query(
          'INSERT INTO event_sales(generation,organizer_id,event_id,title,orders,tickets,revenue) VALUES($1,$2,$3,$4,1,1,15000)',
          [g, order.organizerId, order.eventId, order.eventTitle],
        );
      else
        await analytics.query(
          "INSERT INTO daily_revenue(generation,organizer_id,day,orders,tickets,revenue) VALUES($1,$2,'2026-09-01',1,1,15000)",
          [g, order.organizerId],
        );
    }
    const kafka = new Kafka({ brokers: f.settings.kafkaBrokers, logLevel: logLevel.NOTHING }),
      admin = kafka.admin(),
      producer = kafka.producer();
    try {
      await admin.connect();
      await admin.createTopics({
        topics: [{ topic: f.env.KAFKA_TOPIC, numPartitions: 3, replicationFactor: 1 }],
        waitForLeaders: true,
      });
      await producer.connect();
      await producer.send({
        topic: f.env.KAFKA_TOPIC,
        messages: [{ key: order.eventId, value: JSON.stringify(message) }],
      });
    } finally {
      await producer.disconnect();
      await admin.disconnect();
    }
    await migrateAll(settings);
    // Refuse an offline upgrade while any participating application's SQL session is alive.
    for (const [database, app] of [
      [sales, 'sales-worker'],
      [fulfillment, 'fulfillment'],
      [analytics, 'ticketing-analytics'],
      [f.databases.paymentMock, 'payment-mock'],
    ]) {
      const client = await database.connect();
      try {
        await client.query("SELECT set_config('application_name',$1,false)", [app]);
        await assert.rejects(upgrade(settings), /Stop application processes before upgrade:state/);
      } finally {
        await client.query('RESET application_name');
        client.release();
      }
    }
    await upgrade(settings);
    assert.equal(
      (await fulfillment.query('SELECT id FROM tickets WHERE order_id=$1', [order.id])).rows[0].id,
      ticketId,
    );
    assert.equal(
      (await sales.query('SELECT status FROM orders WHERE id=$1', [order.id])).rows[0].status,
      'confirmed',
    );
    const payment = (await sales.query('SELECT * FROM payments WHERE order_id=$1', [order.id])).rows[0];
    assert.equal(payment.status, 'succeeded');
    assert.equal(
      (await f.databases.paymentMock.query('SELECT count(*)::integer AS n FROM provider_payments')).rows[0].n,
      1,
    );
    for (const projection of ['events', 'daily']) {
      const rows = (
        await analytics.query(
          'SELECT g.format_version FROM projection_active a JOIN projection_generations g USING(projection,generation) WHERE a.projection=$1',
          [projection],
        )
      ).rows;
      assert.equal(rows[0].format_version, 2);
    }
    // Simulate interruption after provider import but before recording its version in Sales.
    await sales.query('UPDATE payments SET provider_version=0 WHERE id=$1', [payment.id]);
    await upgrade(settings);
    await upgrade(settings);
    assert.equal(
      (await f.databases.paymentMock.query('SELECT count(*)::integer AS n FROM provider_payments')).rows[0].n,
      1,
    );
    f.start('sales');
    f.start('payment-mock');
    f.start('worker');
    f.start('fulfillment');
    f.start('analytics');
    f.start('relay');
    await Promise.all([f.urls.sales, f.urls.paymentMock, f.urls.fulfillment, f.urls.analytics].map(f.ready));
    const buyer = await f.login(),
      organizer = await f.login(f.DEMO.organizer);
    const report = (await request(f.urls.sales, '/reports/events', { auth: organizer })).data;
    assert.equal(report[0].revenue, 15000);
    assert.equal(report[0].tickets, 1);
    await sales.query("UPDATE outbox_deliveries SET status='pending',available_at=now()");
    await until(
      async () =>
        (await sales.query("SELECT count(*)::integer AS n FROM outbox_deliveries WHERE status<>'delivered'"))
          .rows[0].n === 0,
      'legacy redelivery',
    );
    const result = await request(f.urls.sales, `/orders/${order.id}/cancel`, { method: 'POST', auth: buyer });
    assert.equal(result.status, 200);
    await until(
      async () =>
        (await request(f.urls.sales, `/orders/${order.id}`, { auth: buyer })).data.payment.status ===
        'refunded',
      'legacy payment refunded',
    );
    await until(async () => {
      const rows = (await request(f.urls.sales, '/reports/events', { auth: organizer })).data;
      return rows[0].revenue === 0 && rows[0].tickets === 0 && rows[0].refunds === 15000;
    }, 'legacy reports corrected');
    assert.equal(
      (await fulfillment.query('SELECT count(*)::integer AS n FROM tickets WHERE order_id=$1', [order.id]))
        .rows[0].n,
      1,
    );
  },
);
