const { test } = require('node:test');
const assert = require('node:assert/strict');
const { randomUUID } = require('node:crypto');
const { fixture, request, until } = require('./support.cjs');

test(
  'OTLP traces cross durable payment jobs and both brokers; metrics reach Prometheus',
  { timeout: 90000 },
  async (t) => {
    const f = await fixture();
    t.after(() => f.cleanup());
    try {
      for (const app of ['sales', 'payment-mock', 'worker', 'relay', 'fulfillment', 'analytics'])
        f.start(app, { OTEL_SDK_DISABLED: 'false' });
      await Promise.all(
        [f.urls.sales, f.urls.paymentMock, f.urls.fulfillment, f.urls.analytics].map(f.ready),
      );
      const buyer = await f.login();
      const reserved = await request(f.urls.sales, '/orders', {
        method: 'POST',
        auth: buyer,
        headers: { 'Idempotency-Key': randomUUID() },
        body: { eventId: f.DEMO.eventId, quantity: 1 },
      });
      assert.equal(reserved.status, 201);
      const payment = await request(f.urls.sales, `/orders/${reserved.data.id}/payments`, {
        method: 'POST',
        auth: buyer,
      });
      assert.equal(payment.status, 202);
      const traceId = payment.headers.get('x-trace-id');
      assert.match(traceId, /^[a-f0-9]{32}$/);
      await until(
        async () =>
          (await request(f.urls.sales, `/orders/${reserved.data.id}/tickets`, { auth: buyer })).data
            .status === 'completed',
        'purchase delivered',
      );
      let spanNames;
      await until(
        async () => {
          const r = await fetch(
            `http://127.0.0.1:${process.env.JAEGER_UI_PORT ?? 36686}/api/traces/${traceId}`,
          );
          const data = await r.json();
          spanNames = data.data?.flatMap((t) => t.spans.map((s) => s.operationName)) ?? [];
          return [
            'payment.charge',
            'payment.reconcile',
            'outbox.rabbitmq',
            'outbox.kafka',
            'fulfillment.tickets',
            'analytics.events',
            'analytics.daily',
          ].every((n) => spanNames.includes(n));
        },
        'trace spans exported',
        30000,
      );
      assert.ok(spanNames.includes('payment.webhook'));
      await until(
        async () => {
          const url = new URL(`http://127.0.0.1:${process.env.PROMETHEUS_PORT ?? 39090}/api/v1/query`);
          url.searchParams.set('query', 'outbox_pending');
          const json = await (await fetch(url)).json();
          return json.data?.result?.length > 0;
        },
        'metrics scraped',
        20000,
      );
      // A separate API has no reachable Collector, but still books an order.
      const isolated = f.start('sales', {
        SALES_PORT: String(f.ports.second),
        OTEL_SDK_DISABLED: 'false',
        OTEL_EXPORTER_OTLP_ENDPOINT: 'http://127.0.0.1:1',
      });
      await f.ready(f.urls.second);
      const healthy = await request(f.urls.second, '/orders', {
        method: 'POST',
        auth: buyer,
        headers: { 'Idempotency-Key': randomUUID() },
        body: { eventId: f.DEMO.eventId, quantity: 1 },
      });
      assert.equal(healthy.status, 201);
      await f.stop(isolated);
    } catch (e) {
      console.error(f.logTail());
      throw e;
    }
  },
);
