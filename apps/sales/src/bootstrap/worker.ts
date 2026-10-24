import { log, sleep } from '@ticketing/core';
import { startTelemetry, stopTelemetry, traced, count } from '@ticketing/observability';
import { createSalesWorker } from '../composition/sales';
async function main() {
  startTelemetry('sales-worker');
  const runtime = await createSalesWorker(),
    stop = new AbortController();
  for (const signal of ['SIGTERM', 'SIGINT'] as const) process.once(signal, () => stop.abort());
  const loop = async (name: string, fn: () => Promise<unknown>, interval = 250) => {
    while (!stop.signal.aborted) {
      try {
        await fn();
      } catch (e) {
        count('worker_errors', { worker: name });
        log('worker.error', { worker: name, error: e instanceof Error ? e.message : String(e) });
      }
      await sleep(interval, stop.signal);
    }
  };
  try {
    await Promise.all([
      loop('reservation.expire', () => runtime.checkout.expire()),
      loop('payment.operation', () => runtime.worker.operation()),
      loop('payment.reconcile', () => runtime.worker.sync()),
      loop('worker.metrics', () => runtime.metrics(), 5000),
    ]);
  } finally {
    await runtime.close();
    await stopTelemetry();
  }
}
void main().catch((e) => {
  log('worker.failed', { error: String(e) });
  process.exitCode = 1;
});
