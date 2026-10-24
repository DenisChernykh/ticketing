import { startTelemetry, stopTelemetry } from '@ticketing/observability';
import { log, sleep } from '@ticketing/core';
import { createSalesRelay } from '../composition/sales';

/**
 * Запускает два независимых цикла доставки и завершает их по сигналу процесса.
 *
 * @remarks
 * Циклы разделены по брокерам: сетевое ожидание одного не задерживает другой.
 * Пауза при пустой очереди или ошибке не позволяет непрерывно нагружать БД.
 * AbortController прекращает новые итерации; уже начатая публикация завершает
 * свой исход, после чего закрываются клиенты и пул. При принудительном падении
 * устойчивые аренды будут восстановлены следующим процессом после истечения срока.
 */
async function main() {
  startTelemetry('sales-relay');
  const runtime = createSalesRelay();
  const stop = new AbortController();
  for (const signal of ['SIGINT', 'SIGTERM'] as const) process.once(signal, () => stop.abort());
  const run = async (destination: 'rabbitmq' | 'kafka') => {
    while (!stop.signal.aborted) {
      try {
        const result = await runtime.relay.deliverOne(destination);
        if (result === 'retry') log('sales.outbox.retry', { destination });
        if (result !== 'delivered') await sleep(300, stop.signal);
      } catch {
        if (!stop.signal.aborted) {
          log('sales.outbox.unavailable', { destination });
          await sleep(1000, stop.signal);
        }
      }
    }
  };
  log('sales.outbox.started');
  try {
    await Promise.all([run('rabbitmq'), run('kafka')]);
  } finally {
    await runtime.close();
    await stopTelemetry();
  }
}
void main().catch(() => {
  log('sales.outbox.failed');
  process.exitCode = 1;
});
