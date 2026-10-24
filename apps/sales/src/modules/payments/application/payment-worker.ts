import type { PaymentJobs, PaymentProvider, PaymentSettlement, PaymentOperation, Payment } from './ports';
/**
 * Выполняет устойчивое платёжное задание за пределами транзакции заказа.
 * Сбой между ответом провайдера и локальным коммитом оставляет неизвестный исход:
 * повтор использует тот же paymentId, а независимая сверка читает актуальный результат.
 * Функции runContext/syncContext внедряются композиционным корнем; SDK здесь не нужен.
 */
export class PaymentWorker {
  constructor(
    private readonly jobs: PaymentJobs,
    private readonly provider: PaymentProvider,
    private readonly settlement: PaymentSettlement,
    private readonly runContext: <T>(job: PaymentOperation, fn: () => Promise<T>) => Promise<T> = (
      _job,
      fn,
    ) => fn(),
    private readonly syncContext: <T>(
      item: { payment: Payment; traceContext: Record<string, string> },
      fn: () => Promise<T>,
    ) => Promise<T> = (_item, fn) => fn(),
  ) {}
  async operation() {
    const job = await this.jobs.claim();
    if (!job) return false;
    return this.runContext(job, async () => {
      try {
        const remote = await (job.kind === 'charge'
          ? this.provider.charge(job.payment)
          : this.provider.refund(job.payment));
        await this.settlement.settle(job.payment.orderId, remote);
        await this.jobs.finish(job, true);
      } catch (error) {
        await this.jobs.finish(job, false);
        throw error;
      }
      return true;
    });
  }
  async sync() {
    const item = await this.jobs.nextSync();
    if (!item) return false;
    return this.syncContext(item, async () => {
      const remote = await this.provider.get(item.payment);
      if (remote) await this.settlement.settle(item.payment.orderId, remote);
      return true;
    });
  }
}
