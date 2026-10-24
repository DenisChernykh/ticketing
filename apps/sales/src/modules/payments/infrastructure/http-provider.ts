import { parsePayment } from '@ticketing/contracts';
import { carrier, traced } from '@ticketing/observability';
import type { Payment, PaymentProvider } from '../application/ports';
export class HttpPaymentProvider implements PaymentProvider {
  constructor(
    private readonly base: string,
    private readonly token: string,
  ) {}
  private async call(p: Payment, path: string, method = 'GET') {
    return traced('payment.http', { orderId: p.orderId, paymentId: p.id }, async () => {
      const res = await fetch(new URL(path, this.base), {
        method,
        redirect: 'error',
        signal: AbortSignal.timeout(2500),
        headers: {
          'Content-Type': 'application/json',
          'X-Internal-Token': this.token,
          'Idempotency-Key': p.id,
          ...carrier(),
        },
        ...(method === 'POST'
          ? { body: JSON.stringify({ id: p.id, orderId: p.orderId, amount: p.amount, currency: 'RUB' }) }
          : {}),
      });
      if (res.status === 404 && method === 'GET') return undefined;
      if (!res.ok) throw new Error('PAYMENT_PROVIDER_UNAVAILABLE');
      return parsePayment(await res.json());
    });
  }
  async charge(p: Payment) {
    return (await this.call(p, '/payments', 'POST'))!;
  }
  async refund(p: Payment) {
    return (await this.call(p, `/payments/${p.id}/refund`, 'POST'))!;
  }
  get(p: Payment) {
    return this.call(p, `/payments/${p.id}`);
  }
}
