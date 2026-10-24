import { traced } from '@ticketing/observability';
import { config, Database } from '@ticketing/core';
import type { Inventory } from '../../catalog';
import type { OrdersRepository } from '../../orders';
import type { Payments, PaymentJobs, PaymentProvider } from '../../payments';
import { CheckoutService } from '../application/checkout.service';
import { PgEventWriter } from '../../messaging/composition/writer';
import { PaymentWorker } from '../../payments/composition/worker';
export function checkoutComposition(
  db: Database,
  orders: OrdersRepository,
  inventory: Inventory,
  payments: Payments & PaymentJobs,
  provider: PaymentProvider,
) {
  const seconds = Number(process.env.RESERVATION_SECONDS ?? 600);
  if (!Number.isInteger(seconds) || seconds < 1 || seconds > 86400)
    throw new Error('Invalid RESERVATION_SECONDS');
  const service = new CheckoutService(db, orders, inventory, payments, new PgEventWriter(db), seconds);
  return {
    service,
    worker: new PaymentWorker(
      payments,
      provider,
      service,
      (job, fn) =>
        traced(
          'payment.' + job.kind,
          { orderId: job.payment.orderId, paymentId: job.payment.id, operationId: job.id },
          fn,
          job.traceContext,
        ),
      (item, fn) =>
        traced(
          'payment.reconcile',
          { orderId: item.payment.orderId, paymentId: item.payment.id },
          fn,
          item.traceContext,
        ),
    ),
    providers: [{ provide: CheckoutService, useValue: service }],
  };
}
