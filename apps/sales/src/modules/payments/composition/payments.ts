import { config, Database } from '@ticketing/core';
import { PgPayments } from '../infrastructure/pg-payments';
import { HttpPaymentProvider } from '../infrastructure/http-provider';
import { WebhooksController, PAYMENT_WEBHOOKS, WEBHOOK_SECRET } from '../presentation/webhooks.controller';
export function paymentsComposition(db: Database) {
  const settings = config(),
    repository = new PgPayments(db);
  return {
    repository,
    provider: new HttpPaymentProvider(settings.paymentUrl, settings.internalToken),
    controllers: [WebhooksController],
    providers: [
      { provide: PAYMENT_WEBHOOKS, useValue: repository },
      { provide: WEBHOOK_SECRET, useValue: settings.webhookSecret },
    ],
  };
}
