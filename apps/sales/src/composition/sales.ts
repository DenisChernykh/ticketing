import { PgWorkMetrics } from '../modules/messaging/composition/metrics';
import { paymentsComposition } from '../modules/payments/composition/payments';
import { checkoutComposition } from '../modules/checkout/composition/checkout';
import { relayComposition } from '../modules/messaging/composition/messaging';
import { Module, type DynamicModule } from '@nestjs/common';
import { config, Database, log, RedisConnection } from '@ticketing/core';
import { identityComposition } from '../modules/identity-access/composition/identity';
import { catalogComposition } from '../modules/catalog/composition/catalog';
import { ordersComposition } from '../modules/orders/composition/orders';
import { queriesComposition } from '../modules/integrations/composition/integrations';

function positive(name: string, fallback: number) {
  const value = Number(process.env[name] ?? fallback);
  if (!Number.isSafeInteger(value) || value < 1) throw new Error(`Invalid ${name}`);
  return value;
}
@Module({})
class SalesModule {}
/**
 * Собирает API: связывает порты сценариев с SQL/Redis/HTTP-адаптерами и Nest-контроллерами.
 *
 * @remarks
 * Только composition знает одновременно конкретные адаптеры и сценарии.
 * Одно Database на процесс даёт общий контекст транзакции для всех репозиториев.
 * Сбой начального подключения Redis не мешает запустить публичный каталог;
 * защищённые операции и readiness до восстановления будут отвечать 503.
 * Lifecycle закрывает ресурсы один раз и при штатном завершении Nest, и при
 * ошибке bootstrap. Это сборка API, а не запуск фоновой доставки outbox.
 */
export async function createSalesApi(): Promise<{
  module: DynamicModule;
  port: number;
  close(): Promise<void>;
}> {
  const settings = config();
  const db = new Database(settings.salesDatabaseUrl, 'sales-api');
  const redis = new RedisConnection(settings.redisUrl, 'sales-api');
  try {
    await redis.connect();
  } catch {
    log('sales.redis.starting_unavailable');
  }
  const identity = identityComposition(db, redis, {
    prefix: settings.prefix,
    ttl: positive('SESSION_TTL_SECONDS', 3600),
    secure: process.env.COOKIE_SECURE === 'true',
    loginLimit: positive('LOGIN_RATE_LIMIT_MAX', 10),
    windowSeconds: positive('RATE_LIMIT_WINDOW_SECONDS', 60),
    orderLimit: positive('RATE_LIMIT_MAX', 100),
  });
  const catalog = catalogComposition(db, redis, settings.prefix, positive('CACHE_TTL_SECONDS', 30));
  const orders = ordersComposition(db);
  const payments = paymentsComposition(db);
  const checkout = checkoutComposition(
    db,
    orders.repository,
    catalog.inventory,
    payments.repository,
    payments.provider,
  );
  const queries = queriesComposition(settings, db, redis, orders.orders);
  let closed = false;
  const close = async () => {
    if (closed) return;
    closed = true;
    await Promise.allSettled([db.close(), redis.close()]);
  };
  return {
    module: {
      module: SalesModule,
      controllers: [
        ...identity.controllers,
        ...catalog.controllers,
        ...orders.controllers,
        ...queries.controllers,
        ...payments.controllers,
      ],
      providers: [
        ...identity.providers,
        ...catalog.providers,
        ...orders.providers,
        ...queries.providers,
        ...payments.providers,
        ...checkout.providers,
        { provide: 'SALES_LIFECYCLE', useValue: { onApplicationShutdown: close } },
      ],
    },
    port: settings.salesPort,
    close,
  };
}
/**
 * Создаёт отдельный runtime relay со своим пулом PostgreSQL и клиентами брокеров.
 * Он может перезапускаться независимо от API; устойчивое состояние работы
 * хранится в outbox_deliveries, поэтому память процесса не является журналом доставки.
 */
export function createSalesRelay() {
  const settings = config();
  const db = new Database(settings.salesDatabaseUrl, 'sales-outbox-relay');
  const composition = relayComposition(db, settings);
  return {
    relay: composition.relay,
    close: async () => {
      await composition.close();
      await db.close();
    },
  };
}

export async function createSalesWorker() {
  const settings = config(),
    db = new Database(settings.salesDatabaseUrl, 'sales-worker');
  const orders = ordersComposition(db),
    payments = paymentsComposition(db);
  // Inventory composition does not need a Redis connection for write operations.
  const { inventory } = catalogComposition(
    db,
    new RedisConnection(settings.redisUrl, 'unused-worker-cache'),
    settings.prefix,
    30,
  );
  const checkout = checkoutComposition(
    db,
    orders.repository,
    inventory,
    payments.repository,
    payments.provider,
  );
  return {
    checkout: checkout.service,
    worker: checkout.worker,
    metrics: () => new PgWorkMetrics(db).refresh(),
    close: () => db.close(),
  };
}
