import { config, Database, RedisConnection } from '@ticketing/core';
import { ProductQueriesService } from '../application/product-queries.service';
import { HttpProductQueries } from '../infrastructure/http-product-queries';
import { ProductQueriesController } from '../presentation/product-queries.controller';
import { HealthService } from '../application/health.service';
import { Readiness } from '../infrastructure/readiness';
import { HealthController } from '../presentation/health.controller';
import { OrdersService } from '../../orders';

type Settings = ReturnType<typeof config>;
/** Подключает публичный сценарий Orders к порту владения без зависимости application от другого модуля. */
export function queriesComposition(
  settings: Settings,
  db: Database,
  redis: RedisConnection,
  orders: OrdersService,
) {
  const http = new HttpProductQueries(settings.fulfillmentUrl, settings.analyticsUrl, settings.internalToken);
  const queries = new ProductQueriesService(
    http,
    {
      assertOwnedBy: async (orderId, buyerId) => {
        return { status: (await orders.get(orderId, buyerId)).status };
      },
    },
    http,
  );
  return {
    controllers: [ProductQueriesController, HealthController],
    providers: [
      { provide: ProductQueriesService, useValue: queries },
      { provide: HealthService, useValue: new HealthService(new Readiness(db, redis)) },
    ],
  };
}
