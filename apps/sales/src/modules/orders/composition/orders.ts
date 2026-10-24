import { PgOrderQueries } from '../infrastructure/pg-order-queries';
import { Database } from '@ticketing/core';
import { OrdersService } from '../application/orders.service';
import { PgOrders } from '../infrastructure/pg-orders';
import { OrdersController } from '../presentation/orders.controller';
export function ordersComposition(db: Database) {
  const repository = new PgOrders(db),
    orders = new OrdersService(new PgOrderQueries(db));
  return {
    orders,
    repository,
    controllers: [OrdersController],
    providers: [{ provide: OrdersService, useValue: orders }],
  };
}
