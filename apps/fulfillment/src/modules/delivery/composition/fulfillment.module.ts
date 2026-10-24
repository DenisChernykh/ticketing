import { Module, type DynamicModule, type OnApplicationShutdown } from '@nestjs/common';
import { config, Database } from '@ticketing/core';
import { FulfillOrder } from '../application/fulfill-order';
import { ReadDelivery } from '../application/read-delivery';
import { PostgresDeliveryRepository } from '../infrastructure/postgres-delivery-repository';
import { RabbitWorkers } from '../infrastructure/rabbit-workers';
import { DeliveryController, HealthController, INTERNAL_TOKEN } from '../presentation/delivery.controller';

class FulfillmentLifecycle implements OnApplicationShutdown {
  constructor(private readonly workers: RabbitWorkers, private readonly database: Database) {}
  async onApplicationShutdown(): Promise<void> {
    await this.workers.stop();
    await this.database.close();
  }
}

@Module({})
export class FulfillmentModule {
  static create(settings: ReturnType<typeof config>): { module: DynamicModule; startWorkers(): void } {
    const database = new Database(settings.fulfillmentDatabaseUrl, 'fulfillment');
    const repository = new PostgresDeliveryRepository(database);
    const fulfillOrder = new FulfillOrder(repository, database);
    const workers = new RabbitWorkers(settings.amqpUrl, settings.prefix, fulfillOrder);
    const readDelivery = new ReadDelivery(repository, workers);
    return {
      startWorkers: () => workers.start(),
      module: {
        module: FulfillmentModule,
        controllers: [DeliveryController, HealthController],
        providers: [
          { provide: ReadDelivery, useValue: readDelivery },
          { provide: INTERNAL_TOKEN, useValue: settings.internalToken },
          { provide: FulfillmentLifecycle, useValue: new FulfillmentLifecycle(workers, database) },
        ],
      },
    };
  }
}
