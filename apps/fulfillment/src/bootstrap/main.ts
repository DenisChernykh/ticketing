import 'reflect-metadata';
import { config, log, startHttp } from '@ticketing/core';
import { FulfillmentModule } from '../modules/delivery/composition/fulfillment.module';

async function main(): Promise<void> {
  const settings = config();
  const runtime = FulfillmentModule.create(settings);
  await startHttp(runtime.module, settings.fulfillmentPort, 'Ticketing · Fulfillment');
  runtime.startWorkers();
}

void main().catch(error => {
  log('fulfillment.startup_failed', { error: error instanceof Error ? error.message : String(error) });
  process.exitCode = 1;
});
