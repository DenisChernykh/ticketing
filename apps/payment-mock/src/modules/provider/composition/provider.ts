import { Module, type DynamicModule } from '@nestjs/common';
import { config, Database, log, sleep } from '@ticketing/core';
import { ProviderService } from '../application/provider.service';
import { PgProvider } from '../infrastructure/pg-provider';
import {
  ProviderController,
  ProviderHealthController,
  ProviderGuard,
  PROVIDER_TOKEN,
} from '../presentation/provider.controller';
@Module({})
class ProviderModule {}
export function providerComposition() {
  const settings = config(),
    db = new Database(settings.paymentMockDatabaseUrl, 'payment-mock'),
    repository = new PgProvider(db, settings.webhookUrl, settings.webhookSecret),
    service = new ProviderService(repository),
    stop = new AbortController();
  let loops: Promise<void>[] = [];
  const run = async (fn: () => Promise<unknown>) => {
    while (!stop.signal.aborted) {
      try {
        await fn();
      } catch (e) {
        log('provider.worker.error', { error: String(e) });
      }
      await sleep(100, stop.signal);
    }
  };
  const close = async () => {
    stop.abort();
    await Promise.allSettled(loops);
    await db.close();
  };
  return {
    port: settings.paymentMockPort,
    start() {
      loops = [run(() => repository.tick()), run(() => repository.deliver())];
    },
    module: {
      module: ProviderModule,
      controllers: [ProviderController, ProviderHealthController],
      providers: [
        ProviderGuard,
        { provide: PROVIDER_TOKEN, useValue: settings.internalToken },
        { provide: ProviderService, useValue: service },
        { provide: 'PROVIDER_LIFECYCLE', useValue: { onApplicationShutdown: close } },
      ],
    } as DynamicModule,
  };
}
