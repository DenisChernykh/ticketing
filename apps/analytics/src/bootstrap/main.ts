import 'reflect-metadata';
import { config, startHttp } from '@ticketing/core';
import { AnalyticsModule } from '../modules/reports/composition/analytics.module';

void startHttp(
  AnalyticsModule,
  config().analyticsPort,
  'Ticketing — Analytics',
).catch((error: unknown) => {
  console.error(error);
  process.exitCode = 1;
});
