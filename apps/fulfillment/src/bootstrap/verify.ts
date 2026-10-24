import 'reflect-metadata';
import { log } from '@ticketing/core';
import { verifyFulfillment } from '../modules/delivery/composition/verify';

void verifyFulfillment().catch(error => {
  log('fulfillment.verify_failed', { error: error instanceof Error ? error.message : String(error) });
  process.exitCode = 1;
});
