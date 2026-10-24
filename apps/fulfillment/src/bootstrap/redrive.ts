import 'reflect-metadata';
import { log } from '@ticketing/core';
import { redriveDeadLetters } from '../modules/delivery/composition/redrive';

void redriveDeadLetters(process.argv.slice(2)).catch(error => {
  log('fulfillment.redrive_failed', { error: error instanceof Error ? error.message : String(error) });
  process.exitCode = 1;
});
