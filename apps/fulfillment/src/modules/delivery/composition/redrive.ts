import { config, log } from '@ticketing/core';
import { redrive } from '../infrastructure/redrive';
import type { ConsumerName } from '../domain/delivery';

export async function redriveDeadLetters(args: string[]): Promise<void> {
  if (args.some(value => value !== 'tickets' && value !== 'notifications')) {
    throw new Error('Usage: pnpm redrive [tickets|notifications]');
  }
  const consumers: ConsumerName[] = args.length ? [...new Set(args)] as ConsumerName[] : ['tickets', 'notifications'];
  const settings = config();
  const result = await redrive(settings.amqpUrl, settings.prefix, consumers);
  log('fulfillment.redrive_complete', result);
  if (result.invalid) process.exitCode = 2;
}
