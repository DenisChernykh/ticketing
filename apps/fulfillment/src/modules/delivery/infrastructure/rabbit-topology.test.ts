import { equal, rejects } from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { test } from 'node:test';
import type { ConfirmChannel, Options } from 'amqplib';
import { ConfirmedPublisher } from './rabbit-topology';

class FakeChannel extends EventEmitter {
  sent: string[] = [];
  handler: (queue: string, options: Options.Publish, confirm: (error: Error | null) => void) => void = (_queue, _options, confirm) => confirm(null);
  sendToQueue(queue: string, _body: Buffer, options: Options.Publish, confirm: (error: Error | null) => void): boolean {
    this.sent.push(queue);
    this.handler(queue, options, confirm);
    return true;
  }
  async close() { this.emit('close'); }
  publisher() { return new ConfirmedPublisher(this as unknown as ConfirmChannel); }
}

test('mandatory returned messages fail even when RabbitMQ positively confirms them', async () => {
  const channel = new FakeChannel();
  channel.handler = (_queue, options, confirm) => {
    equal(options.mandatory, true);
    equal(options.persistent, true);
    channel.emit('return');
    confirm(null);
  };
  await rejects(channel.publisher().send('missing.queue', Buffer.from('{}'), {}), /Unroutable/);
});

test('a connection loss while waiting for confirmation never counts as delivered', async () => {
  const channel = new FakeChannel();
  channel.handler = () => { channel.emit('close'); };
  await rejects(channel.publisher().send('queue', Buffer.from('{}'), {}), /closed before confirmation/);
});

test('concurrent retry publications serialize so a return belongs to exactly one message', async () => {
  const channel = new FakeChannel();
  let firstConfirm: ((error: Error | null) => void) | undefined;
  channel.handler = (queue, _options, confirm) => {
    if (queue === 'first') firstConfirm = confirm;
    else confirm(null);
  };
  const publisher = channel.publisher();
  const first = publisher.send('first', Buffer.from('{}'), {});
  const firstRejected = rejects(first, /Unroutable/);
  const second = publisher.send('second', Buffer.from('{}'), {});
  await Promise.resolve();
  equal(channel.sent.join(','), 'first');
  channel.emit('return');
  firstConfirm!(null);
  await firstRejected;
  await second;
  equal(channel.sent.join(','), 'first,second');
  equal(channel.listenerCount('return'), 0);
});
