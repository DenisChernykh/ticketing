import { equal } from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { PassThrough } from 'node:stream';
import { setTimeout as wait } from 'node:timers/promises';
import { test } from 'node:test';
import type { ChannelModel } from 'amqplib';
import { closeRabbitConnection } from './rabbit-close';

class FakeConnection extends EventEmitter {
  destroyed = 0;
  connection = { stream: { destroy: () => { this.destroyed += 1; } } };
  close: () => Promise<void> = () => new Promise(() => undefined);
  stop(deadline = 10) { return closeRabbitConnection(this as unknown as ChannelModel, deadline); }
}

test('connection close event settles cleanup even when its close promise never resolves', async () => {
  const connection = new FakeConnection();
  const closing = connection.stop();
  connection.emit('close');
  await closing;
  equal(connection.listenerCount('close'), 0);
  await wait(25);
  equal(connection.destroyed, 0, 'successful cleanup cleared its deadline timer');
});

test('close handshake deadline destroys the socket and removes its listener', async () => {
  const connection = new FakeConnection();
  await connection.stop();
  equal(connection.destroyed, 1);
  equal(connection.listenerCount('close'), 0);
  connection.emit('close');
  await wait(25);
  equal(connection.destroyed, 1);
});

test('resolved and rejected close promises both clean up their timers', async () => {
  const resolved = new FakeConnection();
  resolved.close = async () => undefined;
  await resolved.stop();
  const rejected = new FakeConnection();
  rejected.close = async () => { throw new Error('closing handshake failed'); };
  await rejected.stop();
  await wait(25);
  equal(resolved.destroyed, 0);
  equal(rejected.destroyed, 1);
  equal(resolved.listenerCount('close'), 0);
  equal(rejected.listenerCount('close'), 0);
});

test('actual amqplib socket error before CloseOk cannot leave cleanup pending', async () => {
  const { Connection } = require('amqplib/lib/connection');
  const { ChannelModel: Model } = require('amqplib/lib/channel_model');
  const stream = new PassThrough();
  stream.resume();
  const raw = new Connection(stream);
  const model = new Model(raw) as ChannelModel;
  model.on('error', () => undefined);
  const closing = closeRabbitConnection(model, 500);
  raw.onSocketError(new Error('socket vanished during CloseOk'));
  await closing;
  equal(model.listenerCount('close'), 0);
  stream.destroy();
});
