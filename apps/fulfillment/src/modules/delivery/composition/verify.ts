import { equal, rejects } from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { connect } from 'amqplib';
import { config, Database, log, sleep } from '@ticketing/core';
import { rabbitNames, type OrderConfirmed, type OrderEvent } from '@ticketing/contracts';
import { FulfillOrder } from '../application/fulfill-order';
import { PostgresDeliveryRepository } from '../infrastructure/postgres-delivery-repository';
import { RabbitWorkers } from '../infrastructure/rabbit-workers';
import { closeRabbitConnection } from '../infrastructure/rabbit-close';
import { ConfirmedPublisher, declareTopology, retryDelay } from '../infrastructure/rabbit-topology';
import { redrive } from '../infrastructure/redrive';

/** Inject a real transaction failure after INSERT; no production fault switches are needed. */
class FaultingRepository extends PostgresDeliveryRepository {
  readonly failures = new Map<string, number>();
  override async issueTickets(message: OrderEvent): Promise<void> {
    await super.issueTickets(message);
    const remaining = this.failures.get(message.order.id) ?? 0;
    if (remaining > 0) {
      this.failures.set(message.order.id, remaining - 1);
      throw new Error('Controlled verification failure after ticket INSERT');
    }
  }
}

async function eventually(check: () => Promise<boolean>, label: string, timeout = 20000): Promise<void> {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    if (await check()) return;
    await sleep(50);
  }
  throw new Error(`Timed out: ${label}`);
}

export async function verifyFulfillment(): Promise<void> {
  const settings = config();
  const prefix = `${settings.prefix.slice(0, 24)}_probe_${randomUUID().slice(0, 8)}`;
  const names = rabbitNames(prefix);
  const database = new Database(settings.fulfillmentDatabaseUrl, 'fulfillment-verification');
  const repository = new FaultingRepository(database);
  const workers = new RabbitWorkers(settings.amqpUrl, prefix, new FulfillOrder(repository, database));
  const buyerId = randomUUID();
  const eventId = randomUUID();
  const organizerId = randomUUID();
  const orderIds: string[] = [];
  let connection: Awaited<ReturnType<typeof connect>> | undefined;
  function sale(quantity = 2): OrderConfirmed {
    const orderId = randomUUID();
    orderIds.push(orderId);
    return {
      messageId: randomUUID(),
      type: 'order.confirmed',
      version: 1,
      occurredAt: new Date().toISOString(),
      order: {
        id: orderId,
        buyerId,
        organizerId,
        eventId,
        eventTitle: 'Проверка Fulfillment',
        venue: 'Локальная площадка',
        startsAt: new Date(Date.now() + 86_400_000).toISOString(),
        quantity,
        unitPrice: 15000,
        totalPrice: quantity * 15000,
        currency: 'RUB',
      },
    };
  }
  try {
    const schema = await database.query(
      "SELECT to_regclass('delivery_receipts') AS receipts, to_regclass('delivery_failures') AS failures",
    );
    if (!schema.rows[0].receipts || !schema.rows[0].failures)
      throw new Error('Fulfillment schema is missing; run pnpm migrate first');
    connection = await connect(settings.amqpUrl, { timeout: 5000 });
    connection.on('error', () => undefined);
    const channel = await connection.createConfirmChannel();
    channel.on('error', () => undefined);
    await declareTopology(channel, prefix);
    const publisher = new ConfirmedPublisher(channel);
    const send = (queue: string, message: OrderConfirmed) =>
      publisher.send(queue, Buffer.from(JSON.stringify(message)), {
        contentType: 'application/json',
        messageId: message.messageId,
      });
    const publish = async (message: OrderConfirmed) => {
      await send(names.tickets, message);
      await send(names.notifications, message);
    };
    workers.start();
    await eventually(
      async () => Object.values(workers.states()).every((state) => state.status === 'running'),
      'workers running',
    );

    const normal = sale();
    await publish(normal);
    await publish(normal);
    await eventually(
      async () => (await repository.readOrder(normal.order.id, buyerId)).status === 'completed',
      'both handlers commit',
    );
    equal(
      (
        await database.query('SELECT count(*)::int AS count FROM delivery_receipts WHERE message_id = $1', [
          normal.messageId,
        ])
      ).rows[0].count,
      2,
    );
    equal((await repository.readOrder(normal.order.id, buyerId)).tickets.length, 2);
    await rejects(repository.readOrder(normal.order.id, randomUUID()), /Заказ не найден/);
    equal((await repository.readOrder(randomUUID(), buyerId)).status, 'pending');
    log('fulfillment.verify_duplicate_and_ownership', { prefix });

    const conflicting = { ...normal, order: { ...normal.order, quantity: 3, totalPrice: 45000 } };
    await send(names.tickets, conflicting);
    await eventually(
      async () => (await channel.checkQueue(`${names.tickets}.dead`)).messageCount === 1,
      'conflicting message in DLQ',
    );
    equal((await repository.readOrder(normal.order.id, buyerId)).status, 'completed');
    equal((await repository.readOrder(normal.order.id, buyerId)).tickets.length, 2);
    equal(
      (
        await database.query('SELECT count(*)::int AS count FROM delivery_failures WHERE order_id = $1', [
          normal.order.id,
        ])
      ).rows[0].count,
      0,
    );
    await channel.purgeQueue(`${names.tickets}.dead`);

    await publisher.send(names.tickets, Buffer.from('{malformed'), { contentType: 'application/json' });
    await eventually(
      async () => (await channel.checkQueue(`${names.tickets}.dead`)).messageCount === 1,
      'malformed message in DLQ',
    );
    const invalid = await redrive(settings.amqpUrl, prefix, ['tickets']);
    equal(invalid.invalid, 1);
    equal(invalid.moved, 0);
    equal((await channel.checkQueue(`${names.tickets}.dead`)).messageCount, 1);
    await channel.purgeQueue(`${names.tickets}.dead`);
    log('fulfillment.verify_poison_messages', { prefix });

    const transient = sale(3);
    repository.failures.set(transient.order.id, 1);
    await publish(transient);
    await eventually(
      async () => (await channel.checkQueue(`${names.tickets}.retry`)).messageCount === 1,
      'transient failure scheduled',
    );
    equal(
      (
        await database.query('SELECT count(*)::int AS count FROM tickets WHERE order_id = $1', [
          transient.order.id,
        ])
      ).rows[0].count,
      0,
    );
    equal(
      (
        await database.query(
          "SELECT count(*)::int AS count FROM delivery_receipts WHERE consumer = 'tickets' AND message_id = $1",
          [transient.messageId],
        )
      ).rows[0].count,
      0,
    );
    await eventually(
      async () => (await repository.readOrder(transient.order.id, buyerId)).status === 'completed',
      'retry commits',
      retryDelay() * 4 + 15000,
    );
    equal((await repository.readOrder(transient.order.id, buyerId)).tickets.length, 3);
    log('fulfillment.verify_transaction_rollback_and_retry', { prefix });

    const exhausted = sale();
    repository.failures.set(exhausted.order.id, 100);
    await publish(exhausted);
    await eventually(
      async () => (await repository.readOrder(exhausted.order.id, buyerId)).status === 'failed',
      'retry exhaustion becomes failed',
      retryDelay() * 4 + 15000,
    );
    equal((await channel.checkQueue(`${names.tickets}.dead`)).messageCount, 1);
    equal((await repository.readOrder(exhausted.order.id, buyerId)).tickets.length, 0);
    repository.failures.delete(exhausted.order.id);
    const retried = await redrive(settings.amqpUrl, prefix, ['tickets']);
    equal(retried.moved, 1);
    equal(retried.invalid, 0);
    await eventually(
      async () => (await repository.readOrder(exhausted.order.id, buyerId)).status === 'completed',
      'redrive repairs delivery',
    );
    equal(
      (
        await database.query('SELECT count(*)::int AS count FROM delivery_failures WHERE order_id = $1', [
          exhausted.order.id,
        ])
      ).rows[0].count,
      0,
    );
    log('fulfillment.verify_dead_letter_and_redrive', { prefix });

    await rejects(publisher.send(`${prefix}.missing`, Buffer.from('{}'), {}), /Unroutable/);
    log('fulfillment.verify_passed', { prefix, orders: orderIds.length });
  } finally {
    await workers.stop();
    if (connection) {
      try {
        const cleanup = await connection.createChannel();
        cleanup.on('error', () => undefined);
        for (const queue of [names.tickets, names.notifications]) {
          for (const suffix of ['', '.retry', '.dead']) await cleanup.deleteQueue(`${queue}${suffix}`);
        }
        await cleanup.deleteExchange(names.exchange);
        await cleanup.close();
      } finally {
        await closeRabbitConnection(connection);
      }
    }
    try {
      if (orderIds.length)
        await database.run(async () => {
          for (const table of [
            'delivery_failures',
            'tickets',
            'notifications',
            'delivery_receipts',
            'consumer_orders',
            'fulfillment_orders',
          ]) {
            // Table names are a fixed internal allowlist, never input.
            await database.query(`DELETE FROM ${table} WHERE order_id = ANY($1::uuid[])`, [orderIds]);
          }
        });
    } finally {
      await database.close();
    }
  }
}
