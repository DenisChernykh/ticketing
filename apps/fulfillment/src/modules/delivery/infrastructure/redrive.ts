import { connect } from 'amqplib';
import { parseOrderEvent } from '@ticketing/contracts';
import { log } from '@ticketing/core';
import type { ConsumerName } from '../domain/delivery';
import { closeRabbitConnection } from './rabbit-close';
import { ConfirmedPublisher, consumerQueue, declareTopology } from './rabbit-topology';

/**
 * Возвращает исправленные сообщения из DLQ в очередь той же обязанности.
 *
 * @remarks
 * В начале работы с каждой DLQ фиксируется её текущая длина. Команда делает не больше
 * этого числа чтений, даже если workers одновременно добавляют новые ошибки. Это
 * конечный бюджет обработки, а не изолированный снимок содержимого очереди.
 *
 * Корректное событие сохраняет тело и `messageId`, получает обнулённый счётчик retry
 * и публикуется прямо в исходную очередь обработчика. Публикация в общий exchange
 * ненужно повторила бы работу другой подписки. Оригинал в DLQ подтверждается только
 * после publisher confirm; разрыв между этими действиями может дать повтор, который
 * должен безопасно принять репозиторий.
 *
 * Повреждённые сообщения не исправляются автоматически. До конца текущей порции они
 * удерживаются без ACK, чтобы повторное получение одного и того же некорректного
 * сообщения не вытеснило из обработки корректные. Затем они возвращаются в DLQ.
 * При аварийном выходе закрытие соединения также вернёт неподтверждённые оригиналы.
 *
 * @param consumers - Очереди обязанностей, выбранные командой: билеты, уведомления или обе.
 * @returns Числа подтверждённо перепубликованных и оставленных невалидных сообщений.
 * @throws Ошибка подключения, топологии или подтверждения; неподтверждённый оригинал не удаляется.
 */
export async function redrive(
  url: string,
  prefix: string,
  consumers: ConsumerName[],
): Promise<{ moved: number; invalid: number }> {
  const connection = await connect(url, {
    timeout: 5000,
    clientProperties: { connection_name: 'fulfillment-redrive' },
  });
  connection.on('error', () => undefined);
  let moved = 0;
  let invalid = 0;
  try {
    const channel = await connection.createConfirmChannel();
    channel.on('error', () => undefined);
    await declareTopology(channel, prefix);
    const publisher = new ConfirmedPublisher(channel);
    for (const consumer of consumers) {
      const queue = consumerQueue(prefix, consumer);
      const count = (await channel.checkQueue(`${queue}.dead`)).messageCount;
      // Держим невалидные оригиналы до конца порции, чтобы не читать один и тот же по кругу.
      const rejected: Awaited<ReturnType<typeof channel.get>>[] = [];
      for (let index = 0; index < count; index += 1) {
        const delivery = await channel.get(`${queue}.dead`, { noAck: false });
        if (!delivery) break;
        let message;
        try {
          message = parseOrderEvent(JSON.parse(delivery.content.toString('utf8')));
        } catch {
          invalid += 1;
          rejected.push(delivery);
          continue;
        }
        const headers: Record<string, unknown> = { ...delivery.properties.headers, 'x-retry-count': 0 };
        delete headers['x-last-error'];
        delete headers['x-death'];
        await publisher.send(queue, delivery.content, {
          contentType: 'application/json',
          messageId: message.messageId,
          headers,
        });
        channel.ack(delivery);
        moved += 1;
      }
      for (const delivery of rejected) if (delivery) channel.nack(delivery, false, true);
      log('fulfillment.redrive_queue', { consumer, captured: count, moved, invalid });
    }
    return { moved, invalid };
  } finally {
    await closeRabbitConnection(connection);
  }
}
