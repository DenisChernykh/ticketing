import { carrier, traced, count } from '@ticketing/observability';
import amqp, { type ChannelModel, type ConfirmChannel } from 'amqplib';
import { rabbitNames, type OrderEvent } from '@ticketing/contracts';
import type { EventPublisher } from '../application/ports';

/**
 * Публикует одно событие в RabbitMQ с подтверждением брокера и постоянным messageId.
 *
 * @remarks
 * Один экземпляр используется последовательным циклом одного направления relay.
 * Это важно для обработчика return: он относится к текущей публикации канала.
 * Подтверждение издателю не означает выпуск билета — потребители отдельно
 * подтверждают обработку после коммита собственной БД.
 */
export class RabbitPublisher implements EventPublisher {
  private connection?: ChannelModel;
  private channel?: ConfirmChannel;
  constructor(
    private readonly url: string,
    private readonly prefix: string,
  ) {}
  /**
   * Лениво соединяется и объявляет exchange, обе quorum-очереди и привязки.
   *
   * @remarks
   * Очереди создаются ещё до запуска Fulfillment: durable exchange сам по себе
   * не хранит сообщение без маршрута. Повторное объявление безопасно только
   * с теми же параметрами, которые использует получатель. Закрытие соединения
   * сбрасывает кеш клиента, позволяя следующей попытке подключиться заново.
   */
  private async ready(): Promise<ConfirmChannel> {
    if (this.channel) return this.channel;
    const connection = await amqp.connect(this.url, { timeout: 4000 });
    connection.on('error', () => {
      /* close сбросит канал; текст ошибки с возможными секретами не журналируем. */
    });
    connection.on('close', () => {
      if (this.connection === connection) {
        this.connection = undefined;
        this.channel = undefined;
      }
    });
    this.connection = connection;
    const channel = await connection.createConfirmChannel();
    channel.on('error', () => {
      /* Результат операции обрабатывается через publish/close. */
    });
    channel.on('close', () => {
      if (this.channel === channel) this.channel = undefined;
    });
    const names = rabbitNames(this.prefix);
    await channel.assertExchange(names.exchange, 'topic', { durable: true });
    for (const queue of [names.tickets, names.notifications]) {
      await channel.assertQueue(queue, {
        durable: true,
        arguments: { 'x-queue-type': 'quorum', 'x-delivery-limit': -1 },
      });
      for (const key of [
        'order.confirmed.v1',
        'order.confirmed.v2',
        'order.cancelled.v2',
        'order.refunded.v2',
      ])
        await channel.bindQueue(queue, names.exchange, key);
    }
    this.channel = channel;
    return channel;
  }
  /**
   * Ждёт publisher confirm и отвергает сообщение, возвращённое как немаршрутизируемое.
   *
   * @remarks
   * persistent сохраняет сообщение в устойчивой очереди, mandatory обнаруживает
   * отсутствие подходящего маршрута. Callback confirm относится к принятию
   * брокером; локальный таймаут 8 секунд и закрытие канала считаются неизвестным
   * исходом. В таком случае соединение сбрасывается, а relay повторяет тот же
   * конверт. Даже потерянное подтверждение может означать уже сохранённое сообщение,
   * поэтому получатели обязательно дедуплицируют messageId.
   */
  async publish(event: OrderEvent, incoming?: Record<string, string>): Promise<void> {
    return traced(
      'outbox.rabbitmq',
      { orderId: event.order.id, messageId: event.messageId },
      async () => {
        try {
          const channel = await this.ready();
          const names = rabbitNames(this.prefix);
          await new Promise<void>((resolve, reject) => {
            let returned = false;
            const onReturn = () => {
              returned = true;
            };
            const finish = (error?: unknown) => {
              clearTimeout(timer);
              channel.off('return', onReturn);
              channel.off('close', onClose);
              error ? reject(error) : resolve();
            };
            const onClose = () => finish(new Error('RABBIT_CHANNEL_CLOSED'));
            const timer = setTimeout(() => finish(new Error('RABBIT_CONFIRM_TIMEOUT')), 8000);
            channel.on('return', onReturn);
            channel.once('close', onClose);
            try {
              channel.publish(
                names.exchange,
                `${event.type}.v${event.version}`,
                Buffer.from(JSON.stringify(event)),
                {
                  persistent: true,
                  mandatory: true,
                  contentType: 'application/json',
                  messageId: event.messageId,
                  headers: carrier(),
                  type: event.type,
                  timestamp: Date.parse(event.occurredAt),
                },
                (error) => finish(error ?? (returned ? new Error('RABBIT_UNROUTABLE') : undefined)),
              );
            } catch (error) {
              finish(error);
            }
          });
        } catch (error) {
          count('outbox_retries', { destination: 'rabbitmq' });
          await this.close();
          throw error;
        }
      },
      incoming,
    );
  }
  async close() {
    const connection = this.connection;
    this.channel = undefined;
    this.connection = undefined;
    await closeRabbitConnection(connection);
  }
}

/**
 * Ограничивает завершение AMQP-соединения, чтобы сбой сети не остановил повторы outbox.
 *
 * @remarks
 * У закреплённой версии amqplib событие close может наступить, а обещание close()
 * остаться незавершённым, если сокет пропал до CloseOk. Поэтому завершение ждёт
 * либо обещание, либо событие, либо deadline. По истечении срока уничтожается
 * нижележащий stream. Доступ к нему зависит от внутренней структуры библиотеки
 * и должен проверяться при её обновлении. Повторные сигналы завершения безопасны.
 *
 * @param deadlineMs - Максимальное ожидание нормального закрытия; по умолчанию 1000 мс.
 */
export function closeRabbitConnection(
  connection: ChannelModel | undefined,
  deadlineMs = 1000,
): Promise<void> {
  if (!connection) return Promise.resolve();
  return new Promise((resolve) => {
    let finished = false;
    const finish = () => {
      if (finished) return;
      finished = true;
      clearTimeout(timer);
      connection.off('close', finish);
      resolve();
    };
    const destroy = () => {
      // В этой закреплённой версии amqplib доступ к stream нужен для прерывания зависшего handshake.
      const stream = (connection.connection as unknown as { stream?: { destroy(): void } }).stream;
      try {
        stream?.destroy();
      } catch {
        /* Очистка завершается и для уже закрытого сокета. */
      }
      finish();
    };
    const timer = setTimeout(destroy, deadlineMs);
    connection.once('close', finish);
    try {
      void connection.close().then(finish, destroy);
    } catch {
      destroy();
    }
  });
}
