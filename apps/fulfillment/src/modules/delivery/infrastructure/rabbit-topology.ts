import { rabbitNames } from '@ticketing/contracts';
import type { Channel, ConfirmChannel, Options } from 'amqplib';
import type { ConsumerName } from '../domain/delivery';

/**
 * Читает задержку повторной обработки в миллисекундах для TTL retry-очередей.
 * Аргументы существующей очереди нельзя произвольно поменять повторным объявлением:
 * изменение этого значения требует согласованного изменения её топологии.
 * @throws Ошибка конфигурации, если значение не целое или находится вне диапазона 1–60000.
 */
export function retryDelay(): number {
  const value = Number(process.env.RABBIT_RETRY_MS ?? 1000);
  if (!Number.isInteger(value) || value < 1 || value > 60000)
    throw new Error('RABBIT_RETRY_MS must be between 1 and 60000');
  return value;
}

/**
 * Создаёт две независимые подписки на продажу и отдельные пути повторов и ошибок.
 *
 * @remarks
 * Topic exchange направляет одно событие в очереди билетов и уведомлений. Это две
 * копии для разных обязанностей, а не два конкурирующих получателя одной очереди.
 * Основные очереди объявляются с теми же аргументами, что и в издателе Sales.
 *
 * Retry — quorum-очередь с TTL. После задержки она возвращает сообщение через default
 * exchange в исходную очередь конкретного обработчика. Пара `at-least-once` и
 * `reject-publish` включает безопасную передачу через dead-letter механизм: источник
 * удерживает сообщение до подтверждения принимающей очередью. Обычная перепубликация
 * через DLX без этой гарантии могла бы потерять сообщение при недоступной цели.
 *
 * `x-delivery-limit = -1` отключает автоматическое удаление после заданного брокером
 * числа доставок. Лимит прикладных повторов задаёт worker, который сначала подтверждённо
 * перепубликует сообщение в retry/DLQ. Durable/quorum и persistent-сообщения защищают
 * сохранение при штатном восстановлении брокера, но один локальный узел не даёт
 * отказоустойчивость к потере его диска и не отменяет необходимость дедупликации.
 *
 * @throws Ошибка AMQP, в том числе конфликт аргументов уже существующей очереди.
 */
export async function declareTopology(channel: Channel, prefix: string): Promise<void> {
  const names = rabbitNames(prefix);
  await channel.assertExchange(names.exchange, 'topic', { durable: true });
  for (const consumer of ['tickets', 'notifications'] as const) {
    const queue = names[consumer];
    await channel.assertQueue(queue, {
      durable: true,
      arguments: {
        'x-queue-type': 'quorum',
        'x-delivery-limit': -1,
      },
    });
    for (const key of ['order.confirmed.v1', 'order.confirmed.v2', 'order.cancelled.v2', 'order.refunded.v2'])
      await channel.bindQueue(queue, names.exchange, key);
    await channel.assertQueue(`${queue}.dead`, {
      durable: true,
      arguments: { 'x-queue-type': 'quorum', 'x-delivery-limit': -1 },
    });
    await channel.assertQueue(`${queue}.retry`, {
      durable: true,
      arguments: {
        'x-queue-type': 'quorum',
        'x-message-ttl': retryDelay(),
        'x-delivery-limit': -1,
        'x-dead-letter-exchange': '',
        'x-dead-letter-routing-key': queue,
        // Внутренний обработчик quorum удалит источник только после подтверждения цели.
        'x-dead-letter-strategy': 'at-least-once',
        'x-overflow': 'reject-publish',
      },
    });
  }
}

/**
 * Публикует сообщения так, чтобы worker мог безопасно решить, когда подтвердить оригинал.
 *
 * @remarks
 * На одном confirm-канале одновременно ожидается только одна публикация. Событие
 * `basic.return` сообщает о невозможности маршрутизации отдельно от publisher confirm;
 * последовательная отправка позволяет однозначно связать их с текущим сообщением.
 * RabbitMQ может подтвердить приём и при отсутствии получателя, поэтому одного ACK
 * издателю недостаточно: при `mandatory` также проверяется отсутствие возврата.
 */
export class ConfirmedPublisher {
  private chain: Promise<unknown> = Promise.resolve();
  constructor(private readonly channel: ConfirmChannel) {}

  /**
   * Ставит публикацию в локальную последовательность и возвращает её собственный результат.
   * Ошибка предыдущей отправки не навсегда блокирует цепочку, но остаётся видна её
   * вызывающему коду. Подтверждение означает принятие брокером, а не выполнение consumer.
   * @throws Ошибка маршрутизации, отрицательное подтверждение, закрытие канала или таймаут.
   */
  send(queue: string, body: Buffer, properties: Options.Publish): Promise<void> {
    const next = this.chain.then(() => this.publish(queue, body, properties));
    this.chain = next.catch(() => undefined);
    return next;
  }

  /**
   * Ждёт confirm не более десяти секунд, одновременно отслеживая возврат и потерю канала.
   * Таймаут оставляет исход доставки неизвестным: сообщение могло сохраниться, а ответ
   * потеряться. Вызывающий worker не должен ACK-ать оригинал; последующая повторная
   * доставка может дать дубликат, который устраняется на стороне PostgreSQL.
   */
  private publish(queue: string, body: Buffer, properties: Options.Publish): Promise<void> {
    return new Promise((resolve, reject) => {
      let returned = false;
      let finished = false;
      const onReturn = () => {
        returned = true;
      };
      const onClose = () => finish(new Error('RabbitMQ channel closed before confirmation'));
      const timer = setTimeout(() => {
        finish(new Error('RabbitMQ publish confirmation timed out'));
        void this.channel.close().catch(() => undefined);
      }, 10000);
      const finish = (error?: Error | null) => {
        if (finished) return;
        finished = true;
        clearTimeout(timer);
        this.channel.off('return', onReturn);
        this.channel.off('close', onClose);
        if (error) reject(error);
        else if (returned) reject(new Error(`Unroutable RabbitMQ message for ${queue}`));
        else resolve();
      };
      this.channel.on('return', onReturn);
      this.channel.once('close', onClose);
      try {
        this.channel.sendToQueue(queue, body, { ...properties, mandatory: true, persistent: true }, (error) =>
          finish(error),
        );
      } catch (error) {
        finish(error instanceof Error ? error : new Error(String(error)));
      }
    });
  }
}

export function consumerQueue(prefix: string, consumer: ConsumerName): string {
  return rabbitNames(prefix)[consumer];
}
