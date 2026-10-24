import type { Destination } from '../domain/delivery';
import type { EventPublisher, OutboxRepository } from './ports';

/**
 * Доставляет уже закоммиченные события outbox, не участвуя в HTTP-покупке.
 *
 * @remarks
 * Сценарий обрабатывает одно направление за вызов. Bootstrap запускает два
 * независимых цикла, поэтому недоступный RabbitMQ не останавливает публикацию
 * в Kafka. Несколько процессов relay разделяют работу через аренды в PostgreSQL.
 */
export class RelayService {
  constructor(
    private readonly outbox: OutboxRepository,
    private readonly publishers: Record<Destination, EventPublisher>,
  ) {}
  /**
   * Захватывает доставку, ждёт подтверждения брокера и только затем отмечает успех.
   *
   * @remarks
   * Между подтверждением брокера и обновлением PostgreSQL возможен сбой. В таком
   * случае сообщение будет опубликовано повторно: outbox предотвращает потерю
   * события при сбое процесса, но не устраняет дубликаты. Даже ошибка complete
   * переводит доставку к повтору, а неизменный messageId позволяет получателю
   * распознать уже выполненную работу. Сбой самого retry оставляет аренду,
   * которую другой цикл сможет забрать после истечения срока.
   *
   * @returns empty при отсутствии доступной работы, delivered при подтверждённой
   * публикации и выполненной попытке фиксации, retry при отложенном повторе.
   */
  async deliverOne(destination: Destination): Promise<'empty' | 'delivered' | 'retry'> {
    const delivery = await this.outbox.claim(destination);
    if (!delivery) return 'empty';
    try {
      await this.publishers[destination].publish(delivery.event, delivery.traceContext);
      await this.outbox.complete(destination, delivery);
      return 'delivered';
    } catch {
      // Неизвестный исход публикации повторяется с прежним messageId для дедупликации получателем.
      await this.outbox.retry(destination, delivery);
      return 'retry';
    }
  }
}
