import { randomUUID } from 'node:crypto';
import { Database } from '@ticketing/core';
import { parseOrderEvent } from '@ticketing/contracts';
import type { OutboxRepository } from '../application/ports';
import type { Delivery, Destination } from '../domain/delivery';

/**
 * Координирует relay-процессы короткими атомарными SQL-операциями.
 *
 * @remarks
 * Блокировка строки не удерживается во время сетевой публикации. Вместо этого
 * в БД записывается аренда на 45 секунд с уникальным owner. Потерявший процесс
 * не блокирует очередь навсегда; истёкшая аренда доступна следующему владельцу.
 * Долгая публикация может пережить аренду, поэтому повторная доставка допустима.
 */
export class PgOutbox implements OutboxRepository {
  constructor(private readonly db: Database) {}
  /**
   * Одним SQL-запросом выбирает доступную доставку и назначает владельца аренды.
   *
   * @remarks
   * FOR UPDATE SKIP LOCKED пропускает строки, захваченные другим relay, вместо
   * ожидания в общей очереди. CTE candidate выбирает работу, leased меняет её
   * состояние, заключительный JOIN читает сохранённый конверт. Фильтр destination
   * проходит через выборку и UPDATE: успешная доставка в один брокер не влияет
   * на вторую. attempts увеличивается на каждое получение аренды, в том числе
   * после падения предыдущего владельца. Случайный owner служит защитным маркером
   * для следующих UPDATE, но не является секретом аутентификации.
   */
  async claim(destination: Destination): Promise<Delivery | undefined> {
    const owner = randomUUID();
    const result = await this.db.query(
      `WITH candidate AS (
      SELECT message_id FROM outbox_deliveries
      WHERE destination=$1 AND available_at <= now()
        AND (status='pending' OR (status='leased' AND lease_until < now()))
      ORDER BY available_at, message_id FOR UPDATE SKIP LOCKED LIMIT 1
    ), leased AS (
      UPDATE outbox_deliveries d SET status='leased', lease_owner=$2,
        lease_until=now()+interval '45 seconds', attempts=attempts+1
      FROM candidate c WHERE d.message_id=c.message_id AND d.destination=$1
      RETURNING d.message_id, d.attempts
    ) SELECT l.message_id, l.attempts, m.payload, m.trace_context FROM leased l
      INNER JOIN outbox_messages m ON m.message_id=l.message_id`,
      [destination, owner],
    );
    const row = result.rows[0];
    if (!row) return undefined;
    return {
      messageId: row.message_id,
      owner,
      attempts: row.attempts,
      event: parseOrderEvent(row.payload),
      traceContext: row.trace_context,
    };
  }
  /**
   * Подтверждает только доставку, которой всё ещё владеет данный relay.
   * Если аренду уже перехватили, UPDATE не изменит строк: старый процесс
   * не вправе закрыть работу нового владельца. Это fencing на границе PostgreSQL;
   * отправленное старым процессом сообщение у брокера отменить он не может.
   */
  async complete(destination: Destination, delivery: Delivery) {
    await this.db.query(
      `UPDATE outbox_deliveries SET status='delivered', delivered_at=now(),
      lease_owner=NULL, lease_until=NULL, last_error=NULL
      WHERE message_id=$1 AND destination=$2 AND lease_owner=$3 AND status='leased'`,
      [delivery.messageId, destination, delivery.owner],
    );
  }
  /**
   * Освобождает собственную аренду и откладывает повтор с растущей задержкой до 30 секунд.
   *
   * @remarks
   * Число повторов публикации не ограничено: подтверждённая покупка должна дождаться
   * восстановления брокера. Хранится код PUBLISH_UNCONFIRMED, а не текст сетевой
   * ошибки, который мог бы содержать адрес или учётные данные. Просроченный владелец
   * не меняет чужую новую аренду благодаря условию lease_owner.
   */
  async retry(destination: Destination, delivery: Delivery) {
    const seconds = Math.min(30, 2 ** Math.min(delivery.attempts - 1, 5));
    await this.db.query(
      `UPDATE outbox_deliveries SET status='pending', lease_owner=NULL, lease_until=NULL,
      last_error='PUBLISH_UNCONFIRMED', available_at=now()+($4::integer * interval '1 second')
      WHERE message_id=$1 AND destination=$2 AND lease_owner=$3 AND status='leased'`,
      [delivery.messageId, destination, delivery.owner, seconds],
    );
  }
}
