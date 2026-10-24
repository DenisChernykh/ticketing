import { createHash, randomUUID } from 'node:crypto';
import { AppError, Database } from '@ticketing/core';
import type { OrderEvent } from '@ticketing/contracts';
import { DeliveryRepository } from '../application/ports';
import {
  ConsumerName,
  deliveryStatus,
  MessageConflict,
  Notification,
  OrderDelivery,
  Ticket,
} from '../domain/delivery';

/**
 * Приводит значимые поля продажи к стабильному порядку для сравнения повторов.
 * Порядок ключей входного JSON не должен менять результат сравнения. Транспортный
 * `messageId` сюда не входит: одна и та же продажа не становится другой из-за новой
 * оболочки сообщения. Хеш такого описания — проверка совпадения, не подпись отправителя.
 */
function orderSnapshot(message: OrderEvent): string {
  const order = message.order;
  return JSON.stringify([
    order.id,
    order.buyerId,
    order.organizerId,
    order.eventId,
    order.eventTitle,
    order.venue,
    order.startsAt,
    order.quantity,
    order.unitPrice,
    order.totalPrice,
    order.currency,
  ]);
}
function hash(value: string): string {
  return createHash('sha256').update(value).digest('hex');
}

/**
 * Сохраняет локальное представление продажи и результаты двух обработчиков.
 *
 * @remarks
 * Все SQL-запросы внутри `Database.run` получают одно соединение из контекста
 * транзакции. Поэтому последовательность вызовов репозитория в use case остаётся
 * атомарной, хотя каждый метод содержит собственный запрос. Внешние ключи и UNIQUE
 * защищают целостность дополнительно к проверкам приложения.
 */
export class PostgresDeliveryRepository extends DeliveryRepository {
  constructor(private readonly database: Database) {
    super();
  }

  /**
   * Отличает новую обработку, точный повтор и повторное использование чужого идентификатора.
   *
   * @remarks
   * Уникальность задаётся парой `(consumer, message_id)`: обработанные билеты не должны
   * мешать обработке уведомления для того же события. `ON CONFLICT DO NOTHING RETURNING`
   * позволяет конкурентным доставкам договориться через ограничение БД. При конфликте
   * PostgreSQL дожидается исхода конкурирующей вставки; следующий SELECT при READ COMMITTED
   * читает уже зафиксированную запись.
   *
   * Отпечаток включает оболочку события и значимые поля продажи. Совпавший идентификатор
   * с изменённым содержимым нельзя молча принять за успешный повтор. Запись `claim`
   * откатится вместе с результатом, если дальнейший шаг текущей транзакции не выполнится.
   *
   * @returns `true`, если текущая транзакция вставила запись; `false` для точного повтора.
   * @throws {@link MessageConflict}, если сохранённый отпечаток отличается.
   */
  async claim(consumer: ConsumerName, message: OrderEvent): Promise<boolean> {
    const fingerprint = hash(
      JSON.stringify([
        message.messageId,
        message.type,
        message.version,
        message.occurredAt,
        orderSnapshot(message),
        ...(message.version === 2 ? [message.orderRevision, message.state] : []),
      ]),
    );
    const inserted = await this.database.query(
      `INSERT INTO delivery_receipts (consumer, message_id, fingerprint, order_id)
       VALUES ($1, $2, $3, $4) ON CONFLICT (consumer, message_id) DO NOTHING RETURNING message_id`,
      [consumer, message.messageId, fingerprint, message.order.id],
    );
    if (inserted.rowCount) return true;
    const existing = await this.database.query<{ fingerprint: string }>(
      'SELECT fingerprint FROM delivery_receipts WHERE consumer = $1 AND message_id = $2',
      [consumer, message.messageId],
    );
    if (existing.rows[0]?.fingerprint !== fingerprint) {
      throw new MessageConflict('messageId was reused with a different envelope');
    }
    return false;
  }

  /**
   * Создаёт общую для двух обработчиков запись о продаже и проверяет её неизменность.
   *
   * Новая оболочка с другим `messageId` обходит проверку пары в `claim`, поэтому нужна
   * отдельная защита по `orderId`. Повтор разрешён, только если покупатель, мероприятие,
   * количество, стоимость и остальные значимые данные совпадают с принятыми ранее.
   * Оба обработчика могут прийти первыми; уникальность заказа согласует их без обращения
   * к Sales и без обязательного порядка доставки.
   * @throws {@link MessageConflict}, если тот же заказ описывает другую продажу.
   */
  async registerOrder(message: OrderEvent): Promise<void> {
    const order = message.order;
    const snapshotHash = hash(orderSnapshot(message));
    await this.database.query(
      `INSERT INTO fulfillment_orders (order_id, buyer_id, event_id, quantity, snapshot_hash,revision,state)
       VALUES ($1, $2, $3, $4, $5,0,'reserved') ON CONFLICT (order_id) DO NOTHING`,
      [order.id, order.buyerId, order.eventId, order.quantity, snapshotHash],
    );
    const existing = await this.database.query<{ snapshot_hash: string }>(
      'SELECT snapshot_hash FROM fulfillment_orders WHERE order_id = $1 FOR UPDATE',
      [order.id],
    );
    if (existing.rows[0]?.snapshot_hash !== snapshotHash) {
      throw new MessageConflict('orderId was reused with a different sale');
    }
  }

  /**
   * Выпускает `quantity` самостоятельных билетов одним параметризованным SQL-запросом.
   *
   * UUID и код принадлежат конкретному билету, а `ordinal` обозначает купленную единицу
   * внутри заказа. Пара `(order_id, ordinal)` остаётся стабильной при повторе, даже если
   * новые случайные UUID уже сгенерированы. Поэтому `ON CONFLICT` не добавляет лишние
   * билеты при повторной публикации продажи с новой оболочкой события.
   *
   * `jsonb_to_recordset` превращает переданный параметром массив в строки SQL. Вся
   * вставка участвует в транзакции use case: частично выпущенного набора не остаётся.
   */
  async advance(consumer: ConsumerName, message: OrderEvent): Promise<boolean> {
    const row = (
      await this.database.query(
        'SELECT revision,state,state_hash FROM fulfillment_orders WHERE order_id=$1 FOR UPDATE',
        [message.order.id],
      )
    ).rows[0];
    const stateHash = hash(JSON.stringify(message.state));
    if (row.revision > message.orderRevision) return false;
    if (row.revision === message.orderRevision && row.state_hash && row.state_hash !== stateHash)
      throw new MessageConflict('Conflicting order revision');
    const progress = (
      await this.database.query('SELECT revision FROM consumer_orders WHERE consumer=$1 AND order_id=$2', [
        consumer,
        message.order.id,
      ])
    ).rows[0];
    if (progress && progress.revision >= message.orderRevision) return false;
    await this.database.query(
      'UPDATE fulfillment_orders SET revision=$2,state=$3,state_hash=$4 WHERE order_id=$1',
      [message.order.id, message.orderRevision, message.state.status, stateHash],
    );
    await this.database.query(
      `INSERT INTO consumer_orders(consumer,order_id,revision) VALUES($1,$2,$3) ON CONFLICT(consumer,order_id) DO UPDATE SET revision=EXCLUDED.revision`,
      [consumer, message.order.id, message.orderRevision],
    );
    return true;
  }
  async invalidate(orderId: string) {
    await this.database.query('UPDATE tickets SET valid=false WHERE order_id=$1', [orderId]);
  }
  async issueTickets(message: OrderEvent): Promise<void> {
    const order = message.order;
    const tickets = Array.from({ length: order.quantity }, (_, index) => ({
      id: randomUUID(),
      code: randomUUID().replaceAll('-', '').toUpperCase(),
      ordinal: index + 1,
    }));
    await this.database.query(
      `INSERT INTO tickets (id, order_id, event_id, buyer_id, ordinal, code)
       SELECT t.id, $1::uuid, $2::uuid, $3::uuid, t.ordinal, t.code
       FROM jsonb_to_recordset($4::jsonb) AS t(id uuid, ordinal integer, code text)
       ON CONFLICT (order_id, ordinal) DO NOTHING`,
      [order.id, order.eventId, order.buyerId, JSON.stringify(tickets)],
    );
  }

  /**
   * Сохраняет одно уведомление каждого назначения: подтверждение, отмена, возврат.
   * Пара (order_id, kind) ограничивает повторный результат; устаревшая ревизия
   * отсекается advance до этого вызова. Внешней отправки писем здесь нет.
   */
  async createNotification(message: OrderEvent): Promise<void> {
    const order = message.order,
      kind = message.state.refundedAt
        ? 'refunded'
        : message.state.status === 'confirmed'
          ? 'confirmed'
          : 'cancelled';
    const text =
      kind === 'confirmed'
        ? `Покупка подтверждена: «${order.eventTitle}», билетов: ${order.quantity}.`
        : kind === 'refunded'
          ? `Возврат за «${order.eventTitle}» завершён.`
          : `Заказ на «${order.eventTitle}» отменён; билеты недействительны.`;
    await this.database.query(
      `INSERT INTO notifications(id,order_id,buyer_id,message,kind) VALUES($1,$2,$3,$4,$5) ON CONFLICT(order_id,kind) DO NOTHING`,
      [randomUUID(), order.id, order.buyerId, text, kind],
    );
  }

  async clearFailure(consumer: ConsumerName, orderId: string): Promise<void> {
    await this.database.query('DELETE FROM delivery_failures WHERE consumer = $1 AND order_id = $2', [
      consumer,
      orderId,
    ]);
  }

  /**
   * Обновляет последний известный окончательный сбой одной части заказа.
   * Ошибки билетов и уведомлений имеют разные ключи, поэтому один worker не затирает
   * состояние другого. Запись предназначена для статуса API; исходное сообщение
   * для восстановления остаётся в DLQ. Успешный use case удалит этот след ошибки.
   */
  async recordFailure(consumer: ConsumerName, message: OrderEvent, reason: string): Promise<void> {
    await this.database.query(
      `INSERT INTO delivery_failures (consumer, order_id, message_id, reason)
       VALUES ($1, $2, $3, $4)
       ON CONFLICT (consumer, order_id) DO UPDATE
       SET message_id = EXCLUDED.message_id, reason = EXCLUDED.reason, failed_at = now()`,
      [consumer, message.order.id, message.messageId, reason.slice(0, 500)],
    );
  }

  /**
   * Читает выдачу только для владельца и собирает постепенно обновляющееся состояние.
   *
   * @remarks
   * Sales заранее проверяет право покупателя на заказ и передаёт его идентификатор
   * по защищённому внутреннему HTTP API. Fulfillment дополнительно проверяет владельца
   * в своей записи и ограничивает выборки билетов и уведомлений тем же `buyerId`.
   * Если событие ещё не дошло, локальной записи нет: ответ `pending` означает ожидание,
   * а не подтверждение существования заказа в Sales.
   *
   * Три выборки выполняются параллельно и не образуют общий снимок транзакции.
   * Пока workers коммитят результаты, один ответ может ещё показывать `pending`;
   * следующий запрос увидит завершение. Данные другого покупателя такой порядок не открывает.
   *
   * @throws {@link AppError} со статусом 404, если известный локально заказ принадлежит другому покупателю.
   */
  async readOrder(orderId: string, buyerId: string): Promise<OrderDelivery> {
    const order = await this.database.query<{ buyer_id: string; quantity: number; state: string }>(
      'SELECT buyer_id, quantity,state FROM fulfillment_orders WHERE order_id = $1',
      [orderId],
    );
    if (!order.rows[0]) return { status: 'pending', tickets: [], notifications: [] };
    if (order.rows[0].buyer_id !== buyerId) throw new AppError('order_not_found', 'Заказ не найден', 404);
    const [tickets, notifications, failures] = await Promise.all([
      this.database.query<Ticket>(
        `SELECT id, order_id AS "orderId", event_id AS "eventId", buyer_id AS "buyerId", ordinal, code,valid,
                created_at AS "createdAt" FROM tickets WHERE order_id = $1 AND buyer_id = $2 ORDER BY ordinal`,
        [orderId, buyerId],
      ),
      this.database.query<Notification>(
        `SELECT id, order_id AS "orderId", buyer_id AS "buyerId", message,kind, created_at AS "createdAt"
         FROM notifications WHERE order_id = $1 AND buyer_id = $2 ORDER BY created_at, id`,
        [orderId, buyerId],
      ),
      this.database.query<{ consumer: ConsumerName }>(
        'SELECT consumer FROM delivery_failures WHERE order_id = $1',
        [orderId],
      ),
    ]);
    return {
      status: ['cancelled', 'expired'].includes(order.rows[0].state)
        ? 'cancelled'
        : deliveryStatus(
            order.rows[0].quantity,
            tickets.rows.length,
            notifications.rows.length,
            failures.rows.map((f) => f.consumer),
          ),
      tickets: tickets.rows.map((t) => ({
        ...t,
        valid: t.valid && !['cancelled', 'expired'].includes(order.rows[0].state),
      })),
      notifications: notifications.rows,
    };
  }

  async ping(): Promise<void> {
    await this.database.query('SELECT 1');
  }
}
