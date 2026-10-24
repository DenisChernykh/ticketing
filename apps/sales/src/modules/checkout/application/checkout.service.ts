import { createHash, randomUUID } from 'node:crypto';
import { AppError, type TransactionPort } from '@ticketing/core';
import type { OrderEvent, PaymentSnapshot } from '@ticketing/contracts';
import type { OrdersRepository, Order, Purchase } from '../../orders';
import type { Inventory } from '../../catalog';
import type { Payments } from '../../payments';
import type { EventWriter } from '../../messaging';
/**
 * Координирует жизненный цикл покупки через публичные порты модулей Sales.
 * Остаток, состояние заказа, платёжное задание и outbox используют одну локальную
 * транзакцию. Внешний платёж и публикация брокерам выполняются другими процессами.
 */
export class CheckoutService {
  constructor(
    private readonly tx: TransactionPort,
    private readonly orders: OrdersRepository,
    private readonly inventory: Inventory,
    private readonly payments: Payments,
    private readonly events: EventWriter,
    private readonly holdSeconds = 600,
  ) {}
  /** Бронирует места и фиксирует исходный идемпотентный ответ. Повтор не продлевает срок брони. */
  async purchase(buyerId: string, key: string, input: Purchase): Promise<Order> {
    return this.tx.run(async () => {
      const hash = createHash('sha256')
        .update(JSON.stringify({ eventId: input.eventId.toLowerCase(), quantity: input.quantity }))
        .digest('hex');
      const claim = await this.orders.claimKey(buyerId, key, hash);
      if (claim.hash !== hash)
        throw new AppError('IDEMPOTENCY_CONFLICT', 'Ключ использован с другим запросом', 409);
      if (claim.result) return claim.result;
      const now = await this.orders.now(),
        event = await this.inventory.hold(input.eventId, input.quantity, now);
      const order: Order = {
        id: randomUUID(),
        buyerId,
        organizerId: event.organizerId,
        eventId: event.id,
        eventTitle: event.title,
        venue: event.venue,
        startsAt: event.startsAt,
        quantity: input.quantity,
        unitPrice: event.unitPrice,
        totalPrice: input.quantity * event.unitPrice,
        currency: 'RUB',
        status: 'reserved',
        createdAt: now,
        expiresAt: new Date(
          Math.min(Date.parse(now) + this.holdSeconds * 1000, Date.parse(event.startsAt)),
        ).toISOString(),
        confirmedAt: null,
        cancelledAt: null,
        refundedAt: null,
        revision: 1,
      };
      await this.orders.insert(order);
      await this.payments.ensure(order.id, order.totalPrice);
      await this.orders.completeKey(buyerId, key, order);
      return order;
    });
  }
  private async owned(id: string, buyerId: string) {
    const o = await this.orders.lock(id);
    if (!o || o.buyerId !== buyerId) throw new AppError('ORDER_NOT_FOUND', 'Заказ не найден', 404);
    return o;
  }
  /**
   * Сохраняет одну логическую операцию оплаты. При просрочке сначала коммитит
   * освобождение мест, затем возвращает 409: ошибка HTTP не должна откатить expiry.
   */
  async pay(id: string, buyerId: string) {
    const result = await this.tx.run(async () => {
      const o = await this.owned(id, buyerId),
        now = await this.orders.now();
      if (o.status === 'reserved' && Date.parse(o.expiresAt!) <= Date.parse(now)) {
        await this.end(o, 'expired', now);
        return null;
      }
      const p = await this.payments.lock(id);
      if (o.status === 'confirmed') return p;
      if (o.status !== 'reserved')
        throw new AppError('ORDER_NOT_PAYABLE', 'Заказ недоступен для оплаты', 409);
      await this.payments.charge(p!);
      return { ...p!, status: p!.status === 'not_started' ? 'pending' : p!.status };
    });
    if (!result) throw new AppError('RESERVATION_EXPIRED', 'Срок брони истёк', 409);
    return result;
  }
  /** Отменяет заказ владельца до начала мероприятия. Возврат имеет собственный асинхронный статус. */
  async cancel(id: string, buyerId: string) {
    return this.tx.run(async () => {
      const o = await this.owned(id, buyerId);
      if (o.status === 'cancelled' || o.status === 'expired') return o;
      const now = await this.orders.now();
      if (Date.parse(o.startsAt) <= Date.parse(now))
        throw new AppError('EVENT_STARTED', 'Отмена после начала мероприятия недоступна', 409);
      await this.end(o, 'cancelled', now);
      const p = await this.payments.lock(id);
      if (p?.status === 'succeeded') await this.payments.refund(p);
      return o;
    });
  }
  private async end(o: Order, status: 'cancelled' | 'expired', now: string) {
    o.status = status;
    o.cancelledAt = now;
    o.revision++;
    await this.inventory.release(o.eventId, o.quantity);
    await this.orders.save(o);
    await this.emit(o, 'order.cancelled', now);
  }
  /**
   * Обрабатывает ограниченную порцию просроченных броней по одной транзакции на заказ.
   * Список кандидатов может устареть: состояние и время проверяются повторно под
   * блокировкой. Два worker поэтому не освобождают одну бронь дважды.
   */
  async expire() {
    let count = 0;
    for (const id of await this.orders.due())
      await this.tx.run(async () => {
        const o = await this.orders.lock(id),
          now = await this.orders.now();
        if (o?.status === 'reserved' && Date.parse(o.expiresAt!) <= Date.parse(now)) {
          await this.end(o, 'expired', now);
          count++;
        }
      });
    return count;
  }
  /**
   * Применяет подтверждённое чтением провайдера состояние платежа.
   * Позднее списание компенсируется возвратом, сохраняя отменённый/просроченный заказ.
   * Версия провайдера отсекает старые результаты параллельной сверки; версия заказа
   * увеличивается только при новом бизнес-переходе и сохраняется вместе с событием.
   */
  async settle(id: string, remote: PaymentSnapshot) {
    await this.tx.run(async () => {
      const o = await this.orders.lock(id);
      if (!o) throw new AppError('ORDER_NOT_FOUND', 'Заказ не найден', 404);
      const now = await this.orders.now();
      // Lock inventory before payment if this transition releases seats.
      if (o.status === 'reserved' && Date.parse(o.expiresAt!) <= Date.parse(now))
        await this.end(o, 'expired', now);
      await this.inventory.lock(o.eventId);
      const p = await this.payments.lock(id);
      if (!p || p.id !== remote.id || remote.orderId !== id || p.amount !== remote.amount)
        throw new AppError('PAYMENT_MISMATCH', 'Неверный платёж', 409);
      if (remote.version < p.providerVersion) return;
      if (remote.status === 'failed' && o.status === 'reserved') {
        await this.end(o, 'cancelled', now);
      }
      await this.payments.record(p, remote);
      if (remote.status === 'succeeded') {
        if (o.status === 'reserved') {
          o.status = 'confirmed';
          o.confirmedAt = now;
          o.revision++;
          await this.orders.save(o);
          await this.emit(o, 'order.confirmed', now);
        } else if (o.status === 'cancelled' || o.status === 'expired') await this.payments.refund(p);
      } else if (remote.status === 'refunded' && !o.refundedAt) {
        if (o.status === 'reserved' || o.status === 'confirmed') await this.end(o, 'cancelled', now);
        o.refundedAt = remote.refundedAt;
        o.revision++;
        await this.orders.save(o);
        await this.emit(o, 'order.refunded', remote.refundedAt!);
      }
    });
  }
  private async emit(o: Order, type: OrderEvent['type'], occurredAt: string) {
    const {
      status,
      createdAt,
      expiresAt,
      confirmedAt,
      cancelledAt,
      refundedAt,
      revision,
      payment,
      ...snapshot
    } = o;
    await this.events.append({
      messageId: randomUUID(),
      type,
      version: 2,
      occurredAt,
      orderRevision: revision,
      order: snapshot,
      state: { status, createdAt, expiresAt, confirmedAt, cancelledAt, refundedAt },
    });
  }
}
