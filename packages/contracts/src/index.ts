/**
 * Снимок подтверждённой покупки, достаточный получателям без доступа к базе Sales.
 *
 * @remarks
 * messageId назначается при записи outbox и сохраняется при каждой доставке
 * в RabbitMQ и Kafka. Получатели используют его для защиты от повторных эффектов.
 * type и version задают формат события; изменение контракта требует явной версии.
 * Цена, название, место и время мероприятия фиксируются на момент покупки:
 * последующее редактирование каталога не переписывает исторический заказ.
 * unitPrice и totalPrice — целые копейки, количество — число билетов без мест.
 */
export interface OrderConfirmed {
  messageId: string;
  type: 'order.confirmed';
  version: 1;
  occurredAt: string;
  order: {
    id: string;
    buyerId: string;
    organizerId: string;
    eventId: string;
    eventTitle: string;
    venue: string;
    startsAt: string;
    quantity: number;
    unitPrice: number;
    totalPrice: number;
    currency: 'RUB';
  };
}

const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
export function isUuid(value: unknown): value is string {
  return typeof value === 'string' && uuid.test(value);
}

/**
 * Проверяет недоверенные транспортные данные до записи в базу получателя.
 *
 * @remarks
 * Проверяются версия, идентификаторы, даты, положительные целые суммы и равенство
 * totalPrice = quantity × unitPrice. Возвращается новый объект только с известными
 * полями: лишние транспортные поля не попадают в хранимые бизнес-данные.
 * Проверка формы сообщения не заменяет дедупликацию в транзакции получателя
 * и не проверяет существование заказа в Sales.
 *
 * @param input - Объект после разбора JSON; TypeScript-тип не считается валидацией.
 * @returns Проверенный снимок order.confirmed версии 1.
 * @throws Error при неподдерживаемой версии или нарушении контракта.
 */
export function parseOrderConfirmed(input: unknown): OrderConfirmed {
  const fail = (): never => {
    throw new Error('Invalid order.confirmed v1 message');
  };
  if (!input || typeof input !== 'object' || Array.isArray(input)) return fail();
  const event = input as Record<string, unknown>;
  if (
    !isUuid(event.messageId) ||
    event.type !== 'order.confirmed' ||
    event.version !== 1 ||
    typeof event.occurredAt !== 'string' ||
    !Number.isFinite(Date.parse(event.occurredAt))
  )
    return fail();
  if (!event.order || typeof event.order !== 'object' || Array.isArray(event.order)) return fail();
  const order = event.order as Record<string, unknown>;
  for (const field of ['id', 'buyerId', 'organizerId', 'eventId']) if (!isUuid(order[field])) return fail();
  for (const field of ['eventTitle', 'venue']) {
    if (
      typeof order[field] !== 'string' ||
      !(order[field] as string).trim() ||
      (order[field] as string).length > 300
    )
      return fail();
  }
  if (
    typeof order.startsAt !== 'string' ||
    !Number.isFinite(Date.parse(order.startsAt)) ||
    order.currency !== 'RUB'
  )
    return fail();
  for (const field of ['quantity', 'unitPrice', 'totalPrice']) {
    if (!Number.isSafeInteger(order[field]) || (order[field] as number) < 1) return fail();
  }
  if (
    (order.quantity as number) > 100 ||
    (order.unitPrice as number) * (order.quantity as number) !== order.totalPrice
  )
    return fail();
  // Собираем объект явно: неизвестные поля транспорта не сохраняются вместе с заказом.
  return {
    messageId: event.messageId,
    type: 'order.confirmed',
    version: 1,
    occurredAt: event.occurredAt,
    order: {
      id: order.id as string,
      buyerId: order.buyerId as string,
      organizerId: order.organizerId as string,
      eventId: order.eventId as string,
      eventTitle: order.eventTitle as string,
      venue: order.venue as string,
      startsAt: order.startsAt,
      quantity: order.quantity as number,
      unitPrice: order.unitPrice as number,
      totalPrice: order.totalPrice as number,
      currency: 'RUB',
    },
  };
}

/**
 * Возвращает согласованные имена exchange и двух независимых очередей.
 * Один routing key связывает событие покупки с обоими обработчиками.
 * Проверяемый prefix позволяет изолировать проверочные запуски от данных приложения.
 */
export function rabbitNames(prefix: string) {
  if (!/^[a-z][a-z0-9_-]{0,48}$/.test(prefix)) throw new Error('Invalid resource prefix');
  return {
    exchange: `${prefix}.orders`,
    routingKey: 'order.confirmed.v1',
    tickets: `${prefix}.tickets`,
    notifications: `${prefix}.notifications`,
  };
}

export type OrderStatus = 'reserved' | 'confirmed' | 'cancelled' | 'expired';
export interface OrderState {
  status: OrderStatus;
  createdAt: string;
  expiresAt: string | null;
  confirmedAt: string | null;
  cancelledAt: string | null;
  refundedAt: string | null;
}
export interface OrderEvent {
  messageId: string;
  type: 'order.confirmed' | 'order.cancelled' | 'order.refunded';
  version: 1 | 2;
  occurredAt: string;
  orderRevision: number;
  order: OrderConfirmed['order'];
  state: OrderState;
}
/** Full state makes later revisions independently applicable after retries and replay. */
export function parseOrderEvent(input: unknown): OrderEvent {
  const fail = (): never => {
    throw new Error('Invalid order lifecycle event');
  };
  if (!input || typeof input !== 'object') return fail();
  const raw = input as Record<string, unknown>;
  if (raw.version === 1) {
    const old = parseOrderConfirmed(raw);
    return {
      ...old,
      orderRevision: 1,
      state: {
        status: 'confirmed',
        createdAt: old.occurredAt,
        expiresAt: null,
        confirmedAt: old.occurredAt,
        cancelledAt: null,
        refundedAt: null,
      },
    };
  }
  if (
    raw.version !== 2 ||
    !['order.confirmed', 'order.cancelled', 'order.refunded'].includes(String(raw.type)) ||
    !Number.isSafeInteger(raw.orderRevision) ||
    Number(raw.orderRevision) < 1
  )
    return fail();
  const base = parseOrderConfirmed({ ...raw, type: 'order.confirmed', version: 1 });
  const st = raw.state as Record<string, unknown>;
  if (!st || !['reserved', 'confirmed', 'cancelled', 'expired'].includes(String(st.status))) return fail();
  const date = (v: unknown, nullable = true): string | null => {
    if (v === null && nullable) return null;
    if (typeof v !== 'string' || !Number.isFinite(Date.parse(v))) return fail();
    return new Date(v).toISOString();
  };
  const state: OrderState = {
    status: st.status as OrderStatus,
    createdAt: date(st.createdAt, false)!,
    expiresAt: date(st.expiresAt),
    confirmedAt: date(st.confirmedAt),
    cancelledAt: date(st.cancelledAt),
    refundedAt: date(st.refundedAt),
  };
  if (
    raw.type === 'order.confirmed' &&
    (state.status !== 'confirmed' || !state.confirmedAt || state.cancelledAt || state.refundedAt)
  )
    return fail();
  if (
    raw.type === 'order.cancelled' &&
    (!['cancelled', 'expired'].includes(state.status) || !state.cancelledAt || state.refundedAt)
  )
    return fail();
  if (
    raw.type === 'order.refunded' &&
    (!['cancelled', 'expired'].includes(state.status) || !state.cancelledAt || !state.refundedAt)
  )
    return fail();
  if (state.confirmedAt && Date.parse(state.confirmedAt) < Date.parse(state.createdAt)) return fail();
  return {
    ...base,
    type: raw.type as OrderEvent['type'],
    version: 2,
    orderRevision: Number(raw.orderRevision),
    state,
  };
}
export interface PaymentSnapshot {
  id: string;
  orderId: string;
  amount: number;
  currency: 'RUB';
  status: 'pending' | 'succeeded' | 'failed' | 'refunded';
  version: number;
  capturedAt: string | null;
  refundedAt: string | null;
}
export function parsePayment(input: unknown): PaymentSnapshot {
  const p = input as PaymentSnapshot;
  if (
    !p ||
    !isUuid(p.id) ||
    !isUuid(p.orderId) ||
    !Number.isSafeInteger(p.amount) ||
    p.amount < 1 ||
    p.currency !== 'RUB' ||
    !Number.isSafeInteger(p.version) ||
    p.version < 1 ||
    !['pending', 'succeeded', 'failed', 'refunded'].includes(p.status)
  )
    throw new Error('Invalid payment');
  for (const d of [p.capturedAt, p.refundedAt])
    if (d !== null && (typeof d !== 'string' || !Number.isFinite(Date.parse(d))))
      throw new Error('Invalid payment timestamp');
  if (
    ((p.status === 'succeeded' || p.status === 'refunded') && !p.capturedAt) ||
    (p.status === 'refunded' && !p.refundedAt)
  )
    throw new Error('Incomplete payment');
  return {
    id: p.id,
    orderId: p.orderId,
    amount: p.amount,
    currency: 'RUB',
    status: p.status,
    version: p.version,
    capturedAt: p.capturedAt,
    refundedAt: p.refundedAt,
  };
}
export interface PageInput {
  limit: number;
  cursor?: string;
}
export interface Page<T> {
  items: T[];
  nextCursor: string | null;
}
