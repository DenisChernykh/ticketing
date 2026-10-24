export type ConsumerName = 'tickets' | 'notifications';

export interface Ticket {
  id: string;
  orderId: string;
  eventId: string;
  buyerId: string;
  ordinal: number;
  code: string;
  valid: boolean;
  createdAt: string;
}

export interface Notification {
  id: string;
  orderId: string;
  buyerId: string;
  message: string;
  createdAt: string;
}

export interface OrderDelivery {
  status: 'pending' | 'completed' | 'failed' | 'cancelled';
  tickets: Ticket[];
  notifications: Notification[];
}

/**
 * Существующий идентификатор сообщения или заказа пришёл с другим содержимым.
 * Повтор с задержкой такой конфликт не исправит: worker отправляет его сразу в DLQ,
 * сохраняя ранее принятые данные и статус правильного заказа.
 */
export class MessageConflict extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'MessageConflict';
  }
}

/**
 * Собирает пользовательский статус из двух независимо сохраняемых результатов.
 *
 * @remarks
 * Заказ завершён, когда выпущено ожидаемое число билетов и есть одно уведомление.
 * Пока выполнена только одна часть без окончательной ошибки другой, статус `pending`.
 * Ошибка учитывается только для незавершённой части: старый след сбоя не затмевает готовый
 * результат после восстановления. Здесь нет состояния брокера: его недоступность
 * не означает, что конкретный заказ окончательно обработан с ошибкой.
 */
export function deliveryStatus(
  expectedTickets: number,
  ticketCount: number,
  notificationCount: number,
  failedConsumers: ConsumerName[],
): OrderDelivery['status'] {
  const ticketsComplete = ticketCount === expectedTickets;
  const notificationsComplete = notificationCount === 1;
  if (ticketsComplete && notificationsComplete) return 'completed';
  if (
    (!ticketsComplete && failedConsumers.includes('tickets')) ||
    (!notificationsComplete && failedConsumers.includes('notifications'))
  )
    return 'failed';
  return 'pending';
}
