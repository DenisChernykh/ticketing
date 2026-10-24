import type { FulfillmentResult } from '../domain/delivery';
/**
 * Подтверждает право покупателя читать результат обработки конкретного заказа.
 *
 * @remarks
 * Успешное завершение означает, что заказ найден именно у buyerId. Отказ должен
 * прервать сценарий до обращения в Fulfillment. Порт не раскрывает устройство
 * Orders и не заставляет интеграционный сценарий загружать поля заказа.
 */
export interface OrderOwnership {
  assertOwnedBy(orderId: string, buyerId: string): Promise<void | { status: string }>;
}
/**
 * Читает данные других сервисов по доверенному идентификатору владельца.
 * Параметры buyerId/organizerId поступают из проверенного principal, а не из
 * произвольной строки запроса пользователя; получатель также ограничивает ими SQL.
 */
export interface ProductQueries {
  fulfillment(orderId: string, buyerId: string): Promise<FulfillmentResult>;
  report(kind: 'events' | 'daily' | 'popular', organizerId: string, limit: number): Promise<unknown[]>;
}

export interface ReportExports {
  csv(
    owner: string,
    filter: { from?: string; to?: string; eventId?: string },
    signal: AbortSignal,
  ): Promise<AsyncIterable<Uint8Array>>;
}
