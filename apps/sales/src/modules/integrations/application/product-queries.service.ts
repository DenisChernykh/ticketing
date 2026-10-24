import type { OrderOwnership, ProductQueries, ReportExports } from './ports';
/**
 * Читает результаты других сервисов от имени проверенного пользователя Sales.
 *
 * @remarks
 * Проверка роли принадлежит HTTP-границе. Владение конкретным заказом остаётся
 * правилом сценария: любой его вызывающий обязан пройти эту проверку до чтения
 * Fulfillment, даже если обращается к use case без HTTP-контроллера.
 */
export class ProductQueriesService {
  constructor(
    private readonly queries: ProductQueries,
    private readonly ownership: OrderOwnership,
    private readonly exports?: ReportExports,
  ) {}
  csv(owner: string, filter: { from?: string; to?: string; eventId?: string }, signal: AbortSignal) {
    if (!this.exports) throw new Error('Missing export adapter');
    return this.exports.csv(owner, filter, signal);
  }
  /**
   * Проверяет принадлежность заказа и только затем читает его билеты и уведомление.
   *
   * Отказ проверки не вызывает HTTP-порт. Один и тот же buyerId передаётся обеим
   * операциям: локальная проверка и выборка получателя ограничены одним владельцем.
   * Ошибки владения и чтения не заменяются статусом pending и доходят до HTTP-слоя.
   */
  async fulfillment(orderId: string, buyerId: string) {
    const order = await this.ownership.assertOwnedBy(orderId, buyerId);
    const delivery = await this.queries.fulfillment(orderId, buyerId);
    if (order && ['cancelled', 'expired'].includes(order.status))
      return {
        ...delivery,
        status: 'cancelled' as const,
        tickets: delivery.tickets.map((t) => ({ ...(t as Record<string, unknown>), valid: false })),
      };
    return delivery;
  }
  /** Читает выбранный отчёт по id организатора из проверенного principal. */
  report(kind: 'events' | 'daily' | 'popular', organizerId: string, limit = 10) {
    return this.queries.report(kind, organizerId, limit);
  }
}
