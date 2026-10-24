import { carrier } from '@ticketing/observability';
import { AppError } from '@ticketing/core';
import type { ProductQueries, ReportExports } from '../application/ports';
import type { FulfillmentResult } from '../domain/delivery';

/**
 * Адаптер чтения Fulfillment и Analytics без доступа к их таблицам.
 *
 * @remarks
 * Служебный токен подтверждает отправителя внутреннего запроса, но сам по себе
 * не задаёт владельца данных. buyerId/organizerId передаются отдельно после
 * авторизации в Sales; принимающий сервис применяет их в SQL. Эти внутренние
 * адреса и общий секрет предназначены для доверенного локального окружения.
 */
export class HttpProductQueries implements ProductQueries, ReportExports {
  constructor(
    private readonly fulfillmentUrl: string,
    private readonly analyticsUrl: string,
    private readonly token: string,
  ) {}
  async csv(
    owner: string,
    filter: { from?: string; to?: string; eventId?: string },
    signal: AbortSignal,
  ): Promise<AsyncIterable<Uint8Array>> {
    const url = new URL('/internal/reports/orders.csv', this.analyticsUrl);
    url.searchParams.set('organizerId', owner);
    for (const [key, value] of Object.entries(filter)) if (value) url.searchParams.set(key, value);
    const result = await fetch(url, {
      redirect: 'error',
      signal,
      headers: { 'X-Internal-Token': this.token, ...carrier() },
    });
    if (!result.ok || !result.body) {
      await result.body?.cancel();
      throw new AppError('EXPORT_UNAVAILABLE', 'Выгрузка недоступна', 503);
    }
    const reader = result.body.getReader();
    return (async function* () {
      try {
        while (true) {
          const next = await reader.read();
          if (next.done) break;
          yield next.value;
        }
      } finally {
        await reader.cancel().catch(() => {});
        reader.releaseLock();
      }
    })();
  }
  /**
   * Выполняет ограниченный по времени внутренний GET со служебной аутентификацией.
   *
   * @remarks
   * Таймаут 2500 мс ограничивает ожидание ответа и чтение JSON. Перенаправления
   * запрещены: служебный запрос не должен незаметно уйти на другой адрес.
   * Сетевые сбои, неуспешный HTTP-статус и невалидный JSON преобразуются в 503;
   * клиент не получает внутренний URL или чужую диагностическую ошибку.
   * Автоматического повтора GET здесь нет — повтор может сделать пользователь.
   */
  private async get(base: string, path: string, query: Record<string, string>): Promise<unknown> {
    const url = new URL(path, base);
    for (const [key, value] of Object.entries(query)) url.searchParams.set(key, value);
    try {
      const response = await fetch(url, {
        headers: { 'X-Internal-Token': this.token, ...carrier() },
        signal: AbortSignal.timeout(2500),
        redirect: 'error',
      });
      if (!response.ok) throw new Error('INTERNAL_SERVICE_RESPONSE');
      return await response.json();
    } catch {
      throw new AppError(
        'DEPENDENCY_UNAVAILABLE',
        'Сервис обработки билетов или отчётов временно недоступен',
        503,
      );
    }
  }
  /**
   * Сохраняет различие между асинхронной обработкой и недоступностью сервиса.
   * pending/completed/failed — содержательные состояния от Fulfillment; сетевой
   * сбой не превращается в pending, а возвращается клиенту как 503. Проверка
   * внешней формы ответа защищает HTTP-контракт Sales от повреждённого ответа.
   */
  async fulfillment(orderId: string, buyerId: string): Promise<FulfillmentResult> {
    const value = (await this.get(this.fulfillmentUrl, `/internal/orders/${encodeURIComponent(orderId)}`, {
      buyerId,
    })) as FulfillmentResult;
    if (
      !value ||
      !['pending', 'completed', 'failed', 'cancelled'].includes(value.status) ||
      !Array.isArray(value.tickets) ||
      !Array.isArray(value.notifications)
    )
      throw new AppError('DEPENDENCY_UNAVAILABLE', 'Неверный ответ сервиса билетов', 503);
    return value;
  }
  async report(kind: 'events' | 'daily' | 'popular', organizerId: string, limit: number): Promise<unknown[]> {
    const value = await this.get(this.analyticsUrl, `/internal/reports/${kind}`, {
      organizerId,
      limit: String(limit),
    });
    if (!Array.isArray(value))
      throw new AppError('DEPENDENCY_UNAVAILABLE', 'Неверный ответ сервиса отчётов', 503);
    return value;
  }
}
