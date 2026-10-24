import { AppError } from '@ticketing/core';
import { DeliveryRepository, WorkerHealth } from './ports';

/**
 * Отдаёт уже сохранённые результаты и состояние сервиса, не запуская их обработку.
 * Это позволяет читать ранее выпущенные билеты даже при недоступном RabbitMQ.
 */
export class ReadDelivery {
  constructor(private readonly repository: DeliveryRepository, private readonly workers: WorkerHealth) {}

  order(orderId: string, buyerId: string) { return this.repository.readOrder(orderId, buyerId); }
  /**
   * Проверяет способность обслужить запрос к своим данным.
   *
   * Готовность зависит от PostgreSQL. Состояние workers возвращается отдельным полем:
   * отключённый брокер задерживает новые билеты, но не делает сохранённые недоступными.
   * @throws {@link AppError} со статусом 503, если проверка БД не проходит.
   */
  async ready() {
    try { await this.repository.ping(); }
    catch { throw new AppError('database_unavailable', 'База Fulfillment недоступна', 503); }
    return { status: 'ready', database: 'up', workers: this.workers.states() };
  }
  workerStates() { return this.workers.states(); }
}
