import { AppError, Database, RedisConnection } from '@ticketing/core';
import type { ReadinessPort } from '../application/health.service';
/**
 * Проверяет готовность Sales обслуживать защищённые операции: нужны PostgreSQL и Redis.
 *
 * @remarks
 * Ответ 503 readiness при отказе Redis не означает, что весь API перестал
 * отвечать: `/health/live` и публичный каталог могут работать. RabbitMQ и Kafka
 * сюда не входят, потому что покупка коммитит outbox без ожидания брокеров.
 */
export class Readiness implements ReadinessPort {
  constructor(private readonly db: Database, private readonly redis: RedisConnection) {}
  async check() {
    try { await Promise.all([this.db.query('SELECT 1'), this.redis.client.ping()]); }
    catch { throw new AppError('DEPENDENCY_UNAVAILABLE', 'PostgreSQL или Redis недоступен', 503); }
  }
}
