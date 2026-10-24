import { AppError } from '@ticketing/core';
import type { Projection, Sale } from '../domain/reports';
import type { PopularRanking, ProjectionRuntime, ReportsRepository, OrderExports } from './ports';

/**
 * Прикладные операции чтения отчётов и запуска их перестроения.
 *
 * @remarks
 * Сервис проверяет форму параметров, а не пользовательскую сессию. Sales
 * проверяет роль организатора и передаёт его подтверждённый идентификатор;
 * внутренний HTTP-вход Analytics дополнительно защищён InternalGuard.
 * SQL, Redis и управление Kafka скрыты за прикладными портами.
 */
export class ReportsService {
  constructor(
    private readonly repository: ReportsRepository,
    private readonly ranking: PopularRanking,
    private readonly runtime: ProjectionRuntime,
    private readonly exports?: OrderExports,
  ) {}

  exportRows(owner: unknown, filter: { from?: string; to?: string; eventId?: string }, signal: AbortSignal) {
    if (!this.exports) throw new AppError('EXPORT_UNAVAILABLE', 'Выгрузка недоступна', 503);
    return this.exports.rows(this.organizer(owner), filter, signal);
  }
  /** Читает продажи мероприятий активного поколения после проверки UUID. */
  events(organizerId: unknown) {
    return this.repository.events(this.organizer(organizerId));
  }
  /** Читает дневные суммы; выбранный organizerId ограничивает всё SQL-окно. */
  daily(organizerId: unknown) {
    return this.repository.daily(this.organizer(organizerId));
  }

  /**
   * Возвращает ограниченную верхнюю часть рейтинга организатора.
   * По умолчанию выдаются десять строк; допустимый предел — от 1 до 100.
   * Решение о чтении Redis или возврате SQL-снимка принимает адаптер рейтинга.
   */
  popular(organizerId: unknown, rawLimit: unknown) {
    const limit = rawLimit === undefined ? 10 : Number(rawLimit);
    if (!Number.isInteger(limit) || limit < 1 || limit > 100) {
      throw new AppError('INVALID_LIMIT', 'limit должен быть целым числом от 1 до 100', 400);
    }
    return this.ranking.popular(this.organizer(organizerId), limit);
  }

  /**
   * Проверяет имя проекции и передаёт запрос владельцу работающих consumers.
   * Сам use case не запускает второй процесс и не меняет offsets вручную;
   * сериализация повторных запросов и переключение поколений принадлежат runtime.
   */
  replay(selection: unknown) {
    if (selection !== 'events' && selection !== 'daily' && selection !== 'all') {
      throw new AppError('INVALID_PROJECTION', 'projection: events, daily или all', 400);
    }
    return this.runtime.replay(selection);
  }

  /**
   * Возвращает готовность обеих фоновых проекций и их отдельные состояния.
   * ready=false здесь не означает, что уже построенные SQL-отчёты недоступны.
   */
  workers() {
    const workers = this.runtime.statuses();
    return { ready: workers.every((worker) => worker.ready), workers };
  }

  /**
   * Проверяет возможность обслуживать чтения из собственной PostgreSQL.
   * Kafka может восстанавливаться, а Redis — работать через SQL fallback;
   * эти состояния не закрывают HTTP-чтение сохранённых отчётов.
   */
  async ready() {
    try {
      await this.repository.health();
    } catch {
      throw new AppError('ANALYTICS_DATABASE_UNAVAILABLE', 'База analytics недоступна', 503);
    }
    return { status: 'ready', dependency: 'analytics-postgresql' };
  }

  /** Проверяет синтаксис UUID; принадлежность аккаунта проверена до этого слоя. */
  private organizer(value: unknown): string {
    if (
      typeof value !== 'string' ||
      !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(value)
    ) {
      throw new AppError('INVALID_ORGANIZER', 'organizerId должен быть UUID', 400);
    }
    return value;
  }
}

/**
 * Прикладной вход для учёта подтверждённой продажи одним consumer.
 * Конкретная SQL-транзакция и проверка повторов принадлежат адаптеру репозитория.
 */
export class RecordSale {
  constructor(private readonly repository: ReportsRepository) {}

  /**
   * Дожидается долговременного результата записи до возврата в Kafka-адаптер.
   * projection различает два независимых отчёта, generation — обычную обработку
   * и конкретное перестроение; fingerprint выявляет подмену содержимого повтора.
   */
  execute(projection: Projection, generation: string, sale: Sale, fingerprint: string) {
    return this.repository.apply(projection, generation, sale, fingerprint);
  }
}
