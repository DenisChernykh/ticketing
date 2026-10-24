import type {
  DailyReport,
  EventReport,
  PopularReport,
  Projection,
  ReplaySelection,
  Sale,
  WorkerStatus,
} from '../domain/reports';

/**
 * Хранит результаты обработки продаж и читает активные проекции Analytics.
 *
 * @remarks
 * При записи поколение задаёт вызывающий consumer: во время replay это ещё
 * невидимое читателям поколение. При чтении адаптер сам выбирает активное
 * поколение из БД. Все данные принадлежат Analytics; доступ к БД Sales не нужен.
 */
export interface ReportsRepository {
  /** Возвращает продажи и места в рейтинге только указанного организатора. */
  events(organizerId: string): Promise<EventReport[]>;
  /** Возвращает дневные суммы и накопленную выручку организатора в днях UTC. */
  daily(organizerId: string): Promise<DailyReport[]>;
  /**
   * Атомарно сохраняет receipt и вклад продажи в выбранную проекцию.
   *
   * Повтор с теми же идентификаторами и fingerprint не меняет агрегаты.
   * Область дедупликации включает projection и generation: другая проекция
   * и новое поколение replay должны обработать эту продажу независимо.
   * Успешное завершение означает завершённый SQL-коммит, после которого
   * транспортный адаптер может подтверждать Kafka offset.
   */
  apply(projection: Projection, generation: string, sale: Sale, fingerprint: string): Promise<void>;
  /** Проверяет доступность собственного хранилища, не брокера и не Redis. */
  health(): Promise<void>;
}

/**
 * Читает рейтинг по числу проданных билетов из активной событийной проекции.
 * Недоступный или неполный Redis не должен менять результат, доступный в SQL.
 */
export interface PopularRanking {
  /** Возвращает не более limit мероприятий одного организатора. */
  popular(organizerId: string, limit: number): Promise<PopularReport[]>;
}

/** Управляет фоновыми проекциями внутри единственного процесса Analytics. */
export interface ProjectionRuntime {
  /**
   * Перестраивает выбранные проекции в новых поколениях и возвращает их UUID.
   * Для all переключения независимы: успех одной проекции не откатывается
   * из-за ошибки другой. Старые данные остаются доступны до переключения.
   */
  replay(selection: ReplaySelection): Promise<{ projection: Projection; generation: string }[]>;
  /** Отдаёт состояние consumers отдельно от готовности HTTP-чтения из БД. */
  statuses(): WorkerStatus[];
}

export interface OrderExports {
  rows(
    owner: string,
    filter: { from?: string; to?: string; eventId?: string },
    signal: AbortSignal,
  ): AsyncIterable<Record<string, unknown>>;
}
