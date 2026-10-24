/** Два независимых представления одних продаж, каждое со своей Kafka-группой. */
export type Projection = 'events' | 'daily';
export type ReplaySelection = Projection | 'all';

/** Данные подтверждённой продажи, необходимые собственным агрегатам Analytics. */
export type Sale = import('@ticketing/contracts').OrderEvent;

export interface EventReport {
  eventId: string;
  title: string;
  tickets: number;
  revenue: number;
  grossRevenue: number;
  refunds: number;
  rank: number;
}

export interface DailyReport {
  day: string;
  orders: number;
  tickets: number;
  revenue: number;
  grossRevenue: number;
  refunds: number;
  cumulativeRevenue: number;
}

export type PopularReport = Pick<EventReport, 'eventId' | 'title' | 'tickets' | 'rank'>;
/**
 * Согласованный SQL-снимок рейтинга: поколение, его revision и строки получены
 * одним запросом. revision меняется при каждой новой продаже всего поколения,
 * поэтому может измениться и из-за продажи другого организатора.
 */
export interface RankingSnapshot {
  generation: string;
  revision: string;
  items: PopularReport[];
}

/** Наблюдаемое состояние consumer, не проверка доступности HTTP read API. */
export interface WorkerStatus {
  projection: Projection;
  generation: string | null;
  state: 'starting' | 'running' | 'replaying' | 'error' | 'stopped';
  ready: boolean;
  error: string | null;
}

/**
 * Неисправимое автоматическим повтором нарушение контракта события.
 * Runtime оставляет offset неподтверждённым и блокирует consumer для разбора
 * причины, вместо пропуска записи и построения неполного отчёта.
 */
export class InvalidSaleEvent extends Error {}
