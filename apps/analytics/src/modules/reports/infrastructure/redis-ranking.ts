import { RedisConnection, log } from '@ticketing/core';
import type { PopularRanking } from '../application/ports';
import type { PopularReport, RankingSnapshot } from '../domain/reports';
import { SqlReportsRepository } from './sql-reports.repository';

/**
 * Атомарно заменяет все абсолютные значения рейтинга и его revision.
 *
 * @remarks
 * KEYS[1] — sorted set, KEYS[2] — revision; ARGV содержит новую revision и пары
 * «число билетов, eventId». Если Redis хранит более новую revision, запись
 * отклоняется. DEL убирает прежний состав, ZADD записывает полные SQL-счётчики,
 * а не приращения: повторный ремонт не удваивает продажи. Lua не позволяет
 * читателю увидеть промежуточный набор между DEL и последним ZADD.
 * Пустому рейтингу достаточно ключа revision; оба вида ключей имеют TTL.
 */
const REPLACE_RANKING = `
local old = redis.call('GET', KEYS[2])
if old and tonumber(old) > tonumber(ARGV[1]) then return 0 end
redis.call('DEL', KEYS[1])
for i = 2, #ARGV, 2 do redis.call('ZADD', KEYS[1], ARGV[i], ARGV[i+1]) end
redis.call('SET', KEYS[2], ARGV[1], 'EX', 120)
if #ARGV > 1 then redis.call('EXPIRE', KEYS[1], 120) end
return 1
`;

/**
 * Проверяет ожидаемую revision и читает верхние места одной атомарной операцией.
 * Несовпадение версии возвращает отказ, после которого вызывающий метод берёт
 * SQL-снимок. Даже совпадение версии ещё требует проверки состава и счётчиков:
 * sorted set мог быть частично потерян отдельно от ключа revision.
 */
const READ_RANKING = `
if redis.call('GET', KEYS[2]) ~= ARGV[1] then return false end
return redis.call('ZRANGE', KEYS[1], 0, tonumber(ARGV[2])-1, 'REV', 'WITHSCORES')
`;

/**
 * Поддерживает восстанавливаемый рейтинг Redis на основе SQL-проекции events.
 *
 * @remarks
 * У каждого организатора и generation своя пара ключей. Поэтому запоздавшее
 * обновление старого поколения не может заменить рейтинг после replay.
 * Внутри одного поколения Lua сравнивает revision и меняет весь набор атомарно.
 * SQL-снимок читается при каждом публичном запросе: он задаёт версию, проверяет
 * полноту результата Redis и остаётся готовым ответом при отказе кеша.
 */
export class RedisRanking implements PopularRanking {
  private refreshing = false;
  private degraded = false;

  constructor(
    private readonly repository: SqlReportsRepository,
    private readonly redis: RedisConnection,
    private readonly prefix: string,
  ) {}

  /**
   * Возвращает верхние места, согласованные с одним SQL-снимком организатора.
   *
   * @remarks
   * Сначала восстанавливается полный sorted set по абсолютным SQL-счётчикам.
   * Затем Lua проверяет revision и читает ZRANGE REV WITHSCORES. Метод сверяет
   * размер, порядок, eventId и число билетов с ожидаемой частью SQL-снимка:
   * одного существующего ключа или одной revision недостаточно.
   *
   * При истечении TTL, частичной потере ключей, конкурирующем обновлении,
   * недоступности Redis или превышении его срока ожидания возвращается SQL.
   * Ошибка получения самого SQL-снимка не маскируется устаревшим рейтингом.
   */
  async popular(organizerId: string, limit: number): Promise<PopularReport[]> {
    const snapshot = await this.repository.rankingSnapshot(organizerId);
    try {
      await this.replace(organizerId, snapshot);
      const keys = this.keys(organizerId, snapshot.generation);
      const result = await this.command(['EVAL', READ_RANKING, '2', ...keys, snapshot.revision, String(limit)]);
      if (Array.isArray(result)) {
        const expected = snapshot.items.slice(0, limit);
        // Совпадение revision не доказывает полноту отдельного sorted set.
        const valid = result.length === expected.length * 2 && expected.every((item, index) =>
          String(result[index * 2]) === item.eventId && Number(result[index * 2 + 1]) === item.tickets);
        if (valid) {
          this.degraded = false;
          return expected.map((item, index) => ({ ...item, rank: index + 1 }));
        }
      }
    } catch (error) { this.reportFailure(error); }
    return snapshot.items.slice(0, limit);
  }

  /**
   * Периодически ремонтирует рейтинги всех организаторов активного поколения.
   *
   * Флаг refreshing исключает второй одновременный полный обход в этом процессе.
   * Публичные чтения могут обновлять те же ключи параллельно: их согласование
   * выполняется Lua-проверкой revision. Ошибка ремонта не закрывает SQL-отчёты;
   * следующий обход или публичное чтение повторят восстановление.
   */
  async refreshAll(): Promise<void> {
    if (this.refreshing) return;
    this.refreshing = true;
    try {
      for (const organizerId of await this.repository.organizers()) {
        await this.replace(organizerId, await this.repository.rankingSnapshot(organizerId));
      }
      this.degraded = false;
    } catch (error) { this.reportFailure(error); }
    finally { this.refreshing = false; }
  }

  /** Передаёт полный SQL-снимок атомарной замене, не используя ZINCRBY. */
  private async replace(organizerId: string, snapshot: RankingSnapshot): Promise<void> {
    const members = snapshot.items.flatMap((item) => [String(item.tickets), item.eventId]);
    await this.command(['EVAL', REPLACE_RANKING, '2', ...this.keys(organizerId, snapshot.generation), snapshot.revision, ...members]);
  }

  /**
   * Ограничивает ожидание необязательного Redis до 750 мс для SQL fallback.
   *
   * Promise.race не отменяет уже отправленную команду. Поздняя запись всё ещё
   * использует отдельный ключ поколения и проверку revision, а публичное чтение
   * в любом случае проверяет Redis относительно собственного SQL-снимка.
   */
  private async command(arguments_: string[]): Promise<unknown> {
    let timeout: NodeJS.Timeout | undefined;
    try {
      return await Promise.race([
        this.redis.client.sendCommand(arguments_),
        new Promise<never>((_, reject) => { timeout = setTimeout(() => reject(new Error('Ranking cache deadline exceeded')), 750); }),
      ]);
    } finally { if (timeout) clearTimeout(timeout); }
  }

  /**
   * Разделяет рейтинги по namespace, generation и владельцу мероприятий.
   * Старый refresh после переключения replay может обновить только старые ключи.
   */
  private keys(organizerId: string, generation: string): [string, string] {
    const base = `${this.prefix}:analytics:ranking:${generation}:${organizerId}`;
    return [base, `${base}:revision`];
  }

  /** Записывает начало сбоя кеша, не повторяя один лог на каждом SQL fallback. */
  private reportFailure(error: unknown): void {
    if (!this.degraded) log('analytics.ranking.fallback', { error: error instanceof Error ? error.message : String(error) });
    this.degraded = true;
  }
}
