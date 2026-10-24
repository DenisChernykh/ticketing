import type { Page } from '@ticketing/contracts';
import { RedisConnection } from '@ticketing/core';
import type { CatalogCache } from '../application/ports';
import type { Event } from '../domain/event';
/**
 * Кеш публичного каталога с отдельным ключом на каждое поколение PostgreSQL.
 *
 * @remarks
 * Redis здесь ускоряет чтение и может быть временно потерян. Ошибка GET/JSON
 * трактуется как промах, ошибка SET не мешает отдать уже полученные данные.
 * Это намеренно отличается от сессий и обязательных лимитов, где сбой Redis
 * запрещает операцию. TTL ограничивает жизнь старых поколений и расход памяти;
 * немедленная видимость обычного изменения обеспечивается сменой поколения.
 */
export class RedisCatalog implements CatalogCache {
  constructor(
    private readonly redis: RedisConnection,
    private readonly prefix: string,
    private readonly ttl: number,
  ) {}
  private key(revision: string) {
    return `${this.prefix}:catalog:${revision}`;
  }
  async get(revision: string): Promise<Page<Event> | undefined> {
    try {
      const value = await this.redis.client.get(this.key(revision));
      return value ? (JSON.parse(value) as Page<Event>) : undefined;
    } catch {
      return undefined;
    }
  }
  async put(revision: string, events: Page<Event>): Promise<void> {
    try {
      await this.redis.client.set(this.key(revision), JSON.stringify(events), { EX: this.ttl });
    } catch {
      /* Каталог уже получен из SQL; необязательный кеш не должен сорвать ответ. */
    }
  }
}
