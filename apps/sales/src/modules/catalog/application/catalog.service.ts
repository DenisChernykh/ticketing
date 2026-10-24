import { randomUUID } from 'node:crypto';
import { AppError } from '@ticketing/core';
import type { PageInput } from '@ticketing/contracts';
import type { EventInput } from '../domain/event';
import type { CatalogCache, CatalogReader, CatalogWriter } from './ports';
export class CatalogService {
  constructor(
    private readonly repository: CatalogReader,
    private readonly cache: CatalogCache,
    private readonly writer: CatalogWriter,
  ) {}
  async list(page: PageInput = { limit: 20 }) {
    const revision = await this.repository.revision();
    // Cursor validation belongs to the repository and is also performed on cache hits.
    const ordering = await this.repository.orderingRevision();
    if (page.cursor) {
      try {
        const c = JSON.parse(Buffer.from(page.cursor, 'base64url').toString());
        if (c.scope !== 'events') throw new Error();
        if (c.revision !== ordering)
          throw new AppError('CURSOR_STALE', 'Каталог изменился; начните просмотр заново', 409);
      } catch (e) {
        if (e instanceof AppError) throw e;
        throw new AppError('INVALID_CURSOR', 'Неверный курсор', 400);
      }
    }
    const key = revision + ':' + ordering + ':' + page.limit + ':' + (page.cursor ?? '');
    const cached = await this.cache.get(key);
    if (cached) return cached;
    const result = await this.repository.list(page);
    await this.cache.put(key, result);
    return result;
  }
  organizer(id: string, page: PageInput = { limit: 20 }) {
    return this.repository.listOrganizer(id, page);
  }
  create(id: string, input: EventInput) {
    return this.writer.insert(randomUUID(), id, input);
  }
  update(id: string, owner: string, input: Partial<EventInput> & { version: number }) {
    return this.writer.update(id, owner, input);
  }
}
