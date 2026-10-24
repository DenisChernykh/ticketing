import type { Page, PageInput } from '@ticketing/contracts';
import type { Event, EventInput, OrganizerEvent } from '../domain/event';
/**
 * Контракт каталога с устойчивым поколением данных для кеша.
 *
 * @remarks
 * Реализация должна изменять revision в той же транзакции, что и данные
 * мероприятия. Иначе успешное изменение могло бы оставить доступным старый
 * кеш. Владение мероприятием при update определяется парой id/organizerId.
 */
export interface CatalogReader {
  revision(): Promise<string>;
  orderingRevision(): Promise<string>;
  list(page: PageInput): Promise<Page<Event>>;
  listOrganizer(organizerId: string, page: PageInput): Promise<Page<OrganizerEvent>>;
}
export interface CatalogWriter {
  insert(id: string, organizerId: string, input: EventInput): Promise<Event>;
  update(id: string, organizerId: string, input: Partial<EventInput> & { version: number }): Promise<Event>;
}
/**
 * Необязательный кеш одного поколения каталога: ошибка чтения равнозначна промаху,
 * ошибка записи не должна отменять чтение PostgreSQL или подтверждённое изменение.
 */
export interface CatalogCache {
  get(revision: string): Promise<Page<Event> | undefined>;
  put(revision: string, events: Page<Event>): Promise<void>;
}
