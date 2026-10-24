import { AppError, Database } from '@ticketing/core';
import type { CatalogWriter } from '../application/ports';
import type { Event, EventInput } from '../domain/event';

const COLUMNS = `id, organizer_id AS "organizerId", title, venue, starts_at AS "startsAt", version, capacity, available, unit_price AS "unitPrice", currency`;
function normalize<T extends Event>(event: T): T {
  return { ...event, startsAt: new Date(event.startsAt).toISOString() };
}
/**
 * SQL-адаптер каталога Sales: владение, остатки и поколение кеша находятся в одной БД.
 *
 * @remarks
 * Все значения запроса передаются отдельно от SQL. Транзакции изменения охватывают
 * и мероприятие, и catalog_revision, поэтому ни частичный успех, ни сбой Redis
 * не оставляют подтверждённую запись привязанной к прежнему поколению каталога.
 */
export class PgCatalog implements CatalogWriter {
  constructor(private readonly db: Database) {}
  async insert(id: string, organizerId: string, input: EventInput): Promise<Event> {
    return this.db.run(async () => {
      const result = await this.db.query<Event>(
        `INSERT INTO events
        (id, organizer_id, title, venue, starts_at, capacity, available, unit_price)
        VALUES ($1, $2, $3, $4, $5, $6, $6, $7) RETURNING ${COLUMNS}`,
        [id, organizerId, input.title, input.venue, input.startsAt, input.capacity, input.unitPrice],
      );
      await this.db.query(
        'UPDATE catalog_revision SET revision = revision + 1, ordering_revision=ordering_revision+1 WHERE id = true',
      );
      return normalize(result.rows[0]);
    });
  }
  /**
   * Изменяет своё мероприятие, не допуская вместимости меньше занятых мест.
   *
   * @remarks
   * FOR UPDATE блокирует ту же строку, которую блокирует покупка. Пока редактор
   * вычисляет новый available, другой запрос не может незаметно занять ещё места.
   * available пересчитывается как новая вместимость минус брони и оплаченные билеты.
   * Фильтр по organizer_id применяется до получения строки: чужое мероприятие
   * возвращает тот же 404, что и отсутствующее, без раскрытия его содержимого.
   * Изменения цены, названия и даты не переписывают снимки в прежних заказах.
   *
   * @throws AppError с 404 для отсутствующего/чужого мероприятия либо 409 для недопустимой вместимости.
   */
  async update(
    id: string,
    organizerId: string,
    input: Partial<EventInput> & { version: number },
  ): Promise<Event> {
    return this.db.run(async () => {
      const existing = (
        await this.db.query<Event>(
          `SELECT ${COLUMNS} FROM events WHERE id = $1 AND organizer_id = $2 FOR UPDATE`,
          [id, organizerId],
        )
      ).rows[0];
      if (!existing) throw new AppError('EVENT_NOT_FOUND', 'Мероприятие не найдено', 404);
      if (existing.version !== input.version)
        throw new AppError('EVENT_VERSION_CONFLICT', 'Мероприятие уже изменено; обновите данные', 409);
      const occupied = existing.capacity - existing.available;
      const next = { ...existing, ...input };
      if (next.capacity < occupied)
        throw new AppError(
          'CAPACITY_BELOW_SALES',
          'Вместимость не может быть меньше количества занятых мест',
          409,
        );
      const result = await this.db.query<Event>(
        `UPDATE events SET title=$3, venue=$4, starts_at=$5,
        capacity=$6, available=$7, unit_price=$8, version=version+1, updated_at=now()
        WHERE id=$1 AND organizer_id=$2 RETURNING ${COLUMNS}`,
        [
          id,
          organizerId,
          next.title,
          next.venue,
          next.startsAt,
          next.capacity,
          next.capacity - occupied,
          next.unitPrice,
        ],
      );
      await this.db.query(
        'UPDATE catalog_revision SET revision=revision+1,ordering_revision=ordering_revision+$1 WHERE id=true',
        [new Date(existing.startsAt).getTime() !== new Date(next.startsAt).getTime() ? 1 : 0],
      );
      return normalize(result.rows[0]);
    });
  }
}
