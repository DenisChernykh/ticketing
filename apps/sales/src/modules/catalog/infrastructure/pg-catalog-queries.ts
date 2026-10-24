import { AppError, Database, decodeCursor, encodeCursor } from '@ticketing/core';
import type { Page, PageInput } from '@ticketing/contracts';
import type { Event, OrganizerEvent } from '../domain/event';
import type { CatalogReader } from '../application/ports';
function normalize<T extends Event>(event: T): T {
  return { ...event, startsAt: new Date(event.startsAt).toISOString() };
}
/** Read-only view over the Sales database: catalogue plus order aggregates. */
export class PgCatalogQueries implements CatalogReader {
  constructor(private readonly db: Database) {}
  async revision() {
    return String(
      (await this.db.query('SELECT revision FROM catalog_revision WHERE id = true')).rows[0].revision,
    );
  }
  async orderingRevision() {
    return String(
      (await this.db.query('SELECT ordering_revision FROM catalog_revision WHERE id=true')).rows[0]
        .ordering_revision,
    );
  }
  private async page<T extends Event>(page: PageInput, owner?: string): Promise<Page<T>> {
    const scope = owner ? 'organizer:' + owner : 'events',
      cursor = decodeCursor(page.cursor, scope);

    // Страница и ревизия читаются одним SQL-запросом из согласованного снимка.
    const result = await this.db.query(
      `WITH rev AS (SELECT ordering_revision::text AS revision FROM catalog_revision WHERE id=true), entries AS (
        SELECT e.id,e.organizer_id AS "organizerId",e.title,e.venue,e.starts_at AS "startsAt",e.starts_at::text AS cursor_date,e.version,e.capacity,e.available,e.unit_price AS "unitPrice",e.currency,
        COALESCE((SELECT SUM(o.quantity) FROM orders o WHERE o.event_id=e.id AND o.status='confirmed'),0)::integer AS sold,
        COALESCE((SELECT SUM(o.total_price) FROM orders o WHERE o.event_id=e.id AND o.confirmed_at IS NOT NULL AND o.refunded_at IS NULL),0)::float8 AS revenue
        FROM events e WHERE ($1::uuid IS NULL OR e.organizer_id=$1) AND ($2::timestamptz IS NULL OR (e.starts_at,e.id)>($2::timestamptz,$3::uuid))
        ORDER BY e.starts_at,e.id LIMIT $4
      ) SELECT revision,COALESCE((SELECT jsonb_agg(entries ORDER BY "startsAt",id) FROM entries),'[]'::jsonb) AS items FROM rev`,
      [owner ?? null, cursor?.date ?? null, cursor?.id ?? null, page.limit + 1],
    );
    const { revision, items } = result.rows[0];
    if (cursor && cursor.revision !== revision)
      throw new AppError('CURSOR_STALE', 'Каталог изменился; начните просмотр заново', 409);
    const shown = items.slice(0, page.limit),
      last = shown.at(-1);
    const nextCursor =
      items.length > page.limit && last
        ? encodeCursor({ scope, date: last.cursor_date, id: last.id, revision })
        : null;
    return {
      items: shown.map((r: any) => {
        delete r.cursor_date;
        if (!owner) {
          delete r.sold;
          delete r.revenue;
        }
        return normalize(r) as T;
      }),
      nextCursor,
    };
  }
  list(page: PageInput) {
    return this.page<Event>(page);
  }
  listOrganizer(owner: string, page: PageInput) {
    return this.page<OrganizerEvent>(page, owner);
  }
}
