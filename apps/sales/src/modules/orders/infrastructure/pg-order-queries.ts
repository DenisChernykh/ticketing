import { Database, decodeCursor, encodeCursor } from '@ticketing/core';
import type { Page, PageInput } from '@ticketing/contracts';
import type { Order } from '../domain/order';
import type { OrdersReader } from '../application/ports';
import { columns, normalize } from './order-row';
/** Read-only model joins modules inside Sales; no cross-service access. */
export class PgOrderQueries implements OrdersReader {
  constructor(private readonly db: Database) {}
  async get(id: string, buyerId: string) {
    const r = (
      await this.db.query(
        `SELECT ${columns},CASE WHEN p.id IS NULL THEN NULL ELSE jsonb_build_object('id',p.id,'status',p.status,'amount',p.amount) END AS payment FROM orders o LEFT JOIN payments p ON p.order_id=o.id WHERE o.id=$1 AND o.buyer_id=$2`,
        [id, buyerId],
      )
    ).rows[0];
    return r ? normalize(r) : undefined;
  }
  async list(buyerId: string, page: PageInput): Promise<Page<Order>> {
    const scope = 'orders:' + buyerId,
      cur = decodeCursor(page.cursor, scope);
    const rows = (
      await this.db.query(
        `SELECT ${columns},e.title AS "currentEventTitle",o.created_at::text AS cursor_date FROM orders o INNER JOIN events e ON e.id=o.event_id WHERE o.buyer_id=$1 AND ($2::timestamptz IS NULL OR (o.created_at,o.id)<($2::timestamptz,$3::uuid)) ORDER BY o.created_at DESC,o.id DESC LIMIT $4`,
        [buyerId, cur?.date ?? null, cur?.id ?? null, page.limit + 1],
      )
    ).rows;
    const more = rows.length > page.limit;
    const shown = rows.slice(0, page.limit),
      last = shown.at(-1);
    const nextCursor = more && last ? encodeCursor({ scope, date: last.cursor_date, id: last.id }) : null;
    return {
      items: shown.map((r) => {
        delete r.cursor_date;
        return normalize(r);
      }),
      nextCursor,
    };
  }
}
