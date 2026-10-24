import { SqlRowStream } from '@ticketing/core';
import type { OrderExports } from '../application/ports';
export class SqlOrderExports implements OrderExports {
  constructor(private readonly stream: SqlRowStream) {}
  rows(owner: string, filter: { from?: string; to?: string; eventId?: string }, signal: AbortSignal) {
    return this.stream.rows<Record<string, unknown>>(
      `SELECT o.order_id AS "orderId",o.event_id AS "eventId",o.snapshot->'order'->>'eventTitle' AS "eventTitle",o.confirmed_at AS "confirmedAt",(o.snapshot->'order'->>'quantity')::integer AS quantity,(o.snapshot->'order'->>'totalPrice')::bigint::text AS "totalPrice",o.snapshot->'state'->>'status' AS status,o.snapshot->'state'->>'cancelledAt' AS "cancelledAt",o.snapshot->'state'->>'refundedAt' AS "refundedAt",CASE WHEN o.snapshot->'state'->>'refundedAt' IS NULL THEN '0' ELSE o.snapshot->'order'->>'totalPrice' END AS "refundedAmount" FROM projection_orders o JOIN projection_active a USING(projection,generation) WHERE o.projection='events' AND o.organizer_id=$1 AND o.confirmed_at IS NOT NULL AND ($2::date IS NULL OR o.confirmed_at>=($2::date::timestamp AT TIME ZONE 'UTC')) AND ($3::date IS NULL OR o.confirmed_at<(($3::date+1)::timestamp AT TIME ZONE 'UTC')) AND ($4::uuid IS NULL OR o.event_id=$4) ORDER BY o.confirmed_at,o.order_id`,
      [owner, filter.from ?? null, filter.to ?? null, filter.eventId ?? null],
      signal,
    );
  }
}
