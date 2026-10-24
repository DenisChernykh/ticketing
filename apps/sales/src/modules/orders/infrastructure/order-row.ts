import type { Order } from '../domain/order';
export const columns = `o.id,o.buyer_id AS "buyerId",o.organizer_id AS "organizerId",o.event_id AS "eventId",o.event_title AS "eventTitle",o.venue,o.starts_at AS "startsAt",o.quantity,o.unit_price AS "unitPrice",o.total_price AS "totalPrice",o.currency,o.status,o.created_at AS "createdAt",o.revision,o.expires_at AS "expiresAt",o.confirmed_at AS "confirmedAt",o.cancelled_at AS "cancelledAt",o.refunded_at AS "refundedAt"`;
export function normalize(row: any): Order {
  for (const key of ['startsAt', 'createdAt', 'expiresAt', 'confirmedAt', 'cancelledAt', 'refundedAt'])
    row[key] = row[key] ? new Date(row[key]).toISOString() : null;
  row.totalPrice = Number(row.totalPrice);
  return row;
}
