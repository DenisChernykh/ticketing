import { Database } from '@ticketing/core';
import type { Order } from '../domain/order';
import type { OrdersRepository } from '../application/ports';
import { columns, normalize } from './order-row';
/** Owns order writes. Inventory and payment tables are only joined by the read model. */
export class PgOrders implements OrdersRepository {
  constructor(private readonly db: Database) {}
  async claimKey(buyerId: string, key: string, hash: string) {
    await this.db.query(
      `INSERT INTO idempotency_keys(buyer_id,operation,key,payload_hash) VALUES($1,'purchase',$2,$3) ON CONFLICT DO NOTHING`,
      [buyerId, key, hash],
    );
    return (
      await this.db.query<{ hash: string; result: Order | null }>(
        `SELECT payload_hash AS hash,result FROM idempotency_keys WHERE buyer_id=$1 AND operation='purchase' AND key=$2 FOR UPDATE`,
        [buyerId, key],
      )
    ).rows[0];
  }
  async completeKey(buyerId: string, key: string, order: Order) {
    await this.db.query(
      `UPDATE idempotency_keys SET result=$3::jsonb WHERE buyer_id=$1 AND operation='purchase' AND key=$2`,
      [buyerId, key, JSON.stringify(order)],
    );
  }
  async insert(o: Order) {
    await this.db.query(
      `INSERT INTO orders(id,buyer_id,organizer_id,event_id,event_title,venue,starts_at,quantity,unit_price,total_price,currency,status,created_at,expires_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`,
      [
        o.id,
        o.buyerId,
        o.organizerId,
        o.eventId,
        o.eventTitle,
        o.venue,
        o.startsAt,
        o.quantity,
        o.unitPrice,
        o.totalPrice,
        o.currency,
        o.status,
        o.createdAt,
        o.expiresAt,
      ],
    );
  }
  async save(o: Order) {
    await this.db.query(
      `UPDATE orders SET status=$2,revision=$3,confirmed_at=$4,cancelled_at=$5,refunded_at=$6 WHERE id=$1`,
      [o.id, o.status, o.revision, o.confirmedAt, o.cancelledAt, o.refundedAt],
    );
  }
  async lock(id: string) {
    const r = (await this.db.query(`SELECT ${columns} FROM orders o WHERE o.id=$1 FOR UPDATE`, [id])).rows[0];
    return r ? normalize(r) : undefined;
  }
  async now() {
    return new Date((await this.db.query('SELECT clock_timestamp() AS now')).rows[0].now).toISOString();
  }
  async due() {
    return (
      await this.db.query<{ id: string }>(
        `SELECT id FROM orders WHERE status='reserved' AND expires_at<=clock_timestamp() ORDER BY expires_at,id LIMIT 100`,
      )
    ).rows.map((r) => r.id);
  }
}
