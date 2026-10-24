import { randomUUID } from 'node:crypto';
import { AppError, Database } from '@ticketing/core';
import { carrier } from '@ticketing/observability';
import type { PaymentSnapshot } from '@ticketing/contracts';
import type { Payment, Payments, PaymentJobs, PaymentOperation } from '../application/ports';
const cols = `id,order_id AS "orderId",amount,status,provider_version AS "providerVersion"`;
function payment(p: any): Payment {
  return { ...p, amount: Number(p.amount) };
}
export class PgPayments implements Payments, PaymentJobs {
  constructor(private readonly db: Database) {}
  async ensure(orderId: string, amount: number) {
    await this.db.query(
      'INSERT INTO payments(id,order_id,amount) VALUES($1,$2,$3) ON CONFLICT(order_id) DO NOTHING',
      [randomUUID(), orderId, amount],
    );
    return (await this.lock(orderId))!;
  }
  async lock(orderId: string) {
    const p = (await this.db.query(`SELECT ${cols} FROM payments WHERE order_id=$1 FOR UPDATE`, [orderId]))
      .rows[0];
    return p ? payment(p) : undefined;
  }
  async charge(p: Payment) {
    if (p.status !== 'not_started') return;
    await this.db.query(
      `UPDATE payments SET status='pending',updated_at=now(),trace_context=$2::jsonb WHERE id=$1`,
      [p.id, JSON.stringify(carrier())],
    );
    await this.enqueue(p, 'charge');
  }
  async refund(p: Payment) {
    if (p.status === 'refunded') return;
    await this.db.query(`UPDATE payments SET status='refund_pending',updated_at=now() WHERE id=$1`, [p.id]);
    await this.enqueue(p, 'refund');
  }
  private async enqueue(p: Payment, kind: string) {
    await this.db.query(
      `INSERT INTO payment_operations(id,payment_id,kind,trace_context) VALUES($1,$2,$3,$4::jsonb) ON CONFLICT(payment_id,kind) DO NOTHING`,
      [randomUUID(), p.id, kind, JSON.stringify(carrier())],
    );
  }
  async record(p: Payment, remote: PaymentSnapshot) {
    if (remote.version < p.providerVersion) return;
    const status =
      p.status === 'refund_pending' && remote.status === 'succeeded' ? 'refund_pending' : remote.status;
    await this.db.query(
      `UPDATE payments SET status=$2,provider_version=$3,updated_at=now(),next_sync_at=now()+interval '2 seconds' WHERE id=$1`,
      [p.id, status, remote.version],
    );
    p.status = status;
    p.providerVersion = remote.version;
  }
  /**
   * Короткий SQL-запрос назначает аренду через SKIP LOCKED. Сеть не удерживает
   * блокировку. finish проверяет owner, поэтому старый worker не завершит чужую аренду.
   */
  async claim(): Promise<PaymentOperation | undefined> {
    const owner = randomUUID();
    const result = await this.db.query(
      `WITH candidate AS (SELECT id FROM payment_operations WHERE available_at<=now() AND (status='pending' OR (status='leased' AND lease_until<now())) ORDER BY available_at,id FOR UPDATE SKIP LOCKED LIMIT 1),claimed AS (UPDATE payment_operations o SET status='leased',lease_owner=$1,lease_until=now()+interval '30 seconds',attempts=attempts+1 FROM candidate c WHERE o.id=c.id RETURNING o.*) SELECT c.id,c.kind,c.trace_context,p.id AS payment_id,p.order_id,p.amount,p.status,p.provider_version FROM claimed c JOIN payments p ON p.id=c.payment_id`,
      [owner],
    );
    const r = result.rows[0];
    return r
      ? {
          id: r.id,
          owner,
          kind: r.kind,
          traceContext: r.trace_context,
          payment: {
            id: r.payment_id,
            orderId: r.order_id,
            amount: Number(r.amount),
            status: r.status,
            providerVersion: r.provider_version,
          },
        }
      : undefined;
  }
  async finish(job: PaymentOperation, success: boolean) {
    await this.db.query(
      `UPDATE payment_operations SET status=$3,lease_owner=NULL,lease_until=NULL,available_at=now()+least(30,power(2,least(attempts,5))) * interval '1 second' WHERE id=$1 AND lease_owner=$2 AND status='leased'`,
      [job.id, job.owner, success ? 'done' : 'pending'],
    );
  }
  /** Атомарно откладывает следующий опрос. После сбоя процесс не теряет платёж: срок снова наступит. */
  async nextSync() {
    const r = (
      await this.db.query(
        `WITH candidate AS (SELECT id FROM payments WHERE status IN ('pending','refund_pending') AND next_sync_at<=now() ORDER BY next_sync_at,id FOR UPDATE SKIP LOCKED LIMIT 1) UPDATE payments p SET next_sync_at=now()+interval '2 seconds' FROM candidate c WHERE p.id=c.id RETURNING p.id,p.order_id AS "orderId",p.amount,p.status,p.provider_version AS "providerVersion",p.trace_context AS "traceContext"`,
      )
    ).rows[0];
    return r ? { payment: payment(r), traceContext: r.traceContext } : undefined;
  }
  /** Коммитит квитанцию webhook и запрос сверки вместе; транспортный контекст не входит в fingerprint. */
  async receive(eventId: string, p: PaymentSnapshot, fingerprint: string) {
    await this.db.run(async () => {
      const existing = (await this.db.query('SELECT order_id,amount FROM payments WHERE id=$1', [p.id]))
        .rows[0];
      if (!existing || existing.order_id !== p.orderId || Number(existing.amount) !== p.amount)
        throw new AppError('INVALID_PAYMENT', 'Платёж не соответствует заказу', 400);
      const inserted = await this.db.query(
        `INSERT INTO payment_webhooks(event_id,payment_id,fingerprint) VALUES($1,$2,$3) ON CONFLICT DO NOTHING RETURNING event_id`,
        [eventId, p.id, fingerprint],
      );
      if (!inserted.rowCount) {
        const row = (
          await this.db.query('SELECT fingerprint FROM payment_webhooks WHERE event_id=$1', [eventId])
        ).rows[0];
        if (row.fingerprint !== fingerprint)
          throw new AppError('WEBHOOK_CONFLICT', 'Идентификатор webhook использован повторно', 409);
      }
      await this.db.query('UPDATE payments SET next_sync_at=now(),trace_context=$2::jsonb WHERE id=$1', [
        p.id,
        JSON.stringify(carrier()),
      ]);
    });
  }
}
