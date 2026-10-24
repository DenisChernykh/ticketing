import { randomUUID, createHmac } from 'node:crypto';
import { AppError, Database, log } from '@ticketing/core';
import { carrier, traced, count } from '@ticketing/observability';
import type { PaymentSnapshot } from '@ticketing/contracts';
import type { ProviderRepository, PaymentInput } from '../application/ports';
function snapshot(r: any): PaymentSnapshot {
  return {
    id: r.id,
    orderId: r.order_id,
    amount: Number(r.amount),
    currency: 'RUB',
    status: r.status,
    version: r.version,
    capturedAt: r.captured_at ? new Date(r.captured_at).toISOString() : null,
    refundedAt: r.refunded_at ? new Date(r.refunded_at).toISOString() : null,
  };
}
/** The provider owns its database; webhook delivery is a durable, independent effect. */
export class PgProvider implements ProviderRepository {
  constructor(
    private readonly db: Database,
    private readonly callback: string,
    private readonly secret: string,
  ) {}
  async create(p: PaymentInput) {
    return this.db.run(async () => {
      const scenario = (
        await this.db.query('SELECT mode,delay_ms FROM provider_scenarios WHERE order_id=$1', [p.orderId])
      ).rows[0] ?? { mode: 'success', delay_ms: 500 };
      const inserted = await this.db.query(
        `INSERT INTO provider_payments(id,order_id,amount,status,settle_at,scenario,callback_url,trace_context) VALUES($1,$2,$3,'pending',now()+$4*interval '1 millisecond',$5,$6,$7::jsonb) ON CONFLICT DO NOTHING RETURNING id`,
        [
          p.id,
          p.orderId,
          p.amount,
          scenario.delay_ms,
          scenario.mode,
          this.callback,
          JSON.stringify(carrier()),
        ],
      );
      const r = (await this.db.query('SELECT * FROM provider_payments WHERE id=$1', [p.id])).rows[0];
      if (!r || r.order_id !== p.orderId || Number(r.amount) !== p.amount)
        throw new AppError('PAYMENT_CONFLICT', 'Платёж уже создан с другими данными', 409);
      return {
        payment: snapshot(r),
        dropResponse: inserted.rowCount === 1 && scenario.mode === 'lost_response',
      };
    });
  }
  async get(id: string) {
    const r = (await this.db.query('SELECT * FROM provider_payments WHERE id=$1', [id])).rows[0];
    return r ? snapshot(r) : undefined;
  }
  async refund(id: string) {
    return this.db.run(async () => {
      let r = (await this.db.query('SELECT * FROM provider_payments WHERE id=$1 FOR UPDATE', [id])).rows[0];
      if (!r) throw new AppError('PAYMENT_NOT_FOUND', 'Платёж не найден', 404);
      if (!['succeeded', 'refunded'].includes(r.status))
        throw new AppError('PAYMENT_NOT_CAPTURED', 'Платёж ещё не проведён', 409);
      if (r.scenario === 'refund_failure') {
        await this.db.query(`UPDATE provider_payments SET scenario='success' WHERE id=$1`, [id]);
        return { payment: snapshot(r), fail: true };
      }
      if (r.status === 'succeeded' && !r.refund_at)
        await this.db.query(
          `UPDATE provider_payments SET refund_at=now()+interval '500 milliseconds' WHERE id=$1`,
          [id],
        );
      return { payment: snapshot(r), fail: false };
    });
  }
  async scenario(id: string, mode: string, delay: number) {
    await this.db.query(
      `INSERT INTO provider_scenarios(order_id,mode,delay_ms) VALUES($1,$2,$3) ON CONFLICT(order_id) DO UPDATE SET mode=EXCLUDED.mode,delay_ms=EXCLUDED.delay_ms`,
      [id, mode, delay],
    );
  }
  async importPayment(p: PaymentInput, capturedAt: string) {
    await this.db.query(
      `INSERT INTO provider_payments(id,order_id,amount,status,version,captured_at,callback_url) VALUES($1,$2,$3,'succeeded',2,$4,$5) ON CONFLICT(id) DO NOTHING`,
      [p.id, p.orderId, p.amount, capturedAt, this.callback],
    );
    const prior = await this.get(p.id);
    if (!prior || prior.orderId !== p.orderId || prior.amount !== p.amount)
      throw new Error('Legacy payment conflict');
  }
  async tick() {
    return this.db.run(async () => {
      const r = (
        await this.db.query(
          `SELECT * FROM provider_payments WHERE (status='pending' AND settle_at<=now()) OR (status='succeeded' AND refund_at<=now()) ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1`,
        )
      ).rows[0];
      if (!r) return false;
      const refunded = r.status === 'succeeded',
        status = refunded ? 'refunded' : r.scenario === 'decline' ? 'failed' : 'succeeded';
      const updated = (
        await this.db.query(
          `UPDATE provider_payments SET status=$2,version=version+1,captured_at=CASE WHEN $2='succeeded' THEN now() ELSE captured_at END,refunded_at=CASE WHEN $2='refunded' THEN now() ELSE refunded_at END WHERE id=$1 RETURNING *`,
          [r.id, status],
        )
      ).rows[0];
      if (r.scenario !== 'no_webhook')
        await this.db.query(
          `INSERT INTO provider_webhooks(id,payment_id,body,available_at,trace_context) VALUES($1,$2,$3::jsonb,now()+$4*interval '1 millisecond',$5::jsonb)`,
          [
            randomUUID(),
            r.id,
            JSON.stringify(snapshot(updated)),
            r.scenario === 'delayed_webhook' ? 10000 : 0,
            JSON.stringify(r.trace_context),
          ],
        );
      return true;
    });
  }
  async deliver() {
    const owner = randomUUID();
    const r = (
      await this.db.query(
        `WITH candidate AS (SELECT id FROM provider_webhooks WHERE NOT delivered AND available_at<=now() AND (lease_until IS NULL OR lease_until<now()) ORDER BY available_at,id FOR UPDATE SKIP LOCKED LIMIT 1),claimed AS (UPDATE provider_webhooks w SET lease_owner=$1,lease_until=now()+interval '15 seconds',attempts=attempts+1 FROM candidate c WHERE w.id=c.id RETURNING w.*) SELECT c.*,p.callback_url,p.scenario FROM claimed c JOIN provider_payments p ON p.id=c.payment_id`,
        [owner],
      )
    ).rows[0];
    if (!r) return false;
    await traced(
      'payment.webhook',
      { paymentId: r.payment_id, eventId: r.id },
      async () => {
        try {
          const body = JSON.stringify({ eventId: r.id, payment: r.body }),
            timestamp = String(Math.floor(Date.now() / 1000)),
            signature = createHmac('sha256', this.secret)
              .update(timestamp + '.' + body)
              .digest('hex');
          const send = () =>
            fetch(r.callback_url, {
              method: 'POST',
              redirect: 'error',
              signal: AbortSignal.timeout(2500),
              headers: {
                'Content-Type': 'application/json',
                'X-Webhook-Timestamp': timestamp,
                'X-Webhook-Signature': signature,
                ...carrier(),
              },
              body,
            });
          const response = await send();
          await response.arrayBuffer();
          if (!response.ok) throw new Error('Webhook rejected');
          if (r.scenario === 'duplicate_webhook') {
            const duplicate = await send();
            await duplicate.arrayBuffer();
            if (!duplicate.ok) throw new Error('Duplicate rejected');
          }
          await this.db.query(
            'UPDATE provider_webhooks SET delivered=true,lease_until=NULL WHERE id=$1 AND lease_owner=$2',
            [r.id, owner],
          );
        } catch (e) {
          await this.db.query(
            `UPDATE provider_webhooks SET lease_until=NULL,lease_owner=NULL,available_at=now()+least(30,power(2,least(attempts,5)))*interval '1 second' WHERE id=$1 AND lease_owner=$2`,
            [r.id, owner],
          );
          count('webhook_retries');
          log('webhook.retry', { eventId: r.id });
        }
      },
      r.trace_context,
      r.attempts > 1,
    );
    return true;
  }
  async health() {
    await this.db.query('SELECT 1');
  }
}
