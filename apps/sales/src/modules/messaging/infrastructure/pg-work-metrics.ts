import { Database } from '@ticketing/core';
import { gauge } from '@ticketing/observability';
/** Read-only operational view over the Sales database. IDs never become metric labels. */
export class PgWorkMetrics {
  constructor(private readonly db: Database) {}
  async refresh() {
    const deliveries = await this.db.query(
      `SELECT d.destination,count(*)::integer AS pending,COALESCE(extract(epoch FROM now()-min(m.created_at)),0)::float8 AS age FROM outbox_deliveries d JOIN outbox_messages m USING(message_id) WHERE d.status<>'delivered' GROUP BY d.destination`,
    );
    for (const destination of ['rabbitmq', 'kafka']) {
      const row = deliveries.rows.find((r) => r.destination === destination);
      gauge('outbox_pending', row?.pending ?? 0, { destination });
      gauge('outbox_age_seconds', row?.age ?? 0, { destination });
    }
    gauge(
      'reservations_due',
      (
        await this.db.query(
          `SELECT count(*)::integer AS n FROM orders WHERE status='reserved' AND expires_at<=now()`,
        )
      ).rows[0].n,
    );
    const payments = (
      await this.db.query(
        `SELECT status,count(*)::integer AS n FROM payments WHERE status IN ('pending','refund_pending') GROUP BY status`,
      )
    ).rows;
    for (const status of ['pending', 'refund_pending'])
      gauge('payments_unsettled', payments.find((r) => r.status === status)?.n ?? 0, { status });
  }
}
