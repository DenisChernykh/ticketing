import { Database } from '@ticketing/core';
import { carrier } from '@ticketing/observability';
import type { OrderEvent } from '@ticketing/contracts';
import type { EventWriter } from '../application/ports';
export class PgEventWriter implements EventWriter {
  constructor(private readonly db: Database) {}
  async append(event: OrderEvent) {
    await this.db.query(
      `INSERT INTO outbox_messages(message_id,order_id,order_revision,payload,trace_context) VALUES($1,$2,$3,$4::jsonb,$5::jsonb)`,
      [
        event.messageId,
        event.order.id,
        event.orderRevision,
        JSON.stringify(event),
        JSON.stringify(carrier()),
      ],
    );
    await this.db.query(
      `INSERT INTO outbox_deliveries(message_id,destination) VALUES($1,'rabbitmq'),($1,'kafka')`,
      [event.messageId],
    );
  }
}
