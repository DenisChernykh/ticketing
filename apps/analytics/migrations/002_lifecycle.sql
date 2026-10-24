ALTER TABLE projection_generations ADD COLUMN format_version integer NOT NULL DEFAULT 1;
ALTER TABLE projection_generations ALTER COLUMN format_version SET DEFAULT 2;
ALTER TABLE message_receipts DROP CONSTRAINT message_receipts_projection_generation_order_id_key;
ALTER TABLE message_receipts ADD COLUMN order_revision integer NOT NULL DEFAULT 1;
CREATE UNIQUE INDEX receipt_order_revision ON message_receipts(projection,generation,order_id,order_revision);
ALTER TABLE event_sales DROP CONSTRAINT event_sales_orders_check;
ALTER TABLE event_sales DROP CONSTRAINT event_sales_tickets_check;
ALTER TABLE event_sales ADD COLUMN gross_revenue bigint NOT NULL DEFAULT 0;
ALTER TABLE event_sales ADD COLUMN refunds bigint NOT NULL DEFAULT 0;
UPDATE event_sales SET gross_revenue=revenue;
ALTER TABLE daily_revenue DROP CONSTRAINT daily_revenue_orders_check;
ALTER TABLE daily_revenue DROP CONSTRAINT daily_revenue_tickets_check;
ALTER TABLE daily_revenue DROP CONSTRAINT daily_revenue_revenue_check;
ALTER TABLE daily_revenue ADD COLUMN gross_revenue bigint NOT NULL DEFAULT 0;
ALTER TABLE daily_revenue ADD COLUMN refunds bigint NOT NULL DEFAULT 0;
UPDATE daily_revenue SET gross_revenue=revenue;
CREATE TABLE projection_orders (
  projection text NOT NULL,
  generation uuid NOT NULL,
  order_id uuid NOT NULL,
  organizer_id uuid NOT NULL,
  event_id uuid NOT NULL,
  confirmed_at timestamptz,
  revision integer NOT NULL,
  snapshot jsonb NOT NULL,
  PRIMARY KEY(projection,generation,order_id),
  FOREIGN KEY(projection,generation) REFERENCES projection_generations(projection,generation)
);
CREATE INDEX projection_orders_export ON projection_orders(projection,generation,organizer_id,confirmed_at,order_id);
ALTER TABLE event_sales DROP CONSTRAINT event_sales_revenue_check;
