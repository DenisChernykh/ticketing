ALTER TABLE fulfillment_orders ADD COLUMN state text NOT NULL DEFAULT 'confirmed';
ALTER TABLE fulfillment_orders ADD COLUMN revision integer NOT NULL DEFAULT 1;
ALTER TABLE tickets ADD COLUMN valid boolean NOT NULL DEFAULT true;
ALTER TABLE notifications DROP CONSTRAINT notifications_order_id_key;
ALTER TABLE notifications ADD COLUMN kind text NOT NULL DEFAULT 'confirmed';
CREATE UNIQUE INDEX notifications_order_kind ON notifications(order_id,kind);
CREATE TABLE consumer_orders (
  consumer text NOT NULL,
  order_id uuid NOT NULL REFERENCES fulfillment_orders(order_id),
  revision integer NOT NULL,
  PRIMARY KEY(consumer,order_id)
);
ALTER TABLE fulfillment_orders ADD COLUMN state_hash text;
