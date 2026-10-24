CREATE TABLE fulfillment_orders (
  order_id uuid PRIMARY KEY,
  buyer_id uuid NOT NULL,
  event_id uuid NOT NULL,
  quantity integer NOT NULL CHECK (quantity > 0 AND quantity <= 100),
  snapshot_hash text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX fulfillment_orders_buyer_idx ON fulfillment_orders (buyer_id, order_id);

-- Each independently subscribed handler commits its receipt with its own result.
CREATE TABLE delivery_receipts (
  consumer text NOT NULL CHECK (consumer IN ('tickets', 'notifications')),
  message_id uuid NOT NULL,
  fingerprint text NOT NULL,
  order_id uuid NOT NULL,
  processed_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer, message_id)
);
CREATE INDEX delivery_receipts_order_idx ON delivery_receipts (order_id, consumer);

CREATE TABLE tickets (
  id uuid PRIMARY KEY,
  order_id uuid NOT NULL REFERENCES fulfillment_orders (order_id),
  event_id uuid NOT NULL,
  buyer_id uuid NOT NULL,
  ordinal integer NOT NULL CHECK (ordinal > 0),
  code text NOT NULL UNIQUE,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (order_id, ordinal)
);
CREATE INDEX tickets_buyer_idx ON tickets (buyer_id, order_id);

CREATE TABLE notifications (
  id uuid PRIMARY KEY,
  order_id uuid NOT NULL UNIQUE REFERENCES fulfillment_orders (order_id),
  buyer_id uuid NOT NULL,
  message text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX notifications_buyer_idx ON notifications (buyer_id, order_id);

CREATE TABLE delivery_failures (
  consumer text NOT NULL CHECK (consumer IN ('tickets', 'notifications')),
  order_id uuid NOT NULL REFERENCES fulfillment_orders (order_id),
  message_id uuid NOT NULL,
  reason text NOT NULL,
  failed_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer, order_id)
);
