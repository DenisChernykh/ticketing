ALTER TABLE events ADD COLUMN version integer NOT NULL DEFAULT 1 CHECK (version > 0);
ALTER TABLE catalog_revision ADD COLUMN ordering_revision bigint NOT NULL DEFAULT 1;
ALTER TABLE orders DROP CONSTRAINT orders_status_check;
ALTER TABLE orders ALTER COLUMN status SET DEFAULT 'reserved';
ALTER TABLE orders ADD CONSTRAINT orders_status_check CHECK (status IN ('reserved','confirmed','cancelled','expired'));
ALTER TABLE orders ADD COLUMN revision integer NOT NULL DEFAULT 1 CHECK (revision > 0);
ALTER TABLE orders ADD COLUMN expires_at timestamptz;
ALTER TABLE orders ADD COLUMN confirmed_at timestamptz;
ALTER TABLE orders ADD COLUMN cancelled_at timestamptz;
ALTER TABLE orders ADD COLUMN refunded_at timestamptz;
UPDATE orders SET confirmed_at=created_at WHERE status='confirmed';
CREATE INDEX orders_expiry_idx ON orders(expires_at,id) WHERE status='reserved';
DROP INDEX orders_buyer_idx;
CREATE INDEX orders_buyer_idx ON orders(buyer_id,created_at DESC,id DESC);
ALTER TABLE outbox_messages DROP CONSTRAINT outbox_messages_order_id_key;
ALTER TABLE outbox_messages ADD COLUMN order_revision integer NOT NULL DEFAULT 1;
ALTER TABLE outbox_messages ADD COLUMN trace_context jsonb NOT NULL DEFAULT '{}';
CREATE UNIQUE INDEX outbox_order_revision_idx ON outbox_messages(order_id,order_revision);

CREATE TABLE payments (
  id uuid PRIMARY KEY,
  order_id uuid NOT NULL UNIQUE REFERENCES orders(id),
  amount bigint NOT NULL CHECK (amount > 0),
  status text NOT NULL DEFAULT 'not_started' CHECK (status IN ('not_started','pending','succeeded','failed','refund_pending','refunded')),
  provider_version integer NOT NULL DEFAULT 0,
  next_sync_at timestamptz NOT NULL DEFAULT now(),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX payments_sync_idx ON payments(next_sync_at,id) WHERE status IN ('pending','refund_pending');
CREATE TABLE payment_operations (
  id uuid PRIMARY KEY,
  payment_id uuid NOT NULL REFERENCES payments(id),
  kind text NOT NULL CHECK (kind IN ('charge','refund')),
  status text NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','leased','done')),
  available_at timestamptz NOT NULL DEFAULT now(),
  lease_until timestamptz,
  lease_owner uuid,
  attempts integer NOT NULL DEFAULT 0,
  trace_context jsonb NOT NULL DEFAULT '{}',
  UNIQUE(payment_id,kind)
);
CREATE TABLE payment_webhooks (
  event_id uuid PRIMARY KEY,
  payment_id uuid NOT NULL REFERENCES payments(id),
  fingerprint text NOT NULL,
  received_at timestamptz NOT NULL DEFAULT now()
);
