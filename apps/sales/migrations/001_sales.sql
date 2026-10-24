CREATE TABLE users (
  id uuid PRIMARY KEY,
  email text NOT NULL UNIQUE CHECK (email = lower(email)),
  password_hash text NOT NULL,
  role text NOT NULL CHECK (role IN ('buyer', 'organizer')),
  is_active boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE events (
  id uuid PRIMARY KEY,
  organizer_id uuid NOT NULL REFERENCES users(id),
  title text NOT NULL CHECK (length(title) BETWEEN 1 AND 200),
  venue text NOT NULL CHECK (length(venue) BETWEEN 1 AND 300),
  starts_at timestamptz NOT NULL,
  capacity integer NOT NULL CHECK (capacity BETWEEN 1 AND 1000000),
  available integer NOT NULL CHECK (available >= 0 AND available <= capacity),
  unit_price integer NOT NULL CHECK (unit_price BETWEEN 1 AND 100000000),
  currency text NOT NULL DEFAULT 'RUB' CHECK (currency = 'RUB'),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX events_organizer_idx ON events(organizer_id, starts_at, id);
CREATE INDEX events_starts_idx ON events(starts_at, id);
CREATE TABLE catalog_revision (id boolean PRIMARY KEY DEFAULT true CHECK (id), revision bigint NOT NULL DEFAULT 1);
INSERT INTO catalog_revision(id) VALUES (true);

CREATE TABLE orders (
  id uuid PRIMARY KEY,
  buyer_id uuid NOT NULL REFERENCES users(id),
  event_id uuid NOT NULL REFERENCES events(id),
  organizer_id uuid NOT NULL REFERENCES users(id),
  quantity integer NOT NULL CHECK (quantity BETWEEN 1 AND 100),
  unit_price integer NOT NULL CHECK (unit_price BETWEEN 1 AND 100000000),
  total_price bigint NOT NULL CHECK (total_price = unit_price::bigint * quantity),
  currency text NOT NULL DEFAULT 'RUB' CHECK (currency = 'RUB'),
  event_title text NOT NULL,
  venue text NOT NULL,
  starts_at timestamptz NOT NULL,
  status text NOT NULL DEFAULT 'confirmed' CHECK (status = 'confirmed'),
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX orders_buyer_idx ON orders(buyer_id, created_at DESC, id);
CREATE INDEX orders_event_idx ON orders(event_id);
CREATE TABLE idempotency_keys (
  buyer_id uuid NOT NULL REFERENCES users(id),
  operation text NOT NULL,
  key text NOT NULL CHECK (length(key) BETWEEN 1 AND 128),
  payload_hash text NOT NULL,
  result jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (buyer_id, operation, key)
);
CREATE TABLE outbox_messages (
  message_id uuid PRIMARY KEY,
  order_id uuid NOT NULL UNIQUE REFERENCES orders(id),
  payload jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE outbox_deliveries (
  message_id uuid NOT NULL REFERENCES outbox_messages(message_id),
  destination text NOT NULL CHECK (destination IN ('rabbitmq', 'kafka')),
  status text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'leased', 'delivered')),
  attempts integer NOT NULL DEFAULT 0,
  available_at timestamptz NOT NULL DEFAULT now(),
  lease_until timestamptz,
  lease_owner uuid,
  delivered_at timestamptz,
  last_error text,
  PRIMARY KEY (message_id, destination)
);
CREATE INDEX outbox_pending_idx ON outbox_deliveries(destination, available_at, message_id) WHERE status <> 'delivered';
