CREATE TABLE projection_generations (
  projection text NOT NULL CHECK (projection IN ('events', 'daily')),
  generation uuid NOT NULL,
  status text NOT NULL CHECK (status IN ('building', 'active', 'retired', 'failed')),
  revision bigint NOT NULL DEFAULT 0 CHECK (revision >= 0),
  created_at timestamptz NOT NULL DEFAULT now(),
  activated_at timestamptz,
  failure text,
  PRIMARY KEY (projection, generation)
);

CREATE TABLE projection_active (
  projection text PRIMARY KEY CHECK (projection IN ('events', 'daily')),
  generation uuid NOT NULL,
  FOREIGN KEY (projection, generation) REFERENCES projection_generations(projection, generation)
);

CREATE TABLE message_receipts (
  projection text NOT NULL,
  generation uuid NOT NULL,
  message_id uuid NOT NULL,
  order_id uuid NOT NULL,
  fingerprint text NOT NULL,
  received_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (projection, generation, message_id),
  UNIQUE (projection, generation, order_id),
  FOREIGN KEY (projection, generation) REFERENCES projection_generations(projection, generation)
);

CREATE TABLE event_sales (
  projection text NOT NULL DEFAULT 'events' CHECK (projection = 'events'),
  generation uuid NOT NULL,
  organizer_id uuid NOT NULL,
  event_id uuid NOT NULL,
  title text NOT NULL,
  orders bigint NOT NULL CHECK (orders > 0),
  tickets bigint NOT NULL CHECK (tickets > 0),
  revenue bigint NOT NULL CHECK (revenue >= 0),
  PRIMARY KEY (generation, event_id),
  FOREIGN KEY (projection, generation) REFERENCES projection_generations(projection, generation)
);
CREATE INDEX event_sales_organizer ON event_sales(generation, organizer_id, tickets DESC, event_id DESC);

CREATE TABLE daily_revenue (
  projection text NOT NULL DEFAULT 'daily' CHECK (projection = 'daily'),
  generation uuid NOT NULL,
  organizer_id uuid NOT NULL,
  day date NOT NULL,
  orders bigint NOT NULL CHECK (orders > 0),
  tickets bigint NOT NULL CHECK (tickets > 0),
  revenue bigint NOT NULL CHECK (revenue >= 0),
  PRIMARY KEY (generation, organizer_id, day),
  FOREIGN KEY (projection, generation) REFERENCES projection_generations(projection, generation)
);
