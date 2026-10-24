CREATE TABLE provider_payments (
 id uuid PRIMARY KEY, order_id uuid NOT NULL UNIQUE, amount bigint NOT NULL CHECK(amount>0),
 status text NOT NULL CHECK(status IN ('pending','succeeded','failed','refunded')),
 version integer NOT NULL DEFAULT 1, captured_at timestamptz, refunded_at timestamptz,
 settle_at timestamptz, refund_at timestamptz,
 scenario text NOT NULL DEFAULT 'success', callback_url text NOT NULL, trace_context jsonb NOT NULL DEFAULT '{}'
);
CREATE TABLE provider_scenarios (
 order_id uuid PRIMARY KEY, mode text NOT NULL CHECK(mode IN ('success','decline','lost_response','duplicate_webhook','delayed_webhook','no_webhook','refund_failure')),
 delay_ms integer NOT NULL DEFAULT 500 CHECK(delay_ms BETWEEN 0 AND 60000)
);
CREATE TABLE provider_webhooks (
 id uuid PRIMARY KEY, payment_id uuid NOT NULL REFERENCES provider_payments(id), body jsonb NOT NULL,
 available_at timestamptz NOT NULL DEFAULT now(), delivered boolean NOT NULL DEFAULT false, attempts integer NOT NULL DEFAULT 0,
 lease_owner uuid, lease_until timestamptz, trace_context jsonb NOT NULL DEFAULT '{}'
);
