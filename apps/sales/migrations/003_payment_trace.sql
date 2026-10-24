ALTER TABLE payments ADD COLUMN trace_context jsonb NOT NULL DEFAULT '{}';
