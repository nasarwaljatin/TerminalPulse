-- Terminals: one row per device, holding the latest reported health.
CREATE TABLE IF NOT EXISTS terminals (
  id            text PRIMARY KEY,
  merchant_id   text NOT NULL,
  model         text,
  firmware      text,
  city          text,
  lat           double precision,
  lng           double precision,
  first_seen    timestamptz NOT NULL DEFAULT now(),
  last_seen     timestamptz,
  battery       smallint,
  signal_dbm    smallint,
  temperature_c real,
  queue_depth   integer,
  boot_count    integer,
  updated_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS terminals_last_seen_idx ON terminals (last_seen);
CREATE INDEX IF NOT EXISTS terminals_city_idx ON terminals (city);

-- Transactions: exactly one row per idempotency key, however many times the
-- message is redelivered (MQTT QoS 1 retries, stream reclaims, simulator dups).
CREATE TABLE IF NOT EXISTS transactions (
  id               bigserial PRIMARY KEY,
  idempotency_key  text NOT NULL,
  terminal_id      text NOT NULL,
  merchant_id      text NOT NULL,
  amount_minor     bigint NOT NULL CHECK (amount_minor >= 0),
  currency         char(3) NOT NULL,
  method           text NOT NULL,
  status           text NOT NULL CHECK (status IN ('approved', 'declined', 'error')),
  error_code       text,
  auth_latency_ms  integer,
  device_ts        timestamptz NOT NULL,
  received_at      timestamptz NOT NULL,
  persisted_at     timestamptz NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT transactions_idempotency_key_uq UNIQUE (idempotency_key)
);
CREATE INDEX IF NOT EXISTS transactions_terminal_ts_idx ON transactions (terminal_id, device_ts DESC);
CREATE INDEX IF NOT EXISTS transactions_persisted_idx ON transactions (persisted_at);

-- Heartbeats: short-retention health samples used for anomaly features.
CREATE TABLE IF NOT EXISTS heartbeats (
  terminal_id   text NOT NULL,
  device_ts     timestamptz NOT NULL,
  battery       smallint,
  signal_dbm    smallint,
  temperature_c real,
  queue_depth   integer,
  boot_count    integer,
  PRIMARY KEY (terminal_id, device_ts)
);
CREATE INDEX IF NOT EXISTS heartbeats_ts_idx ON heartbeats (device_ts);

-- Latest anomaly verdict per terminal (Isolation Forest).
CREATE TABLE IF NOT EXISTS anomalies (
  terminal_id  text PRIMARY KEY,
  score        double precision NOT NULL,
  flagged      boolean NOT NULL,
  reasons      jsonb NOT NULL DEFAULT '[]'::jsonb,
  features     jsonb NOT NULL DEFAULT '{}'::jsonb,
  scored_at    timestamptz NOT NULL DEFAULT now()
);

-- Remote commands sent to terminals over MQTT, acknowledged back through ingest.
CREATE TABLE IF NOT EXISTS commands (
  id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  terminal_id   text NOT NULL,
  command       text NOT NULL,
  status        text NOT NULL DEFAULT 'sent' CHECK (status IN ('sent', 'acked', 'failed')),
  requested_by  text,
  result        text,
  created_at    timestamptz NOT NULL DEFAULT now(),
  acked_at      timestamptz
);
CREATE INDEX IF NOT EXISTS commands_terminal_idx ON commands (terminal_id, created_at DESC);
