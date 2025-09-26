-- Chains & heads
CREATE TABLE chain_head (
  chain_id INT PRIMARY KEY,
  tip_number BIGINT NOT NULL,
  tip_hash BYTEA NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Blocks
CREATE TABLE blocks (
  chain_id INT NOT NULL,
  number BIGINT NOT NULL,
  hash BYTEA PRIMARY KEY,
  parent_hash BYTEA NOT NULL,
  ts TIMESTAMPTZ NOT NULL,
  canonical BOOLEAN NOT NULL
);
CREATE INDEX idx_blocks_chain_number ON blocks(chain_id, number);

-- Raw logs (one row per log)
CREATE TABLE evm_logs (
  id BIGSERIAL PRIMARY KEY,
  chain_id INT NOT NULL,
  block_hash BYTEA NOT NULL,
  block_number BIGINT NOT NULL,
  tx_hash BYTEA NOT NULL,
  log_index INT NOT NULL,
  address BYTEA NOT NULL,
  topic0 BYTEA,
  topic1 BYTEA,
  topic2 BYTEA,
  topic3 BYTEA,
  data BYTEA NOT NULL,
  status TEXT NOT NULL CHECK(status IN ('UNCONFIRMED','CONFIRMED','RETRACTED')),
  UNIQUE(chain_id, tx_hash, log_index, block_hash)
);
CREATE INDEX idx_evm_logs_chain_block ON evm_logs(chain_id, block_number);
CREATE INDEX idx_evm_logs_status ON evm_logs(status);

-- Subscriptions (multi-tenant)
CREATE TABLE subscriptions (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant TEXT NOT NULL,
  chain_id INT NOT NULL,
  url TEXT NOT NULL,
  secret TEXT NOT NULL,
  min_confs INT NOT NULL DEFAULT 12,
  address BYTEA,
  topic0 BYTEA,
  topic1 BYTEA,
  topic2 BYTEA,
  topic3 BYTEA,
  from_block BIGINT,
  created_at TIMESTAMPTZ DEFAULT now(),
  active BOOLEAN DEFAULT true
);
CREATE INDEX idx_subscriptions_filters ON subscriptions(active, chain_id, address, topic0);

-- Outbox (transactional publish)
CREATE TABLE outbox (
  id BIGSERIAL PRIMARY KEY,
  topic TEXT NOT NULL,
  payload JSONB NOT NULL,
  created_at TIMESTAMPTZ DEFAULT now(),
  published_at TIMESTAMPTZ
);
CREATE INDEX idx_outbox_unpublished ON outbox(published_at) WHERE published_at IS NULL;

-- Deliveries (audit + idempotency)
CREATE TABLE deliveries (
  id BIGSERIAL PRIMARY KEY,
  subscription_id UUID NOT NULL REFERENCES subscriptions(id) ON DELETE CASCADE,
  event_id TEXT NOT NULL,
  attempt INT NOT NULL,
  status TEXT NOT NULL CHECK(status IN ('PENDING','OK','FAILED')),
  next_attempt_at TIMESTAMPTZ,
  last_error TEXT,
  UNIQUE(subscription_id, event_id)
);
CREATE INDEX idx_deliveries_pending ON deliveries(status, next_attempt_at) WHERE status = 'PENDING';