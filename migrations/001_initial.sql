-- ChainWatch: Initial schema for multi-chain stablecoin indexer

-- Known stablecoin tokens per chain
CREATE TABLE IF NOT EXISTS known_tokens (
    chain_id       BIGINT       NOT NULL,
    token_address  BYTEA        NOT NULL,
    symbol         VARCHAR(10)  NOT NULL,
    decimals       SMALLINT     NOT NULL,
    PRIMARY KEY (chain_id, token_address)
);

-- Indexer checkpoint state for resumability
CREATE TABLE IF NOT EXISTS indexer_state (
    chain_id            BIGINT  PRIMARY KEY,
    last_indexed_block  BIGINT  NOT NULL,
    last_block_hash     BYTEA,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Core transfer event table
CREATE TABLE IF NOT EXISTS transfers (
    id              BIGSERIAL    PRIMARY KEY,
    chain_id        BIGINT       NOT NULL,
    block_number    BIGINT       NOT NULL,
    block_hash      BYTEA        NOT NULL,
    tx_hash         BYTEA        NOT NULL,
    log_index       INTEGER      NOT NULL,
    token_address   BYTEA        NOT NULL,
    from_address    BYTEA        NOT NULL,
    to_address      BYTEA        NOT NULL,
    amount          NUMERIC      NOT NULL,
    token_symbol    VARCHAR(10)  NOT NULL,
    token_decimals  SMALLINT     NOT NULL,
    block_timestamp TIMESTAMPTZ  NOT NULL,

    UNIQUE (chain_id, tx_hash, log_index)
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_transfers_chain_block ON transfers (chain_id, block_number);
CREATE INDEX IF NOT EXISTS idx_transfers_from ON transfers (from_address);
CREATE INDEX IF NOT EXISTS idx_transfers_to ON transfers (to_address);
CREATE INDEX IF NOT EXISTS idx_transfers_token ON transfers (token_address);
CREATE INDEX IF NOT EXISTS idx_transfers_timestamp ON transfers (block_timestamp);
CREATE INDEX IF NOT EXISTS idx_transfers_tx_hash ON transfers (tx_hash);

-- Block hash tracking for reorg detection
CREATE TABLE IF NOT EXISTS block_hashes (
    chain_id     BIGINT NOT NULL,
    block_number BIGINT NOT NULL,
    block_hash   BYTEA  NOT NULL,
    parent_hash  BYTEA  NOT NULL,
    PRIMARY KEY (chain_id, block_number)
);
