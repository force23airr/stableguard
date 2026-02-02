CREATE TABLE IF NOT EXISTS defi_events (
    id BIGSERIAL PRIMARY KEY,
    chain_id BIGINT NOT NULL,
    block_number BIGINT NOT NULL,
    tx_hash BYTEA NOT NULL,
    log_index INT NOT NULL,
    protocol TEXT NOT NULL,
    event_type TEXT NOT NULL,
    contract_address BYTEA NOT NULL,
    account BYTEA,
    token_in BYTEA,
    token_out BYTEA,
    amount_in NUMERIC,
    amount_out NUMERIC,
    block_timestamp TIMESTAMPTZ NOT NULL,
    raw_data JSONB,
    UNIQUE (chain_id, tx_hash, log_index)
);

CREATE INDEX idx_defi_events_chain_block ON defi_events (chain_id, block_number);
CREATE INDEX idx_defi_events_account ON defi_events (account);
CREATE INDEX idx_defi_events_protocol ON defi_events (protocol);
CREATE INDEX idx_defi_events_event_type ON defi_events (event_type);
CREATE INDEX idx_defi_events_tx_hash ON defi_events (tx_hash);
CREATE INDEX idx_defi_events_timestamp ON defi_events (block_timestamp);
