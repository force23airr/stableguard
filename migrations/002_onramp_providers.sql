-- On-ramp providers: exchanges and fiat-to-crypto services
CREATE TABLE IF NOT EXISTS onramp_providers (
    id              SERIAL       PRIMARY KEY,
    name            VARCHAR(64)  NOT NULL UNIQUE,
    provider_type   VARCHAR(16)  NOT NULL, -- 'exchange', 'onramp', 'p2p'
    website         VARCHAR(256),
    kyc_required    BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Fiat currencies supported by on-ramp providers
CREATE TABLE IF NOT EXISTS provider_fiat_currencies (
    provider_id     INTEGER      NOT NULL REFERENCES onramp_providers(id),
    currency_code   VARCHAR(6)   NOT NULL,
    PRIMARY KEY (provider_id, currency_code)
);

-- Known exchange/provider wallet addresses for attribution
CREATE TABLE IF NOT EXISTS provider_wallets (
    id              SERIAL       PRIMARY KEY,
    provider_id     INTEGER      NOT NULL REFERENCES onramp_providers(id),
    chain_name      VARCHAR(32)  NOT NULL,
    address         BYTEA        NOT NULL,
    label           VARCHAR(128),
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (chain_name, address)
);

CREATE INDEX IF NOT EXISTS idx_provider_wallets_address ON provider_wallets (address);
CREATE INDEX IF NOT EXISTS idx_provider_wallets_chain ON provider_wallets (chain_name);

-- Fiat currency registry with risk metadata
CREATE TABLE IF NOT EXISTS fiat_currencies (
    code                VARCHAR(6)   PRIMARY KEY,
    name                VARCHAR(64)  NOT NULL,
    country             VARCHAR(64)  NOT NULL,
    region              VARCHAR(32)  NOT NULL, -- 'americas', 'europe', 'africa', 'asia_pacific', 'middle_east'
    primary_stablecoin  VARCHAR(10)  NOT NULL, -- most commonly used stablecoin for this currency
    risk_tier           VARCHAR(16)  NOT NULL DEFAULT 'medium' -- 'low', 'medium', 'high', 'critical'
);

-- Track which transfers originated from known on-ramp providers
-- This is populated by matching transfer from/to addresses against provider_wallets
CREATE TABLE IF NOT EXISTS onramp_transfers (
    transfer_id     BIGINT       NOT NULL REFERENCES transfers(id) ON DELETE CASCADE,
    provider_id     INTEGER      NOT NULL REFERENCES onramp_providers(id),
    direction       VARCHAR(8)   NOT NULL, -- 'deposit' (user -> exchange) or 'withdrawal' (exchange -> user)
    PRIMARY KEY (transfer_id)
);

CREATE INDEX IF NOT EXISTS idx_onramp_transfers_provider ON onramp_transfers (provider_id);

-- Widen symbol columns to support longer symbols like "USDC.e"
ALTER TABLE known_tokens ALTER COLUMN symbol TYPE VARCHAR(16);
ALTER TABLE transfers ALTER COLUMN token_symbol TYPE VARCHAR(16);
