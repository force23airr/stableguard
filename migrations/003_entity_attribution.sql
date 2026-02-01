-- Entity labels: reusable identities applied to wallet addresses
CREATE TABLE IF NOT EXISTS entity_labels (
    id              SERIAL       PRIMARY KEY,
    address         BYTEA        NOT NULL,
    chain_id        BIGINT,                          -- NULL = applies to all chains
    entity_name     VARCHAR(256) NOT NULL,
    entity_type     VARCHAR(32)  NOT NULL,            -- 'exchange','company','individual','contract','mixer','sanctioned','unknown'
    label_source    VARCHAR(64)  NOT NULL,            -- 'ofac_sdn','config','heuristic','custom_watchlist'
    confidence      REAL         NOT NULL DEFAULT 1.0,
    metadata        JSONB,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (address, chain_id, label_source, entity_name)
);

CREATE INDEX IF NOT EXISTS idx_entity_labels_address ON entity_labels (address);
CREATE INDEX IF NOT EXISTS idx_entity_labels_type ON entity_labels (entity_type);
CREATE INDEX IF NOT EXISTS idx_entity_labels_source ON entity_labels (label_source);

-- Sanctions/watchlist entries parsed from OFAC SDN or custom lists
CREATE TABLE IF NOT EXISTS watchlist_entries (
    id              SERIAL       PRIMARY KEY,
    list_name       VARCHAR(64)  NOT NULL,
    address         BYTEA        NOT NULL,
    entity_name     VARCHAR(256),
    sdn_id          VARCHAR(32),
    program         VARCHAR(128),
    metadata        JSONB,
    added_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (list_name, address)
);

CREATE INDEX IF NOT EXISTS idx_watchlist_address ON watchlist_entries (address);

-- Transfer-level entity attribution
CREATE TABLE IF NOT EXISTS transfer_entity_flags (
    id              BIGSERIAL    PRIMARY KEY,
    transfer_id     BIGINT       NOT NULL REFERENCES transfers(id) ON DELETE CASCADE,
    entity_label_id INTEGER      NOT NULL REFERENCES entity_labels(id),
    side            VARCHAR(4)   NOT NULL,
    UNIQUE (transfer_id, entity_label_id, side)
);

CREATE INDEX IF NOT EXISTS idx_transfer_entity_flags_transfer ON transfer_entity_flags (transfer_id);
CREATE INDEX IF NOT EXISTS idx_transfer_entity_flags_entity ON transfer_entity_flags (entity_label_id);
