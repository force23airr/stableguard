-- Track first-seen timestamp for every address per chain
CREATE TABLE IF NOT EXISTS wallet_first_seen (
    address         BYTEA        NOT NULL,
    chain_id        BIGINT       NOT NULL,
    first_seen_at   TIMESTAMPTZ  NOT NULL,
    first_block     BIGINT       NOT NULL,
    first_tx_hash   BYTEA,
    first_direction VARCHAR(4)   NOT NULL,
    PRIMARY KEY (address, chain_id)
);

CREATE INDEX IF NOT EXISTS idx_wallet_first_seen_time ON wallet_first_seen (first_seen_at);
CREATE INDEX IF NOT EXISTS idx_wallet_first_seen_chain ON wallet_first_seen (chain_id, first_seen_at);

-- Wallet transaction graph: edges between addresses
CREATE TABLE IF NOT EXISTS wallet_graph_edges (
    source_address  BYTEA        NOT NULL,
    dest_address    BYTEA        NOT NULL,
    chain_id        BIGINT       NOT NULL,
    transfer_count  BIGINT       NOT NULL DEFAULT 1,
    total_amount    NUMERIC      NOT NULL DEFAULT 0,
    first_seen      TIMESTAMPTZ  NOT NULL,
    last_seen       TIMESTAMPTZ  NOT NULL,
    PRIMARY KEY (source_address, dest_address, chain_id)
);

CREATE INDEX IF NOT EXISTS idx_graph_edges_source ON wallet_graph_edges (source_address);
CREATE INDEX IF NOT EXISTS idx_graph_edges_dest ON wallet_graph_edges (dest_address);
CREATE INDEX IF NOT EXISTS idx_graph_edges_last_seen ON wallet_graph_edges (last_seen);

-- Wallet clusters: group related wallets
CREATE TABLE IF NOT EXISTS wallet_clusters (
    address         BYTEA        NOT NULL,
    chain_id        BIGINT       NOT NULL,
    cluster_id      BIGINT       NOT NULL,
    assigned_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (address, chain_id)
);

CREATE INDEX IF NOT EXISTS idx_wallet_clusters_cluster ON wallet_clusters (cluster_id);
