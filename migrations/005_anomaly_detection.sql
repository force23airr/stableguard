-- Detected anomalies
CREATE TABLE IF NOT EXISTS anomalies (
    id              BIGSERIAL    PRIMARY KEY,
    transfer_id     BIGINT       REFERENCES transfers(id) ON DELETE CASCADE,
    chain_id        BIGINT       NOT NULL,
    anomaly_type    VARCHAR(64)  NOT NULL,
    risk_score      REAL         NOT NULL,
    flags           TEXT[]       NOT NULL DEFAULT '{}',
    details         JSONB,
    address         BYTEA,
    detected_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    resolved        BOOLEAN      NOT NULL DEFAULT FALSE,
    UNIQUE (transfer_id, anomaly_type)
);

CREATE INDEX IF NOT EXISTS idx_anomalies_type ON anomalies (anomaly_type);
CREATE INDEX IF NOT EXISTS idx_anomalies_risk ON anomalies (risk_score DESC);
CREATE INDEX IF NOT EXISTS idx_anomalies_chain ON anomalies (chain_id, detected_at);
CREATE INDEX IF NOT EXISTS idx_anomalies_address ON anomalies (address);
CREATE INDEX IF NOT EXISTS idx_anomalies_unresolved ON anomalies (resolved) WHERE resolved = FALSE;
