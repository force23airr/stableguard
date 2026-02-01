use serde_json::Value as JsonValue;

/// Types of anomalies the engine can detect.
#[derive(Debug, Clone)]
pub enum AnomalyType {
    LargeTransfer,
    Velocity,
    SanctionedCounterparty,
    RoundNumber,
    NewWalletLargeReceive,
    CrossChainActivity,
}

impl AnomalyType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::LargeTransfer => "large_transfer",
            Self::Velocity => "velocity",
            Self::SanctionedCounterparty => "sanctioned_counterparty",
            Self::RoundNumber => "round_number",
            Self::NewWalletLargeReceive => "new_wallet_large_receive",
            Self::CrossChainActivity => "cross_chain_activity",
        }
    }
}

/// A detected anomaly ready for database insertion.
#[derive(Debug, Clone)]
pub struct AnomalyRecord {
    pub chain_id: i64,
    pub anomaly_type: AnomalyType,
    pub risk_score: f32,
    pub flags: Vec<String>,
    pub details: JsonValue,
    pub address: Option<Vec<u8>>,
    pub tx_hash: Vec<u8>,
    pub log_index: i32,
}
