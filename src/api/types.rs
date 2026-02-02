use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

// ============================================================
// Hex conversion helpers
// ============================================================

pub fn bytes_to_hex(bytes: &[u8]) -> String {
    format!("0x{}", hex::encode(bytes))
}

pub fn hex_to_bytes(hex_str: &str) -> Result<Vec<u8>, String> {
    let stripped = hex_str.strip_prefix("0x").unwrap_or(hex_str);
    hex::decode(stripped).map_err(|e| format!("Invalid hex address: {}", e))
}

// ============================================================
// Query params
// ============================================================

#[derive(Debug, Deserialize)]
pub struct ChainFilter {
    pub chain_id: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct PaginatedChainFilter {
    pub chain_id: Option<i64>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct SimilarParams {
    pub chain_id: Option<i64>,
    pub limit: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct TransferParams {
    pub chain_id: Option<i64>,
    pub from: Option<String>,
    pub to: Option<String>,
    pub token: Option<String>,
    pub min_amount: Option<f64>,
    pub since: Option<String>,
    pub until: Option<String>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct AnomalyParams {
    pub chain_id: Option<i64>,
    #[serde(rename = "type")]
    pub anomaly_type: Option<String>,
    pub min_risk: Option<f64>,
    pub address: Option<String>,
    pub resolved: Option<bool>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct EntityParams {
    #[serde(rename = "type")]
    pub entity_type: Option<String>,
    pub source: Option<String>,
    pub search: Option<String>,
    pub limit: Option<i64>,
}

// ============================================================
// Response types
// ============================================================

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub total_transfers: i64,
    pub indexed_chains: Vec<ChainStatus>,
}

#[derive(Debug, Serialize)]
pub struct ChainStatus {
    pub chain_id: i64,
    pub last_block: i64,
}

#[derive(Debug, Serialize)]
pub struct StatsResponse {
    pub total_transfers: i64,
    pub total_wallets: i64,
    pub total_anomalies: i64,
    pub chains: Vec<ChainStats>,
}

#[derive(Debug, Serialize)]
pub struct ChainStats {
    pub chain_id: i64,
    pub last_block: i64,
    pub transfer_count: i64,
}

#[derive(Debug, Serialize)]
pub struct WalletProfileResponse {
    pub address: String,
    pub first_seen: Option<FirstSeenInfo>,
    pub labels: Vec<LabelInfo>,
    pub cluster_id: Option<i64>,
    pub graph_summary: GraphSummary,
    pub anomaly_count: i64,
    pub max_risk_score: f64,
}

#[derive(Debug, Serialize)]
pub struct FirstSeenInfo {
    pub chain_id: i64,
    pub at: DateTime<Utc>,
    pub block: i64,
    pub direction: String,
}

#[derive(Debug, Serialize)]
pub struct LabelInfo {
    pub entity_name: String,
    pub entity_type: String,
    pub source: String,
    pub confidence: f32,
}

#[derive(Debug, Serialize)]
pub struct GraphSummary {
    pub outgoing_count: i64,
    pub incoming_count: i64,
    pub total_sent: BigDecimal,
    pub total_received: BigDecimal,
}

#[derive(Debug, Serialize)]
pub struct WalletJourneyResponse {
    pub address: String,
    pub journey: Vec<JourneyEntry>,
    pub entity_sequence: Vec<String>,
    pub total: i64,
}

#[derive(Debug, Serialize)]
pub struct JourneyEntry {
    pub timestamp: DateTime<Utc>,
    pub direction: String,
    pub counterparty: String,
    pub entity_name: Option<String>,
    pub entity_type: Option<String>,
    pub amount: BigDecimal,
    pub token: String,
    pub chain_id: i64,
    pub tx_hash: String,
}

#[derive(Debug, Serialize)]
pub struct FingerprintResponse {
    pub address: String,
    pub entity_type_distribution: Vec<EntityTypeCount>,
    pub entity_sequence: Vec<String>,
    pub total_transfers: i64,
    pub avg_transfer_amount: Option<BigDecimal>,
    pub active_days: i64,
    pub chains_used: i64,
    pub first_activity: Option<DateTime<Utc>>,
    pub last_activity: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize)]
pub struct EntityTypeCount {
    pub entity_type: String,
    pub count: i64,
}

#[derive(Debug, Serialize)]
pub struct SimilarWalletsResponse {
    pub address: String,
    pub similar_wallets: Vec<SimilarWallet>,
}

#[derive(Debug, Serialize)]
pub struct SimilarWallet {
    pub address: String,
    pub similarity_score: f64,
    pub match_reasons: Vec<String>,
    pub shared_entities: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct TransfersResponse {
    pub transfers: Vec<TransferEntry>,
    pub total: i64,
    pub limit: i64,
    pub offset: i64,
}

#[derive(Debug, Serialize)]
pub struct TransferEntry {
    pub id: i64,
    pub chain_id: i64,
    pub block_number: i64,
    pub tx_hash: String,
    pub from_address: String,
    pub to_address: String,
    pub amount: BigDecimal,
    pub token: String,
    pub timestamp: DateTime<Utc>,
    pub from_entity: Option<String>,
    pub to_entity: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct AnomaliesResponse {
    pub anomalies: Vec<AnomalyEntry>,
    pub total: i64,
    pub limit: i64,
    pub offset: i64,
}

#[derive(Debug, Serialize)]
pub struct AnomalyEntry {
    pub id: i64,
    pub chain_id: i64,
    pub anomaly_type: String,
    pub risk_score: f64,
    pub flags: Vec<String>,
    pub address: String,
    pub detected_at: DateTime<Utc>,
    pub resolved: bool,
}

#[derive(Debug, Serialize)]
pub struct EntitiesResponse {
    pub entities: Vec<EntityEntry>,
    pub total: i64,
}

#[derive(Debug, Serialize)]
pub struct EntityEntry {
    pub address: String,
    pub chain_id: Option<i64>,
    pub entity_name: String,
    pub entity_type: String,
    pub source: String,
    pub confidence: f32,
}

#[derive(Debug, Serialize)]
pub struct ClusterResponse {
    pub cluster_id: i64,
    pub chain_id: i64,
    pub addresses: Vec<String>,
    pub size: usize,
}

// ============================================================
// DeFi Events
// ============================================================

#[derive(Debug, Deserialize)]
pub struct DefiParams {
    pub chain_id: Option<i64>,
    pub protocol: Option<String>,
    pub event_type: Option<String>,
    pub account: Option<String>,
    pub since: Option<String>,
    pub until: Option<String>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct DefiEventsResponse {
    pub events: Vec<DefiEventEntry>,
    pub total: i64,
    pub limit: i64,
    pub offset: i64,
}

#[derive(Debug, Serialize)]
pub struct DefiEventEntry {
    pub id: i64,
    pub chain_id: i64,
    pub block_number: i64,
    pub tx_hash: String,
    pub log_index: i32,
    pub protocol: String,
    pub event_type: String,
    pub contract_address: String,
    pub account: Option<String>,
    pub token_in: Option<String>,
    pub token_out: Option<String>,
    pub amount_in: Option<BigDecimal>,
    pub amount_out: Option<BigDecimal>,
    pub timestamp: DateTime<Utc>,
    pub raw_data: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
pub struct TxContextResponse {
    pub tx_hash: String,
    pub transfers: Vec<TransferEntry>,
    pub defi_events: Vec<DefiEventEntry>,
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
}
