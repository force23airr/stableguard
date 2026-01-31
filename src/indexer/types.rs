use alloy::primitives::{Address, B256};
use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};

/// A decoded stablecoin Transfer event, ready for DB insertion.
#[derive(Debug, Clone)]
pub struct StablecoinTransfer {
    pub chain_id: i64,
    pub block_number: i64,
    pub block_hash: Vec<u8>,
    pub tx_hash: Vec<u8>,
    pub log_index: i32,
    pub token_address: Vec<u8>,
    pub from_address: Vec<u8>,
    pub to_address: Vec<u8>,
    pub amount: BigDecimal,
    pub token_symbol: String,
    pub token_decimals: i16,
    pub block_timestamp: DateTime<Utc>,
}

/// Minimal block info needed by the indexer.
#[derive(Debug, Clone)]
pub struct BlockInfo {
    pub chain_id: i64,
    pub block_number: u64,
    pub block_hash: B256,
    pub parent_hash: B256,
    pub timestamp: u64,
}

/// Metadata for a watched token.
#[derive(Debug, Clone)]
pub struct TokenMeta {
    pub symbol: String,
    pub decimals: i16,
}
