use alloy::primitives::Address;
use alloy::rpc::types::Log;
use alloy::sol;
use alloy::sol_types::SolEvent;
use bigdecimal::BigDecimal;
use std::collections::HashMap;
use std::str::FromStr;

use super::types::TokenMeta;

// Generate the Transfer event ABI using alloy's sol! macro.
// This gives us Transfer::SIGNATURE_HASH and a typed decoder.
sol! {
    event Transfer(address indexed from, address indexed to, uint256 value);
}

/// Decoded transfer data before being combined with block info.
#[derive(Debug)]
pub struct DecodedTransfer {
    pub from: Address,
    pub to: Address,
    pub amount: BigDecimal,
    pub token_address: Address,
    pub token_symbol: String,
    pub token_decimals: i16,
    pub log_index: u64,
    pub tx_hash: alloy::primitives::B256,
}

/// Attempt to decode a log as an ERC-20 Transfer event.
///
/// Returns `None` if:
/// - The log's contract address is not in our watched set
/// - The log doesn't match the Transfer event signature
/// - Decoding fails (malformed log)
pub fn decode_transfer_log(
    log: &Log,
    watched_tokens: &HashMap<Address, TokenMeta>,
) -> Option<DecodedTransfer> {
    let inner = log.inner.clone();

    // Check if this log's contract address is a watched stablecoin
    let token_meta = watched_tokens.get(&inner.address)?;

    // Check topic[0] matches Transfer event signature
    let topics = inner.data.topics();
    if topics.is_empty() || topics[0] != Transfer::SIGNATURE_HASH {
        return None;
    }

    // We need exactly 3 topics (signature + from + to) and 32 bytes of data (value)
    if topics.len() != 3 {
        return None;
    }

    // Decode indexed parameters from topics
    let from = Address::from_word(topics[1]);
    let to = Address::from_word(topics[2]);

    // Decode the value from log data
    let data = inner.data.data.as_ref();
    if data.len() < 32 {
        return None;
    }

    let value = alloy::primitives::U256::from_be_slice(&data[..32]);
    let amount = BigDecimal::from_str(&value.to_string()).ok()?;

    let tx_hash = log.transaction_hash.unwrap_or_default();
    let log_index = log.log_index.unwrap_or(0);

    Some(DecodedTransfer {
        from,
        to,
        amount,
        token_address: inner.address,
        token_symbol: token_meta.symbol.clone(),
        token_decimals: token_meta.decimals,
        log_index,
        tx_hash,
    })
}
