use bigdecimal::BigDecimal;
use bigdecimal::ToPrimitive;
use sqlx::PgPool;
use std::collections::HashMap;

use crate::entity::label_store::EntityLabelStore;
use crate::indexer::types::StablecoinTransfer;
use crate::wallet::first_seen::NewWalletEvent;

use super::types::{AnomalyRecord, AnomalyType};

/// Check if a transfer exceeds the large transfer threshold for its token.
pub fn check_large_transfer(
    transfer: &StablecoinTransfer,
    thresholds: &HashMap<String, f64>,
) -> Option<AnomalyRecord> {
    let threshold = thresholds
        .get(&transfer.token_symbol)
        .or_else(|| thresholds.get("default"))
        .copied()
        .unwrap_or(100_000.0);

    // Convert raw amount to human-readable using decimals
    let human_amount = raw_to_human(&transfer.amount, transfer.token_decimals);

    if human_amount >= threshold {
        let risk = if human_amount >= threshold * 10.0 {
            80.0
        } else if human_amount >= threshold * 5.0 {
            60.0
        } else {
            40.0
        };

        return Some(AnomalyRecord {
            chain_id: transfer.chain_id,
            anomaly_type: AnomalyType::LargeTransfer,
            risk_score: risk,
            flags: vec![format!(
                "transfer_amount_{:.0}_{}_exceeds_{:.0}",
                human_amount, transfer.token_symbol, threshold
            )],
            details: serde_json::json!({
                "amount": human_amount,
                "token": transfer.token_symbol,
                "threshold": threshold,
            }),
            address: None,
            tx_hash: transfer.tx_hash.clone(),
            log_index: transfer.log_index,
        });
    }

    None
}

/// Check if the sender has exceeded the velocity limit (too many transfers in a window).
pub async fn check_velocity(
    pool: &PgPool,
    transfer: &StablecoinTransfer,
    window_secs: u64,
    max_transfers: u32,
) -> eyre::Result<Option<AnomalyRecord>> {
    let count: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM transfers
         WHERE from_address = $1
         AND chain_id = $2
         AND block_timestamp > $3 - make_interval(secs => $4)",
    )
    .bind(&transfer.from_address)
    .bind(transfer.chain_id)
    .bind(transfer.block_timestamp)
    .bind(window_secs as f64)
    .fetch_one(pool)
    .await?;

    if count.0 > max_transfers as i64 {
        let risk = if count.0 > (max_transfers * 5) as i64 {
            70.0
        } else {
            50.0
        };

        return Ok(Some(AnomalyRecord {
            chain_id: transfer.chain_id,
            anomaly_type: AnomalyType::Velocity,
            risk_score: risk,
            flags: vec![format!(
                "velocity_{}_transfers_in_{}_secs",
                count.0, window_secs
            )],
            details: serde_json::json!({
                "transfer_count": count.0,
                "window_secs": window_secs,
                "max_allowed": max_transfers,
            }),
            address: Some(transfer.from_address.clone()),
            tx_hash: transfer.tx_hash.clone(),
            log_index: transfer.log_index,
        }));
    }

    Ok(None)
}

/// Check if either counterparty is on a sanctions list.
pub fn check_sanctioned_counterparty(
    transfer: &StablecoinTransfer,
    label_store: &EntityLabelStore,
) -> Option<AnomalyRecord> {
    let from_sanctioned = label_store.is_sanctioned(&transfer.from_address);
    let to_sanctioned = label_store.is_sanctioned(&transfer.to_address);

    if from_sanctioned || to_sanctioned {
        let side = if from_sanctioned { "from" } else { "to" };
        let flagged_address = if from_sanctioned {
            &transfer.from_address
        } else {
            &transfer.to_address
        };

        return Some(AnomalyRecord {
            chain_id: transfer.chain_id,
            anomaly_type: AnomalyType::SanctionedCounterparty,
            risk_score: 95.0,
            flags: vec![format!("sanctioned_{}_address", side)],
            details: serde_json::json!({
                "side": side,
                "sanctioned_address": hex::encode(flagged_address),
            }),
            address: Some(flagged_address.clone()),
            tx_hash: transfer.tx_hash.clone(),
            log_index: transfer.log_index,
        });
    }

    None
}

/// Check if the transfer amount is a suspiciously round number.
pub fn check_round_number(
    transfer: &StablecoinTransfer,
    tolerance: f64,
) -> Option<AnomalyRecord> {
    let human_amount = raw_to_human(&transfer.amount, transfer.token_decimals);

    // Only flag amounts above $1000
    if human_amount < 1000.0 {
        return None;
    }

    // Check if the amount is a round number (divisible by 1000, 5000, 10000, etc.)
    let round_thresholds = [100_000.0, 50_000.0, 25_000.0, 10_000.0, 5_000.0, 1_000.0];

    for &threshold in &round_thresholds {
        if human_amount >= threshold {
            let remainder = human_amount % threshold;
            let fraction = remainder / threshold;
            if fraction < tolerance || fraction > (1.0 - tolerance) {
                let risk = if threshold >= 100_000.0 {
                    40.0
                } else if threshold >= 10_000.0 {
                    30.0
                } else {
                    20.0
                };

                return Some(AnomalyRecord {
                    chain_id: transfer.chain_id,
                    anomaly_type: AnomalyType::RoundNumber,
                    risk_score: risk,
                    flags: vec![format!("round_amount_{:.0}", human_amount)],
                    details: serde_json::json!({
                        "amount": human_amount,
                        "nearest_round": threshold,
                        "token": transfer.token_symbol,
                    }),
                    address: None,
                    tx_hash: transfer.tx_hash.clone(),
                    log_index: transfer.log_index,
                });
            }
        }
    }

    None
}

/// Check if a newly seen wallet is receiving a large amount immediately.
pub fn check_new_wallet_large_receive(
    transfer: &StablecoinTransfer,
    new_wallets: &[NewWalletEvent],
    threshold_usd: f64,
) -> Option<AnomalyRecord> {
    let human_amount = raw_to_human(&transfer.amount, transfer.token_decimals);

    if human_amount < threshold_usd {
        return None;
    }

    // Check if the to_address was just seen for the first time
    let is_new = new_wallets.iter().any(|w| {
        w.address == transfer.to_address
            && w.chain_id == transfer.chain_id
            && w.direction == "to"
    });

    if is_new {
        let risk = if human_amount >= threshold_usd * 10.0 {
            80.0
        } else {
            60.0
        };

        return Some(AnomalyRecord {
            chain_id: transfer.chain_id,
            anomaly_type: AnomalyType::NewWalletLargeReceive,
            risk_score: risk,
            flags: vec![format!(
                "new_wallet_received_{:.0}_{}",
                human_amount, transfer.token_symbol
            )],
            details: serde_json::json!({
                "amount": human_amount,
                "token": transfer.token_symbol,
                "new_wallet": hex::encode(&transfer.to_address),
            }),
            address: Some(transfer.to_address.clone()),
            tx_hash: transfer.tx_hash.clone(),
            log_index: transfer.log_index,
        });
    }

    None
}

/// Check if an address is active on multiple chains within a short window.
pub async fn check_cross_chain_activity(
    pool: &PgPool,
    transfer: &StablecoinTransfer,
    window_secs: u64,
) -> eyre::Result<Option<AnomalyRecord>> {
    // Count distinct chains this address has been active on recently
    let count: (i64,) = sqlx::query_as(
        "SELECT COUNT(DISTINCT chain_id) FROM transfers
         WHERE (from_address = $1 OR to_address = $1)
         AND block_timestamp > $2 - make_interval(secs => $3)",
    )
    .bind(&transfer.from_address)
    .bind(transfer.block_timestamp)
    .bind(window_secs as f64)
    .fetch_one(pool)
    .await?;

    if count.0 >= 3 {
        let risk = if count.0 >= 5 { 50.0 } else { 30.0 };

        return Ok(Some(AnomalyRecord {
            chain_id: transfer.chain_id,
            anomaly_type: AnomalyType::CrossChainActivity,
            risk_score: risk,
            flags: vec![format!(
                "active_on_{}_chains_in_{}_secs",
                count.0, window_secs
            )],
            details: serde_json::json!({
                "chain_count": count.0,
                "window_secs": window_secs,
                "address": hex::encode(&transfer.from_address),
            }),
            address: Some(transfer.from_address.clone()),
            tx_hash: transfer.tx_hash.clone(),
            log_index: transfer.log_index,
        }));
    }

    Ok(None)
}

/// Convert a raw token amount to human-readable using token decimals.
fn raw_to_human(amount: &BigDecimal, decimals: i16) -> f64 {
    let divisor = BigDecimal::from(10u64.pow(decimals as u32));
    let result = amount / divisor;
    result.to_f64().unwrap_or(0.0)
}
