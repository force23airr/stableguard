use sqlx::PgPool;
use std::collections::HashMap;

use crate::indexer::types::StablecoinTransfer;
use crate::onramp::registry::ProviderWalletInfo;

/// Match a batch of transfers against known provider wallets and record attributions.
/// For each transfer where from_address or to_address matches a known exchange wallet,
/// insert a record into onramp_transfers.
pub async fn attribute_onramp_transfers(
    pool: &PgPool,
    chain_name: &str,
    transfers: &[StablecoinTransfer],
    wallet_index: &HashMap<(String, Vec<u8>), ProviderWalletInfo>,
) -> eyre::Result<u64> {
    let mut attributed = 0u64;

    for transfer in transfers {
        let chain_key = chain_name.to_string();

        // Check if from_address is a known provider wallet (withdrawal: exchange -> user)
        if let Some(info) = wallet_index.get(&(chain_key.clone(), transfer.from_address.clone())) {
            record_attribution(pool, transfer, info.provider_id, "withdrawal").await?;
            attributed += 1;
            tracing::debug!(
                provider = %info.provider_name,
                direction = "withdrawal",
                tx_hash = hex::encode(&transfer.tx_hash),
                "Attributed transfer to on-ramp provider"
            );
            continue;
        }

        // Check if to_address is a known provider wallet (deposit: user -> exchange)
        if let Some(info) = wallet_index.get(&(chain_key, transfer.to_address.clone())) {
            record_attribution(pool, transfer, info.provider_id, "deposit").await?;
            attributed += 1;
            tracing::debug!(
                provider = %info.provider_name,
                direction = "deposit",
                tx_hash = hex::encode(&transfer.tx_hash),
                "Attributed transfer to on-ramp provider"
            );
        }
    }

    Ok(attributed)
}

/// Insert an attribution record linking a transfer to a provider.
/// Uses the transfer's chain_id + tx_hash + log_index to find the transfer ID.
async fn record_attribution(
    pool: &PgPool,
    transfer: &StablecoinTransfer,
    provider_id: i32,
    direction: &str,
) -> eyre::Result<()> {
    // Look up the transfer ID (it was just inserted in the same batch)
    let row: Option<(i64,)> = sqlx::query_as(
        "SELECT id FROM transfers
         WHERE chain_id = $1 AND tx_hash = $2 AND log_index = $3",
    )
    .bind(transfer.chain_id)
    .bind(&transfer.tx_hash)
    .bind(transfer.log_index)
    .fetch_optional(pool)
    .await?;

    if let Some((transfer_id,)) = row {
        sqlx::query(
            "INSERT INTO onramp_transfers (transfer_id, provider_id, direction)
             VALUES ($1, $2, $3)
             ON CONFLICT (transfer_id) DO NOTHING",
        )
        .bind(transfer_id)
        .bind(provider_id)
        .bind(direction)
        .execute(pool)
        .await?;
    }

    Ok(())
}
