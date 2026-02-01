use sqlx::PgPool;

use crate::indexer::types::StablecoinTransfer;

use super::label_store::EntityLabelStore;

/// Match a batch of transfers against known entity labels.
/// For each transfer where from_address or to_address has a known label,
/// insert a record into transfer_entity_flags.
pub async fn attribute_entities(
    pool: &PgPool,
    transfers: &[StablecoinTransfer],
    label_store: &EntityLabelStore,
) -> eyre::Result<u64> {
    let mut attributed = 0u64;

    for transfer in transfers {
        // Check from_address
        if let Some(labels) = label_store.lookup(&transfer.from_address) {
            for label in labels {
                // Check chain scope: label applies if chain_id is None (global) or matches
                if label.chain_id.is_some() && label.chain_id != Some(transfer.chain_id) {
                    continue;
                }
                insert_flag(pool, transfer, label.id, "from").await?;
                attributed += 1;

                tracing::debug!(
                    entity = %label.entity_name,
                    entity_type = %label.entity_type,
                    side = "from",
                    "Attributed entity to transfer"
                );
            }
        }

        // Check to_address
        if let Some(labels) = label_store.lookup(&transfer.to_address) {
            for label in labels {
                if label.chain_id.is_some() && label.chain_id != Some(transfer.chain_id) {
                    continue;
                }
                insert_flag(pool, transfer, label.id, "to").await?;
                attributed += 1;

                tracing::debug!(
                    entity = %label.entity_name,
                    entity_type = %label.entity_type,
                    side = "to",
                    "Attributed entity to transfer"
                );
            }
        }
    }

    Ok(attributed)
}

/// Insert a transfer_entity_flag record linking a transfer to an entity label.
async fn insert_flag(
    pool: &PgPool,
    transfer: &StablecoinTransfer,
    entity_label_id: i32,
    side: &str,
) -> eyre::Result<()> {
    // Look up the transfer ID
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
            "INSERT INTO transfer_entity_flags (transfer_id, entity_label_id, side)
             VALUES ($1, $2, $3)
             ON CONFLICT (transfer_id, entity_label_id, side) DO NOTHING",
        )
        .bind(transfer_id)
        .bind(entity_label_id)
        .bind(side)
        .execute(pool)
        .await?;
    }

    Ok(())
}
