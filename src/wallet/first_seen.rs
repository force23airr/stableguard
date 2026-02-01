use chrono::{DateTime, Utc};
use sqlx::PgPool;
use std::collections::HashSet;

use crate::indexer::types::StablecoinTransfer;

/// Event emitted when a wallet address is seen for the first time on a chain.
#[derive(Debug, Clone)]
pub struct NewWalletEvent {
    pub address: Vec<u8>,
    pub chain_id: i64,
    pub first_seen_at: DateTime<Utc>,
    pub first_block: i64,
    pub first_tx_hash: Vec<u8>,
    pub direction: String, // "from" or "to"
}

/// Tracks first-seen timestamps for wallet addresses.
/// Uses an in-memory HashSet to avoid DB lookups on every transfer.
pub struct WalletTracker {
    known: HashSet<(Vec<u8>, i64)>, // (address, chain_id)
}

impl WalletTracker {
    /// Load all known addresses from the database.
    pub async fn load_from_db(pool: &PgPool) -> eyre::Result<Self> {
        let rows: Vec<(Vec<u8>, i64)> =
            sqlx::query_as("SELECT address, chain_id FROM wallet_first_seen")
                .fetch_all(pool)
                .await?;

        let mut known = HashSet::with_capacity(rows.len());
        for (address, chain_id) in rows {
            known.insert((address, chain_id));
        }

        tracing::info!(wallets = known.len(), "Loaded wallet tracker");
        Ok(Self { known })
    }

    /// Process a batch of transfers, detecting new wallet addresses.
    /// Returns a list of NewWalletEvent for addresses seen for the first time.
    pub async fn process_transfers(
        &mut self,
        pool: &PgPool,
        transfers: &[StablecoinTransfer],
    ) -> eyre::Result<Vec<NewWalletEvent>> {
        let mut new_wallets = Vec::new();

        for transfer in transfers {
            // Check from_address
            let from_key = (transfer.from_address.clone(), transfer.chain_id);
            if !self.known.contains(&from_key) {
                self.known.insert(from_key);

                let event = NewWalletEvent {
                    address: transfer.from_address.clone(),
                    chain_id: transfer.chain_id,
                    first_seen_at: transfer.block_timestamp,
                    first_block: transfer.block_number,
                    first_tx_hash: transfer.tx_hash.clone(),
                    direction: "from".to_string(),
                };

                upsert_first_seen(pool, &event).await?;
                new_wallets.push(event);
            }

            // Check to_address
            let to_key = (transfer.to_address.clone(), transfer.chain_id);
            if !self.known.contains(&to_key) {
                self.known.insert(to_key);

                let event = NewWalletEvent {
                    address: transfer.to_address.clone(),
                    chain_id: transfer.chain_id,
                    first_seen_at: transfer.block_timestamp,
                    first_block: transfer.block_number,
                    first_tx_hash: transfer.tx_hash.clone(),
                    direction: "to".to_string(),
                };

                upsert_first_seen(pool, &event).await?;
                new_wallets.push(event);
            }
        }

        if !new_wallets.is_empty() {
            tracing::debug!(count = new_wallets.len(), "New wallets detected");
        }

        Ok(new_wallets)
    }
}

/// Insert a first-seen record. Uses ON CONFLICT to keep the earliest sighting.
async fn upsert_first_seen(pool: &PgPool, event: &NewWalletEvent) -> eyre::Result<()> {
    sqlx::query(
        "INSERT INTO wallet_first_seen (address, chain_id, first_seen_at, first_block, first_tx_hash, first_direction)
         VALUES ($1, $2, $3, $4, $5, $6)
         ON CONFLICT (address, chain_id) DO NOTHING",
    )
    .bind(&event.address)
    .bind(event.chain_id)
    .bind(event.first_seen_at)
    .bind(event.first_block)
    .bind(&event.first_tx_hash)
    .bind(&event.direction)
    .execute(pool)
    .await?;

    Ok(())
}
