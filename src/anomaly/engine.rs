use sqlx::PgPool;

use crate::config::AnomalyDetectionConfig;
use crate::entity::label_store::EntityLabelStore;
use crate::indexer::types::StablecoinTransfer;
use crate::wallet::first_seen::NewWalletEvent;

use super::rules;
use super::types::AnomalyRecord;

/// The anomaly detection engine. Runs all configured rules against a batch of transfers.
pub struct AnomalyEngine {
    config: AnomalyDetectionConfig,
}

impl AnomalyEngine {
    pub fn new(config: AnomalyDetectionConfig) -> Self {
        Self { config }
    }

    /// Analyze a batch of transfers for anomalies.
    /// Returns all detected anomaly records.
    pub async fn analyze_batch(
        &self,
        pool: &PgPool,
        transfers: &[StablecoinTransfer],
        label_store: &EntityLabelStore,
        new_wallets: &[NewWalletEvent],
    ) -> eyre::Result<Vec<AnomalyRecord>> {
        if !self.config.enabled {
            return Ok(Vec::new());
        }

        let mut anomalies = Vec::new();

        for transfer in transfers {
            // Rule 1: Large transfer
            if !self.config.large_transfer_thresholds.is_empty() {
                if let Some(anomaly) =
                    rules::check_large_transfer(transfer, &self.config.large_transfer_thresholds)
                {
                    anomalies.push(anomaly);
                }
            }

            // Rule 2: Sanctioned counterparty (fast, in-memory)
            if let Some(anomaly) = rules::check_sanctioned_counterparty(transfer, label_store) {
                anomalies.push(anomaly);
            }

            // Rule 3: Round number
            if let Some(anomaly) =
                rules::check_round_number(transfer, self.config.round_number.tolerance)
            {
                anomalies.push(anomaly);
            }

            // Rule 4: New wallet receiving large amount
            if let Some(anomaly) = rules::check_new_wallet_large_receive(
                transfer,
                new_wallets,
                self.config.new_wallet.threshold_usd,
            ) {
                anomalies.push(anomaly);
            }

            // Rule 5: Velocity (requires DB query — only run if batch is small enough)
            if transfers.len() <= 100 {
                if let Some(anomaly) = rules::check_velocity(
                    pool,
                    transfer,
                    self.config.velocity.window_secs,
                    self.config.velocity.max_transfers,
                )
                .await?
                {
                    anomalies.push(anomaly);
                }
            }

            // Rule 6: Cross-chain activity (requires DB query — only run if batch is small)
            if transfers.len() <= 50 {
                if let Some(anomaly) = rules::check_cross_chain_activity(
                    pool,
                    transfer,
                    self.config.cross_chain.window_secs,
                )
                .await?
                {
                    anomalies.push(anomaly);
                }
            }
        }

        Ok(anomalies)
    }
}

/// Insert detected anomalies into the database.
pub async fn persist_anomalies(
    pool: &PgPool,
    anomalies: &[AnomalyRecord],
) -> eyre::Result<u64> {
    let mut count = 0u64;

    for anomaly in anomalies {
        // Look up the transfer ID
        let transfer_id: Option<(i64,)> = sqlx::query_as(
            "SELECT id FROM transfers
             WHERE chain_id = $1 AND tx_hash = $2 AND log_index = $3",
        )
        .bind(anomaly.chain_id)
        .bind(&anomaly.tx_hash)
        .bind(anomaly.log_index)
        .fetch_optional(pool)
        .await?;

        let transfer_id = transfer_id.map(|(id,)| id);

        let flags: Vec<&str> = anomaly.flags.iter().map(|s| s.as_str()).collect();

        let result = sqlx::query(
            "INSERT INTO anomalies (transfer_id, chain_id, anomaly_type, risk_score, flags, details, address)
             VALUES ($1, $2, $3, $4, $5, $6, $7)
             ON CONFLICT (transfer_id, anomaly_type) DO NOTHING",
        )
        .bind(transfer_id)
        .bind(anomaly.chain_id)
        .bind(anomaly.anomaly_type.as_str())
        .bind(anomaly.risk_score)
        .bind(&flags)
        .bind(&anomaly.details)
        .bind(&anomaly.address)
        .execute(pool)
        .await?;

        count += result.rows_affected();
    }

    Ok(count)
}
