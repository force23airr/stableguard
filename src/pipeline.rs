use sqlx::PgPool;

use crate::anomaly::engine::{self, AnomalyEngine};
use crate::config::Config;
use crate::entity::label_store::EntityLabelStore;
use crate::entity::matcher;
use crate::entity::ofac;
use crate::graph::tracker;
use crate::indexer::types::StablecoinTransfer;
use crate::wallet::first_seen::WalletTracker;

/// Result of running the enrichment pipeline on a batch of transfers.
#[derive(Debug, Default)]
pub struct EnrichmentResult {
    pub entities_attributed: u64,
    pub new_wallets_found: u64,
    pub anomalies_detected: u64,
    pub graph_edges_updated: u64,
}

/// Orchestrates all post-insert enrichment steps:
/// 1. Wallet first-seen detection
/// 2. Entity attribution (label matching)
/// 3. Graph edge updates
/// 4. Anomaly detection
pub struct TransferPipeline {
    pub entity_store: EntityLabelStore,
    pub wallet_tracker: WalletTracker,
    pub anomaly_engine: AnomalyEngine,
}

impl TransferPipeline {
    /// Initialize the pipeline: load entity labels, wallet tracker, and anomaly config.
    pub async fn init(pool: &PgPool, config: &Config) -> eyre::Result<Self> {
        // Load entity labels from DB
        let mut entity_store = EntityLabelStore::load_from_db(pool).await?;

        // Seed OFAC entries if configured
        if let Some(ofac_path) = &config.entity_attribution.ofac_sdn_path {
            match ofac::parse_ofac_csv(ofac_path) {
                Ok(entries) => {
                    let count =
                        ofac::seed_ofac_entries(pool, &mut entity_store, &entries).await?;
                    tracing::info!(count, "OFAC SDN entries loaded");
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Failed to load OFAC SDN file, continuing without");
                }
            }
        }

        // Seed manual labels from config
        if !config.entity_attribution.manual_labels.is_empty() {
            ofac::seed_manual_labels(pool, &mut entity_store, &config.entity_attribution.manual_labels)
                .await?;
        }

        // Load wallet tracker
        let wallet_tracker = WalletTracker::load_from_db(pool).await?;

        // Create anomaly engine
        let anomaly_engine = AnomalyEngine::new(config.anomaly_detection.clone());

        Ok(Self {
            entity_store,
            wallet_tracker,
            anomaly_engine,
        })
    }

    /// Run all enrichment steps on a batch of just-inserted transfers.
    pub async fn enrich(
        &mut self,
        pool: &PgPool,
        _chain_name: &str,
        transfers: &[StablecoinTransfer],
    ) -> eyre::Result<EnrichmentResult> {
        if transfers.is_empty() {
            return Ok(EnrichmentResult::default());
        }

        // Step 1: Detect new wallets
        let new_wallets = self
            .wallet_tracker
            .process_transfers(pool, transfers)
            .await?;
        let new_wallets_found = new_wallets.len() as u64;

        // Step 2: Entity attribution
        let entities_attributed =
            matcher::attribute_entities(pool, transfers, &self.entity_store).await?;

        // Step 3: Update graph edges
        let graph_edges_updated = tracker::update_edges(pool, transfers).await?;

        // Step 4: Anomaly detection
        let anomalies = self
            .anomaly_engine
            .analyze_batch(pool, transfers, &self.entity_store, &new_wallets)
            .await?;
        let anomalies_detected = engine::persist_anomalies(pool, &anomalies).await?;

        if anomalies_detected > 0 {
            for anomaly in &anomalies {
                tracing::warn!(
                    anomaly_type = anomaly.anomaly_type.as_str(),
                    risk_score = anomaly.risk_score,
                    flags = ?anomaly.flags,
                    "ANOMALY DETECTED"
                );
            }
        }

        Ok(EnrichmentResult {
            entities_attributed,
            new_wallets_found,
            anomalies_detected,
            graph_edges_updated,
        })
    }
}
