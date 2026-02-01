use sqlx::PgPool;
use std::collections::HashMap;

/// An entity label loaded from the database or config.
#[derive(Debug, Clone)]
pub struct EntityLabel {
    pub id: i32,
    pub address: Vec<u8>,
    pub chain_id: Option<i64>,
    pub entity_name: String,
    pub entity_type: String,
    pub label_source: String,
    pub confidence: f32,
}

/// In-memory index of entity labels keyed by address bytes.
/// One address can have multiple labels (e.g., "Binance" from config + "Exchange" from heuristic).
pub struct EntityLabelStore {
    by_address: HashMap<Vec<u8>, Vec<EntityLabel>>,
}

impl EntityLabelStore {
    /// Load all entity labels from the database into memory.
    pub async fn load_from_db(pool: &PgPool) -> eyre::Result<Self> {
        let rows: Vec<(i32, Vec<u8>, Option<i64>, String, String, String, f32)> = sqlx::query_as(
            "SELECT id, address, chain_id, entity_name, entity_type, label_source, confidence
             FROM entity_labels",
        )
        .fetch_all(pool)
        .await?;

        let mut by_address: HashMap<Vec<u8>, Vec<EntityLabel>> = HashMap::new();
        for (id, address, chain_id, entity_name, entity_type, label_source, confidence) in rows {
            let label = EntityLabel {
                id,
                address: address.clone(),
                chain_id,
                entity_name,
                entity_type,
                label_source,
                confidence,
            };
            by_address.entry(address).or_default().push(label);
        }

        tracing::info!(labels = by_address.len(), "Loaded entity label store");
        Ok(Self { by_address })
    }

    /// Look up labels for an address. Returns None if no labels exist.
    pub fn lookup(&self, address: &[u8]) -> Option<&[EntityLabel]> {
        self.by_address.get(address).map(|v| v.as_slice())
    }

    /// Check if an address is labeled as sanctioned.
    pub fn is_sanctioned(&self, address: &[u8]) -> bool {
        self.by_address
            .get(address)
            .map(|labels| {
                labels
                    .iter()
                    .any(|l| l.entity_type == "sanctioned" || l.label_source == "ofac_sdn")
            })
            .unwrap_or(false)
    }

    /// Insert a label into the in-memory store (after DB insertion).
    pub fn insert_memory(&mut self, label: EntityLabel) {
        self.by_address
            .entry(label.address.clone())
            .or_default()
            .push(label);
    }

    /// Seed a label into the database and the in-memory store. Returns the label ID.
    pub async fn seed_label(
        &mut self,
        pool: &PgPool,
        address: &[u8],
        chain_id: Option<i64>,
        entity_name: &str,
        entity_type: &str,
        label_source: &str,
        confidence: f32,
        metadata: Option<serde_json::Value>,
    ) -> eyre::Result<i32> {
        let row: (i32,) = sqlx::query_as(
            "INSERT INTO entity_labels (address, chain_id, entity_name, entity_type, label_source, confidence, metadata)
             VALUES ($1, $2, $3, $4, $5, $6, $7)
             ON CONFLICT (address, chain_id, label_source, entity_name) DO UPDATE
             SET entity_type = $4, confidence = $6, metadata = $7, updated_at = NOW()
             RETURNING id",
        )
        .bind(address)
        .bind(chain_id)
        .bind(entity_name)
        .bind(entity_type)
        .bind(label_source)
        .bind(confidence)
        .bind(metadata)
        .fetch_one(pool)
        .await?;

        let label = EntityLabel {
            id: row.0,
            address: address.to_vec(),
            chain_id,
            entity_name: entity_name.to_string(),
            entity_type: entity_type.to_string(),
            label_source: label_source.to_string(),
            confidence,
        };
        self.insert_memory(label);

        Ok(row.0)
    }
}
