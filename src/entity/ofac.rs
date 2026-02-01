use alloy::primitives::Address;
use sqlx::PgPool;
use std::str::FromStr;

use super::label_store::EntityLabelStore;

/// A parsed OFAC SDN entry with crypto addresses.
#[derive(Debug, Clone)]
pub struct OfacEntry {
    pub sdn_id: String,
    pub entity_name: String,
    pub program: String,
    pub addresses: Vec<String>,
}

/// Parse an OFAC SDN CSV file (simplified format).
/// Expected CSV columns: sdn_id, entity_name, program, address
/// Each row represents one crypto address for one SDN entry.
pub fn parse_ofac_csv(path: &str) -> eyre::Result<Vec<OfacEntry>> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_path(path)
        .map_err(|e| eyre::eyre!("Failed to open OFAC CSV '{}': {}", path, e))?;

    let mut entries: Vec<OfacEntry> = Vec::new();
    let mut by_sdn: std::collections::HashMap<String, OfacEntry> = std::collections::HashMap::new();

    for result in reader.records() {
        let record = result?;
        let sdn_id = record.get(0).unwrap_or("").trim().to_string();
        let entity_name = record.get(1).unwrap_or("").trim().to_string();
        let program = record.get(2).unwrap_or("").trim().to_string();
        let address = record.get(3).unwrap_or("").trim().to_string();

        if address.is_empty() || !address.starts_with("0x") {
            continue;
        }

        let entry = by_sdn.entry(sdn_id.clone()).or_insert_with(|| OfacEntry {
            sdn_id,
            entity_name,
            program,
            addresses: Vec::new(),
        });
        entry.addresses.push(address);
    }

    entries.extend(by_sdn.into_values());
    tracing::info!(entries = entries.len(), "Parsed OFAC SDN entries");
    Ok(entries)
}

/// Seed OFAC entries into the watchlist_entries table and entity_labels table.
pub async fn seed_ofac_entries(
    pool: &PgPool,
    label_store: &mut EntityLabelStore,
    entries: &[OfacEntry],
) -> eyre::Result<usize> {
    let mut count = 0;

    for entry in entries {
        for addr_hex in &entry.addresses {
            let address = match Address::from_str(addr_hex) {
                Ok(a) => a,
                Err(_) => continue,
            };
            let addr_bytes = address.as_slice();

            // Insert into watchlist_entries
            sqlx::query(
                "INSERT INTO watchlist_entries (list_name, address, entity_name, sdn_id, program)
                 VALUES ('ofac_sdn', $1, $2, $3, $4)
                 ON CONFLICT (list_name, address) DO UPDATE
                 SET entity_name = $2, sdn_id = $3, program = $4",
            )
            .bind(addr_bytes)
            .bind(&entry.entity_name)
            .bind(&entry.sdn_id)
            .bind(&entry.program)
            .execute(pool)
            .await?;

            // Also create an entity label for fast in-memory lookups
            let metadata = serde_json::json!({
                "sdn_id": entry.sdn_id,
                "program": entry.program,
            });

            label_store
                .seed_label(
                    pool,
                    addr_bytes,
                    None, // applies to all chains
                    &entry.entity_name,
                    "sanctioned",
                    "ofac_sdn",
                    1.0,
                    Some(metadata),
                )
                .await?;

            count += 1;
        }
    }

    tracing::info!(addresses = count, "Seeded OFAC SDN addresses");
    Ok(count)
}

/// Seed manual labels from config into the database and label store.
pub async fn seed_manual_labels(
    pool: &PgPool,
    label_store: &mut EntityLabelStore,
    labels: &[crate::config::ManualLabelConfig],
) -> eyre::Result<usize> {
    let mut count = 0;

    for label_cfg in labels {
        let address = match Address::from_str(&label_cfg.address) {
            Ok(a) => a,
            Err(e) => {
                tracing::warn!(
                    address = %label_cfg.address,
                    error = %e,
                    "Invalid address in manual label, skipping"
                );
                continue;
            }
        };

        label_store
            .seed_label(
                pool,
                address.as_slice(),
                label_cfg.chain_id,
                &label_cfg.entity_name,
                &label_cfg.entity_type,
                &label_cfg.source,
                label_cfg.confidence,
                None,
            )
            .await?;

        count += 1;
    }

    tracing::info!(labels = count, "Seeded manual entity labels");
    Ok(count)
}
