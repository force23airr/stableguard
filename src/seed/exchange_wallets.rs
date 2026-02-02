use alloy::primitives::Address;
use serde::Deserialize;
use sqlx::PgPool;
use std::str::FromStr;

use crate::entity::label_store::EntityLabelStore;

#[derive(Debug, Deserialize)]
struct ExchangeProvider {
    provider: String,
    chain: String,
    chain_id: i64,
    wallets: Vec<ExchangeWallet>,
}

#[derive(Debug, Deserialize)]
struct ExchangeWallet {
    address: String,
    label: String,
}

/// Seed exchange wallet addresses from a JSON file into provider_wallets and entity_labels.
pub async fn seed_exchange_wallets(
    pool: &PgPool,
    entity_store: &mut EntityLabelStore,
    path: &str,
) -> eyre::Result<u64> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| eyre::eyre!("Failed to read exchange wallets file '{}': {}", path, e))?;

    let providers: Vec<ExchangeProvider> = serde_json::from_str(&content)
        .map_err(|e| eyre::eyre!("Failed to parse exchange wallets JSON: {}", e))?;

    let mut count = 0u64;

    for provider in &providers {
        // Look up or create the provider in onramp_providers
        let provider_id: i32 = match sqlx::query_as::<_, (i32,)>(
            "SELECT id FROM onramp_providers WHERE name = $1",
        )
        .bind(&provider.provider)
        .fetch_optional(pool)
        .await?
        {
            Some((id,)) => id,
            None => {
                let (id,): (i32,) = sqlx::query_as(
                    "INSERT INTO onramp_providers (name, provider_type, kyc_required)
                     VALUES ($1, 'exchange', true)
                     RETURNING id",
                )
                .bind(&provider.provider)
                .fetch_one(pool)
                .await?;
                id
            }
        };

        for wallet in &provider.wallets {
            let address = Address::from_str(&wallet.address)
                .map_err(|e| eyre::eyre!("Invalid address '{}': {}", wallet.address, e))?;

            // Upsert into provider_wallets
            sqlx::query(
                "INSERT INTO provider_wallets (provider_id, chain_name, address, label)
                 VALUES ($1, $2, $3, $4)
                 ON CONFLICT (chain_name, address) DO UPDATE
                 SET provider_id = $1, label = $4",
            )
            .bind(provider_id)
            .bind(&provider.chain)
            .bind(address.as_slice())
            .bind(&wallet.label)
            .execute(pool)
            .await?;

            // Seed into entity_labels for attribution pipeline
            entity_store
                .seed_label(
                    pool,
                    address.as_slice(),
                    Some(provider.chain_id),
                    &wallet.label,
                    "exchange",
                    "seed_data",
                    1.0,
                    None,
                )
                .await?;

            count += 1;
        }

        tracing::debug!(
            provider = %provider.provider,
            chain = %provider.chain,
            wallets = provider.wallets.len(),
            "Seeded exchange wallets"
        );
    }

    tracing::info!(count, "Exchange wallets seeded from JSON");
    Ok(count)
}
