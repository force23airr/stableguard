use alloy::primitives::Address;
use sqlx::PgPool;
use std::collections::HashMap;
use std::str::FromStr;

use crate::config::{FiatCurrencyConfig, OnrampProviderConfig};

/// Seed all on-ramp providers, their supported fiat currencies, and known wallet addresses
/// into the database from config. Idempotent.
pub async fn seed_onramp_providers(
    pool: &PgPool,
    providers: &[OnrampProviderConfig],
) -> eyre::Result<()> {
    for provider in providers {
        // Upsert provider
        let row: (i32,) = sqlx::query_as(
            "INSERT INTO onramp_providers (name, provider_type, website, kyc_required)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (name) DO UPDATE
             SET provider_type = $2, website = $3, kyc_required = $4
             RETURNING id",
        )
        .bind(&provider.name)
        .bind(&provider.provider_type)
        .bind(&provider.website)
        .bind(provider.kyc_required)
        .fetch_one(pool)
        .await?;

        let provider_id = row.0;

        // Seed supported fiat currencies
        for fiat_code in &provider.supported_fiat {
            sqlx::query(
                "INSERT INTO provider_fiat_currencies (provider_id, currency_code)
                 VALUES ($1, $2)
                 ON CONFLICT DO NOTHING",
            )
            .bind(provider_id)
            .bind(fiat_code)
            .execute(pool)
            .await?;
        }

        // Seed known wallet addresses
        if let Some(wallets) = &provider.wallets {
            for wallet in wallets {
                let address = Address::from_str(&wallet.address)
                    .map_err(|e| eyre::eyre!("Invalid wallet address '{}': {}", wallet.address, e))?;

                sqlx::query(
                    "INSERT INTO provider_wallets (provider_id, chain_name, address, label)
                     VALUES ($1, $2, $3, $4)
                     ON CONFLICT (chain_name, address) DO UPDATE
                     SET provider_id = $1, label = $4",
                )
                .bind(provider_id)
                .bind(&wallet.chain)
                .bind(address.as_slice())
                .bind(&wallet.label)
                .execute(pool)
                .await?;

                tracing::debug!(
                    provider = %provider.name,
                    chain = %wallet.chain,
                    address = %wallet.address,
                    "Seeded provider wallet"
                );
            }
        }

        tracing::debug!(
            provider = %provider.name,
            fiat_currencies = provider.supported_fiat.len(),
            "Seeded on-ramp provider"
        );
    }

    Ok(())
}

/// Seed the fiat currency registry from config. Idempotent.
pub async fn seed_fiat_currencies(
    pool: &PgPool,
    currencies: &[FiatCurrencyConfig],
) -> eyre::Result<()> {
    for currency in currencies {
        sqlx::query(
            "INSERT INTO fiat_currencies (code, name, country, region, primary_stablecoin, risk_tier)
             VALUES ($1, $2, $3, $4, $5, $6)
             ON CONFLICT (code) DO UPDATE
             SET name = $2, country = $3, region = $4, primary_stablecoin = $5, risk_tier = $6",
        )
        .bind(&currency.code)
        .bind(&currency.name)
        .bind(&currency.country)
        .bind(&currency.region)
        .bind(&currency.primary_stablecoin)
        .bind(&currency.risk_tier)
        .execute(pool)
        .await?;
    }

    tracing::info!(count = currencies.len(), "Seeded fiat currency registry");
    Ok(())
}

/// Build an in-memory lookup of provider wallet addresses to provider IDs.
/// Used at runtime to match incoming transfers against known exchange wallets.
pub async fn load_provider_wallet_index(
    pool: &PgPool,
) -> eyre::Result<HashMap<(String, Vec<u8>), ProviderWalletInfo>> {
    let rows: Vec<(String, Vec<u8>, i32, String, Option<String>)> = sqlx::query_as(
        "SELECT pw.chain_name, pw.address, pw.provider_id, op.name, pw.label
         FROM provider_wallets pw
         JOIN onramp_providers op ON op.id = pw.provider_id",
    )
    .fetch_all(pool)
    .await?;

    let mut index = HashMap::new();
    for (chain_name, address, provider_id, provider_name, label) in rows {
        index.insert(
            (chain_name, address),
            ProviderWalletInfo {
                provider_id,
                provider_name,
                label,
            },
        );
    }

    tracing::info!(wallets = index.len(), "Loaded provider wallet index");
    Ok(index)
}

#[derive(Debug, Clone)]
pub struct ProviderWalletInfo {
    pub provider_id: i32,
    pub provider_name: String,
    pub label: Option<String>,
}
