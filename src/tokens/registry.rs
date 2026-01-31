use alloy::primitives::Address;
use sqlx::PgPool;
use std::collections::HashMap;
use std::str::FromStr;

use crate::config::ChainConfig;
use crate::db::repository;
use crate::indexer::types::TokenMeta;

/// Build an in-memory lookup map of watched token addresses for a chain.
/// Used by the decoder to quickly check if a log is from a tracked stablecoin.
pub fn build_watched_tokens(config: &ChainConfig) -> HashMap<Address, TokenMeta> {
    let mut map = HashMap::new();
    for token in &config.tokens {
        match Address::from_str(&token.address) {
            Ok(address) => {
                map.insert(
                    address,
                    TokenMeta {
                        symbol: token.symbol.clone(),
                        decimals: token.decimals as i16,
                    },
                );
            }
            Err(e) => {
                tracing::error!(
                    symbol = %token.symbol,
                    address = %token.address,
                    error = %e,
                    "Invalid token address in config, skipping"
                );
            }
        }
    }
    map
}

/// Seed the known_tokens table from config at startup (idempotent).
pub async fn seed_known_tokens(pool: &PgPool, chains: &[ChainConfig]) -> eyre::Result<()> {
    for chain in chains {
        for token in &chain.tokens {
            let address = Address::from_str(&token.address)
                .map_err(|e| eyre::eyre!("Invalid address '{}': {}", token.address, e))?;

            repository::upsert_known_token(
                pool,
                chain.chain_id as i64,
                address.as_slice(),
                &token.symbol,
                token.decimals as i16,
            )
            .await?;

            tracing::debug!(
                chain = %chain.name,
                symbol = %token.symbol,
                address = %token.address,
                "Seeded known token"
            );
        }
    }

    Ok(())
}
