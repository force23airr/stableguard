use sqlx::PgPool;

use crate::indexer::types::StablecoinTransfer;

/// Insert a batch of transfers using multi-row INSERT with ON CONFLICT DO NOTHING.
/// Chunks into groups of 1000 to stay within PostgreSQL parameter limits.
pub async fn insert_transfers_batch(
    pool: &PgPool,
    transfers: &[StablecoinTransfer],
) -> eyre::Result<()> {
    if transfers.is_empty() {
        return Ok(());
    }

    for chunk in transfers.chunks(1000) {
        let mut query_builder: sqlx::QueryBuilder<sqlx::Postgres> = sqlx::QueryBuilder::new(
            "INSERT INTO transfers (chain_id, block_number, block_hash, tx_hash, log_index, \
             token_address, from_address, to_address, amount, token_symbol, token_decimals, \
             block_timestamp) ",
        );

        query_builder.push_values(chunk, |mut b, t| {
            b.push_bind(t.chain_id)
                .push_bind(t.block_number)
                .push_bind(&t.block_hash)
                .push_bind(&t.tx_hash)
                .push_bind(t.log_index)
                .push_bind(&t.token_address)
                .push_bind(&t.from_address)
                .push_bind(&t.to_address)
                .push_bind(&t.amount)
                .push_bind(&t.token_symbol)
                .push_bind(t.token_decimals)
                .push_bind(t.block_timestamp);
        });

        query_builder.push(" ON CONFLICT (chain_id, tx_hash, log_index) DO NOTHING");
        query_builder.build().execute(pool).await?;
    }

    Ok(())
}

/// Get the last indexed block number for a chain. Returns None if never indexed.
pub async fn get_last_indexed_block(
    pool: &PgPool,
    chain_id: i64,
) -> eyre::Result<Option<u64>> {
    let row: Option<(i64,)> = sqlx::query_as(
        "SELECT last_indexed_block FROM indexer_state WHERE chain_id = $1",
    )
    .bind(chain_id)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|(b,)| b as u64))
}

/// Upsert the indexer checkpoint for a chain.
pub async fn upsert_indexer_state(
    pool: &PgPool,
    chain_id: i64,
    block_number: i64,
    block_hash: Option<&[u8]>,
) -> eyre::Result<()> {
    sqlx::query(
        "INSERT INTO indexer_state (chain_id, last_indexed_block, last_block_hash, updated_at)
         VALUES ($1, $2, $3, NOW())
         ON CONFLICT (chain_id) DO UPDATE
         SET last_indexed_block = $2, last_block_hash = $3, updated_at = NOW()",
    )
    .bind(chain_id)
    .bind(block_number)
    .bind(block_hash)
    .execute(pool)
    .await?;

    Ok(())
}

/// Store a block hash for reorg detection.
pub async fn upsert_block_hash(
    pool: &PgPool,
    chain_id: i64,
    block_number: i64,
    block_hash: &[u8],
    parent_hash: &[u8],
) -> eyre::Result<()> {
    sqlx::query(
        "INSERT INTO block_hashes (chain_id, block_number, block_hash, parent_hash)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (chain_id, block_number) DO UPDATE
         SET block_hash = $3, parent_hash = $4",
    )
    .bind(chain_id)
    .bind(block_number)
    .bind(block_hash)
    .bind(parent_hash)
    .execute(pool)
    .await?;

    Ok(())
}

/// Get the stored block hash for a specific block number.
pub async fn get_block_hash(
    pool: &PgPool,
    chain_id: i64,
    block_number: i64,
) -> eyre::Result<Option<Vec<u8>>> {
    let row: Option<(Vec<u8>,)> = sqlx::query_as(
        "SELECT block_hash FROM block_hashes WHERE chain_id = $1 AND block_number = $2",
    )
    .bind(chain_id)
    .bind(block_number)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|(h,)| h))
}

/// Delete all transfers at or above a block number (reorg rollback).
pub async fn delete_transfers_from_block(
    pool: &PgPool,
    chain_id: i64,
    from_block: i64,
) -> eyre::Result<u64> {
    let result = sqlx::query(
        "DELETE FROM transfers WHERE chain_id = $1 AND block_number >= $2",
    )
    .bind(chain_id)
    .bind(from_block)
    .execute(pool)
    .await?;

    Ok(result.rows_affected())
}

/// Delete block hashes at or above a block number (reorg rollback).
pub async fn delete_block_hashes_from(
    pool: &PgPool,
    chain_id: i64,
    from_block: i64,
) -> eyre::Result<()> {
    sqlx::query(
        "DELETE FROM block_hashes WHERE chain_id = $1 AND block_number >= $2",
    )
    .bind(chain_id)
    .bind(from_block)
    .execute(pool)
    .await?;

    Ok(())
}

/// Prune block hashes older than a cutoff block.
pub async fn prune_block_hashes(
    pool: &PgPool,
    chain_id: i64,
    below_block: i64,
) -> eyre::Result<()> {
    sqlx::query(
        "DELETE FROM block_hashes WHERE chain_id = $1 AND block_number < $2",
    )
    .bind(chain_id)
    .bind(below_block)
    .execute(pool)
    .await?;

    Ok(())
}

/// Seed a known token into the database (idempotent).
pub async fn upsert_known_token(
    pool: &PgPool,
    chain_id: i64,
    token_address: &[u8],
    symbol: &str,
    decimals: i16,
) -> eyre::Result<()> {
    sqlx::query(
        "INSERT INTO known_tokens (chain_id, token_address, symbol, decimals)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (chain_id, token_address) DO UPDATE
         SET symbol = $3, decimals = $4",
    )
    .bind(chain_id)
    .bind(token_address)
    .bind(symbol)
    .bind(decimals)
    .execute(pool)
    .await?;

    Ok(())
}
