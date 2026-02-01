use std::collections::HashMap;

use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use sqlx::PgPool;

use crate::indexer::types::StablecoinTransfer;

/// Pre-aggregated edge for a single (source, dest, chain_id) tuple.
struct AggregatedEdge {
    source_address: Vec<u8>,
    dest_address: Vec<u8>,
    chain_id: i64,
    transfer_count: i64,
    total_amount: BigDecimal,
    first_seen: DateTime<Utc>,
    last_seen: DateTime<Utc>,
}

/// Update wallet graph edges for a batch of transfers.
/// Pre-aggregates edges with the same (source, dest, chain_id) to avoid
/// PostgreSQL's "ON CONFLICT DO UPDATE cannot affect row a second time" error.
pub async fn update_edges(
    pool: &PgPool,
    transfers: &[StablecoinTransfer],
) -> eyre::Result<u64> {
    if transfers.is_empty() {
        return Ok(0);
    }

    // Pre-aggregate: group transfers by (source, dest, chain_id)
    let mut edge_map: HashMap<(Vec<u8>, Vec<u8>, i64), AggregatedEdge> = HashMap::new();
    for t in transfers {
        let key = (t.from_address.clone(), t.to_address.clone(), t.chain_id);
        let entry = edge_map.entry(key).or_insert_with(|| AggregatedEdge {
            source_address: t.from_address.clone(),
            dest_address: t.to_address.clone(),
            chain_id: t.chain_id,
            transfer_count: 0,
            total_amount: BigDecimal::from(0),
            first_seen: t.block_timestamp,
            last_seen: t.block_timestamp,
        });
        entry.transfer_count += 1;
        entry.total_amount += &t.amount;
        if t.block_timestamp < entry.first_seen {
            entry.first_seen = t.block_timestamp;
        }
        if t.block_timestamp > entry.last_seen {
            entry.last_seen = t.block_timestamp;
        }
    }

    let edges: Vec<AggregatedEdge> = edge_map.into_values().collect();
    let mut count = 0u64;

    for chunk in edges.chunks(500) {
        let mut query_builder: sqlx::QueryBuilder<sqlx::Postgres> = sqlx::QueryBuilder::new(
            "INSERT INTO wallet_graph_edges (source_address, dest_address, chain_id, transfer_count, total_amount, first_seen, last_seen) ",
        );

        query_builder.push_values(chunk, |mut b, e| {
            b.push_bind(&e.source_address)
                .push_bind(&e.dest_address)
                .push_bind(e.chain_id)
                .push_bind(e.transfer_count)
                .push_bind(&e.total_amount)
                .push_bind(e.first_seen)
                .push_bind(e.last_seen);
        });

        query_builder.push(
            " ON CONFLICT (source_address, dest_address, chain_id) DO UPDATE
              SET transfer_count = wallet_graph_edges.transfer_count + EXCLUDED.transfer_count,
                  total_amount = wallet_graph_edges.total_amount + EXCLUDED.total_amount,
                  last_seen = GREATEST(wallet_graph_edges.last_seen, EXCLUDED.last_seen)",
        );

        let result = query_builder.build().execute(pool).await?;
        count += result.rows_affected();
    }

    Ok(count)
}

/// Get all outgoing edges from an address (who did this wallet send money to).
pub async fn get_outgoing_edges(
    pool: &PgPool,
    address: &[u8],
    chain_id: Option<i64>,
) -> eyre::Result<Vec<GraphEdge>> {
    let rows: Vec<(Vec<u8>, i64, i64, bigdecimal::BigDecimal)> = if let Some(chain) = chain_id {
        sqlx::query_as(
            "SELECT dest_address, chain_id, transfer_count, total_amount
             FROM wallet_graph_edges
             WHERE source_address = $1 AND chain_id = $2
             ORDER BY total_amount DESC",
        )
        .bind(address)
        .bind(chain)
        .fetch_all(pool)
        .await?
    } else {
        sqlx::query_as(
            "SELECT dest_address, chain_id, transfer_count, total_amount
             FROM wallet_graph_edges
             WHERE source_address = $1
             ORDER BY total_amount DESC",
        )
        .bind(address)
        .fetch_all(pool)
        .await?
    };

    Ok(rows
        .into_iter()
        .map(|(dest, chain, count, amount)| GraphEdge {
            dest_address: dest,
            chain_id: chain,
            transfer_count: count,
            total_amount: amount,
        })
        .collect())
}

/// Get all incoming edges to an address (who sent money to this wallet).
pub async fn get_incoming_edges(
    pool: &PgPool,
    address: &[u8],
    chain_id: Option<i64>,
) -> eyre::Result<Vec<GraphEdge>> {
    let rows: Vec<(Vec<u8>, i64, i64, bigdecimal::BigDecimal)> = if let Some(chain) = chain_id {
        sqlx::query_as(
            "SELECT source_address, chain_id, transfer_count, total_amount
             FROM wallet_graph_edges
             WHERE dest_address = $1 AND chain_id = $2
             ORDER BY total_amount DESC",
        )
        .bind(address)
        .bind(chain)
        .fetch_all(pool)
        .await?
    } else {
        sqlx::query_as(
            "SELECT source_address, chain_id, transfer_count, total_amount
             FROM wallet_graph_edges
             WHERE dest_address = $1
             ORDER BY total_amount DESC",
        )
        .bind(address)
        .fetch_all(pool)
        .await?
    };

    Ok(rows
        .into_iter()
        .map(|(source, chain, count, amount)| GraphEdge {
            dest_address: source, // reusing struct — this is the counterparty
            chain_id: chain,
            transfer_count: count,
            total_amount: amount,
        })
        .collect())
}

#[derive(Debug, Clone)]
pub struct GraphEdge {
    pub dest_address: Vec<u8>,
    pub chain_id: i64,
    pub transfer_count: i64,
    pub total_amount: bigdecimal::BigDecimal,
}
