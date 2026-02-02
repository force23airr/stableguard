use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use sqlx::PgPool;

use super::types::*;

// ============================================================
// Health & Stats
// ============================================================

pub async fn get_health(pool: &PgPool) -> eyre::Result<HealthResponse> {
    let (total_transfers,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM transfers")
            .fetch_one(pool)
            .await?;

    let chains: Vec<(i64, i64)> =
        sqlx::query_as("SELECT chain_id, last_indexed_block FROM indexer_state ORDER BY chain_id")
            .fetch_all(pool)
            .await?;

    Ok(HealthResponse {
        status: "ok".to_string(),
        total_transfers,
        indexed_chains: chains
            .into_iter()
            .map(|(chain_id, last_block)| ChainStatus {
                chain_id,
                last_block,
            })
            .collect(),
    })
}

pub async fn get_stats(pool: &PgPool) -> eyre::Result<StatsResponse> {
    let (total_transfers,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM transfers")
            .fetch_one(pool)
            .await?;

    let (total_wallets,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM wallet_first_seen")
            .fetch_one(pool)
            .await?;

    let (total_anomalies,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM anomalies")
            .fetch_one(pool)
            .await?;

    let chains: Vec<(i64, i64, i64)> = sqlx::query_as(
        "SELECT i.chain_id, i.last_indexed_block,
                (SELECT COUNT(*) FROM transfers t WHERE t.chain_id = i.chain_id)
         FROM indexer_state i ORDER BY i.chain_id",
    )
    .fetch_all(pool)
    .await?;

    Ok(StatsResponse {
        total_transfers,
        total_wallets,
        total_anomalies,
        chains: chains
            .into_iter()
            .map(|(chain_id, last_block, transfer_count)| ChainStats {
                chain_id,
                last_block,
                transfer_count,
            })
            .collect(),
    })
}

// ============================================================
// Wallet Profile
// ============================================================

pub async fn get_wallet_profile(
    pool: &PgPool,
    address: &[u8],
    chain_id: Option<i64>,
) -> eyre::Result<WalletProfileResponse> {
    let hex_addr = bytes_to_hex(address);

    // First seen
    let first_seen: Option<(i64, DateTime<Utc>, i64, String)> = sqlx::query_as(
        "SELECT chain_id, first_seen_at, first_block, first_direction
         FROM wallet_first_seen WHERE address = $1
         ORDER BY first_seen_at ASC LIMIT 1",
    )
    .bind(address)
    .fetch_optional(pool)
    .await?;

    // Labels
    let labels: Vec<(String, String, String, f32)> = if let Some(cid) = chain_id {
        sqlx::query_as(
            "SELECT entity_name, entity_type, label_source, confidence
             FROM entity_labels WHERE address = $1 AND (chain_id = $2 OR chain_id IS NULL)",
        )
        .bind(address)
        .bind(cid)
        .fetch_all(pool)
        .await?
    } else {
        sqlx::query_as(
            "SELECT entity_name, entity_type, label_source, confidence
             FROM entity_labels WHERE address = $1",
        )
        .bind(address)
        .fetch_all(pool)
        .await?
    };

    // Cluster
    let cluster: Option<(i64,)> = sqlx::query_as(
        "SELECT cluster_id FROM wallet_clusters WHERE address = $1 LIMIT 1",
    )
    .bind(address)
    .fetch_optional(pool)
    .await?;

    // Graph summary
    let (outgoing_count,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM wallet_graph_edges WHERE source_address = $1",
    )
    .bind(address)
    .fetch_one(pool)
    .await?;

    let (incoming_count,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM wallet_graph_edges WHERE dest_address = $1",
    )
    .bind(address)
    .fetch_one(pool)
    .await?;

    let total_sent: Option<(BigDecimal,)> = sqlx::query_as(
        "SELECT COALESCE(SUM(total_amount), 0) FROM wallet_graph_edges WHERE source_address = $1",
    )
    .bind(address)
    .fetch_optional(pool)
    .await?;

    let total_received: Option<(BigDecimal,)> = sqlx::query_as(
        "SELECT COALESCE(SUM(total_amount), 0) FROM wallet_graph_edges WHERE dest_address = $1",
    )
    .bind(address)
    .fetch_optional(pool)
    .await?;

    // Anomaly stats
    let (anomaly_count, max_risk): (i64, Option<f32>) = sqlx::query_as(
        "SELECT COUNT(*), MAX(risk_score) FROM anomalies WHERE address = $1",
    )
    .bind(address)
    .fetch_one(pool)
    .await?;

    Ok(WalletProfileResponse {
        address: hex_addr,
        first_seen: first_seen.map(|(cid, at, block, dir)| FirstSeenInfo {
            chain_id: cid,
            at,
            block,
            direction: dir,
        }),
        labels: labels
            .into_iter()
            .map(|(name, etype, source, conf)| LabelInfo {
                entity_name: name,
                entity_type: etype,
                source,
                confidence: conf,
            })
            .collect(),
        cluster_id: cluster.map(|(id,)| id),
        graph_summary: GraphSummary {
            outgoing_count,
            incoming_count,
            total_sent: total_sent.map(|(v,)| v).unwrap_or_default(),
            total_received: total_received.map(|(v,)| v).unwrap_or_default(),
        },
        anomaly_count,
        max_risk_score: max_risk.unwrap_or(0.0) as f64,
    })
}

// ============================================================
// Wallet Journey
// ============================================================

pub async fn get_wallet_journey(
    pool: &PgPool,
    address: &[u8],
    chain_id: Option<i64>,
    limit: i64,
    offset: i64,
) -> eyre::Result<WalletJourneyResponse> {
    let hex_addr = bytes_to_hex(address);

    // Total count
    let (total,): (i64,) = if let Some(cid) = chain_id {
        sqlx::query_as(
            "SELECT COUNT(*) FROM transfers
             WHERE (from_address = $1 OR to_address = $1) AND chain_id = $2",
        )
        .bind(address)
        .bind(cid)
        .fetch_one(pool)
        .await?
    } else {
        sqlx::query_as(
            "SELECT COUNT(*) FROM transfers WHERE from_address = $1 OR to_address = $1",
        )
        .bind(address)
        .fetch_one(pool)
        .await?
    };

    // Journey entries with entity labels on the counterparty side
    let rows: Vec<(
        DateTime<Utc>,   // block_timestamp
        Vec<u8>,         // from_address
        Vec<u8>,         // to_address
        Option<String>,  // entity_name
        Option<String>,  // entity_type
        BigDecimal,      // amount
        String,          // token_symbol
        i64,             // chain_id
        Vec<u8>,         // tx_hash
    )> = if let Some(cid) = chain_id {
        sqlx::query_as(
            "SELECT t.block_timestamp, t.from_address, t.to_address,
                    el.entity_name, el.entity_type,
                    t.amount, t.token_symbol, t.chain_id, t.tx_hash
             FROM transfers t
             LEFT JOIN transfer_entity_flags tef ON tef.transfer_id = t.id
             LEFT JOIN entity_labels el ON el.id = tef.entity_label_id
                 AND ((t.from_address = $1 AND tef.side = 'to')
                   OR (t.to_address = $1 AND tef.side = 'from'))
             WHERE (t.from_address = $1 OR t.to_address = $1) AND t.chain_id = $2
             ORDER BY t.block_timestamp ASC
             LIMIT $3 OFFSET $4",
        )
        .bind(address)
        .bind(cid)
        .bind(limit)
        .bind(offset)
        .fetch_all(pool)
        .await?
    } else {
        sqlx::query_as(
            "SELECT t.block_timestamp, t.from_address, t.to_address,
                    el.entity_name, el.entity_type,
                    t.amount, t.token_symbol, t.chain_id, t.tx_hash
             FROM transfers t
             LEFT JOIN transfer_entity_flags tef ON tef.transfer_id = t.id
             LEFT JOIN entity_labels el ON el.id = tef.entity_label_id
                 AND ((t.from_address = $1 AND tef.side = 'to')
                   OR (t.to_address = $1 AND tef.side = 'from'))
             WHERE t.from_address = $1 OR t.to_address = $1
             ORDER BY t.block_timestamp ASC
             LIMIT $2 OFFSET $3",
        )
        .bind(address)
        .bind(limit)
        .bind(offset)
        .fetch_all(pool)
        .await?
    };

    let mut journey = Vec::new();
    let mut entity_sequence = Vec::new();
    let mut seen_entities = std::collections::HashSet::new();

    for (ts, from, to, ename, etype, amount, token, cid, tx) in rows {
        let is_sender = from == address;
        let counterparty = if is_sender { &to } else { &from };

        if let Some(ref name) = ename {
            let key = format!("{} ({})", name, etype.as_deref().unwrap_or("unknown"));
            if seen_entities.insert(key.clone()) {
                entity_sequence.push(key);
            }
        }

        journey.push(JourneyEntry {
            timestamp: ts,
            direction: if is_sender { "sent" } else { "received" }.to_string(),
            counterparty: bytes_to_hex(counterparty),
            entity_name: ename,
            entity_type: etype,
            amount,
            token,
            chain_id: cid,
            tx_hash: bytes_to_hex(&tx),
        });
    }

    Ok(WalletJourneyResponse {
        address: hex_addr,
        journey,
        entity_sequence,
        total,
    })
}

// ============================================================
// Wallet Fingerprint
// ============================================================

pub async fn get_wallet_fingerprint(
    pool: &PgPool,
    address: &[u8],
    _chain_id: Option<i64>,
) -> eyre::Result<FingerprintResponse> {
    let hex_addr = bytes_to_hex(address);

    // Entity type distribution
    let type_rows: Vec<(String, i64)> = sqlx::query_as(
        "SELECT el.entity_type, COUNT(*) AS cnt
         FROM transfers t
         JOIN transfer_entity_flags tef ON tef.transfer_id = t.id
         JOIN entity_labels el ON el.id = tef.entity_label_id
         WHERE (t.from_address = $1 OR t.to_address = $1)
           AND ((t.from_address = $1 AND tef.side = 'to')
             OR (t.to_address = $1 AND tef.side = 'from'))
         GROUP BY el.entity_type
         ORDER BY cnt DESC",
    )
    .bind(address)
    .fetch_all(pool)
    .await?;

    // Entity sequence (ordered by first interaction time)
    let seq_rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT el.entity_name, el.entity_type
         FROM transfers t
         JOIN transfer_entity_flags tef ON tef.transfer_id = t.id
         JOIN entity_labels el ON el.id = tef.entity_label_id
         WHERE (t.from_address = $1 OR t.to_address = $1)
           AND ((t.from_address = $1 AND tef.side = 'to')
             OR (t.to_address = $1 AND tef.side = 'from'))
         GROUP BY el.entity_name, el.entity_type
         ORDER BY MIN(t.block_timestamp) ASC",
    )
    .bind(address)
    .fetch_all(pool)
    .await?;

    // Transfer stats
    let stats: Option<(i64, Option<BigDecimal>, Option<DateTime<Utc>>, Option<DateTime<Utc>>, i64, i64)> =
        sqlx::query_as(
            "SELECT COUNT(*), AVG(amount),
                    MIN(block_timestamp), MAX(block_timestamp),
                    COUNT(DISTINCT DATE(block_timestamp)),
                    COUNT(DISTINCT chain_id)
             FROM transfers
             WHERE from_address = $1 OR to_address = $1",
        )
        .bind(address)
        .fetch_optional(pool)
        .await?;

    let (total_transfers, avg_amount, first_act, last_act, active_days, chains_used) =
        stats.unwrap_or((0, None, None, None, 0, 0));

    Ok(FingerprintResponse {
        address: hex_addr,
        entity_type_distribution: type_rows
            .into_iter()
            .map(|(etype, count)| EntityTypeCount {
                entity_type: etype,
                count,
            })
            .collect(),
        entity_sequence: seq_rows
            .into_iter()
            .map(|(name, etype)| format!("{} ({})", name, etype))
            .collect(),
        total_transfers,
        avg_transfer_amount: avg_amount,
        active_days,
        chains_used,
        first_activity: first_act,
        last_activity: last_act,
    })
}

// ============================================================
// Similar Wallets
// ============================================================

pub async fn get_similar_wallets(
    pool: &PgPool,
    address: &[u8],
    _chain_id: Option<i64>,
    limit: i64,
) -> eyre::Result<SimilarWalletsResponse> {
    let hex_addr = bytes_to_hex(address);

    let mut candidates: Vec<SimilarWallet> = Vec::new();

    // 1. Same cluster members
    let cluster_members: Vec<(Vec<u8>,)> = sqlx::query_as(
        "SELECT wc2.address FROM wallet_clusters wc1
         JOIN wallet_clusters wc2 ON wc1.cluster_id = wc2.cluster_id
             AND wc1.chain_id = wc2.chain_id
         WHERE wc1.address = $1 AND wc2.address != $1",
    )
    .bind(address)
    .fetch_all(pool)
    .await?;

    let mut seen = std::collections::HashSet::new();
    for (addr,) in &cluster_members {
        if seen.insert(addr.clone()) {
            candidates.push(SimilarWallet {
                address: bytes_to_hex(addr),
                similarity_score: 0.0,
                match_reasons: vec!["same_cluster".to_string()],
                shared_entities: vec![],
            });
        }
    }

    // 2. Wallets sharing 2+ counterparties
    let shared: Vec<(Vec<u8>, i64)> = sqlx::query_as(
        "WITH target_cp AS (
            SELECT DISTINCT
                CASE WHEN from_address = $1 THEN to_address ELSE from_address END AS cp
            FROM transfers WHERE from_address = $1 OR to_address = $1
        )
        SELECT
            CASE WHEN t.from_address = tc.cp THEN t.to_address ELSE t.from_address END AS candidate,
            COUNT(DISTINCT tc.cp) AS shared
        FROM transfers t
        JOIN target_cp tc ON (t.from_address = tc.cp OR t.to_address = tc.cp)
        WHERE t.from_address != $1 AND t.to_address != $1
        GROUP BY candidate
        HAVING COUNT(DISTINCT tc.cp) >= 2
        ORDER BY shared DESC
        LIMIT $2",
    )
    .bind(address)
    .bind(limit * 2)
    .fetch_all(pool)
    .await?;

    for (addr, shared_count) in &shared {
        if seen.insert(addr.clone()) {
            candidates.push(SimilarWallet {
                address: bytes_to_hex(addr),
                similarity_score: *shared_count as f64 / 10.0, // rough score
                match_reasons: vec!["shared_counterparties".to_string()],
                shared_entities: vec![],
            });
        }
    }

    // 3. Score candidates by shared entity types
    let target_entities: Vec<(String,)> = sqlx::query_as(
        "SELECT DISTINCT el.entity_type
         FROM transfers t
         JOIN transfer_entity_flags tef ON tef.transfer_id = t.id
         JOIN entity_labels el ON el.id = tef.entity_label_id
         WHERE (t.from_address = $1 OR t.to_address = $1)
           AND ((t.from_address = $1 AND tef.side = 'to')
             OR (t.to_address = $1 AND tef.side = 'from'))",
    )
    .bind(address)
    .fetch_all(pool)
    .await?;

    let target_entity_set: std::collections::HashSet<String> =
        target_entities.into_iter().map(|(e,)| e).collect();

    for candidate in &mut candidates {
        let cand_bytes = hex_to_bytes(&candidate.address).unwrap_or_default();

        let cand_entities: Vec<(String,)> = sqlx::query_as(
            "SELECT DISTINCT el.entity_type
             FROM transfers t
             JOIN transfer_entity_flags tef ON tef.transfer_id = t.id
             JOIN entity_labels el ON el.id = tef.entity_label_id
             WHERE (t.from_address = $1 OR t.to_address = $1)
               AND ((t.from_address = $1 AND tef.side = 'to')
                 OR (t.to_address = $1 AND tef.side = 'from'))",
        )
        .bind(&cand_bytes)
        .fetch_all(pool)
        .await
        .unwrap_or_default();

        let cand_set: std::collections::HashSet<String> =
            cand_entities.into_iter().map(|(e,)| e).collect();

        // Jaccard similarity
        let intersection = target_entity_set.intersection(&cand_set).count();
        let union = target_entity_set.union(&cand_set).count();
        let jaccard = if union > 0 {
            intersection as f64 / union as f64
        } else {
            0.0
        };

        let shared_ents: Vec<String> = target_entity_set
            .intersection(&cand_set)
            .cloned()
            .collect();
        candidate.shared_entities = shared_ents;

        // Composite score
        let cluster_bonus = if candidate.match_reasons.contains(&"same_cluster".to_string()) {
            0.3
        } else {
            0.0
        };
        candidate.similarity_score = 0.5 * jaccard + 0.2 * candidate.similarity_score + cluster_bonus;
    }

    // Sort by score descending
    candidates.sort_by(|a, b| b.similarity_score.partial_cmp(&a.similarity_score).unwrap());
    candidates.truncate(limit as usize);

    Ok(SimilarWalletsResponse {
        address: hex_addr,
        similar_wallets: candidates,
    })
}

// ============================================================
// Transfers
// ============================================================

pub async fn get_transfers(
    pool: &PgPool,
    params: &TransferParams,
) -> eyre::Result<TransfersResponse> {
    let limit = params.limit.unwrap_or(50).min(1000);
    let offset = params.offset.unwrap_or(0);

    let from_bytes = params.from.as_ref().and_then(|f| hex_to_bytes(f).ok());
    let to_bytes = params.to.as_ref().and_then(|t| hex_to_bytes(t).ok());
    let since: Option<DateTime<Utc>> = params
        .since
        .as_ref()
        .and_then(|s| s.parse::<DateTime<Utc>>().ok());
    let until: Option<DateTime<Utc>> = params
        .until
        .as_ref()
        .and_then(|u| u.parse::<DateTime<Utc>>().ok());
    let min_amount_bd: Option<BigDecimal> = params
        .min_amount
        .map(|a| BigDecimal::try_from(a).unwrap_or_default());

    // Use a single parameterized query with optional conditions via COALESCE/IS NULL trick
    let (total,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM transfers t
         WHERE ($1::BIGINT IS NULL OR t.chain_id = $1)
           AND ($2::BYTEA IS NULL OR t.from_address = $2)
           AND ($3::BYTEA IS NULL OR t.to_address = $3)
           AND ($4::TEXT IS NULL OR t.token_symbol = $4)
           AND ($5::NUMERIC IS NULL OR t.amount >= $5)
           AND ($6::TIMESTAMPTZ IS NULL OR t.block_timestamp >= $6)
           AND ($7::TIMESTAMPTZ IS NULL OR t.block_timestamp <= $7)",
    )
    .bind(params.chain_id)
    .bind(&from_bytes)
    .bind(&to_bytes)
    .bind(&params.token)
    .bind(&min_amount_bd)
    .bind(since)
    .bind(until)
    .fetch_one(pool)
    .await?;

    let rows: Vec<(i64, i64, i64, Vec<u8>, Vec<u8>, Vec<u8>, BigDecimal, String, DateTime<Utc>)> =
        sqlx::query_as(
            "SELECT t.id, t.chain_id, t.block_number, t.tx_hash,
                    t.from_address, t.to_address, t.amount, t.token_symbol, t.block_timestamp
             FROM transfers t
             WHERE ($1::BIGINT IS NULL OR t.chain_id = $1)
               AND ($2::BYTEA IS NULL OR t.from_address = $2)
               AND ($3::BYTEA IS NULL OR t.to_address = $3)
               AND ($4::TEXT IS NULL OR t.token_symbol = $4)
               AND ($5::NUMERIC IS NULL OR t.amount >= $5)
               AND ($6::TIMESTAMPTZ IS NULL OR t.block_timestamp >= $6)
               AND ($7::TIMESTAMPTZ IS NULL OR t.block_timestamp <= $7)
             ORDER BY t.block_timestamp DESC
             LIMIT $8 OFFSET $9",
        )
        .bind(params.chain_id)
        .bind(&from_bytes)
        .bind(&to_bytes)
        .bind(&params.token)
        .bind(&min_amount_bd)
        .bind(since)
        .bind(until)
        .bind(limit)
        .bind(offset)
        .fetch_all(pool)
        .await?;

    let transfers = rows
        .into_iter()
        .map(|(id, cid, block, tx, from, to, amount, token, ts)| TransferEntry {
            id,
            chain_id: cid,
            block_number: block,
            tx_hash: bytes_to_hex(&tx),
            from_address: bytes_to_hex(&from),
            to_address: bytes_to_hex(&to),
            amount,
            token,
            timestamp: ts,
            from_entity: None,
            to_entity: None,
        })
        .collect();

    Ok(TransfersResponse {
        transfers,
        total,
        limit,
        offset,
    })
}

// ============================================================
// Anomalies
// ============================================================

pub async fn get_anomalies(
    pool: &PgPool,
    params: &AnomalyParams,
) -> eyre::Result<AnomaliesResponse> {
    let limit = params.limit.unwrap_or(50).min(1000);
    let offset = params.offset.unwrap_or(0);

    let addr_bytes = params.address.as_ref().and_then(|a| hex_to_bytes(a).ok());
    let min_risk_f32: Option<f32> = params.min_risk.map(|r| r as f32);

    let (total,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM anomalies
         WHERE ($1::BIGINT IS NULL OR chain_id = $1)
           AND ($2::TEXT IS NULL OR anomaly_type = $2)
           AND ($3::REAL IS NULL OR risk_score >= $3)
           AND ($4::BYTEA IS NULL OR address = $4)
           AND ($5::BOOL IS NULL OR resolved = $5)",
    )
    .bind(params.chain_id)
    .bind(&params.anomaly_type)
    .bind(min_risk_f32)
    .bind(&addr_bytes)
    .bind(params.resolved)
    .fetch_one(pool)
    .await?;

    let rows: Vec<(i64, i64, String, f32, Vec<String>, Option<Vec<u8>>, DateTime<Utc>, bool)> =
        sqlx::query_as(
            "SELECT id, chain_id, anomaly_type, risk_score, flags, address, detected_at, resolved
             FROM anomalies
             WHERE ($1::BIGINT IS NULL OR chain_id = $1)
               AND ($2::TEXT IS NULL OR anomaly_type = $2)
               AND ($3::REAL IS NULL OR risk_score >= $3)
               AND ($4::BYTEA IS NULL OR address = $4)
               AND ($5::BOOL IS NULL OR resolved = $5)
             ORDER BY risk_score DESC, detected_at DESC
             LIMIT $6 OFFSET $7",
        )
        .bind(params.chain_id)
        .bind(&params.anomaly_type)
        .bind(min_risk_f32)
        .bind(&addr_bytes)
        .bind(params.resolved)
        .bind(limit)
        .bind(offset)
        .fetch_all(pool)
        .await?;

    let anomalies = rows
        .into_iter()
        .map(|(id, cid, atype, risk, flags, addr, ts, resolved)| AnomalyEntry {
            id,
            chain_id: cid,
            anomaly_type: atype,
            risk_score: risk as f64,
            flags,
            address: addr.map(|a| bytes_to_hex(&a)).unwrap_or_default(),
            detected_at: ts,
            resolved,
        })
        .collect();

    Ok(AnomaliesResponse {
        anomalies,
        total,
        limit,
        offset,
    })
}

// ============================================================
// Entities
// ============================================================

pub async fn get_entities(
    pool: &PgPool,
    params: &EntityParams,
) -> eyre::Result<EntitiesResponse> {
    let limit = params.limit.unwrap_or(50).min(1000);
    let search_pattern = params.search.as_ref().map(|s| format!("%{}%", s));

    let (total,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM entity_labels
         WHERE ($1::TEXT IS NULL OR entity_type = $1)
           AND ($2::TEXT IS NULL OR label_source = $2)
           AND ($3::TEXT IS NULL OR entity_name ILIKE $3)",
    )
    .bind(&params.entity_type)
    .bind(&params.source)
    .bind(&search_pattern)
    .fetch_one(pool)
    .await?;

    let rows: Vec<(Vec<u8>, Option<i64>, String, String, String, f32)> = sqlx::query_as(
        "SELECT address, chain_id, entity_name, entity_type, label_source, confidence
         FROM entity_labels
         WHERE ($1::TEXT IS NULL OR entity_type = $1)
           AND ($2::TEXT IS NULL OR label_source = $2)
           AND ($3::TEXT IS NULL OR entity_name ILIKE $3)
         ORDER BY entity_name ASC
         LIMIT $4",
    )
    .bind(&params.entity_type)
    .bind(&params.source)
    .bind(&search_pattern)
    .bind(limit)
    .fetch_all(pool)
    .await?;

    let entities = rows
        .into_iter()
        .map(|(addr, cid, name, etype, source, conf)| EntityEntry {
            address: bytes_to_hex(&addr),
            chain_id: cid,
            entity_name: name,
            entity_type: etype,
            source,
            confidence: conf,
        })
        .collect();

    Ok(EntitiesResponse { entities, total })
}

pub async fn get_entity_by_address(
    pool: &PgPool,
    address: &[u8],
) -> eyre::Result<EntitiesResponse> {
    let rows: Vec<(Vec<u8>, Option<i64>, String, String, String, f32)> = sqlx::query_as(
        "SELECT address, chain_id, entity_name, entity_type, label_source, confidence
         FROM entity_labels WHERE address = $1",
    )
    .bind(address)
    .fetch_all(pool)
    .await?;

    let total = rows.len() as i64;
    let entities = rows
        .into_iter()
        .map(|(addr, cid, name, etype, source, conf)| EntityEntry {
            address: bytes_to_hex(&addr),
            chain_id: cid,
            entity_name: name,
            entity_type: etype,
            source,
            confidence: conf,
        })
        .collect();

    Ok(EntitiesResponse { entities, total })
}

// ============================================================
// DeFi Events
// ============================================================

pub async fn get_defi_events(
    pool: &PgPool,
    params: &DefiParams,
) -> eyre::Result<DefiEventsResponse> {
    let limit = params.limit.unwrap_or(50).min(1000);
    let offset = params.offset.unwrap_or(0);

    let account_bytes = params.account.as_ref().and_then(|a| hex_to_bytes(a).ok());
    let since: Option<DateTime<Utc>> = params
        .since
        .as_ref()
        .and_then(|s| s.parse::<DateTime<Utc>>().ok());
    let until: Option<DateTime<Utc>> = params
        .until
        .as_ref()
        .and_then(|u| u.parse::<DateTime<Utc>>().ok());

    let (total,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM defi_events
         WHERE ($1::BIGINT IS NULL OR chain_id = $1)
           AND ($2::TEXT IS NULL OR protocol = $2)
           AND ($3::TEXT IS NULL OR event_type = $3)
           AND ($4::BYTEA IS NULL OR account = $4)
           AND ($5::TIMESTAMPTZ IS NULL OR block_timestamp >= $5)
           AND ($6::TIMESTAMPTZ IS NULL OR block_timestamp <= $6)",
    )
    .bind(params.chain_id)
    .bind(&params.protocol)
    .bind(&params.event_type)
    .bind(&account_bytes)
    .bind(since)
    .bind(until)
    .fetch_one(pool)
    .await?;

    let rows: Vec<(
        i64,
        i64,
        i64,
        Vec<u8>,
        i32,
        String,
        String,
        Vec<u8>,
        Option<Vec<u8>>,
        Option<Vec<u8>>,
        Option<Vec<u8>>,
        Option<BigDecimal>,
        Option<BigDecimal>,
        DateTime<Utc>,
        Option<serde_json::Value>,
    )> = sqlx::query_as(
        "SELECT id, chain_id, block_number, tx_hash, log_index,
                protocol, event_type, contract_address, account,
                token_in, token_out, amount_in, amount_out,
                block_timestamp, raw_data
         FROM defi_events
         WHERE ($1::BIGINT IS NULL OR chain_id = $1)
           AND ($2::TEXT IS NULL OR protocol = $2)
           AND ($3::TEXT IS NULL OR event_type = $3)
           AND ($4::BYTEA IS NULL OR account = $4)
           AND ($5::TIMESTAMPTZ IS NULL OR block_timestamp >= $5)
           AND ($6::TIMESTAMPTZ IS NULL OR block_timestamp <= $6)
         ORDER BY block_timestamp DESC
         LIMIT $7 OFFSET $8",
    )
    .bind(params.chain_id)
    .bind(&params.protocol)
    .bind(&params.event_type)
    .bind(&account_bytes)
    .bind(since)
    .bind(until)
    .bind(limit)
    .bind(offset)
    .fetch_all(pool)
    .await?;

    let events = rows
        .into_iter()
        .map(
            |(id, cid, block, tx, li, proto, etype, contract, acct, tin, tout, ain, aout, ts, raw)| {
                DefiEventEntry {
                    id,
                    chain_id: cid,
                    block_number: block,
                    tx_hash: bytes_to_hex(&tx),
                    log_index: li,
                    protocol: proto,
                    event_type: etype,
                    contract_address: bytes_to_hex(&contract),
                    account: acct.as_deref().map(bytes_to_hex),
                    token_in: tin.as_deref().map(bytes_to_hex),
                    token_out: tout.as_deref().map(bytes_to_hex),
                    amount_in: ain,
                    amount_out: aout,
                    timestamp: ts,
                    raw_data: raw,
                }
            },
        )
        .collect();

    Ok(DefiEventsResponse {
        events,
        total,
        limit,
        offset,
    })
}

pub async fn get_wallet_defi(
    pool: &PgPool,
    address: &[u8],
    params: &DefiParams,
) -> eyre::Result<DefiEventsResponse> {
    let limit = params.limit.unwrap_or(50).min(1000);
    let offset = params.offset.unwrap_or(0);

    let since: Option<DateTime<Utc>> = params
        .since
        .as_ref()
        .and_then(|s| s.parse::<DateTime<Utc>>().ok());
    let until: Option<DateTime<Utc>> = params
        .until
        .as_ref()
        .and_then(|u| u.parse::<DateTime<Utc>>().ok());

    let (total,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM defi_events
         WHERE account = $1
           AND ($2::BIGINT IS NULL OR chain_id = $2)
           AND ($3::TEXT IS NULL OR protocol = $3)
           AND ($4::TEXT IS NULL OR event_type = $4)
           AND ($5::TIMESTAMPTZ IS NULL OR block_timestamp >= $5)
           AND ($6::TIMESTAMPTZ IS NULL OR block_timestamp <= $6)",
    )
    .bind(address)
    .bind(params.chain_id)
    .bind(&params.protocol)
    .bind(&params.event_type)
    .bind(since)
    .bind(until)
    .fetch_one(pool)
    .await?;

    let rows: Vec<(
        i64,
        i64,
        i64,
        Vec<u8>,
        i32,
        String,
        String,
        Vec<u8>,
        Option<Vec<u8>>,
        Option<Vec<u8>>,
        Option<Vec<u8>>,
        Option<BigDecimal>,
        Option<BigDecimal>,
        DateTime<Utc>,
        Option<serde_json::Value>,
    )> = sqlx::query_as(
        "SELECT id, chain_id, block_number, tx_hash, log_index,
                protocol, event_type, contract_address, account,
                token_in, token_out, amount_in, amount_out,
                block_timestamp, raw_data
         FROM defi_events
         WHERE account = $1
           AND ($2::BIGINT IS NULL OR chain_id = $2)
           AND ($3::TEXT IS NULL OR protocol = $3)
           AND ($4::TEXT IS NULL OR event_type = $4)
           AND ($5::TIMESTAMPTZ IS NULL OR block_timestamp >= $5)
           AND ($6::TIMESTAMPTZ IS NULL OR block_timestamp <= $6)
         ORDER BY block_timestamp DESC
         LIMIT $7 OFFSET $8",
    )
    .bind(address)
    .bind(params.chain_id)
    .bind(&params.protocol)
    .bind(&params.event_type)
    .bind(since)
    .bind(until)
    .bind(limit)
    .bind(offset)
    .fetch_all(pool)
    .await?;

    let events = rows
        .into_iter()
        .map(
            |(id, cid, block, tx, li, proto, etype, contract, acct, tin, tout, ain, aout, ts, raw)| {
                DefiEventEntry {
                    id,
                    chain_id: cid,
                    block_number: block,
                    tx_hash: bytes_to_hex(&tx),
                    log_index: li,
                    protocol: proto,
                    event_type: etype,
                    contract_address: bytes_to_hex(&contract),
                    account: acct.as_deref().map(bytes_to_hex),
                    token_in: tin.as_deref().map(bytes_to_hex),
                    token_out: tout.as_deref().map(bytes_to_hex),
                    amount_in: ain,
                    amount_out: aout,
                    timestamp: ts,
                    raw_data: raw,
                }
            },
        )
        .collect();

    Ok(DefiEventsResponse {
        events,
        total,
        limit,
        offset,
    })
}

pub async fn get_tx_context(
    pool: &PgPool,
    tx_hash: &[u8],
) -> eyre::Result<TxContextResponse> {
    let hex_hash = bytes_to_hex(tx_hash);

    // Fetch transfers for this tx
    let transfer_rows: Vec<(i64, i64, i64, Vec<u8>, Vec<u8>, Vec<u8>, BigDecimal, String, DateTime<Utc>)> =
        sqlx::query_as(
            "SELECT id, chain_id, block_number, tx_hash,
                    from_address, to_address, amount, token_symbol, block_timestamp
             FROM transfers WHERE tx_hash = $1
             ORDER BY log_index ASC",
        )
        .bind(tx_hash)
        .fetch_all(pool)
        .await?;

    let transfers = transfer_rows
        .into_iter()
        .map(|(id, cid, block, tx, from, to, amount, token, ts)| TransferEntry {
            id,
            chain_id: cid,
            block_number: block,
            tx_hash: bytes_to_hex(&tx),
            from_address: bytes_to_hex(&from),
            to_address: bytes_to_hex(&to),
            amount,
            token,
            timestamp: ts,
            from_entity: None,
            to_entity: None,
        })
        .collect();

    // Fetch DeFi events for this tx
    let defi_rows: Vec<(
        i64,
        i64,
        i64,
        Vec<u8>,
        i32,
        String,
        String,
        Vec<u8>,
        Option<Vec<u8>>,
        Option<Vec<u8>>,
        Option<Vec<u8>>,
        Option<BigDecimal>,
        Option<BigDecimal>,
        DateTime<Utc>,
        Option<serde_json::Value>,
    )> = sqlx::query_as(
        "SELECT id, chain_id, block_number, tx_hash, log_index,
                protocol, event_type, contract_address, account,
                token_in, token_out, amount_in, amount_out,
                block_timestamp, raw_data
         FROM defi_events WHERE tx_hash = $1
         ORDER BY log_index ASC",
    )
    .bind(tx_hash)
    .fetch_all(pool)
    .await?;

    let defi_events = defi_rows
        .into_iter()
        .map(
            |(id, cid, block, tx, li, proto, etype, contract, acct, tin, tout, ain, aout, ts, raw)| {
                DefiEventEntry {
                    id,
                    chain_id: cid,
                    block_number: block,
                    tx_hash: bytes_to_hex(&tx),
                    log_index: li,
                    protocol: proto,
                    event_type: etype,
                    contract_address: bytes_to_hex(&contract),
                    account: acct.as_deref().map(bytes_to_hex),
                    token_in: tin.as_deref().map(bytes_to_hex),
                    token_out: tout.as_deref().map(bytes_to_hex),
                    amount_in: ain,
                    amount_out: aout,
                    timestamp: ts,
                    raw_data: raw,
                }
            },
        )
        .collect();

    Ok(TxContextResponse {
        tx_hash: hex_hash,
        transfers,
        defi_events,
    })
}

// ============================================================
// Cluster
// ============================================================

pub async fn get_cluster(
    pool: &PgPool,
    cluster_id: i64,
    chain_id: i64,
) -> eyre::Result<ClusterResponse> {
    let rows: Vec<(Vec<u8>,)> = sqlx::query_as(
        "SELECT address FROM wallet_clusters WHERE cluster_id = $1 AND chain_id = $2",
    )
    .bind(cluster_id)
    .bind(chain_id)
    .fetch_all(pool)
    .await?;

    let addresses: Vec<String> = rows.iter().map(|(a,)| bytes_to_hex(a)).collect();
    let size = addresses.len();

    Ok(ClusterResponse {
        cluster_id,
        chain_id,
        addresses,
        size,
    })
}
