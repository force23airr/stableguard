use alloy::primitives::{Address, B256};
use alloy::providers::{Provider, ProviderBuilder, WsConnect};
use alloy::rpc::types::{BlockNumberOrTag, Filter};
use chrono::{DateTime, Utc};
use futures::StreamExt;
use sqlx::PgPool;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::config::ChainConfig;
use crate::db::repository;
use crate::indexer::decoder;
use crate::indexer::defi_decoder;
use crate::indexer::receipt_fetcher;
use crate::indexer::types::{StablecoinTransfer, TokenMeta};
use crate::pipeline::TransferPipeline;
use crate::tokens::registry::build_watched_tokens;

/// Main entry point for a single chain's indexer task.
/// Runs backfill (if configured), then switches to live indexing.
pub async fn run_chain_indexer(
    config: ChainConfig,
    pool: PgPool,
    shutdown: CancellationToken,
    pipeline: Arc<Mutex<TransferPipeline>>,
) -> eyre::Result<()> {
    let chain_id = config.chain_id as i64;
    tracing::info!(chain = %config.name, chain_id, "Starting chain indexer");

    let watched_tokens = build_watched_tokens(&config);
    if watched_tokens.is_empty() {
        tracing::warn!(chain = %config.name, "No valid tokens configured, exiting");
        return Ok(());
    }

    tracing::info!(
        chain = %config.name,
        tokens = watched_tokens.len(),
        "Watching tokens: {:?}",
        watched_tokens.values().map(|t| &t.symbol).collect::<Vec<_>>()
    );

    // Determine where to resume from
    let last_indexed = repository::get_last_indexed_block(&pool, chain_id).await?;
    let start_block = last_indexed
        .map(|b| b + 1)
        .or(config.start_block);

    // Phase 1: Backfill historical blocks
    if let Some(start) = start_block {
        if !shutdown.is_cancelled() {
            tracing::info!(chain = %config.name, start_block = start, "Starting backfill");
            backfill(&config, &pool, &watched_tokens, start, &shutdown, &pipeline).await?;
        }
    }

    // Phase 2: Live indexing
    if !shutdown.is_cancelled() {
        tracing::info!(chain = %config.name, "Switching to live indexing");
        live_index(&config, &pool, &watched_tokens, &shutdown, &pipeline).await?;
    }

    tracing::info!(chain = %config.name, "Chain indexer stopped");
    Ok(())
}

/// Backfill historical blocks from `start_block` up to the current chain tip.
async fn backfill(
    config: &ChainConfig,
    pool: &PgPool,
    watched_tokens: &HashMap<Address, TokenMeta>,
    start_block: u64,
    shutdown: &CancellationToken,
    pipeline: &Arc<Mutex<TransferPipeline>>,
) -> eyre::Result<()> {
    let provider = ProviderBuilder::new()
        .connect_http(config.rpc_http.parse().map_err(|e| eyre::eyre!("Invalid RPC URL: {}", e))?);

    let chain_tip = retry_rpc(|| provider.get_block_number()).await?;
    let batch_size = config.batch_size;
    let chain_id = config.chain_id as i64;

    if start_block > chain_tip {
        tracing::info!(
            chain = %config.name,
            start_block,
            chain_tip,
            "Already past chain tip, skipping backfill"
        );
        return Ok(());
    }

    let token_addresses: Vec<Address> = watched_tokens.keys().cloned().collect();
    let mut current = start_block;
    let total_blocks = chain_tip - start_block + 1;

    while current <= chain_tip && !shutdown.is_cancelled() {
        let to_block = std::cmp::min(current + batch_size - 1, chain_tip);
        let progress = ((current - start_block) as f64 / total_blocks as f64 * 100.0) as u32;

        tracing::info!(
            chain = %config.name,
            from = current,
            to = to_block,
            progress = %format!("{}%", progress),
            "Backfilling block range"
        );

        // Fetch all Transfer logs for watched tokens in this range
        let filter = Filter::new()
            .address(token_addresses.clone())
            .event("Transfer(address,address,uint256)")
            .from_block(current)
            .to_block(to_block);

        let logs = retry_rpc(|| provider.get_logs(&filter)).await?;

        // Collect unique block numbers from logs to fetch timestamps
        let mut block_timestamps: HashMap<u64, DateTime<Utc>> = HashMap::new();
        for log in &logs {
            if let Some(block_num) = log.block_number {
                if !block_timestamps.contains_key(&block_num) {
                    let block = retry_rpc(|| async {
                        provider.get_block_by_number(BlockNumberOrTag::Number(block_num)).await
                    })
                    .await?;

                    if let Some(block) = block {
                        let ts = DateTime::from_timestamp(block.header.timestamp as i64, 0)
                            .unwrap_or_default();
                        block_timestamps.insert(block_num, ts);
                    }
                }
            }
        }

        // Decode logs into transfers
        let mut transfers = Vec::new();
        for log in &logs {
            if let Some(decoded) = decoder::decode_transfer_log(log, watched_tokens) {
                let block_num = log.block_number.unwrap_or(0);
                let block_hash = log.block_hash.unwrap_or_default();
                let timestamp = block_timestamps
                    .get(&block_num)
                    .copied()
                    .unwrap_or_default();

                transfers.push(StablecoinTransfer {
                    chain_id,
                    block_number: block_num as i64,
                    block_hash: block_hash.as_slice().to_vec(),
                    tx_hash: decoded.tx_hash.as_slice().to_vec(),
                    log_index: decoded.log_index as i32,
                    token_address: decoded.token_address.as_slice().to_vec(),
                    from_address: decoded.from.as_slice().to_vec(),
                    to_address: decoded.to.as_slice().to_vec(),
                    amount: decoded.amount,
                    token_symbol: decoded.token_symbol,
                    token_decimals: decoded.token_decimals,
                    block_timestamp: timestamp,
                });
            }
        }

        // Batch insert
        if !transfers.is_empty() {
            tracing::info!(
                chain = %config.name,
                count = transfers.len(),
                "Inserting transfers"
            );
            repository::insert_transfers_batch(pool, &transfers).await?;

            // Run enrichment pipeline
            let mut pl = pipeline.lock().await;
            let result = pl.enrich(pool, &config.name, &transfers).await?;
            if result.anomalies_detected > 0 || result.entities_attributed > 0 {
                tracing::info!(
                    chain = %config.name,
                    entities = result.entities_attributed,
                    new_wallets = result.new_wallets_found,
                    anomalies = result.anomalies_detected,
                    edges = result.graph_edges_updated,
                    "Enrichment complete"
                );
            }
        }

        // Fetch receipts and decode DeFi events
        if config.decode_defi && !transfers.is_empty() {
            let unique_tx_hashes: Vec<B256> = transfers
                .iter()
                .map(|t| B256::from_slice(&t.tx_hash))
                .collect::<HashSet<_>>()
                .into_iter()
                .collect();

            if unique_tx_hashes.len() <= 100 {
                match receipt_fetcher::fetch_receipts_for_txs(&provider, &unique_tx_hashes, 50).await {
                    Ok(receipt_logs) => {
                        let all_receipt_logs: Vec<_> = receipt_logs
                            .iter()
                            .flat_map(|(_, logs)| logs.iter())
                            .cloned()
                            .collect();

                        // Use the most common timestamp from the batch
                        let batch_timestamp = block_timestamps.values().next().copied().unwrap_or_default();
                        let defi_events = defi_decoder::decode_defi_logs(&all_receipt_logs, batch_timestamp, chain_id);

                        if !defi_events.is_empty() {
                            tracing::info!(
                                chain = %config.name,
                                defi_events = defi_events.len(),
                                receipts = receipt_logs.len(),
                                "Decoded DeFi events from receipts"
                            );
                            repository::insert_defi_events_batch(pool, &defi_events).await?;
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            chain = %config.name,
                            error = %e,
                            "Failed to fetch receipts for DeFi decoding, continuing"
                        );
                    }
                }
            } else {
                tracing::debug!(
                    chain = %config.name,
                    tx_count = unique_tx_hashes.len(),
                    "Skipping DeFi decoding: too many unique txs in batch"
                );
            }
        }

        // Update checkpoint
        repository::upsert_indexer_state(pool, chain_id, to_block as i64, None).await?;

        current = to_block + 1;
    }

    tracing::info!(chain = %config.name, "Backfill complete");
    Ok(())
}

/// Live indexing: subscribe to new blocks via WebSocket, or poll via HTTP.
async fn live_index(
    config: &ChainConfig,
    pool: &PgPool,
    watched_tokens: &HashMap<Address, TokenMeta>,
    shutdown: &CancellationToken,
    pipeline: &Arc<Mutex<TransferPipeline>>,
) -> eyre::Result<()> {
    if let Some(ws_url) = &config.rpc_ws {
        match live_index_ws(config, ws_url, pool, watched_tokens, shutdown, pipeline).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                tracing::warn!(
                    chain = %config.name,
                    error = %e,
                    "WebSocket connection failed, falling back to HTTP polling"
                );
            }
        }
    }

    live_index_http(config, pool, watched_tokens, shutdown, pipeline).await
}

/// Live indexing via WebSocket block subscription.
async fn live_index_ws(
    config: &ChainConfig,
    ws_url: &str,
    pool: &PgPool,
    watched_tokens: &HashMap<Address, TokenMeta>,
    shutdown: &CancellationToken,
    pipeline: &Arc<Mutex<TransferPipeline>>,
) -> eyre::Result<()> {
    let ws = WsConnect::new(ws_url);
    let provider = ProviderBuilder::new().connect_ws(ws).await?;

    let sub = provider.subscribe_blocks().await?;
    let mut stream = sub.into_stream();

    tracing::info!(chain = %config.name, "WebSocket block subscription active");

    loop {
        tokio::select! {
            maybe_block = stream.next() => {
                match maybe_block {
                    Some(block_header) => {
                        if let Err(e) = process_new_block(
                            &provider, pool, watched_tokens, config, &block_header, pipeline
                        ).await {
                            tracing::error!(
                                chain = %config.name,
                                block = block_header.number,
                                error = %e,
                                "Failed to process block"
                            );
                        }
                    }
                    None => {
                        tracing::warn!(chain = %config.name, "Block stream ended");
                        break;
                    }
                }
            }
            _ = shutdown.cancelled() => {
                tracing::info!(chain = %config.name, "Shutdown received, stopping live indexer");
                break;
            }
        }
    }

    Ok(())
}

/// Live indexing via HTTP polling (fallback when WS is unavailable).
async fn live_index_http(
    config: &ChainConfig,
    pool: &PgPool,
    watched_tokens: &HashMap<Address, TokenMeta>,
    shutdown: &CancellationToken,
    pipeline: &Arc<Mutex<TransferPipeline>>,
) -> eyre::Result<()> {
    let provider = ProviderBuilder::new()
        .connect_http(config.rpc_http.parse().map_err(|e| eyre::eyre!("Invalid RPC URL: {}", e))?);

    let poll_interval = Duration::from_millis(config.poll_interval_ms);
    let mut last_block = retry_rpc(|| provider.get_block_number()).await?;

    tracing::info!(
        chain = %config.name,
        poll_interval_ms = config.poll_interval_ms,
        last_block,
        "HTTP polling active"
    );

    loop {
        tokio::select! {
            _ = tokio::time::sleep(poll_interval) => {}
            _ = shutdown.cancelled() => {
                tracing::info!(chain = %config.name, "Shutdown received, stopping poller");
                break;
            }
        }

        let current = match retry_rpc(|| provider.get_block_number()).await {
            Ok(n) => n,
            Err(e) => {
                tracing::error!(chain = %config.name, error = %e, "Failed to get block number");
                continue;
            }
        };

        if current <= last_block {
            continue;
        }

        for block_num in (last_block + 1)..=current {
            if shutdown.is_cancelled() {
                break;
            }

            let block = retry_rpc(|| async {
                provider.get_block_by_number(BlockNumberOrTag::Number(block_num)).await
            })
            .await?;

            if let Some(block) = block {
                if let Err(e) = process_new_block(
                    &provider, pool, watched_tokens, config, &block.header, pipeline
                ).await {
                    tracing::error!(
                        chain = %config.name,
                        block = block_num,
                        error = %e,
                        "Failed to process block"
                    );
                }
            }
        }

        last_block = current;
    }

    Ok(())
}

/// Process a single new block: detect reorgs, fetch logs, decode, insert, enrich.
async fn process_new_block<P: Provider>(
    provider: &P,
    pool: &PgPool,
    watched_tokens: &HashMap<Address, TokenMeta>,
    config: &ChainConfig,
    block_header: &alloy::consensus::Header,
    pipeline: &Arc<Mutex<TransferPipeline>>,
) -> eyre::Result<()> {
    let chain_id = config.chain_id as i64;
    let block_number = block_header.number;
    let block_hash = block_header.hash_slow();
    let parent_hash = block_header.parent_hash;

    // --- Reorg Detection ---
    if block_number > 0 {
        let stored_hash =
            repository::get_block_hash(pool, chain_id, block_number as i64 - 1).await?;

        if let Some(stored) = stored_hash {
            if stored.as_slice() != parent_hash.as_slice() {
                tracing::warn!(
                    chain = %config.name,
                    block_number,
                    "Reorg detected! Parent hash mismatch. Rolling back..."
                );

                // Walk backwards to find the fork point
                let fork_block = find_fork_point(pool, chain_id, block_number, config.max_reorg_depth).await?;

                let deleted = repository::delete_transfers_from_block(pool, chain_id, fork_block as i64).await?;
                let deleted_defi = repository::delete_defi_events_from_block(pool, chain_id, fork_block as i64).await?;
                repository::delete_block_hashes_from(pool, chain_id, fork_block as i64).await?;

                tracing::info!(
                    chain = %config.name,
                    fork_block,
                    deleted_transfers = deleted,
                    deleted_defi_events = deleted_defi,
                    "Reorg rollback complete"
                );

                // The live loop will re-process these blocks naturally
                return Ok(());
            }
        }
    }

    // --- Fetch and decode transfer logs for this block ---
    let token_addresses: Vec<Address> = watched_tokens.keys().cloned().collect();
    let filter = Filter::new()
        .address(token_addresses)
        .event("Transfer(address,address,uint256)")
        .from_block(block_number)
        .to_block(block_number);

    let logs = provider.get_logs(&filter).await?;

    let timestamp = DateTime::from_timestamp(block_header.timestamp as i64, 0).unwrap_or_default();

    let mut transfers = Vec::new();
    for log in &logs {
        if let Some(decoded) = decoder::decode_transfer_log(log, watched_tokens) {
            transfers.push(StablecoinTransfer {
                chain_id,
                block_number: block_number as i64,
                block_hash: block_hash.as_slice().to_vec(),
                tx_hash: decoded.tx_hash.as_slice().to_vec(),
                log_index: decoded.log_index as i32,
                token_address: decoded.token_address.as_slice().to_vec(),
                from_address: decoded.from.as_slice().to_vec(),
                to_address: decoded.to.as_slice().to_vec(),
                amount: decoded.amount,
                token_symbol: decoded.token_symbol,
                token_decimals: decoded.token_decimals,
                block_timestamp: timestamp,
            });
        }
    }

    // Insert transfers
    if !transfers.is_empty() {
        repository::insert_transfers_batch(pool, &transfers).await?;

        // Run enrichment pipeline
        let mut pl = pipeline.lock().await;
        let result = pl.enrich(pool, &config.name, &transfers).await?;
        if result.anomalies_detected > 0 || result.entities_attributed > 0 {
            tracing::info!(
                chain = %config.name,
                block = block_number,
                entities = result.entities_attributed,
                new_wallets = result.new_wallets_found,
                anomalies = result.anomalies_detected,
                edges = result.graph_edges_updated,
                "Enrichment complete"
            );
        }
    }

    // Fetch receipts and decode DeFi events for live blocks
    if config.decode_defi && !transfers.is_empty() {
        let unique_tx_hashes: Vec<B256> = transfers
            .iter()
            .map(|t| B256::from_slice(&t.tx_hash))
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();

        match receipt_fetcher::fetch_receipts_for_txs(provider, &unique_tx_hashes, 50).await {
            Ok(receipt_logs) => {
                let all_receipt_logs: Vec<_> = receipt_logs
                    .iter()
                    .flat_map(|(_, logs)| logs.iter())
                    .cloned()
                    .collect();

                let defi_events = defi_decoder::decode_defi_logs(&all_receipt_logs, timestamp, chain_id);

                if !defi_events.is_empty() {
                    tracing::info!(
                        chain = %config.name,
                        block = block_number,
                        defi_events = defi_events.len(),
                        "Decoded DeFi events from receipts"
                    );
                    repository::insert_defi_events_batch(pool, &defi_events).await?;
                }
            }
            Err(e) => {
                tracing::warn!(
                    chain = %config.name,
                    block = block_number,
                    error = %e,
                    "Failed to fetch receipts for DeFi decoding, continuing"
                );
            }
        }
    }

    // Store block hash for future reorg detection
    repository::upsert_block_hash(
        pool,
        chain_id,
        block_number as i64,
        block_hash.as_slice(),
        parent_hash.as_slice(),
    )
    .await?;

    // Prune old block hashes
    if block_number > config.max_reorg_depth {
        repository::prune_block_hashes(
            pool,
            chain_id,
            (block_number - config.max_reorg_depth) as i64,
        )
        .await?;
    }

    // Update checkpoint
    repository::upsert_indexer_state(
        pool,
        chain_id,
        block_number as i64,
        Some(block_hash.as_slice()),
    )
    .await?;

    tracing::info!(
        chain = %config.name,
        block = block_number,
        transfers = transfers.len(),
        "Processed block"
    );

    Ok(())
}

/// Walk backwards from a block to find where the chain forked.
async fn find_fork_point(
    pool: &PgPool,
    chain_id: i64,
    block_number: u64,
    max_depth: u64,
) -> eyre::Result<u64> {
    let earliest = if block_number > max_depth {
        block_number - max_depth
    } else {
        0
    };

    // Walk backwards; the first block without a stored hash is the fork point
    for num in (earliest..block_number).rev() {
        let stored = repository::get_block_hash(pool, chain_id, num as i64).await?;
        if stored.is_some() {
            return Ok(num + 1);
        }
    }

    Ok(earliest)
}

/// Retry an async operation with exponential backoff.
/// Handles transient RPC errors (rate limits, network issues).
pub async fn retry_rpc<F, Fut, T, E>(mut f: F) -> eyre::Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    let mut delay = Duration::from_millis(500);
    let max_retries = 5;

    for attempt in 0..max_retries {
        match f().await {
            Ok(val) => return Ok(val),
            Err(e) => {
                tracing::warn!(
                    attempt = attempt + 1,
                    max_retries,
                    error = %e,
                    delay_ms = delay.as_millis() as u64,
                    "RPC call failed, retrying..."
                );
                tokio::time::sleep(delay).await;
                delay = std::cmp::min(delay * 2, Duration::from_secs(30));
            }
        }
    }

    // Final attempt — propagate the error
    f().await.map_err(|e| eyre::eyre!("RPC call failed after {} retries: {}", max_retries, e))
}
