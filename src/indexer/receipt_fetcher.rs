use alloy::primitives::B256;
use alloy::providers::Provider;
use alloy::rpc::types::Log;
use std::time::Duration;

/// Fetch transaction receipts for a set of tx hashes and return all logs from each.
/// Throttles between calls to avoid rate limiting.
pub async fn fetch_receipts_for_txs<P: Provider>(
    provider: &P,
    tx_hashes: &[B256],
    throttle_ms: u64,
) -> eyre::Result<Vec<(B256, Vec<Log>)>> {
    let mut results = Vec::with_capacity(tx_hashes.len());
    let delay = Duration::from_millis(throttle_ms);

    for (i, hash) in tx_hashes.iter().enumerate() {
        let receipt = super::chain::retry_rpc(|| provider.get_transaction_receipt(*hash)).await?;

        if let Some(receipt) = receipt {
            let logs: Vec<Log> = receipt.inner.logs().to_vec();
            results.push((*hash, logs));
        }

        // Throttle between calls (skip after last)
        if i + 1 < tx_hashes.len() {
            tokio::time::sleep(delay).await;
        }
    }

    Ok(results)
}
