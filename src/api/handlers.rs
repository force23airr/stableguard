use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use std::sync::Arc;

use super::queries;
use super::types::*;
use super::AppState;

type ApiResult<T> = Result<Json<T>, (StatusCode, Json<ErrorResponse>)>;

fn api_error(status: StatusCode, msg: impl Into<String>) -> (StatusCode, Json<ErrorResponse>) {
    (
        status,
        Json(ErrorResponse {
            error: msg.into(),
        }),
    )
}

fn parse_address(hex: &str) -> Result<Vec<u8>, (StatusCode, Json<ErrorResponse>)> {
    hex_to_bytes(hex).map_err(|e| api_error(StatusCode::BAD_REQUEST, e))
}

// ============================================================
// Health & Stats
// ============================================================

pub async fn health(State(state): State<Arc<AppState>>) -> ApiResult<HealthResponse> {
    queries::get_health(&state.pool)
        .await
        .map(Json)
        .map_err(|e| api_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

pub async fn stats(State(state): State<Arc<AppState>>) -> ApiResult<StatsResponse> {
    queries::get_stats(&state.pool)
        .await
        .map(Json)
        .map_err(|e| api_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

// ============================================================
// Wallet
// ============================================================

pub async fn wallet_profile(
    State(state): State<Arc<AppState>>,
    Path(address): Path<String>,
    Query(params): Query<ChainFilter>,
) -> ApiResult<WalletProfileResponse> {
    let addr = parse_address(&address)?;
    queries::get_wallet_profile(&state.pool, &addr, params.chain_id)
        .await
        .map(Json)
        .map_err(|e| api_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

pub async fn wallet_journey(
    State(state): State<Arc<AppState>>,
    Path(address): Path<String>,
    Query(params): Query<PaginatedChainFilter>,
) -> ApiResult<WalletJourneyResponse> {
    let addr = parse_address(&address)?;
    let limit = params.limit.unwrap_or(100).min(1000);
    let offset = params.offset.unwrap_or(0);
    queries::get_wallet_journey(&state.pool, &addr, params.chain_id, limit, offset)
        .await
        .map(Json)
        .map_err(|e| api_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

pub async fn wallet_fingerprint(
    State(state): State<Arc<AppState>>,
    Path(address): Path<String>,
    Query(params): Query<ChainFilter>,
) -> ApiResult<FingerprintResponse> {
    let addr = parse_address(&address)?;
    queries::get_wallet_fingerprint(&state.pool, &addr, params.chain_id)
        .await
        .map(Json)
        .map_err(|e| api_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

pub async fn similar_wallets(
    State(state): State<Arc<AppState>>,
    Path(address): Path<String>,
    Query(params): Query<SimilarParams>,
) -> ApiResult<SimilarWalletsResponse> {
    let addr = parse_address(&address)?;
    let limit = params.limit.unwrap_or(20).min(100);
    queries::get_similar_wallets(&state.pool, &addr, params.chain_id, limit)
        .await
        .map(Json)
        .map_err(|e| api_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

// ============================================================
// Transfers
// ============================================================

pub async fn list_transfers(
    State(state): State<Arc<AppState>>,
    Query(params): Query<TransferParams>,
) -> ApiResult<TransfersResponse> {
    queries::get_transfers(&state.pool, &params)
        .await
        .map(Json)
        .map_err(|e| api_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

// ============================================================
// Anomalies
// ============================================================

pub async fn list_anomalies(
    State(state): State<Arc<AppState>>,
    Query(params): Query<AnomalyParams>,
) -> ApiResult<AnomaliesResponse> {
    queries::get_anomalies(&state.pool, &params)
        .await
        .map(Json)
        .map_err(|e| api_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

// ============================================================
// Entities
// ============================================================

pub async fn list_entities(
    State(state): State<Arc<AppState>>,
    Query(params): Query<EntityParams>,
) -> ApiResult<EntitiesResponse> {
    queries::get_entities(&state.pool, &params)
        .await
        .map(Json)
        .map_err(|e| api_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

pub async fn entity_by_address(
    State(state): State<Arc<AppState>>,
    Path(address): Path<String>,
) -> ApiResult<EntitiesResponse> {
    let addr = parse_address(&address)?;
    queries::get_entity_by_address(&state.pool, &addr)
        .await
        .map(Json)
        .map_err(|e| api_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

// ============================================================
// DeFi Events
// ============================================================

pub async fn wallet_defi(
    State(state): State<Arc<AppState>>,
    Path(address): Path<String>,
    Query(params): Query<DefiParams>,
) -> ApiResult<DefiEventsResponse> {
    let addr = parse_address(&address)?;
    queries::get_wallet_defi(&state.pool, &addr, &params)
        .await
        .map(Json)
        .map_err(|e| api_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

pub async fn list_defi_events(
    State(state): State<Arc<AppState>>,
    Query(params): Query<DefiParams>,
) -> ApiResult<DefiEventsResponse> {
    queries::get_defi_events(&state.pool, &params)
        .await
        .map(Json)
        .map_err(|e| api_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

pub async fn tx_context(
    State(state): State<Arc<AppState>>,
    Path(tx_hash): Path<String>,
) -> ApiResult<TxContextResponse> {
    let hash_bytes = parse_address(&tx_hash)?;
    queries::get_tx_context(&state.pool, &hash_bytes)
        .await
        .map(Json)
        .map_err(|e| api_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

// ============================================================
// Cluster
// ============================================================

pub async fn cluster_detail(
    State(state): State<Arc<AppState>>,
    Path(cluster_id): Path<i64>,
    Query(params): Query<ChainFilter>,
) -> ApiResult<ClusterResponse> {
    let chain_id = params.chain_id.unwrap_or(1);
    queries::get_cluster(&state.pool, cluster_id, chain_id)
        .await
        .map(Json)
        .map_err(|e| api_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}
