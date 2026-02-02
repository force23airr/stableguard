pub mod handlers;
pub mod queries;
pub mod types;

use axum::{routing::get, Router};
use sqlx::PgPool;
use std::sync::Arc;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
}

pub fn router(pool: PgPool) -> Router {
    let state = Arc::new(AppState { pool });

    Router::new()
        .route("/api/v1/health", get(handlers::health))
        .route("/api/v1/stats", get(handlers::stats))
        .route("/api/v1/wallet/{address}", get(handlers::wallet_profile))
        .route(
            "/api/v1/wallet/{address}/journey",
            get(handlers::wallet_journey),
        )
        .route(
            "/api/v1/wallet/{address}/fingerprint",
            get(handlers::wallet_fingerprint),
        )
        .route(
            "/api/v1/wallet/{address}/similar",
            get(handlers::similar_wallets),
        )
        .route("/api/v1/transfers", get(handlers::list_transfers))
        .route("/api/v1/anomalies", get(handlers::list_anomalies))
        .route("/api/v1/entities", get(handlers::list_entities))
        .route(
            "/api/v1/entities/{address}",
            get(handlers::entity_by_address),
        )
        .route(
            "/api/v1/wallet/{address}/defi",
            get(handlers::wallet_defi),
        )
        .route("/api/v1/defi/events", get(handlers::list_defi_events))
        .route("/api/v1/tx/{tx_hash}", get(handlers::tx_context))
        .route(
            "/api/v1/cluster/{cluster_id}",
            get(handlers::cluster_detail),
        )
        .with_state(state)
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
}

pub async fn serve(pool: PgPool, host: &str, port: u16) -> eyre::Result<()> {
    let app = router(pool);
    let addr = format!("{}:{}", host, port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!(%addr, "API server listening");
    axum::serve(listener, app).await?;
    Ok(())
}
