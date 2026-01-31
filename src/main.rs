use sqlx::postgres::PgPoolOptions;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

use chainwatch_indexer::config::Config;
use chainwatch_indexer::indexer::chain::run_chain_indexer;
use chainwatch_indexer::tokens::registry::seed_known_tokens;

#[tokio::main]
async fn main() -> eyre::Result<()> {
    color_eyre::install()?;

    // Initialize structured logging (set RUST_LOG=info for output)
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(true)
        .init();

    tracing::info!("ChainWatch Indexer starting");

    // Load configuration
    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "config.toml".to_string());

    let config = Config::load(&config_path)?;
    tracing::info!(
        chains = config.chains.len(),
        "Configuration loaded from {}",
        config_path
    );

    // Create database connection pool
    let pool = PgPoolOptions::new()
        .max_connections(config.database.max_connections)
        .connect(&config.database.url)
        .await
        .map_err(|e| eyre::eyre!("Failed to connect to database: {}", e))?;

    tracing::info!("Connected to PostgreSQL");

    // Run migrations
    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .map_err(|e| eyre::eyre!("Failed to run migrations: {}", e))?;

    tracing::info!("Database migrations complete");

    // Seed known tokens from config
    seed_known_tokens(&pool, &config.chains).await?;
    tracing::info!("Known tokens seeded");

    // Create shutdown signal
    let shutdown = CancellationToken::new();

    // Spawn one indexer task per chain
    let mut handles = Vec::new();
    for chain_config in config.chains {
        let pool = pool.clone();
        let shutdown = shutdown.clone();
        let chain_name = chain_config.name.clone();

        let handle = tokio::spawn(async move {
            if let Err(e) = run_chain_indexer(chain_config, pool, shutdown).await {
                tracing::error!(chain = %chain_name, error = %e, "Chain indexer failed");
            }
        });

        handles.push(handle);
    }

    tracing::info!("All chain indexers started. Press Ctrl+C to stop.");

    // Wait for shutdown signal
    tokio::signal::ctrl_c().await?;
    tracing::info!("Shutdown signal received, stopping all indexers...");
    shutdown.cancel();

    // Wait for all tasks to finish
    for handle in handles {
        let _ = handle.await;
    }

    tracing::info!("ChainWatch Indexer stopped gracefully");
    Ok(())
}
