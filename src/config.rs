use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub database: DatabaseConfig,
    pub chains: Vec<ChainConfig>,
    #[serde(default)]
    pub onramp_providers: Vec<OnrampProviderConfig>,
    #[serde(default)]
    pub fiat_currencies: Vec<FiatCurrencyConfig>,
    #[serde(default)]
    pub entity_attribution: EntityAttributionConfig,
    #[serde(default)]
    pub anomaly_detection: AnomalyDetectionConfig,
    #[serde(default)]
    pub api: ApiConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DatabaseConfig {
    pub url: String,
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
}

fn default_max_connections() -> u32 {
    10
}

#[derive(Debug, Deserialize, Clone)]
pub struct ChainConfig {
    pub name: String,
    pub chain_id: u64,
    pub rpc_http: String,
    pub rpc_ws: Option<String>,
    pub start_block: Option<u64>,
    #[serde(default = "default_batch_size")]
    pub batch_size: u64,
    #[serde(default = "default_poll_interval_ms")]
    pub poll_interval_ms: u64,
    #[serde(default = "default_max_reorg_depth")]
    pub max_reorg_depth: u64,
    pub tokens: Vec<TokenConfig>,
    #[serde(default = "default_true")]
    pub decode_defi: bool,
}

fn default_batch_size() -> u64 {
    100
}

fn default_poll_interval_ms() -> u64 {
    2000
}

fn default_max_reorg_depth() -> u64 {
    64
}

#[derive(Debug, Deserialize, Clone)]
pub struct TokenConfig {
    pub symbol: String,
    pub address: String,
    pub decimals: u8,
}

#[derive(Debug, Deserialize, Clone)]
pub struct OnrampProviderConfig {
    pub name: String,
    pub provider_type: String,
    pub website: Option<String>,
    #[serde(default)]
    pub supported_fiat: Vec<String>,
    #[serde(default = "default_kyc_required")]
    pub kyc_required: bool,
    pub wallets: Option<Vec<ProviderWalletConfig>>,
}

fn default_kyc_required() -> bool {
    true
}

#[derive(Debug, Deserialize, Clone)]
pub struct ProviderWalletConfig {
    pub chain: String,
    pub address: String,
    pub label: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct FiatCurrencyConfig {
    pub code: String,
    pub name: String,
    pub country: String,
    pub region: String,
    pub primary_stablecoin: String,
    #[serde(default = "default_risk_tier")]
    pub risk_tier: String,
}

fn default_risk_tier() -> String {
    "medium".to_string()
}

// ============================================================
// Entity Attribution Config
// ============================================================

#[derive(Debug, Deserialize, Clone, Default)]
pub struct EntityAttributionConfig {
    pub ofac_sdn_path: Option<String>,
    pub custom_watchlist_path: Option<String>,
    #[serde(default)]
    pub manual_labels: Vec<ManualLabelConfig>,
    #[serde(default)]
    pub custom_watchlists: Vec<CustomWatchlistConfig>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ManualLabelConfig {
    pub address: String,
    pub chain_id: Option<i64>,
    pub entity_name: String,
    pub entity_type: String,
    #[serde(default = "default_confidence")]
    pub confidence: f32,
    #[serde(default = "default_label_source")]
    pub source: String,
}

fn default_confidence() -> f32 {
    1.0
}

fn default_label_source() -> String {
    "config".to_string()
}

#[derive(Debug, Deserialize, Clone)]
pub struct CustomWatchlistConfig {
    pub name: String,
    pub file_path: String,
}

// ============================================================
// Anomaly Detection Config
// ============================================================

#[derive(Debug, Deserialize, Clone)]
pub struct AnomalyDetectionConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub large_transfer_thresholds: HashMap<String, f64>,
    #[serde(default)]
    pub velocity: VelocityConfig,
    #[serde(default)]
    pub round_number: RoundNumberConfig,
    #[serde(default)]
    pub new_wallet: NewWalletAnomalyConfig,
    #[serde(default)]
    pub cross_chain: CrossChainConfig,
}

impl Default for AnomalyDetectionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            large_transfer_thresholds: HashMap::new(),
            velocity: VelocityConfig::default(),
            round_number: RoundNumberConfig::default(),
            new_wallet: NewWalletAnomalyConfig::default(),
            cross_chain: CrossChainConfig::default(),
        }
    }
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Deserialize, Clone)]
pub struct VelocityConfig {
    #[serde(default = "default_velocity_window")]
    pub window_secs: u64,
    #[serde(default = "default_velocity_max")]
    pub max_transfers: u32,
}

impl Default for VelocityConfig {
    fn default() -> Self {
        Self {
            window_secs: 3600,
            max_transfers: 10,
        }
    }
}

fn default_velocity_window() -> u64 {
    3600
}

fn default_velocity_max() -> u32 {
    10
}

#[derive(Debug, Deserialize, Clone)]
pub struct RoundNumberConfig {
    #[serde(default = "default_round_tolerance")]
    pub tolerance: f64,
}

impl Default for RoundNumberConfig {
    fn default() -> Self {
        Self { tolerance: 0.01 }
    }
}

fn default_round_tolerance() -> f64 {
    0.01
}

#[derive(Debug, Deserialize, Clone)]
pub struct NewWalletAnomalyConfig {
    #[serde(default = "default_new_wallet_threshold")]
    pub threshold_usd: f64,
}

impl Default for NewWalletAnomalyConfig {
    fn default() -> Self {
        Self {
            threshold_usd: 10000.0,
        }
    }
}

fn default_new_wallet_threshold() -> f64 {
    10000.0
}

#[derive(Debug, Deserialize, Clone)]
pub struct CrossChainConfig {
    #[serde(default = "default_cross_chain_window")]
    pub window_secs: u64,
}

impl Default for CrossChainConfig {
    fn default() -> Self {
        Self { window_secs: 1800 }
    }
}

fn default_cross_chain_window() -> u64 {
    1800
}

// ============================================================
// API Config
// ============================================================

#[derive(Debug, Deserialize, Clone)]
pub struct ApiConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_api_port")]
    pub port: u16,
    #[serde(default = "default_api_host")]
    pub host: String,
    pub exchange_wallets_path: Option<String>,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            port: 3000,
            host: "0.0.0.0".to_string(),
            exchange_wallets_path: None,
        }
    }
}

fn default_api_port() -> u16 {
    3000
}

fn default_api_host() -> String {
    "0.0.0.0".to_string()
}

impl Config {
    pub fn load(path: &str) -> eyre::Result<Self> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| eyre::eyre!("Failed to read config file '{}': {}", path, e))?;
        let config: Config = toml::from_str(&content)
            .map_err(|e| eyre::eyre!("Failed to parse config file '{}': {}", path, e))?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> eyre::Result<()> {
        if self.chains.is_empty() {
            return Err(eyre::eyre!("At least one chain must be configured"));
        }
        for chain in &self.chains {
            if chain.tokens.is_empty() {
                return Err(eyre::eyre!(
                    "Chain '{}' must have at least one token configured",
                    chain.name
                ));
            }
            for token in &chain.tokens {
                if !token.address.starts_with("0x") || token.address.len() != 42 {
                    return Err(eyre::eyre!(
                        "Invalid token address '{}' for {} on chain '{}'",
                        token.address,
                        token.symbol,
                        chain.name
                    ));
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_config() {
        let toml_str = r#"
[database]
url = "postgres://localhost/test"
max_connections = 5

[[chains]]
name = "ethereum"
chain_id = 1
rpc_http = "http://localhost:8545"

[[chains.tokens]]
symbol = "USDC"
address = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
decimals = 6
"#;

        let config: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(config.chains.len(), 1);
        assert_eq!(config.chains[0].name, "ethereum");
        assert_eq!(config.chains[0].chain_id, 1);
        assert_eq!(config.chains[0].tokens[0].symbol, "USDC");
        assert_eq!(config.chains[0].tokens[0].decimals, 6);
        assert_eq!(config.chains[0].batch_size, 100); // default
        assert_eq!(config.chains[0].max_reorg_depth, 64); // default
    }

    #[test]
    fn test_validate_empty_chains() {
        let config = Config {
            database: DatabaseConfig {
                url: "postgres://localhost/test".to_string(),
                max_connections: 5,
            },
            chains: vec![],
            onramp_providers: vec![],
            fiat_currencies: vec![],
            entity_attribution: EntityAttributionConfig::default(),
            anomaly_detection: AnomalyDetectionConfig::default(),
            api: ApiConfig::default(),
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_bad_address() {
        let config = Config {
            database: DatabaseConfig {
                url: "postgres://localhost/test".to_string(),
                max_connections: 5,
            },
            chains: vec![ChainConfig {
                name: "test".to_string(),
                chain_id: 1,
                rpc_http: "http://localhost:8545".to_string(),
                rpc_ws: None,
                start_block: None,
                batch_size: 100,
                poll_interval_ms: 2000,
                max_reorg_depth: 64,
                tokens: vec![TokenConfig {
                    symbol: "BAD".to_string(),
                    address: "not-an-address".to_string(),
                    decimals: 6,
                }],
                decode_defi: true,
            }],
            onramp_providers: vec![],
            fiat_currencies: vec![],
            entity_attribution: EntityAttributionConfig::default(),
            anomaly_detection: AnomalyDetectionConfig::default(),
            api: ApiConfig::default(),
        };
        assert!(config.validate().is_err());
    }
}
