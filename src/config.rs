use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub database: DatabaseConfig,
    pub chains: Vec<ChainConfig>,
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
            }],
        };
        assert!(config.validate().is_err());
    }
}
