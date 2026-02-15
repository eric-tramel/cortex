use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Deserialize)]
pub struct ClickHouseConfig {
    #[serde(default = "default_ch_url")]
    pub url: String,
    #[serde(default = "default_ch_database")]
    pub database: String,
    #[serde(default = "default_ch_username")]
    pub username: String,
    #[serde(default)]
    pub password: String,
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: f64,
    #[serde(default = "default_true")]
    pub async_insert: bool,
    #[serde(default = "default_true")]
    pub wait_for_async_insert: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    #[serde(default)]
    pub clickhouse: ClickHouseConfig,
}

impl Default for ClickHouseConfig {
    fn default() -> Self {
        Self {
            url: default_ch_url(),
            database: default_ch_database(),
            username: default_ch_username(),
            password: String::new(),
            timeout_seconds: default_timeout_seconds(),
            async_insert: true,
            wait_for_async_insert: true,
        }
    }
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            clickhouse: ClickHouseConfig::default(),
        }
    }
}

fn default_ch_url() -> String {
    "http://127.0.0.1:8123".to_string()
}

fn default_ch_database() -> String {
    "cortex".to_string()
}

fn default_ch_username() -> String {
    "default".to_string()
}

fn default_timeout_seconds() -> f64 {
    30.0
}

fn default_true() -> bool {
    true
}

fn resolve_config_path(raw_path: Option<PathBuf>) -> PathBuf {
    if let Some(path) = raw_path {
        return path;
    }

    if let Ok(value) = std::env::var("CORTEX_CONFIG") {
        let path = PathBuf::from(value);
        if path.exists() {
            return path;
        }
    }

    if let Ok(home) = std::env::var("HOME") {
        let path = Path::new(&home).join(".cortex").join("config.toml");
        if path.exists() {
            return path;
        }
    }

    Path::new("config").join("cortex.toml")
}

pub fn load_config(raw_path: Option<PathBuf>) -> Result<AppConfig> {
    let path = resolve_config_path(raw_path);
    let raw = std::fs::read_to_string(&path)
        .with_context(|| format!("failed to read config {}", path.display()))?;
    let cfg: AppConfig = toml::from_str(&raw).context("failed to parse TOML config")?;
    Ok(cfg)
}
