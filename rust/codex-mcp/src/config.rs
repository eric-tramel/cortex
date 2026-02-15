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
    #[serde(default = "default_async_insert")]
    pub async_insert: bool,
    #[serde(default = "default_wait_for_async_insert")]
    pub wait_for_async_insert: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct McpConfig {
    #[serde(default = "default_max_results")]
    pub max_results: u16,
    #[serde(default = "default_preview_chars")]
    pub preview_chars: u16,
    #[serde(default = "default_context_before")]
    pub default_context_before: u16,
    #[serde(default = "default_context_after")]
    pub default_context_after: u16,
    #[serde(default = "default_include_tool_events")]
    pub default_include_tool_events: bool,
    #[serde(default = "default_exclude_codex_mcp")]
    pub default_exclude_codex_mcp: bool,
    #[serde(default = "default_async_log_writes")]
    pub async_log_writes: bool,
    #[serde(default = "default_protocol_version")]
    pub protocol_version: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Bm25Config {
    #[serde(default = "default_k1")]
    pub k1: f64,
    #[serde(default = "default_b")]
    pub b: f64,
    #[serde(default = "default_min_score")]
    pub default_min_score: f64,
    #[serde(default = "default_min_should_match")]
    pub default_min_should_match: u16,
    #[serde(default = "default_max_query_terms")]
    pub max_query_terms: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    #[serde(default)]
    pub clickhouse: ClickHouseConfig,
    #[serde(default)]
    pub mcp: McpConfig,
    #[serde(default)]
    pub bm25: Bm25Config,
}

impl Default for ClickHouseConfig {
    fn default() -> Self {
        Self {
            url: default_ch_url(),
            database: default_ch_database(),
            username: default_ch_username(),
            password: String::new(),
            timeout_seconds: default_timeout_seconds(),
            async_insert: default_async_insert(),
            wait_for_async_insert: default_wait_for_async_insert(),
        }
    }
}

impl Default for McpConfig {
    fn default() -> Self {
        Self {
            max_results: default_max_results(),
            preview_chars: default_preview_chars(),
            default_context_before: default_context_before(),
            default_context_after: default_context_after(),
            default_include_tool_events: default_include_tool_events(),
            default_exclude_codex_mcp: default_exclude_codex_mcp(),
            async_log_writes: default_async_log_writes(),
            protocol_version: default_protocol_version(),
        }
    }
}

impl Default for Bm25Config {
    fn default() -> Self {
        Self {
            k1: default_k1(),
            b: default_b(),
            default_min_score: default_min_score(),
            default_min_should_match: default_min_should_match(),
            max_query_terms: default_max_query_terms(),
        }
    }
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            clickhouse: ClickHouseConfig::default(),
            mcp: McpConfig::default(),
            bm25: Bm25Config::default(),
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

fn default_async_insert() -> bool {
    true
}

fn default_wait_for_async_insert() -> bool {
    true
}

fn default_max_results() -> u16 {
    25
}

fn default_preview_chars() -> u16 {
    320
}

fn default_context_before() -> u16 {
    3
}

fn default_context_after() -> u16 {
    3
}

fn default_include_tool_events() -> bool {
    false
}

fn default_exclude_codex_mcp() -> bool {
    true
}

fn default_async_log_writes() -> bool {
    false
}

fn default_protocol_version() -> String {
    "2024-11-05".to_string()
}

fn default_k1() -> f64 {
    1.2
}

fn default_b() -> f64 {
    0.75
}

fn default_min_score() -> f64 {
    0.0
}

fn default_min_should_match() -> u16 {
    1
}

fn default_max_query_terms() -> usize {
    32
}

pub fn resolve_config_path(raw_path: Option<PathBuf>) -> PathBuf {
    if let Some(path) = raw_path {
        return path;
    }

    if let Some(home) = std::env::var_os("HOME") {
        let shared = PathBuf::from(home).join(".cortex").join("config.toml");
        if shared.exists() {
            return shared;
        }
    }

    let default_path = PathBuf::from("config/cortex.toml");
    if default_path.exists() {
        return default_path;
    }

    let legacy = PathBuf::from("config/codex-mcp.toml");
    if legacy.exists() {
        return legacy;
    }

    PathBuf::from("config/ingestor.toml")
}

pub fn load_config(path: impl AsRef<Path>) -> Result<AppConfig> {
    let content = std::fs::read_to_string(path.as_ref())
        .with_context(|| format!("failed to read config {}", path.as_ref().display()))?;
    let cfg: AppConfig = toml::from_str(&content).context("failed to parse TOML config")?;
    Ok(cfg)
}
