use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Clone, Deserialize)]
pub struct IngestSource {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub provider: String,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    #[serde(default)]
    pub glob: String,
    #[serde(default)]
    pub watch_root: String,
}

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
pub struct IngestConfig {
    #[serde(default = "default_sources")]
    pub sources: Vec<IngestSource>,
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    #[serde(default = "default_flush_interval_seconds")]
    pub flush_interval_seconds: f64,
    #[serde(default = "default_state_dir")]
    pub state_dir: String,
    #[serde(default = "default_backfill")]
    pub backfill_on_start: bool,
    #[serde(default = "default_max_file_workers")]
    pub max_file_workers: usize,
    #[serde(default = "default_max_inflight_batches")]
    pub max_inflight_batches: usize,
    #[serde(default = "default_debounce_ms")]
    pub debounce_ms: u64,
    #[serde(default = "default_reconcile_interval_seconds")]
    pub reconcile_interval_seconds: f64,
    #[serde(default = "default_heartbeat_interval_seconds")]
    pub heartbeat_interval_seconds: f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub clickhouse: ClickHouseConfig,
    pub ingest: IngestConfig,
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

fn default_enabled() -> bool {
    true
}

fn default_sources() -> Vec<IngestSource> {
    vec![
        IngestSource {
            name: "codex".to_string(),
            provider: "codex".to_string(),
            enabled: true,
            glob: "~/.codex/sessions/**/*.jsonl".to_string(),
            watch_root: "~/.codex/sessions".to_string(),
        },
        IngestSource {
            name: "claude".to_string(),
            provider: "claude".to_string(),
            enabled: true,
            glob: "~/.claude/projects/**/*.jsonl".to_string(),
            watch_root: "~/.claude/projects".to_string(),
        },
    ]
}

fn default_batch_size() -> usize {
    4000
}

fn default_flush_interval_seconds() -> f64 {
    0.5
}

fn default_state_dir() -> String {
    "~/.cortex/ingestor".to_string()
}

fn default_backfill() -> bool {
    true
}

fn default_max_file_workers() -> usize {
    8
}

fn default_max_inflight_batches() -> usize {
    64
}

fn default_debounce_ms() -> u64 {
    50
}

fn default_reconcile_interval_seconds() -> f64 {
    30.0
}

fn default_heartbeat_interval_seconds() -> f64 {
    5.0
}

pub fn expand_path(path: &str) -> String {
    if let Some(stripped) = path.strip_prefix("~/") {
        if let Some(home) = std::env::var_os("HOME") {
            return format!("{}/{}", home.to_string_lossy(), stripped);
        }
    }
    path.to_string()
}

pub fn load_config(path: impl AsRef<Path>) -> Result<AppConfig> {
    let content = std::fs::read_to_string(path.as_ref())
        .with_context(|| format!("failed to read config {}", path.as_ref().display()))?;
    let mut cfg: AppConfig = toml::from_str(&content).context("failed to parse TOML config")?;

    for source in &mut cfg.ingest.sources {
        source.glob = expand_path(&source.glob);
        source.watch_root = if source.watch_root.trim().is_empty() {
            watch_root_from_glob(&source.glob)
        } else {
            expand_path(&source.watch_root)
        };
    }
    cfg.ingest.state_dir = expand_path(&cfg.ingest.state_dir);

    Ok(cfg)
}

fn watch_root_from_glob(glob_pattern: &str) -> String {
    if let Some(idx) = glob_pattern.find("/**") {
        return glob_pattern[..idx].to_string();
    }

    if let Some(idx) = glob_pattern.find('*') {
        let prefix = &glob_pattern[..idx];
        let path = std::path::Path::new(prefix);
        if let Some(parent) = path.parent() {
            return parent.to_string_lossy().to_string();
        }
    }

    std::path::Path::new(glob_pattern)
        .parent()
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_else(|| glob_pattern.to_string())
}
