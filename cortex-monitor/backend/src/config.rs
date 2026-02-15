use anyhow::Result;
use std::path::PathBuf;

pub use cortex_config::{AppConfig, ClickHouseConfig};

pub fn load_config(raw_path: Option<PathBuf>) -> Result<AppConfig> {
    let path = cortex_config::resolve_config_path(raw_path);
    cortex_config::load_config(path)
}
