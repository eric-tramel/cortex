use anyhow::Result;
use std::path::Path;

pub use cortex_config::{AppConfig, ClickHouseConfig, IngestConfig, IngestSource};

pub fn expand_path(path: &str) -> String {
    cortex_config::expand_path(path)
}

pub fn load_config(path: impl AsRef<Path>) -> Result<AppConfig> {
    cortex_config::load_config(path)
}
