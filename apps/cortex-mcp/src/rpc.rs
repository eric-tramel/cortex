use anyhow::Result;
use cortex_config::AppConfig;

pub async fn run(cfg: AppConfig) -> Result<()> {
    cortex_mcp_core::run_stdio(cfg).await
}
