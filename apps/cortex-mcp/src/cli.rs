use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct CliArgs {
    pub config_path: PathBuf,
}

pub fn parse_args() -> CliArgs {
    let mut args = std::env::args().skip(1);
    let mut config_path: Option<PathBuf> = None;

    while let Some(arg) = args.next() {
        if arg == "--config" {
            if let Some(value) = args.next() {
                config_path = Some(PathBuf::from(value));
            }
        }
    }

    CliArgs {
        config_path: cortex_config::resolve_mcp_config_path(config_path),
    }
}
