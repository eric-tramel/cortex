use anyhow::{anyhow, bail, Context, Result};
use cortex_clickhouse::{ClickHouseClient, DoctorReport};
use cortex_config::AppConfig;
use serde::Deserialize;
use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, Stdio};
use std::time::Duration;
use tokio::time::{sleep, Instant};

#[derive(Clone)]
struct RuntimePaths {
    root: PathBuf,
    logs_dir: PathBuf,
    pids_dir: PathBuf,
    clickhouse_root: PathBuf,
    clickhouse_config: PathBuf,
    clickhouse_users: PathBuf,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Service {
    ClickHouse,
    Ingest,
    Monitor,
    Mcp,
}

impl Service {
    fn name(self) -> &'static str {
        match self {
            Self::ClickHouse => "clickhouse",
            Self::Ingest => "ingest",
            Self::Monitor => "monitor",
            Self::Mcp => "mcp",
        }
    }

    fn pid_file(self) -> &'static str {
        match self {
            Self::ClickHouse => "clickhouse.pid",
            Self::Ingest => "ingest.pid",
            Self::Monitor => "monitor.pid",
            Self::Mcp => "mcp.pid",
        }
    }

    fn log_file(self) -> &'static str {
        match self {
            Self::ClickHouse => "clickhouse.log",
            Self::Ingest => "ingest.log",
            Self::Monitor => "monitor.log",
            Self::Mcp => "mcp.log",
        }
    }

    fn package(self) -> Option<&'static str> {
        match self {
            Self::ClickHouse => None,
            Self::Ingest => Some("cortex-ingest"),
            Self::Monitor => Some("cortex-monitor"),
            Self::Mcp => Some("cortex-mcp"),
        }
    }
}

#[derive(Debug, Deserialize)]
struct HeartbeatRow {
    latest: String,
    queue_depth: u64,
    files_active: u64,
}

fn project_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root")
        .to_path_buf()
}

fn usage() {
    eprintln!(
        "usage:
  cortexctl up [--config <path>] [--no-ingest] [--monitor] [--mcp]
  cortexctl down [--config <path>]
  cortexctl status [--config <path>]
  cortexctl logs [service] [--lines <n>] [--config <path>]
  cortexctl db migrate [--config <path>]
  cortexctl db doctor [--config <path>]
  cortexctl run ingest [--config <path>] [args...]
  cortexctl run monitor [--config <path>] [args...]
  cortexctl run mcp [--config <path>] [args...]"
    );
}

fn runtime_paths(cfg: &AppConfig) -> RuntimePaths {
    let root = PathBuf::from(&cfg.runtime.root_dir);
    let clickhouse_root = root.join("clickhouse");

    RuntimePaths {
        root,
        logs_dir: PathBuf::from(&cfg.runtime.logs_dir),
        pids_dir: PathBuf::from(&cfg.runtime.pids_dir),
        clickhouse_config: clickhouse_root.join("config.xml"),
        clickhouse_users: clickhouse_root.join("users.xml"),
        clickhouse_root,
    }
}

fn ensure_runtime_dirs(paths: &RuntimePaths) -> Result<()> {
    fs::create_dir_all(&paths.root)
        .with_context(|| format!("failed to create {}", paths.root.display()))?;
    fs::create_dir_all(&paths.logs_dir)
        .with_context(|| format!("failed to create {}", paths.logs_dir.display()))?;
    fs::create_dir_all(&paths.pids_dir)
        .with_context(|| format!("failed to create {}", paths.pids_dir.display()))?;

    fs::create_dir_all(paths.clickhouse_root.join("data"))?;
    fs::create_dir_all(paths.clickhouse_root.join("tmp"))?;
    fs::create_dir_all(paths.clickhouse_root.join("log"))?;
    fs::create_dir_all(paths.clickhouse_root.join("user_files"))?;
    fs::create_dir_all(paths.clickhouse_root.join("format_schemas"))?;

    Ok(())
}

fn pid_path(paths: &RuntimePaths, service: Service) -> PathBuf {
    paths.pids_dir.join(service.pid_file())
}

fn log_path(paths: &RuntimePaths, service: Service) -> PathBuf {
    paths.logs_dir.join(service.log_file())
}

fn read_pid(path: &Path) -> Option<u32> {
    let text = fs::read_to_string(path).ok()?;
    text.trim().parse::<u32>().ok()
}

fn write_pid(path: &Path, pid: u32) -> Result<()> {
    fs::write(path, format!("{}\n", pid))
        .with_context(|| format!("failed to write pid file {}", path.display()))
}

fn is_pid_running(pid: u32) -> bool {
    Command::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

fn ensure_pid_fresh(path: &Path) {
    if let Some(pid) = read_pid(path) {
        if !is_pid_running(pid) {
            let _ = fs::remove_file(path);
        }
    }
}

fn service_running(paths: &RuntimePaths, service: Service) -> Option<u32> {
    let path = pid_path(paths, service);
    ensure_pid_fresh(&path);
    let pid = read_pid(&path)?;
    if is_pid_running(pid) {
        Some(pid)
    } else {
        None
    }
}

fn stop_service(paths: &RuntimePaths, service: Service) -> Result<bool> {
    let path = pid_path(paths, service);
    let Some(pid) = read_pid(&path) else {
        return Ok(false);
    };

    if !is_pid_running(pid) {
        let _ = fs::remove_file(path);
        return Ok(false);
    }

    let _ = Command::new("kill").arg(pid.to_string()).status();

    for _ in 0..20 {
        if !is_pid_running(pid) {
            let _ = fs::remove_file(&path);
            return Ok(true);
        }
        std::thread::sleep(Duration::from_millis(200));
    }

    let _ = Command::new("kill").arg("-9").arg(pid.to_string()).status();
    let _ = fs::remove_file(path);
    Ok(true)
}

fn parse_config_flag(args: &[String]) -> Result<(Option<PathBuf>, Vec<String>)> {
    let mut raw_config = None;
    let mut rest = Vec::new();

    let mut i = 0usize;
    while i < args.len() {
        if args[i] == "--config" {
            if i + 1 >= args.len() {
                bail!("--config requires a path");
            }
            raw_config = Some(PathBuf::from(args[i + 1].clone()));
            i += 2;
            continue;
        }

        rest.push(args[i].clone());
        i += 1;
    }

    Ok((raw_config, rest))
}

fn load_cfg(raw_config: Option<PathBuf>) -> Result<(PathBuf, AppConfig)> {
    let config_path = cortex_config::resolve_config_path(raw_config);
    let cfg = cortex_config::load_config(&config_path)
        .with_context(|| format!("failed to load config {}", config_path.display()))?;
    Ok((config_path, cfg))
}

fn materialize_clickhouse_config(cfg: &AppConfig, paths: &RuntimePaths) -> Result<()> {
    let root = project_root();
    let clickhouse_template = fs::read_to_string(root.join("config/clickhouse.xml"))
        .context("failed reading config/clickhouse.xml")?;
    let users_template = fs::read_to_string(root.join("config/users.xml"))
        .context("failed reading config/users.xml")?;

    let rendered_clickhouse = clickhouse_template.replace("__CORTEX_HOME__", &cfg.runtime.root_dir);
    let rendered_users = users_template.replace("__CORTEX_HOME__", &cfg.runtime.root_dir);

    fs::write(&paths.clickhouse_config, rendered_clickhouse).with_context(|| {
        format!(
            "failed writing clickhouse config {}",
            paths.clickhouse_config.display()
        )
    })?;
    fs::write(&paths.clickhouse_users, rendered_users).with_context(|| {
        format!(
            "failed writing users config {}",
            paths.clickhouse_users.display()
        )
    })?;

    Ok(())
}

fn clickhouse_binary_available() -> bool {
    Command::new("clickhouse-server")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

async fn wait_for_clickhouse(cfg: &AppConfig) -> Result<()> {
    let client = ClickHouseClient::new(cfg.clickhouse.clone())?;
    let timeout = Duration::from_secs_f64(cfg.runtime.clickhouse_start_timeout_seconds.max(1.0));
    let interval = Duration::from_millis(cfg.runtime.healthcheck_interval_ms.max(100));
    let start = Instant::now();

    loop {
        if client.ping().await.is_ok() {
            return Ok(());
        }

        if start.elapsed() >= timeout {
            bail!(
                "clickhouse did not become healthy within {:.1}s",
                timeout.as_secs_f64()
            );
        }

        sleep(interval).await;
    }
}

async fn start_clickhouse(cfg: &AppConfig, paths: &RuntimePaths) -> Result<()> {
    if let Some(pid) = service_running(paths, Service::ClickHouse) {
        println!("clickhouse already running (pid {})", pid);
        return Ok(());
    }

    if !clickhouse_binary_available() {
        bail!("clickhouse-server is not installed or not on PATH");
    }

    materialize_clickhouse_config(cfg, paths)?;

    let logfile = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path(paths, Service::ClickHouse))
        .context("failed to open clickhouse log file")?;
    let logfile_err = logfile
        .try_clone()
        .context("failed to clone clickhouse log file")?;

    let child = Command::new("clickhouse-server")
        .arg("--config-file")
        .arg(&paths.clickhouse_config)
        .stdout(Stdio::from(logfile))
        .stderr(Stdio::from(logfile_err))
        .spawn()
        .context("failed to start clickhouse-server")?;

    write_pid(&pid_path(paths, Service::ClickHouse), child.id())?;

    wait_for_clickhouse(cfg).await?;
    Ok(())
}

fn start_background_service(
    service: Service,
    cfg_path: &Path,
    cfg: &AppConfig,
    paths: &RuntimePaths,
    extra_args: &[String],
) -> Result<()> {
    if service == Service::ClickHouse {
        bail!("clickhouse is not managed by cargo-run service launcher");
    }

    if let Some(pid) = service_running(paths, service) {
        println!("{} already running (pid {})", service.name(), pid);
        return Ok(());
    }

    let package = service
        .package()
        .ok_or_else(|| anyhow!("service {:?} has no cargo package", service.name()))?;

    let logfile = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path(paths, service))
        .with_context(|| format!("failed to open {} log", service.name()))?;
    let logfile_err = logfile
        .try_clone()
        .with_context(|| format!("failed to clone {} log", service.name()))?;

    let mut args = vec![
        "run".to_string(),
        "--quiet".to_string(),
        "--package".to_string(),
        package.to_string(),
        "--".to_string(),
        "--config".to_string(),
        cfg_path.to_string_lossy().to_string(),
    ];

    if service == Service::Monitor {
        args.push("--host".to_string());
        args.push(cfg.monitor.host.clone());
        args.push("--port".to_string());
        args.push(cfg.monitor.port.to_string());
    }

    args.extend(extra_args.iter().cloned());

    let child = Command::new("cargo")
        .current_dir(project_root())
        .args(args)
        .stdout(Stdio::from(logfile))
        .stderr(Stdio::from(logfile_err))
        .spawn()
        .with_context(|| format!("failed to start {}", service.name()))?;

    write_pid(&pid_path(paths, service), child.id())?;
    println!(
        "{} started (pid {}) log={} ",
        service.name(),
        child.id(),
        log_path(paths, service).display()
    );

    Ok(())
}

fn run_foreground_service(
    service: Service,
    cfg_path: &Path,
    passthrough_args: &[String],
) -> Result<ExitCode> {
    let package = service
        .package()
        .ok_or_else(|| anyhow!("service {:?} has no cargo package", service.name()))?;

    let mut args = vec![
        "run".to_string(),
        "--package".to_string(),
        package.to_string(),
        "--".to_string(),
    ];

    let has_config = passthrough_args
        .windows(1)
        .any(|w| w.first().is_some_and(|arg| arg == "--config"));

    if !has_config {
        args.push("--config".to_string());
        args.push(cfg_path.to_string_lossy().to_string());
    }

    args.extend(passthrough_args.iter().cloned());

    let status = Command::new("cargo")
        .current_dir(project_root())
        .args(args)
        .status()
        .with_context(|| format!("failed to run {}", service.name()))?;

    Ok(ExitCode::from(status.code().unwrap_or(1) as u8))
}

async fn cmd_db_migrate(cfg: &AppConfig) -> Result<()> {
    let ch = ClickHouseClient::new(cfg.clickhouse.clone())?;
    let applied = ch.run_migrations().await?;
    if applied.is_empty() {
        println!("migrations already up to date");
    } else {
        println!("applied migrations: {}", applied.join(", "));
    }
    Ok(())
}

async fn cmd_db_doctor(cfg: &AppConfig) -> Result<DoctorReport> {
    let ch = ClickHouseClient::new(cfg.clickhouse.clone())?;
    ch.doctor_report().await
}

async fn query_heartbeat(cfg: &AppConfig) -> Result<Option<HeartbeatRow>> {
    let ch = ClickHouseClient::new(cfg.clickhouse.clone())?;
    let db = quote_identifier(&cfg.clickhouse.database);
    let query = format!(
        "SELECT toString(max(ts)) AS latest, toUInt64(argMax(queue_depth, ts)) AS queue_depth, toUInt64(argMax(files_active, ts)) AS files_active FROM {db}.ingest_heartbeats"
    );

    let rows: Vec<HeartbeatRow> = ch.query_json_data(&query, None).await?;
    Ok(rows.into_iter().next())
}

fn quote_identifier(value: &str) -> String {
    format!("`{}`", value.replace('`', "``"))
}

async fn cmd_status(paths: &RuntimePaths, cfg: &AppConfig) -> Result<()> {
    for service in [
        Service::ClickHouse,
        Service::Ingest,
        Service::Monitor,
        Service::Mcp,
    ] {
        match service_running(paths, service) {
            Some(pid) => println!("{}: running (pid {})", service.name(), pid),
            None => println!("{}: stopped", service.name()),
        }
    }

    let report = cmd_db_doctor(cfg).await?;
    println!("clickhouse healthy: {}", report.clickhouse_healthy);
    if let Some(version) = report.clickhouse_version {
        println!("clickhouse version: {}", version);
    }
    println!("database exists: {}", report.database_exists);
    println!(
        "pending migrations: {}",
        report.pending_migrations.join(",")
    );
    println!("missing tables: {}", report.missing_tables.join(","));
    if !report.errors.is_empty() {
        println!("doctor errors: {}", report.errors.join(" | "));
    }

    match query_heartbeat(cfg).await {
        Ok(Some(row)) => {
            println!("ingest heartbeat latest: {}", row.latest);
            println!("ingest queue depth: {}", row.queue_depth);
            println!("ingest files active: {}", row.files_active);
        }
        Ok(None) => println!("ingest heartbeat: unavailable"),
        Err(err) => println!("ingest heartbeat error: {}", err),
    }

    Ok(())
}

fn tail_lines(path: &Path, lines: usize) -> Result<Vec<String>> {
    let content = fs::read_to_string(path)
        .with_context(|| format!("failed to read log file {}", path.display()))?;
    let mut collected = content
        .lines()
        .rev()
        .take(lines)
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    collected.reverse();
    Ok(collected)
}

fn print_logs(paths: &RuntimePaths, service: Option<Service>, lines: usize) -> Result<()> {
    let targets = match service {
        Some(svc) => vec![svc],
        None => vec![
            Service::ClickHouse,
            Service::Ingest,
            Service::Monitor,
            Service::Mcp,
        ],
    };

    for svc in targets {
        let path = log_path(paths, svc);
        println!("== {} ({}) ==", svc.name(), path.display());
        if !path.exists() {
            println!("<no log file>");
            continue;
        }

        for line in tail_lines(&path, lines)? {
            println!("{}", line);
        }
    }

    Ok(())
}

fn parse_service(name: &str) -> Option<Service> {
    match name {
        "clickhouse" => Some(Service::ClickHouse),
        "ingest" => Some(Service::Ingest),
        "monitor" => Some(Service::Monitor),
        "mcp" => Some(Service::Mcp),
        _ => None,
    }
}

fn parse_logs_args(args: &[String]) -> Result<(Option<Service>, usize, Option<PathBuf>)> {
    let mut service = None;
    let mut lines = 200usize;

    let mut i = 0usize;
    let mut raw_config = None;
    while i < args.len() {
        match args[i].as_str() {
            "--lines" => {
                if i + 1 >= args.len() {
                    bail!("--lines requires a number");
                }
                lines = args[i + 1]
                    .parse::<usize>()
                    .map_err(|e| anyhow!("invalid --lines value: {e}"))?;
                i += 2;
            }
            "--config" => {
                if i + 1 >= args.len() {
                    bail!("--config requires a path");
                }
                raw_config = Some(PathBuf::from(args[i + 1].clone()));
                i += 2;
            }
            other => {
                if service.is_none() {
                    service = parse_service(other);
                    if service.is_none() {
                        bail!("unknown service: {}", other);
                    }
                } else {
                    bail!("unexpected argument: {}", other);
                }
                i += 1;
            }
        }
    }

    Ok((service, lines, raw_config))
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<ExitCode> {
    let mut args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.is_empty() {
        usage();
        return Ok(ExitCode::from(2));
    }

    let command = args.remove(0);

    match command.as_str() {
        "up" => {
            let (raw_config, rest) = parse_config_flag(&args)?;
            let (config_path, cfg) = load_cfg(raw_config)?;
            let paths = runtime_paths(&cfg);
            ensure_runtime_dirs(&paths)?;

            let no_ingest = rest.iter().any(|arg| arg == "--no-ingest");
            let force_monitor = rest.iter().any(|arg| arg == "--monitor");
            let force_mcp = rest.iter().any(|arg| arg == "--mcp");

            start_clickhouse(&cfg, &paths).await?;
            cmd_db_migrate(&cfg).await?;

            if !no_ingest {
                start_background_service(Service::Ingest, &config_path, &cfg, &paths, &[])?;
            }
            if force_monitor || cfg.runtime.start_monitor_on_up {
                start_background_service(Service::Monitor, &config_path, &cfg, &paths, &[])?;
            }
            if force_mcp || cfg.runtime.start_mcp_on_up {
                start_background_service(Service::Mcp, &config_path, &cfg, &paths, &[])?;
            }

            cmd_status(&paths, &cfg).await?;
            Ok(ExitCode::SUCCESS)
        }
        "down" => {
            let (raw_config, rest) = parse_config_flag(&args)?;
            if !rest.is_empty() {
                bail!("unexpected arguments for down: {}", rest.join(" "));
            }
            let (_, cfg) = load_cfg(raw_config)?;
            let paths = runtime_paths(&cfg);

            for service in [
                Service::Mcp,
                Service::Monitor,
                Service::Ingest,
                Service::ClickHouse,
            ] {
                if stop_service(&paths, service)? {
                    println!("stopped {}", service.name());
                }
            }

            Ok(ExitCode::SUCCESS)
        }
        "status" => {
            let (raw_config, rest) = parse_config_flag(&args)?;
            if !rest.is_empty() {
                bail!("unexpected arguments for status: {}", rest.join(" "));
            }
            let (_, cfg) = load_cfg(raw_config)?;
            let paths = runtime_paths(&cfg);
            cmd_status(&paths, &cfg).await?;
            Ok(ExitCode::SUCCESS)
        }
        "logs" => {
            let (service, lines, raw_config) = parse_logs_args(&args)?;
            let (_, cfg) = load_cfg(raw_config)?;
            let paths = runtime_paths(&cfg);
            print_logs(&paths, service, lines)?;
            Ok(ExitCode::SUCCESS)
        }
        "db" => {
            if args.is_empty() {
                usage();
                return Ok(ExitCode::from(2));
            }

            let sub = args.remove(0);
            let (raw_config, rest) = parse_config_flag(&args)?;
            if !rest.is_empty() {
                bail!("unexpected db args: {}", rest.join(" "));
            }

            let (_, cfg) = load_cfg(raw_config)?;

            match sub.as_str() {
                "migrate" => {
                    cmd_db_migrate(&cfg).await?;
                    Ok(ExitCode::SUCCESS)
                }
                "doctor" => {
                    let report = cmd_db_doctor(&cfg).await?;
                    println!("{}", serde_json::to_string_pretty(&report)?);
                    let healthy = report.clickhouse_healthy
                        && report.database_exists
                        && report.pending_migrations.is_empty()
                        && report.missing_tables.is_empty()
                        && report.errors.is_empty();

                    if healthy {
                        Ok(ExitCode::SUCCESS)
                    } else {
                        Ok(ExitCode::from(1))
                    }
                }
                _ => {
                    usage();
                    Ok(ExitCode::from(2))
                }
            }
        }
        "run" => {
            if args.is_empty() {
                usage();
                return Ok(ExitCode::from(2));
            }

            let service_name = args.remove(0);
            let (raw_config, passthrough) = parse_config_flag(&args)?;
            let (config_path, _) = load_cfg(raw_config)?;

            let Some(service) = parse_service(&service_name) else {
                usage();
                return Ok(ExitCode::from(2));
            };

            if service == Service::ClickHouse {
                bail!("use `cortexctl up` to manage clickhouse");
            }

            run_foreground_service(service, &config_path, &passthrough)
        }
        _ => {
            usage();
            Ok(ExitCode::from(2))
        }
    }
}
