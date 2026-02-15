use crate::clickhouse::ClickHouseClient;
use crate::config::AppConfig;
use crate::model::{Checkpoint, RowBatch};
use crate::normalize::normalize_record;
use anyhow::{Context, Result};
use glob::glob;
use notify::{Event, RecursiveMode, Watcher};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, RwLock, Semaphore};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

#[derive(Default)]
struct DispatchState {
    pending: HashSet<String>,
    inflight: HashSet<String>,
    dirty: HashSet<String>,
}

#[derive(Default)]
struct Metrics {
    raw_rows_written: AtomicU64,
    norm_rows_written: AtomicU64,
    err_rows_written: AtomicU64,
    last_flush_ms: AtomicU64,
    flush_failures: AtomicU64,
    queue_depth: AtomicU64,
    last_error: Mutex<String>,
}

#[derive(Debug)]
enum SinkMessage {
    Batch(RowBatch),
}

pub async fn run_ingestor(config: AppConfig) -> Result<()> {
    let clickhouse = ClickHouseClient::new(config.clickhouse.clone())?;
    clickhouse.ping().await.context("clickhouse ping failed")?;

    let checkpoint_map = clickhouse
        .load_checkpoints()
        .await
        .context("failed to load checkpoints from clickhouse")?;

    info!("loaded {} checkpoints", checkpoint_map.len());

    let checkpoints = Arc::new(RwLock::new(checkpoint_map));
    let dispatch = Arc::new(Mutex::new(DispatchState::default()));
    let metrics = Arc::new(Metrics::default());

    let process_queue_capacity = config.ingest.max_inflight_batches.saturating_mul(16).max(1024);
    let (process_tx, mut process_rx) = mpsc::channel::<String>(process_queue_capacity);
    let (sink_tx, sink_rx) = mpsc::channel::<SinkMessage>(config.ingest.max_inflight_batches.max(16));
    let (watch_path_tx, watch_path_rx) = mpsc::unbounded_channel::<String>();

    let sink_handle = spawn_sink_task(
        config.clone(),
        clickhouse.clone(),
        checkpoints.clone(),
        metrics.clone(),
        sink_rx,
        dispatch.clone(),
    );

    let sem = Arc::new(Semaphore::new(config.ingest.max_file_workers.max(1)));
    let processor_handle = {
        let process_tx_clone = process_tx.clone();
        let sink_tx_clone = sink_tx.clone();
        let checkpoints_clone = checkpoints.clone();
        let dispatch_clone = dispatch.clone();
        let sem_clone = sem.clone();
        let metrics_clone = metrics.clone();
        let cfg_clone = config.clone();

        tokio::spawn(async move {
            while let Some(path) = process_rx.recv().await {
                metrics_clone.queue_depth.fetch_sub(1, Ordering::Relaxed);

                {
                    let mut state = dispatch_clone.lock().expect("dispatch mutex poisoned");
                    state.pending.remove(&path);
                    state.inflight.insert(path.clone());
                }

                let permit = match sem_clone.clone().acquire_owned().await {
                    Ok(permit) => permit,
                    Err(_) => break,
                };

                let sink_tx_worker = sink_tx_clone.clone();
                let process_tx_worker = process_tx_clone.clone();
                let checkpoints_worker = checkpoints_clone.clone();
                let dispatch_worker = dispatch_clone.clone();
                let cfg_worker = cfg_clone.clone();
                let metrics_worker = metrics_clone.clone();

                tokio::spawn(async move {
                    let _permit = permit;
                    if let Err(exc) = process_file(
                        &cfg_worker,
                        &path,
                        checkpoints_worker,
                        sink_tx_worker,
                        &metrics_worker,
                    )
                    .await
                    {
                        error!("failed processing {}: {exc}", path);
                        *metrics_worker
                            .last_error
                            .lock()
                            .expect("metrics last_error mutex poisoned") = exc.to_string();
                    }

                    let mut reschedule = false;
                    {
                        let mut state = dispatch_worker.lock().expect("dispatch mutex poisoned");
                        state.inflight.remove(&path);
                        if state.dirty.remove(&path) {
                            if state.pending.insert(path.clone()) {
                                reschedule = true;
                            }
                        }
                    }

                    if reschedule {
                        if process_tx_worker.send(path.clone()).await.is_ok() {
                            metrics_worker.queue_depth.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                });
            }
        })
    };

    let debounce_handle = spawn_debounce_task(
        config.clone(),
        watch_path_rx,
        process_tx.clone(),
        dispatch.clone(),
        metrics.clone(),
    );

    let reconcile_handle = spawn_reconcile_task(
        config.clone(),
        process_tx.clone(),
        dispatch.clone(),
        metrics.clone(),
    );

    let watcher_thread = spawn_watcher_thread(config.clone(), watch_path_tx)?;

    if config.ingest.backfill_on_start {
        let files = enumerate_jsonl_files(&config.ingest.sessions_glob)?;
        info!("startup backfill queueing {} files", files.len());
        for path in files {
            enqueue_file(path, &process_tx, &dispatch, &metrics).await;
        }
    }

    info!("rust ingestor running; waiting for shutdown signal");
    tokio::signal::ctrl_c().await.context("signal handler failed")?;
    info!("shutdown signal received");

    drop(process_tx);
    drop(sink_tx);

    debounce_handle.abort();
    reconcile_handle.abort();
    processor_handle.abort();
    sink_handle.abort();

    let _ = watcher_thread.thread().id();

    Ok(())
}

fn spawn_watcher_thread(
    config: AppConfig,
    tx: mpsc::UnboundedSender<String>,
) -> Result<std::thread::JoinHandle<()>> {
    let watch_root = watch_root_from_glob(&config.ingest.sessions_glob);
    info!("starting watcher on {}", watch_root.display());

    let handle = std::thread::spawn(move || {
        let (event_tx, event_rx) = std::sync::mpsc::channel::<notify::Result<Event>>();

        let mut watcher = match notify::recommended_watcher(move |res| {
            let _ = event_tx.send(res);
        }) {
            Ok(watcher) => watcher,
            Err(exc) => {
                eprintln!("[cortex-rust] failed to create watcher: {exc}");
                return;
            }
        };

        if let Err(exc) = watcher.watch(watch_root.as_path(), RecursiveMode::Recursive) {
            eprintln!("[cortex-rust] failed to watch {}: {exc}", watch_root.display());
            return;
        }

        loop {
            match event_rx.recv() {
                Ok(Ok(event)) => {
                    for path in event.paths {
                        let _ = tx.send(path.to_string_lossy().to_string());
                    }
                }
                Ok(Err(exc)) => {
                    eprintln!("[cortex-rust] watcher event error: {exc}");
                }
                Err(_) => break,
            }
        }
    });

    Ok(handle)
}

fn spawn_debounce_task(
    config: AppConfig,
    mut rx: mpsc::UnboundedReceiver<String>,
    process_tx: mpsc::Sender<String>,
    dispatch: Arc<Mutex<DispatchState>>,
    metrics: Arc<Metrics>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let debounce = Duration::from_millis(config.ingest.debounce_ms.max(5));
        let mut pending = HashMap::<String, Instant>::new();
        let mut tick = tokio::time::interval(Duration::from_millis((config.ingest.debounce_ms / 2).max(10)));

        loop {
            tokio::select! {
                maybe_path = rx.recv() => {
                    match maybe_path {
                        Some(path) => {
                            pending.insert(path, Instant::now());
                        }
                        None => break,
                    }
                }
                _ = tick.tick() => {
                    if pending.is_empty() {
                        continue;
                    }

                    let now = Instant::now();
                    let ready: Vec<String> = pending
                        .iter()
                        .filter_map(|(path, seen_at)| {
                            if now.duration_since(*seen_at) >= debounce {
                                Some(path.clone())
                            } else {
                                None
                            }
                        })
                        .collect();

                    for path in ready {
                        pending.remove(&path);

                        if !path.ends_with(".jsonl") {
                            continue;
                        }

                        enqueue_file(path, &process_tx, &dispatch, &metrics).await;
                    }
                }
            }
        }
    })
}

fn spawn_reconcile_task(
    config: AppConfig,
    process_tx: mpsc::Sender<String>,
    dispatch: Arc<Mutex<DispatchState>>,
    metrics: Arc<Metrics>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let interval = Duration::from_secs_f64(config.ingest.reconcile_interval_seconds.max(5.0));
        let mut ticker = tokio::time::interval(interval);

        loop {
            ticker.tick().await;
            match enumerate_jsonl_files(&config.ingest.sessions_glob) {
                Ok(paths) => {
                    debug!("reconcile scanning {} files", paths.len());
                    for path in paths {
                        enqueue_file(path, &process_tx, &dispatch, &metrics).await;
                    }
                }
                Err(exc) => {
                    warn!("reconcile scan failed: {exc}");
                }
            }
        }
    })
}

fn spawn_sink_task(
    config: AppConfig,
    clickhouse: ClickHouseClient,
    checkpoints: Arc<RwLock<HashMap<String, Checkpoint>>>,
    metrics: Arc<Metrics>,
    mut rx: mpsc::Receiver<SinkMessage>,
    dispatch: Arc<Mutex<DispatchState>>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut raw_rows = Vec::<Value>::new();
        let mut normalized_rows = Vec::<Value>::new();
        let mut expanded_rows = Vec::<Value>::new();
        let mut error_rows = Vec::<Value>::new();
        let mut checkpoint_updates = HashMap::<String, Checkpoint>::new();

        let flush_interval = Duration::from_secs_f64(config.ingest.flush_interval_seconds.max(0.05));
        let heartbeat_interval = Duration::from_secs_f64(config.ingest.heartbeat_interval_seconds.max(1.0));

        let mut flush_tick = tokio::time::interval(flush_interval);
        let mut heartbeat_tick = tokio::time::interval(heartbeat_interval);

        loop {
            tokio::select! {
                maybe_msg = rx.recv() => {
                    match maybe_msg {
                        Some(SinkMessage::Batch(batch)) => {
                            raw_rows.extend(batch.raw_rows);
                            normalized_rows.extend(batch.normalized_rows);
                            expanded_rows.extend(batch.expanded_rows);
                            error_rows.extend(batch.error_rows);
                            if let Some(cp) = batch.checkpoint {
                                merge_checkpoint(&mut checkpoint_updates, cp);
                            }

                            let total_rows = raw_rows.len() + normalized_rows.len() + expanded_rows.len() + error_rows.len();
                            if total_rows >= config.ingest.batch_size {
                                flush_pending(
                                    &clickhouse,
                                    &checkpoints,
                                    &metrics,
                                    &mut raw_rows,
                                    &mut normalized_rows,
                                    &mut expanded_rows,
                                    &mut error_rows,
                                    &mut checkpoint_updates,
                                ).await;
                            }
                        }
                        None => break,
                    }
                }
                _ = flush_tick.tick() => {
                    if !(raw_rows.is_empty() && normalized_rows.is_empty() && expanded_rows.is_empty() && error_rows.is_empty() && checkpoint_updates.is_empty()) {
                        flush_pending(
                            &clickhouse,
                            &checkpoints,
                            &metrics,
                            &mut raw_rows,
                            &mut normalized_rows,
                            &mut expanded_rows,
                            &mut error_rows,
                            &mut checkpoint_updates,
                        ).await;
                    }
                }
                _ = heartbeat_tick.tick() => {
                    let files_active = {
                        let state = dispatch.lock().expect("dispatch mutex poisoned");
                        state.inflight.len() as u32
                    };
                    let files_watched = checkpoints.read().await.len() as u32;
                    let last_error = {
                        metrics
                            .last_error
                            .lock()
                            .expect("metrics last_error mutex poisoned")
                            .clone()
                    };

                    let heartbeat = json!({
                        "host": host_name(),
                        "service_version": env!("CARGO_PKG_VERSION"),
                        "queue_depth": metrics.queue_depth.load(Ordering::Relaxed),
                        "files_active": files_active,
                        "files_watched": files_watched,
                        "rows_raw_written": metrics.raw_rows_written.load(Ordering::Relaxed),
                        "rows_norm_written": metrics.norm_rows_written.load(Ordering::Relaxed),
                        "rows_errors_written": metrics.err_rows_written.load(Ordering::Relaxed),
                        "flush_latency_ms": metrics.last_flush_ms.load(Ordering::Relaxed) as u32,
                        "append_to_visible_p50_ms": 0u32,
                        "append_to_visible_p95_ms": 0u32,
                        "last_error": last_error,
                    });

                    if let Err(exc) = clickhouse.insert_json_rows("ingest_heartbeats", &[heartbeat]).await {
                        warn!("heartbeat insert failed: {exc}");
                    }
                }
            }
        }

        if !(raw_rows.is_empty() && normalized_rows.is_empty() && expanded_rows.is_empty() && error_rows.is_empty() && checkpoint_updates.is_empty()) {
            flush_pending(
                &clickhouse,
                &checkpoints,
                &metrics,
                &mut raw_rows,
                &mut normalized_rows,
                &mut expanded_rows,
                &mut error_rows,
                &mut checkpoint_updates,
            ).await;
        }
    })
}

fn merge_checkpoint(pending: &mut HashMap<String, Checkpoint>, checkpoint: Checkpoint) {
    let key = checkpoint.source_file.clone();
    match pending.get(&key) {
        None => {
            pending.insert(key, checkpoint);
        }
        Some(existing) => {
            let replace = checkpoint.source_generation > existing.source_generation
                || (checkpoint.source_generation == existing.source_generation
                    && checkpoint.last_offset >= existing.last_offset);
            if replace {
                pending.insert(key, checkpoint);
            }
        }
    }
}

async fn flush_pending(
    clickhouse: &ClickHouseClient,
    checkpoints: &Arc<RwLock<HashMap<String, Checkpoint>>>,
    metrics: &Arc<Metrics>,
    raw_rows: &mut Vec<Value>,
    normalized_rows: &mut Vec<Value>,
    expanded_rows: &mut Vec<Value>,
    error_rows: &mut Vec<Value>,
    checkpoint_updates: &mut HashMap<String, Checkpoint>,
) {
    let started = Instant::now();

    let checkpoint_rows: Vec<Value> = checkpoint_updates
        .values()
        .map(|cp| {
            json!({
                "source_file": cp.source_file,
                "source_inode": cp.source_inode,
                "source_generation": cp.source_generation,
                "last_offset": cp.last_offset,
                "last_line_no": cp.last_line_no,
                "status": cp.status,
            })
        })
        .collect();

    let flush_result = async {
        clickhouse.insert_json_rows("raw_events", raw_rows).await?;
        clickhouse
            .insert_json_rows("normalized_events", normalized_rows)
            .await?;
        clickhouse
            .insert_json_rows("compacted_expanded_events", expanded_rows)
            .await?;
        clickhouse.insert_json_rows("ingest_errors", error_rows).await?;
        clickhouse
            .insert_json_rows("ingest_checkpoints", &checkpoint_rows)
            .await?;
        Result::<()>::Ok(())
    }
    .await;

    match flush_result {
        Ok(()) => {
            let raw_len = raw_rows.len() as u64;
            let norm_len = normalized_rows.len() as u64;
            let err_len = error_rows.len() as u64;

            metrics.raw_rows_written.fetch_add(raw_len, Ordering::Relaxed);
            metrics.norm_rows_written.fetch_add(norm_len, Ordering::Relaxed);
            metrics.err_rows_written.fetch_add(err_len, Ordering::Relaxed);
            metrics
                .last_flush_ms
                .store(started.elapsed().as_millis() as u64, Ordering::Relaxed);

            {
                let mut state = checkpoints.write().await;
                for cp in checkpoint_updates.values() {
                    state.insert(cp.source_file.clone(), cp.clone());
                }
            }

            raw_rows.clear();
            normalized_rows.clear();
            expanded_rows.clear();
            error_rows.clear();
            checkpoint_updates.clear();
        }
        Err(exc) => {
            metrics.flush_failures.fetch_add(1, Ordering::Relaxed);
            *metrics
                .last_error
                .lock()
                .expect("metrics last_error mutex poisoned") = exc.to_string();
            warn!("flush failed: {exc}");
        }
    }
}

async fn enqueue_file(
    path: String,
    process_tx: &mpsc::Sender<String>,
    dispatch: &Arc<Mutex<DispatchState>>,
    metrics: &Arc<Metrics>,
) {
    if !path.ends_with(".jsonl") {
        return;
    }

    let mut should_send = false;
    {
        let mut state = dispatch.lock().expect("dispatch mutex poisoned");
        if state.inflight.contains(&path) {
            state.dirty.insert(path.clone());
        } else if state.pending.insert(path.clone()) {
            should_send = true;
        }
    }

    if should_send {
        if process_tx.send(path).await.is_ok() {
            metrics.queue_depth.fetch_add(1, Ordering::Relaxed);
        }
    }
}

async fn process_file(
    config: &AppConfig,
    source_file: &str,
    checkpoints: Arc<RwLock<HashMap<String, Checkpoint>>>,
    sink_tx: mpsc::Sender<SinkMessage>,
    metrics: &Arc<Metrics>,
) -> Result<()> {
    let meta = match std::fs::metadata(source_file) {
        Ok(meta) => meta,
        Err(exc) => {
            debug!("metadata missing for {}: {}", source_file, exc);
            return Ok(());
        }
    };

    #[cfg(unix)]
    let inode = meta.ino();
    #[cfg(not(unix))]
    let inode = 0u64;

    let file_size = meta.len();

    let committed = { checkpoints.read().await.get(source_file).cloned() };

    let mut checkpoint = committed.unwrap_or(Checkpoint {
        source_file: source_file.to_string(),
        source_inode: inode,
        source_generation: 1,
        last_offset: 0,
        last_line_no: 0,
        status: "active".to_string(),
    });

    let mut generation_changed = false;
    if checkpoint.source_inode != inode || file_size < checkpoint.last_offset {
        checkpoint.source_inode = inode;
        checkpoint.source_generation = checkpoint.source_generation.saturating_add(1).max(1);
        checkpoint.last_offset = 0;
        checkpoint.last_line_no = 0;
        checkpoint.status = "active".to_string();
        generation_changed = true;
    }

    if file_size == checkpoint.last_offset && !generation_changed {
        return Ok(());
    }

    let mut file = std::fs::File::open(source_file)
        .with_context(|| format!("failed to open {}", source_file))?;
    file.seek(SeekFrom::Start(checkpoint.last_offset))
        .with_context(|| format!("failed to seek {}", source_file))?;

    let mut reader = BufReader::new(file);
    let mut offset = checkpoint.last_offset;
    let mut line_no = checkpoint.last_line_no;
    let mut session_hint = String::new();

    let mut batch = RowBatch::default();

    loop {
        let start_offset = offset;
        let mut buf = Vec::<u8>::new();
        let bytes_read = reader
            .read_until(b'\n', &mut buf)
            .with_context(|| format!("failed reading {}", source_file))?;

        if bytes_read == 0 {
            break;
        }

        offset = offset.saturating_add(bytes_read as u64);
        line_no = line_no.saturating_add(1);

        let mut text = String::from_utf8_lossy(&buf).to_string();
        if text.ends_with('\n') {
            text.pop();
        }

        if text.trim().is_empty() {
            continue;
        }

        let parsed: Value = match serde_json::from_str::<Value>(&text) {
            Ok(value) if value.is_object() => value,
            Ok(_) => {
                batch.error_rows.push(json!({
                    "source_file": source_file,
                    "source_inode": inode,
                    "source_generation": checkpoint.source_generation,
                    "source_line_no": line_no,
                    "source_offset": start_offset,
                    "error_kind": "json_parse_error",
                    "error_text": "Expected JSON object",
                    "raw_fragment": truncate(&text, 20_000),
                }));
                continue;
            }
            Err(exc) => {
                batch.error_rows.push(json!({
                    "source_file": source_file,
                    "source_inode": inode,
                    "source_generation": checkpoint.source_generation,
                    "source_line_no": line_no,
                    "source_offset": start_offset,
                    "error_kind": "json_parse_error",
                    "error_text": exc.to_string(),
                    "raw_fragment": truncate(&text, 20_000),
                }));
                continue;
            }
        };

        let normalized = normalize_record(
            &parsed,
            source_file,
            inode,
            checkpoint.source_generation,
            line_no,
            start_offset,
            &session_hint,
        );

        session_hint = normalized.session_hint;
        batch.raw_rows.push(normalized.raw_row);
        batch.normalized_rows.extend(normalized.normalized_rows);
        batch.expanded_rows.extend(normalized.expanded_rows);
        batch.lines_processed = batch.lines_processed.saturating_add(1);

        if batch.row_count() >= config.ingest.batch_size {
            let mut chunk = RowBatch::default();
            chunk.raw_rows = std::mem::take(&mut batch.raw_rows);
            chunk.normalized_rows = std::mem::take(&mut batch.normalized_rows);
            chunk.expanded_rows = std::mem::take(&mut batch.expanded_rows);
            chunk.error_rows = std::mem::take(&mut batch.error_rows);
            chunk.lines_processed = batch.lines_processed;
            batch.lines_processed = 0;
            chunk.checkpoint = Some(Checkpoint {
                source_file: source_file.to_string(),
                source_inode: inode,
                source_generation: checkpoint.source_generation,
                last_offset: offset,
                last_line_no: line_no,
                status: "active".to_string(),
            });

            sink_tx
                .send(SinkMessage::Batch(chunk))
                .await
                .context("sink channel closed while sending chunk")?;
        }
    }

    let final_checkpoint = Checkpoint {
        source_file: source_file.to_string(),
        source_inode: inode,
        source_generation: checkpoint.source_generation,
        last_offset: offset,
        last_line_no: line_no,
        status: "active".to_string(),
    };

    if batch.row_count() > 0 || generation_changed || offset != checkpoint.last_offset {
        batch.checkpoint = Some(final_checkpoint);
        sink_tx
            .send(SinkMessage::Batch(batch))
            .await
            .context("sink channel closed while sending final batch")?;
    }

    if metrics.queue_depth.load(Ordering::Relaxed) == 0 {
        debug!("{} caught up at offset {}", source_file, offset);
    }

    Ok(())
}

fn truncate(input: &str, max_chars: usize) -> String {
    if input.chars().count() <= max_chars {
        return input.to_string();
    }
    input.chars().take(max_chars).collect()
}

fn watch_root_from_glob(glob_pattern: &str) -> PathBuf {
    if let Some(idx) = glob_pattern.find("/**") {
        return PathBuf::from(&glob_pattern[..idx]);
    }

    if let Some(idx) = glob_pattern.find('*') {
        let prefix = &glob_pattern[..idx];
        let path = Path::new(prefix);
        if let Some(parent) = path.parent() {
            return parent.to_path_buf();
        }
    }

    Path::new(glob_pattern)
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from(glob_pattern))
}

fn enumerate_jsonl_files(glob_pattern: &str) -> Result<Vec<String>> {
    let mut files = Vec::<String>::new();
    for entry in glob(glob_pattern).with_context(|| format!("invalid glob: {}", glob_pattern))? {
        let path = match entry {
            Ok(path) => path,
            Err(exc) => {
                warn!("glob iteration error: {exc}");
                continue;
            }
        };

        if path.extension().and_then(|s| s.to_str()) == Some("jsonl") {
            files.push(path.to_string_lossy().to_string());
        }
    }
    files.sort();
    Ok(files)
}

fn host_name() -> String {
    std::env::var("HOSTNAME")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .or_else(|| std::env::var("USER").ok())
        .unwrap_or_else(|| "localhost".to_string())
}
