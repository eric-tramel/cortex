use crate::WorkItem;
use anyhow::{Context, Result};
use cortex_config::IngestSource;
use glob::glob;
use notify::{Event, RecursiveMode, Watcher};
use tokio::sync::mpsc;
use tracing::{info, warn};

pub(crate) fn spawn_watcher_threads(
    sources: Vec<IngestSource>,
    tx: mpsc::UnboundedSender<WorkItem>,
) -> Result<Vec<std::thread::JoinHandle<()>>> {
    let mut handles = Vec::<std::thread::JoinHandle<()>>::new();

    for source in sources {
        let source_name = source.name.clone();
        let provider = source.provider.clone();
        let watch_root = std::path::PathBuf::from(source.watch_root.clone());
        let tx_clone = tx.clone();

        info!(
            "starting watcher on {} (source={}, provider={})",
            watch_root.display(),
            source_name,
            provider
        );

        let handle = std::thread::spawn(move || {
            let (event_tx, event_rx) = std::sync::mpsc::channel::<notify::Result<Event>>();

            let mut watcher = match notify::recommended_watcher(move |res| {
                let _ = event_tx.send(res);
            }) {
                Ok(watcher) => watcher,
                Err(exc) => {
                    eprintln!(
                        "[cortex-rust] failed to create watcher for {}: {exc}",
                        source_name
                    );
                    return;
                }
            };

            if let Err(exc) = watcher.watch(watch_root.as_path(), RecursiveMode::Recursive) {
                eprintln!(
                    "[cortex-rust] failed to watch {} ({}): {exc}",
                    watch_root.display(),
                    source_name
                );
                return;
            }

            loop {
                match event_rx.recv() {
                    Ok(Ok(event)) => {
                        for path in event.paths {
                            let _ = tx_clone.send(WorkItem {
                                source_name: source_name.clone(),
                                provider: provider.clone(),
                                path: path.to_string_lossy().to_string(),
                            });
                        }
                    }
                    Ok(Err(exc)) => {
                        eprintln!("[cortex-rust] watcher event error ({source_name}): {exc}");
                    }
                    Err(_) => break,
                }
            }
        });

        handles.push(handle);
    }

    Ok(handles)
}

pub(crate) fn enumerate_jsonl_files(glob_pattern: &str) -> Result<Vec<String>> {
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
