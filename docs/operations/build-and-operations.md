# Build and Operations

## Operational Scope

This runbook describes how to build, launch, verify, and recover the Cortex stack on one machine. The commands assume project root `~/src/cortex` and default state root `~/.cortex`. The objective is not only to start processes, but to verify the full ingest-to-retrieval chain so stale or partial states are caught early.

## Build Pipeline

Build both Rust binaries first:

```bash
cd ~/src/cortex
bin/build-rust-ingestor
bin/build-rust-codex-mcp
```

These scripts are thin wrappers around release cargo builds and fail fast if `cargo` is missing. The output artifacts are `rust/ingestor/target/release/cortex-ingestor` and `rust/codex-mcp/target/release/codex-mcp`. [src: bin/build-rust-ingestor:L7, bin/build-rust-ingestor:L12, bin/build-rust-codex-mcp:L7, bin/build-rust-codex-mcp:L12]

## Database Bootstrap

Start ClickHouse through the project wrapper rather than invoking the daemon manually:

```bash
cd ~/src/cortex
bin/start-clickhouse
```

This script prepares local directories, templates runtime XML with `CORTEX_HOME`, starts `clickhouse-server`, waits for HTTP health, and then applies all SQL files via `bin/init-db`. If startup succeeds but schema is missing, check `bin/init-db` logs first because DDL execution is part of startup contract. [src: bin/start-clickhouse:L13, bin/start-clickhouse:L15, bin/start-clickhouse:L22, bin/start-clickhouse:L25-33, bin/start-clickhouse:L37]

`bin/init-db` executes sorted `*.sql` files and sends each statement through ClickHouse HTTP API. Because file order is lexical, schema compatibility changes should preserve ordering assumptions or include explicit migration scripts outside this bootstrap path. [src: bin/init-db:L100]

## Ingestor Launch

Start ingestion with the managed wrapper:

```bash
cd ~/src/cortex
bin/start-ingestor
```

The wrapper launches `bin/run-ingestor-rust-service` under `nohup`, stores PID in `~/.cortex/ingestor/ingestor.pid`, and writes logs to `~/.cortex/ingestor/ingestor.log`. The runtime wrapper validates ClickHouse health, reapplies schema, and then execs the Rust binary. If ClickHouse is unavailable, ingestor exits intentionally. [src: bin/start-ingestor:L18, bin/start-ingestor:L20, bin/run-ingestor-rust-service:L9, bin/run-ingestor-rust-service:L20]

## MCP Launch

Run the MCP server as needed for interactive clients:

```bash
cd ~/src/cortex
bin/run-codex-mcp
```

The script prefers `config/codex-mcp.toml` and falls back to `config/ingestor.toml` if missing. It requires the release binary to exist and will fail with a build hint otherwise. [src: bin/run-codex-mcp:L5, bin/run-codex-mcp:L8, bin/run-codex-mcp:L12]

## Health Verification Chain

Do not treat process liveness as sufficient. Use `bin/status` to validate process state, ClickHouse health, ingest progress, and heartbeat recency in one place:

```bash
cd ~/src/cortex
bin/status
```

`bin/status` checks launchd and pid-file states, runs ClickHouse health probes, emits raw/normalized row counts, and prints latest heartbeat timestamp. If ClickHouse is healthy but heartbeats are stale, ingestion is degraded even if a process exists. [src: bin/status:L29, bin/status:L44, bin/status:L82, bin/status:L89]

For direct SQL checks, use:

```sql
SELECT count() FROM cortex.raw_events;
SELECT count() FROM cortex.normalized_events;
SELECT max(ts) FROM cortex.ingest_heartbeats;
```

The diagnostic interpretation should be causal. If raw counts grow but normalized counts do not, normalization or sink path is failing. If both counts are static and heartbeat is old, ingestion is likely stalled or down. If heartbeats continue but retrieval quality falls, inspect search index tables next.

## Search Index Verification and Repair

Validate search surfaces with:

```sql
SELECT count() FROM cortex.search_documents;
SELECT count() FROM cortex.search_postings;
SELECT count() FROM cortex.search_term_stats;
```

If search tables are unexpectedly sparse after schema or normalization changes, run backfill:

```bash
cd ~/src/cortex
bin/backfill-search-index
```

Backfill truncates stats/postings/doc tables, reinserts documents from canonical event tables, then relies on MVs to repopulate sparse structures. This is destructive to search index state but not to canonical event history. [src: bin/backfill-search-index:L74, bin/backfill-search-index:L79, bin/backfill-search-index:L81]

## macOS launchd Operations

Install and start persistent agents:

```bash
cd ~/src/cortex
bin/install-launchd
```

This creates two LaunchAgents, one for ClickHouse and one for ingestor, with PATH and `CORTEX_HOME` environment variables baked into plist definitions. Logs are written under `~/.cortex/launchd`. [src: bin/install-launchd:L9, bin/install-launchd:L53, bin/install-launchd:L68, bin/install-launchd:L84]

Remove agents when needed:

```bash
cd ~/src/cortex
bin/uninstall-launchd
```

This unloads both labels and removes plists. [src: bin/uninstall-launchd:L9, bin/uninstall-launchd:L12]

## Failure Triage Playbooks

When ClickHouse is down, first run `bin/start-clickhouse`, then inspect `~/.cortex/clickhouse/log/clickhouse-server.err.log`. Do not restart ingestor repeatedly until DB health probe succeeds, because ingestor startup intentionally blocks on DB ping and repeated restarts add noise without progress. [src: bin/start-clickhouse:L32, rust/ingestor/src/ingestor.rs:L47]

When ingestor appears alive but no new events are visible, inspect heartbeat recency and queue depth. A growing queue depth with repeated flush failures points to database write path issues. Static queue depth with stale heartbeats indicates runtime loop stoppage or process death. Use `~/.cortex/ingestor/ingestor.log` and launchd stderr logs for root-cause extraction. [src: sql/003_ingest_heartbeats.sql:L5, sql/003_ingest_heartbeats.sql:L14, bin/start-ingestor:L8, bin/install-launchd:L87]

When retrieval quality regresses after code changes, verify in order: `text_content` population in canonical rows, search document counts, posting counts, then run backfill. Tuning BM25 constants before confirming index integrity often produces false conclusions. [src: sql/004_search_index.sql:L53, sql/004_search_index.sql:L82, config/codex-mcp.toml:L21]

## Configuration Surfaces

Use `config/ingestor.toml` to tune ingest path behavior and ClickHouse client options. Use `config/codex-mcp.toml` for retrieval defaults and BM25 constants. Use XML files for ClickHouse server binding, storage paths, logging, and user/network rules. Changes in one surface can affect others; for example, changing database name in TOML without matching startup/query scripts can split data and telemetry unexpectedly. [src: config/ingestor.toml:L1, config/codex-mcp.toml:L10, config/clickhouse.xml:L18, config/users.xml:L16]

## Shutdown

For manual local shutdown:

```bash
cd ~/src/cortex
bin/stop-all
```

This terminates processes referenced by pid files and removes those pid markers. If launchd is managing services, use launchd controls or uninstall script to prevent automatic restarts. [src: bin/stop-all:L9, bin/stop-all:L18, bin/install-launchd:L77]

## Operational Discipline

The fastest way to lose confidence in this stack is to treat it as a black box. Keep a routine: run status, inspect heartbeats, verify canonical and index table counts after changes, and document every schema-affecting modification with explicit rebuild expectations. Cortex is local and scriptable by design; disciplined operators should never need guesswork to understand system state.
