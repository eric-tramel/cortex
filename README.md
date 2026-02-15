# cortex

Local ClickHouse-backed realtime indexer for Codex session JSONL logs.

## What it does
- Watches `~/.codex/sessions/**/*.jsonl` in realtime.
- Backfills all historical session files on first run.
- Normalizes new and legacy record formats (`response_item`, `event_msg`, top-level legacy types, `compacted`).
- Preserves metadata needed to reconstruct complete traces: roles, tool calls/outputs, turn sequence, source offsets, and raw payloads.
- Builds realtime lexical index tables for BM25-style retrieval (`search_documents`, `search_postings`, corpus/term stats).
- Includes a Rust stdio MCP server (`codex-mcp`) with `search` and `open` tools.

## Architecture docs
- System docs and design records live in `docs/`
- Start with `docs/README.md`

## Docs site
```bash
cd ~/src/cortex
make docs-qc
make docs-build
make docs-serve
```

Then open `http://127.0.0.1:8000`.

Citation references (`[src: ...]`) are rendered as clickable links in the docs site and open local source mirrors with per-line anchors under `docs/_source/`.

To write a machine-readable docs quality report without failing the command:
```bash
cd ~/src/cortex
make docs-rewrite-report
```

## Runtime paths
- Project: `~/src/cortex`
- Database + service state: `~/.cortex`

## Prerequisites
- Native `clickhouse-server` installed and available on `PATH`
- `curl` installed for health checks and schema bootstrap
- Rust toolchain (`cargo`, `rustc`) for building both Rust binaries

Optional environment configuration:
- `CORTEX_HOME` overrides the default runtime state path (`~/.cortex`)
- `CORTEX_MCP_CONFIG` selects the MCP config passed to `bin/run-codex-mcp`

## Quick start
```bash
cd ~/src/cortex

# Optional: override runtime state path
export CORTEX_HOME=~/.cortex

# Start ClickHouse with local data path ~/.cortex/clickhouse
bin/start-clickhouse

# Build the Rust ingestor binary
bin/build-rust-ingestor

# Build the Rust MCP binary
bin/build-rust-codex-mcp

# Start realtime ingestor daemon
bin/start-ingestor

# Check service health
bin/status
```

## Run codex-mcp (stdio)
```bash
cd ~/src/cortex
bin/run-codex-mcp
```

Config defaults to `config/codex-mcp.toml` (or `config/ingestor.toml` if not present) and can be overridden with:
```bash
bin/run-codex-mcp --config /path/to/config.toml
```

## Search schema
`bin/start-clickhouse` / `bin/init-db` now also applies `sql/004_search_index.sql`, which creates:
- `cortex.search_documents`
- `cortex.search_postings`
- `cortex.search_term_stats`
- `cortex.search_corpus_stats`
- `cortex.search_query_log`
- `cortex.search_hit_log`
- `cortex.search_interaction_log`

If these tables are newly created and you want historical data indexed immediately, run:
```bash
cd ~/src/cortex
bin/backfill-search-index
```

To initialize or re-apply schema manually without restarting services:
```bash
cd ~/src/cortex
bin/init-db
```

## Stop services
```bash
bin/stop-all
```

## Query examples
```sql
SELECT *
FROM cortex.v_session_summary
ORDER BY last_event_time DESC
LIMIT 20;

SELECT session_id, turn_seq, event_order, actor_role, event_class, payload_type, text_content
FROM cortex.v_conversation_trace
WHERE session_id = '019c59f9-6389-77a1-a0cb-304eecf935b6'
ORDER BY event_order;
```

## Auto-start with launchd (macOS)
```bash
cd ~/src/cortex
bin/build-rust-ingestor
bin/install-launchd
```

`bin/install-launchd` expects built release binaries in `rust/*/target/release`. If not built yet, run both build steps before installing launchd.

This installs two user LaunchAgents:
- `com.eric.cortex.clickhouse`
- `com.eric.cortex.ingestor`

To remove them:
```bash
cd ~/src/cortex
bin/uninstall-launchd
```

Agent logs are written to:
- `~/.cortex/launchd/clickhouse.out.log`
- `~/.cortex/launchd/clickhouse.err.log`
- `~/.cortex/launchd/ingestor.out.log`
- `~/.cortex/launchd/ingestor.err.log`
