CREATE DATABASE IF NOT EXISTS cortex;

CREATE TABLE IF NOT EXISTS cortex.raw_events (
  ingested_at DateTime64(3) DEFAULT now64(3),
  source_file String,
  source_inode UInt64,
  source_generation UInt32,
  source_line_no UInt64,
  source_offset UInt64,
  record_ts String,
  top_type LowCardinality(String),
  session_id String,
  raw_json String,
  raw_json_hash UInt64,
  event_uid String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (source_file, source_generation, source_offset, source_line_no, event_uid);

CREATE TABLE IF NOT EXISTS cortex.normalized_events (
  ingested_at DateTime64(3) DEFAULT now64(3),
  event_uid String,
  session_id String,
  session_date Date,
  source_file String,
  source_inode UInt64,
  source_generation UInt32,
  source_line_no UInt64,
  source_offset UInt64,
  source_ref String,
  record_ts String,
  event_class LowCardinality(String),
  payload_type LowCardinality(String),
  actor_role LowCardinality(String),
  turn_id String,
  item_id String,
  call_id String,
  name String,
  phase String,
  text_content String,
  payload_json String,
  token_usage_json String,
  event_version UInt64
)
ENGINE = ReplacingMergeTree(event_version)
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (session_id, source_file, source_generation, source_offset, source_line_no, event_uid);

CREATE TABLE IF NOT EXISTS cortex.compacted_expanded_events (
  ingested_at DateTime64(3) DEFAULT now64(3),
  event_uid String,
  compacted_parent_uid String,
  session_id String,
  session_date Date,
  source_file String,
  source_inode UInt64,
  source_generation UInt32,
  source_line_no UInt64,
  source_offset UInt64,
  source_ref String,
  record_ts String,
  event_class LowCardinality(String),
  payload_type LowCardinality(String),
  actor_role LowCardinality(String),
  turn_id String,
  item_id String,
  call_id String,
  name String,
  phase String,
  text_content String,
  payload_json String,
  token_usage_json String,
  event_version UInt64
)
ENGINE = ReplacingMergeTree(event_version)
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (session_id, source_file, source_generation, source_offset, source_line_no, event_uid);

CREATE TABLE IF NOT EXISTS cortex.ingest_errors (
  ingested_at DateTime64(3) DEFAULT now64(3),
  source_file String,
  source_inode UInt64,
  source_generation UInt32,
  source_line_no UInt64,
  source_offset UInt64,
  error_kind LowCardinality(String),
  error_text String,
  raw_fragment String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (source_file, source_generation, source_offset, source_line_no);

CREATE TABLE IF NOT EXISTS cortex.ingest_checkpoints (
  updated_at DateTime64(3) DEFAULT now64(3),
  source_file String,
  source_inode UInt64,
  source_generation UInt32,
  last_offset UInt64,
  last_line_no UInt64,
  status LowCardinality(String)
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(updated_at)
ORDER BY (source_file, source_generation);
