use crate::model::NormalizedRecord;
use regex::Regex;
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

fn session_id_re() -> &'static Regex {
    static SESSION_ID_RE: OnceLock<Regex> = OnceLock::new();
    SESSION_ID_RE.get_or_init(|| {
        Regex::new(r"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})$")
            .expect("valid session id regex")
    })
}

fn session_date_re() -> &'static Regex {
    static SESSION_DATE_RE: OnceLock<Regex> = OnceLock::new();
    SESSION_DATE_RE
        .get_or_init(|| Regex::new(r"/sessions/(\d{4})/(\d{2})/(\d{2})/").expect("valid session date regex"))
}

fn to_str(value: Option<&Value>) -> String {
    match value {
        None | Some(Value::Null) => String::new(),
        Some(Value::String(s)) => s.clone(),
        Some(other) => other.to_string(),
    }
}

fn compact_json(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "{}".to_string())
}

pub fn infer_session_id_from_file(source_file: &str) -> String {
    let stem = std::path::Path::new(source_file)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or_default();

    session_id_re()
        .captures(stem)
        .and_then(|cap| cap.get(1).map(|m| m.as_str().to_string()))
        .unwrap_or_default()
}

pub fn infer_session_date_from_file(source_file: &str) -> String {
    if let Some(cap) = session_date_re().captures(source_file) {
        return format!("{}-{}-{}", &cap[1], &cap[2], &cap[3]);
    }
    "1970-01-01".to_string()
}

fn extract_message_text(content: &Value) -> String {
    fn walk(node: &Value, out: &mut Vec<String>) {
        match node {
            Value::String(s) => {
                if !s.is_empty() {
                    out.push(s.clone());
                }
            }
            Value::Array(items) => {
                for item in items {
                    walk(item, out);
                }
            }
            Value::Object(map) => {
                for key in ["text", "message", "output"] {
                    if let Some(Value::String(s)) = map.get(key) {
                        if !s.is_empty() {
                            out.push(s.clone());
                        }
                    }
                }

                for key in ["content", "text_elements", "summary"] {
                    if let Some(value) = map.get(key) {
                        walk(value, out);
                    }
                }
            }
            _ => {}
        }
    }

    let mut chunks = Vec::<String>::new();
    walk(content, &mut chunks);

    let joined = chunks.join("\n");
    if joined.len() > 200_000 {
        joined.chars().take(200_000).collect()
    } else {
        joined
    }
}

fn extract_reasoning_summary(summary: &Value) -> String {
    extract_message_text(summary)
}

fn event_uid(
    source_file: &str,
    source_generation: u32,
    source_line_no: u64,
    source_offset: u64,
    record_fingerprint: &str,
    suffix: &str,
) -> String {
    let material = format!(
        "{}|{}|{}|{}|{}|{}",
        source_file, source_generation, source_line_no, source_offset, record_fingerprint, suffix
    );

    let mut hasher = Sha256::new();
    hasher.update(material.as_bytes());
    let digest = hasher.finalize();
    format!("{:x}", digest)
}

fn event_version() -> u64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    now as u64
}

fn raw_hash(raw_json: &str) -> u64 {
    let mut hasher = Sha256::new();
    hasher.update(raw_json.as_bytes());
    let digest = hasher.finalize();
    let hex = format!("{:x}", digest);
    u64::from_str_radix(&hex[..16], 16).unwrap_or(0)
}

fn base_event_row(
    event_uid: &str,
    session_id: &str,
    session_date: &str,
    source_file: &str,
    source_inode: u64,
    source_generation: u32,
    source_line_no: u64,
    source_offset: u64,
    record_ts: &str,
    event_class: &str,
    payload_type: &str,
    actor_role: &str,
    turn_id: &str,
    item_id: &str,
    call_id: &str,
    name: &str,
    phase: &str,
    text_content: &str,
    payload_json: &str,
    token_usage_json: &str,
) -> Value {
    json!({
        "event_uid": event_uid,
        "session_id": session_id,
        "session_date": session_date,
        "source_file": source_file,
        "source_inode": source_inode,
        "source_generation": source_generation,
        "source_line_no": source_line_no,
        "source_offset": source_offset,
        "source_ref": format!("{}:{}:{}", source_file, source_generation, source_line_no),
        "record_ts": record_ts,
        "event_class": event_class,
        "payload_type": payload_type,
        "actor_role": actor_role,
        "turn_id": turn_id,
        "item_id": item_id,
        "call_id": call_id,
        "name": name,
        "phase": phase,
        "text_content": text_content,
        "payload_json": payload_json,
        "token_usage_json": token_usage_json,
        "event_version": event_version()
    })
}

struct RecordContext<'a> {
    session_id: &'a str,
    session_date: &'a str,
    source_file: &'a str,
    source_inode: u64,
    source_generation: u32,
    source_line_no: u64,
    source_offset: u64,
    record_ts: &'a str,
}

fn normalize_legacy_item(item: &Value, ctx: &RecordContext<'_>, uid_suffix: &str) -> Value {
    let item_type = to_str(item.get("type"));
    let payload_json = compact_json(item);
    let fingerprint = payload_json.clone();
    let uid = event_uid(
        ctx.source_file,
        ctx.source_generation,
        ctx.source_line_no,
        ctx.source_offset,
        &fingerprint,
        uid_suffix,
    );

    if item_type == "message" {
        return base_event_row(
            &uid,
            ctx.session_id,
            ctx.session_date,
            ctx.source_file,
            ctx.source_inode,
            ctx.source_generation,
            ctx.source_line_no,
            ctx.source_offset,
            ctx.record_ts,
            "message",
            "message",
            &to_str(item.get("role")),
            "",
            &to_str(item.get("id")),
            "",
            "",
            "",
            &extract_message_text(item.get("content").unwrap_or(&Value::Null)),
            &payload_json,
            "",
        );
    }

    if item_type == "function_call" {
        return base_event_row(
            &uid,
            ctx.session_id,
            ctx.session_date,
            ctx.source_file,
            ctx.source_inode,
            ctx.source_generation,
            ctx.source_line_no,
            ctx.source_offset,
            ctx.record_ts,
            "tool_call",
            "function_call",
            "assistant",
            "",
            "",
            &to_str(item.get("call_id")),
            &to_str(item.get("name")),
            "",
            &to_str(item.get("arguments")),
            &payload_json,
            "",
        );
    }

    if item_type == "function_call_output" {
        let mut output = to_str(item.get("output"));
        if output.len() > 200_000 {
            output = output.chars().take(200_000).collect();
        }

        return base_event_row(
            &uid,
            ctx.session_id,
            ctx.session_date,
            ctx.source_file,
            ctx.source_inode,
            ctx.source_generation,
            ctx.source_line_no,
            ctx.source_offset,
            ctx.record_ts,
            "tool_output",
            "function_call_output",
            "tool",
            "",
            "",
            &to_str(item.get("call_id")),
            "",
            "",
            &output,
            &payload_json,
            "",
        );
    }

    if item_type == "reasoning" {
        return base_event_row(
            &uid,
            ctx.session_id,
            ctx.session_date,
            ctx.source_file,
            ctx.source_inode,
            ctx.source_generation,
            ctx.source_line_no,
            ctx.source_offset,
            ctx.record_ts,
            "reasoning",
            "reasoning",
            "assistant",
            "",
            &to_str(item.get("id")),
            "",
            "",
            "",
            &extract_reasoning_summary(item.get("summary").unwrap_or(&Value::Null)),
            &payload_json,
            "",
        );
    }

    base_event_row(
        &uid,
        ctx.session_id,
        ctx.session_date,
        ctx.source_file,
        ctx.source_inode,
        ctx.source_generation,
        ctx.source_line_no,
        ctx.source_offset,
        ctx.record_ts,
        "unknown",
        if item_type.is_empty() { "unknown" } else { item_type.as_str() },
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        &payload_json,
        "",
    )
}

pub fn normalize_record(
    record: &Value,
    source_file: &str,
    source_inode: u64,
    source_generation: u32,
    source_line_no: u64,
    source_offset: u64,
    session_hint: &str,
) -> NormalizedRecord {
    let record_ts = to_str(record.get("timestamp"));
    let top_type = to_str(record.get("type"));

    let payload_obj = record
        .get("payload")
        .and_then(|v| v.as_object())
        .cloned()
        .unwrap_or_else(Map::new);
    let payload = Value::Object(payload_obj.clone());

    let mut session_id = if session_hint.is_empty() {
        infer_session_id_from_file(source_file)
    } else {
        session_hint.to_string()
    };
    let session_date = infer_session_date_from_file(source_file);

    let raw_json = compact_json(record);
    let base_uid = event_uid(
        source_file,
        source_generation,
        source_line_no,
        source_offset,
        &raw_json,
        "raw",
    );

    let raw_row = json!({
        "source_file": source_file,
        "source_inode": source_inode,
        "source_generation": source_generation,
        "source_line_no": source_line_no,
        "source_offset": source_offset,
        "record_ts": record_ts,
        "top_type": top_type,
        "session_id": session_id,
        "raw_json": raw_json,
        "raw_json_hash": raw_hash(&raw_json),
        "event_uid": base_uid,
    });

    let mut normalized = Vec::<Value>::new();
    let mut expanded = Vec::<Value>::new();

    let ctx = RecordContext {
        session_id: &session_id,
        session_date: &session_date,
        source_file,
        source_inode,
        source_generation,
        source_line_no,
        source_offset,
        record_ts: &record_ts,
    };

    if top_type == "session_meta" {
        let id = to_str(payload.get("id"));
        if !id.is_empty() {
            session_id = id;
        }

        normalized.push(base_event_row(
            &base_uid,
            &session_id,
            &session_date,
            source_file,
            source_inode,
            source_generation,
            source_line_no,
            source_offset,
            &record_ts,
            "session_meta",
            "session_meta",
            "system",
            "",
            "",
            "",
            "",
            "",
            "",
            &compact_json(&payload),
            "",
        ));

        return NormalizedRecord {
            raw_row,
            normalized_rows: normalized,
            expanded_rows: expanded,
            session_hint: session_id,
        };
    }

    if top_type == "turn_context" {
        normalized.push(base_event_row(
            &base_uid,
            &session_id,
            &session_date,
            source_file,
            source_inode,
            source_generation,
            source_line_no,
            source_offset,
            &record_ts,
            "turn_context",
            "turn_context",
            "system",
            &to_str(payload.get("turn_id")),
            "",
            "",
            "",
            "",
            "",
            &compact_json(&payload),
            "",
        ));

        return NormalizedRecord {
            raw_row,
            normalized_rows: normalized,
            expanded_rows: expanded,
            session_hint: session_id,
        };
    }

    if top_type == "response_item" {
        let payload_type = to_str(payload.get("type"));
        let payload_json = compact_json(&payload);

        if payload_type == "message" {
            normalized.push(base_event_row(
                &base_uid,
                &session_id,
                &session_date,
                source_file,
                source_inode,
                source_generation,
                source_line_no,
                source_offset,
                &record_ts,
                "message",
                "message",
                &to_str(payload.get("role")),
                "",
                &to_str(payload.get("id")),
                "",
                "",
                &to_str(payload.get("phase")),
                &extract_message_text(payload.get("content").unwrap_or(&Value::Null)),
                &payload_json,
                "",
            ));
        } else if payload_type == "function_call" {
            let mut arguments = to_str(payload.get("arguments"));
            if arguments.len() > 200_000 {
                arguments = arguments.chars().take(200_000).collect();
            }

            normalized.push(base_event_row(
                &base_uid,
                &session_id,
                &session_date,
                source_file,
                source_inode,
                source_generation,
                source_line_no,
                source_offset,
                &record_ts,
                "tool_call",
                "function_call",
                "assistant",
                "",
                "",
                &to_str(payload.get("call_id")),
                &to_str(payload.get("name")),
                "",
                &arguments,
                &payload_json,
                "",
            ));
        } else if payload_type == "function_call_output" {
            let mut output = to_str(payload.get("output"));
            if output.len() > 200_000 {
                output = output.chars().take(200_000).collect();
            }

            normalized.push(base_event_row(
                &base_uid,
                &session_id,
                &session_date,
                source_file,
                source_inode,
                source_generation,
                source_line_no,
                source_offset,
                &record_ts,
                "tool_output",
                "function_call_output",
                "tool",
                "",
                "",
                &to_str(payload.get("call_id")),
                "",
                "",
                &output,
                &payload_json,
                "",
            ));
        } else if payload_type == "reasoning" {
            normalized.push(base_event_row(
                &base_uid,
                &session_id,
                &session_date,
                source_file,
                source_inode,
                source_generation,
                source_line_no,
                source_offset,
                &record_ts,
                "reasoning",
                "reasoning",
                "assistant",
                "",
                &to_str(payload.get("id")),
                "",
                "",
                "",
                &extract_reasoning_summary(payload.get("summary").unwrap_or(&Value::Null)),
                &payload_json,
                "",
            ));
        } else {
            let ptype = if payload_type.is_empty() {
                "response_item"
            } else {
                payload_type.as_str()
            };

            normalized.push(base_event_row(
                &base_uid,
                &session_id,
                &session_date,
                source_file,
                source_inode,
                source_generation,
                source_line_no,
                source_offset,
                &record_ts,
                "unknown",
                ptype,
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                &payload_json,
                "",
            ));
        }

        return NormalizedRecord {
            raw_row,
            normalized_rows: normalized,
            expanded_rows: expanded,
            session_hint: session_id,
        };
    }

    if top_type == "event_msg" {
        let payload_type = to_str(payload.get("type"));
        let actor_role = if payload_type == "user_message" {
            "user"
        } else if payload_type == "agent_message" || payload_type == "agent_reasoning" {
            "assistant"
        } else {
            "system"
        };

        let token_usage_json = if payload_type == "token_count" {
            compact_json(&json!({
                "info": payload.get("info"),
                "rate_limits": payload.get("rate_limits"),
            }))
        } else {
            String::new()
        };

        normalized.push(base_event_row(
            &base_uid,
            &session_id,
            &session_date,
            source_file,
            source_inode,
            source_generation,
            source_line_no,
            source_offset,
            &record_ts,
            "event_msg",
            &payload_type,
            actor_role,
            "",
            "",
            "",
            "",
            "",
            &extract_message_text(&payload),
            &compact_json(&payload),
            &token_usage_json,
        ));

        return NormalizedRecord {
            raw_row,
            normalized_rows: normalized,
            expanded_rows: expanded,
            session_hint: session_id,
        };
    }

    if ["message", "function_call", "function_call_output", "reasoning"].contains(&top_type.as_str()) {
        normalized.push(normalize_legacy_item(record, &ctx, "legacy"));
        return NormalizedRecord {
            raw_row,
            normalized_rows: normalized,
            expanded_rows: expanded,
            session_hint: session_id,
        };
    }

    if top_type == "compacted" {
        let parent_uid = event_uid(
            source_file,
            source_generation,
            source_line_no,
            source_offset,
            &raw_json,
            "compacted",
        );

        normalized.push(base_event_row(
            &parent_uid,
            &session_id,
            &session_date,
            source_file,
            source_inode,
            source_generation,
            source_line_no,
            source_offset,
            &record_ts,
            "compacted_raw",
            "compacted",
            "system",
            "",
            "",
            "",
            "",
            "",
            "",
            &compact_json(&payload),
            "",
        ));

        if let Some(Value::Array(items)) = payload.get("replacement_history") {
            for (idx, item) in items.iter().enumerate() {
                if !item.is_object() {
                    continue;
                }

                let mut row = normalize_legacy_item(item, &ctx, &format!("compacted:{}", idx));
                if let Some(obj) = row.as_object_mut() {
                    obj.insert(
                        "compacted_parent_uid".to_string(),
                        Value::String(parent_uid.clone()),
                    );
                }
                expanded.push(row);
            }
        }

        return NormalizedRecord {
            raw_row,
            normalized_rows: normalized,
            expanded_rows: expanded,
            session_hint: session_id,
        };
    }

    let ptype = if top_type.is_empty() { "unknown" } else { top_type.as_str() };
    normalized.push(base_event_row(
        &base_uid,
        &session_id,
        &session_date,
        source_file,
        source_inode,
        source_generation,
        source_line_no,
        source_offset,
        &record_ts,
        "unknown",
        ptype,
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        &compact_json(record),
        "",
    ));

    NormalizedRecord {
        raw_row,
        normalized_rows: normalized,
        expanded_rows: expanded,
        session_hint: session_id,
    }
}

#[cfg(test)]
mod tests {
    use super::normalize_record;
    use serde_json::json;

    fn source_path() -> &'static str {
        "/Users/eric/.codex/sessions/2026/02/13/rollout-2026-02-13T21-27-22-019c59f9-6389-77a1-a0cb-304eecf935b6.jsonl"
    }

    #[test]
    fn response_item_message_normalization() {
        let record = json!({
            "timestamp": "2026-02-14T02:28:00.000Z",
            "type": "response_item",
            "payload": {
                "type": "message",
                "role": "assistant",
                "phase": "final",
                "content": [{"type": "output_text", "text": "hello world"}]
            }
        });

        let out = normalize_record(&record, source_path(), 123, 1, 42, 1024, "");
        assert_eq!(out.normalized_rows.len(), 1);
        let row = out.normalized_rows[0].as_object().unwrap();
        assert_eq!(row.get("event_class").unwrap().as_str().unwrap(), "message");
        assert_eq!(row.get("actor_role").unwrap().as_str().unwrap(), "assistant");
        assert!(row
            .get("text_content")
            .unwrap()
            .as_str()
            .unwrap()
            .contains("hello world"));
        assert_eq!(out.session_hint, "019c59f9-6389-77a1-a0cb-304eecf935b6");
    }

    #[test]
    fn compacted_preserved_and_expanded() {
        let record = json!({
            "timestamp": "2026-01-02T01:45:06.004Z",
            "type": "compacted",
            "payload": {
                "replacement_history": [
                    {
                        "type": "message",
                        "role": "user",
                        "content": [{"type": "input_text", "text": "Prompt"}]
                    },
                    {
                        "type": "function_call_output",
                        "call_id": "call_1",
                        "output": "done"
                    }
                ]
            }
        });

        let out = normalize_record(&record, source_path(), 222, 2, 7, 333, "session-y");
        assert_eq!(out.normalized_rows.len(), 1);
        assert_eq!(out.expanded_rows.len(), 2);

        let parent = out.normalized_rows[0].as_object().unwrap();
        assert_eq!(parent.get("event_class").unwrap().as_str().unwrap(), "compacted_raw");

        let row1 = out.expanded_rows[0].as_object().unwrap();
        let row2 = out.expanded_rows[1].as_object().unwrap();
        assert_eq!(row1.get("event_class").unwrap().as_str().unwrap(), "message");
        assert_eq!(row2.get("event_class").unwrap().as_str().unwrap(), "tool_output");
        assert!(row1.get("compacted_parent_uid").is_some());
    }
}
