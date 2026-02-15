use crate::config::ClickHouseConfig;
use crate::model::Checkpoint;
use anyhow::{anyhow, Context, Result};
use reqwest::{Client, Url};
use serde_json::Value;
use std::collections::HashMap;
use std::time::Duration;

#[derive(Clone)]
pub struct ClickHouseClient {
    cfg: ClickHouseConfig,
    http: Client,
}

impl ClickHouseClient {
    pub fn new(cfg: ClickHouseConfig) -> Result<Self> {
        let timeout = Duration::from_secs_f64(cfg.timeout_seconds.max(1.0));
        let http = Client::builder()
            .timeout(timeout)
            .build()
            .context("failed to construct reqwest client")?;

        Ok(Self { cfg, http })
    }

    fn base_url(&self) -> Result<Url> {
        Url::parse(&self.cfg.url).context("invalid ClickHouse URL")
    }

    async fn request(
        &self,
        query: &str,
        body: Option<Vec<u8>>,
        database: Option<&str>,
        async_insert: bool,
    ) -> Result<String> {
        let mut url = self.base_url()?;
        {
            let mut qp = url.query_pairs_mut();
            qp.append_pair("query", query);
            qp.append_pair("database", database.unwrap_or(&self.cfg.database));
            if async_insert && self.cfg.async_insert {
                qp.append_pair("async_insert", "1");
                if self.cfg.wait_for_async_insert {
                    qp.append_pair("wait_for_async_insert", "1");
                }
            }
        }

        let mut req = match body {
            Some(payload) => self
                .http
                .post(url)
                .header("Content-Type", "text/plain; charset=utf-8")
                .body(payload),
            None => self.http.get(url),
        };
        if !self.cfg.username.is_empty() {
            req = req.basic_auth(self.cfg.username.clone(), Some(self.cfg.password.clone()));
        }

        let response = req.send().await.context("clickhouse request failed")?;
        let status = response.status();
        let text = response.text().await.unwrap_or_default();

        if !status.is_success() {
            return Err(anyhow!("clickhouse returned {}: {}", status, text));
        }

        Ok(text)
    }

    pub async fn ping(&self) -> Result<()> {
        let response = self.request("SELECT 1", None, None, false).await?;
        if response.trim() == "1" {
            Ok(())
        } else {
            Err(anyhow!("unexpected ping response: {}", response.trim()))
        }
    }

    pub async fn insert_json_rows(&self, table: &str, rows: &[Value]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        let mut payload = Vec::<u8>::new();
        for row in rows {
            let line = serde_json::to_vec(row).context("failed to encode JSON row")?;
            payload.extend_from_slice(&line);
            payload.push(b'\n');
        }

        let query = format!(
            "INSERT INTO {}.{} FORMAT JSONEachRow",
            self.cfg.database, table
        );
        self.request(&query, Some(payload), None, true).await?;
        Ok(())
    }

    pub async fn load_checkpoints(&self) -> Result<HashMap<String, Checkpoint>> {
        let query = format!(
            "SELECT source_name, source_file, argMax(source_inode, updated_at), argMax(source_generation, updated_at), argMax(last_offset, updated_at), argMax(last_line_no, updated_at), argMax(status, updated_at) FROM {}.ingest_checkpoints GROUP BY source_name, source_file FORMAT TabSeparated",
            self.cfg.database
        );

        let raw = self.request(&query, None, None, false).await?;
        let mut map = HashMap::<String, Checkpoint>::new();

        for line in raw.lines() {
            if line.trim().is_empty() {
                continue;
            }
            let fields: Vec<&str> = line.split('\t').collect();
            if fields.len() < 7 {
                continue;
            }

            let source_name = fields[0].to_string();
            let source_file = fields[1].to_string();
            let source_inode = fields[2].parse::<u64>().unwrap_or(0);
            let source_generation = fields[3].parse::<u32>().unwrap_or(1);
            let last_offset = fields[4].parse::<u64>().unwrap_or(0);
            let last_line_no = fields[5].parse::<u64>().unwrap_or(0);
            let status = fields[6].to_string();

            map.insert(
                format!("{}\n{}", source_name, source_file),
                Checkpoint {
                    source_name,
                    source_file,
                    source_inode,
                    source_generation,
                    last_offset,
                    last_line_no,
                    status,
                },
            );
        }

        Ok(map)
    }
}
