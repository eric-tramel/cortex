use crate::config::ClickHouseConfig;
use anyhow::{anyhow, Context, Result};
use reqwest::{Client, Url};
use serde::de::DeserializeOwned;
use serde_json::Value;
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
        async_insert: bool,
    ) -> Result<String> {
        let mut url = self.base_url()?;
        {
            let mut qp = url.query_pairs_mut();
            qp.append_pair("query", query);
            qp.append_pair("database", &self.cfg.database);
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
        let response = self.request("SELECT 1", None, false).await?;
        if response.trim() == "1" {
            Ok(())
        } else {
            Err(anyhow!("unexpected ping response: {}", response.trim()))
        }
    }

    pub async fn query_json_rows<T: DeserializeOwned>(&self, query: &str) -> Result<Vec<T>> {
        let raw = self.request(query, None, false).await?;
        let mut rows = Vec::new();

        for line in raw.lines() {
            if line.trim().is_empty() {
                continue;
            }
            let row = serde_json::from_str::<T>(line)
                .with_context(|| format!("failed to parse JSONEachRow line: {}", line))?;
            rows.push(row);
        }

        Ok(rows)
    }

    pub async fn insert_json_rows(&self, table: &str, rows: &[Value]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        let mut payload = Vec::new();
        for row in rows {
            let line = serde_json::to_vec(row).context("failed to encode JSON row")?;
            payload.extend_from_slice(&line);
            payload.push(b'\n');
        }

        let query = format!(
            "INSERT INTO {}.{} FORMAT JSONEachRow",
            self.cfg.database, table
        );
        self.request(&query, Some(payload), true).await?;
        Ok(())
    }
}
