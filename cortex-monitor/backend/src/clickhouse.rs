use crate::config::ClickHouseConfig;
use anyhow::{anyhow, Context, Result};
use reqwest::{header, Client, Url};
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::time::Duration;

#[derive(Clone)]
pub struct ClickHouseClient {
    cfg: ClickHouseConfig,
    client: Client,
}

#[derive(serde::Deserialize)]
struct ClickHouseEnvelope<T> {
    data: Vec<T>,
}

impl ClickHouseClient {
    pub fn new(cfg: ClickHouseConfig) -> Result<Self> {
        let timeout = Duration::from_secs_f64(cfg.timeout_seconds.max(1.0));
        let mut headers = header::HeaderMap::new();
        headers.insert(header::ACCEPT, header::HeaderValue::from_static("application/json"));

        let client = Client::builder()
            .timeout(timeout)
            .default_headers(headers)
            .build()
            .context("failed to construct reqwest client")?;
        Ok(Self { cfg, client })
    }

    pub fn config(&self) -> &ClickHouseConfig {
        &self.cfg
    }

    fn base_url(&self) -> Result<Url> {
        Ok(Url::parse(&self.cfg.url).context("invalid ClickHouse URL")?)
    }

    async fn request_text(&self, query: &str, body: Option<Vec<u8>>) -> Result<String> {
        let mut url = self.base_url()?;
        {
            let mut pairs = url.query_pairs_mut();
            pairs.append_pair("database", &self.cfg.database);
            pairs.append_pair("default_format", "JSON");
            pairs.append_pair("query", query);
        }

        let request = match body {
            Some(payload) => self
                .client
                .post(url)
                .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
                .body(payload),
            None => self.client.get(url),
        };

        let request = request.basic_auth(&self.cfg.username, Some(self.cfg.password.clone()));
        let response = request.send().await.context("clickhouse request failed")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "<unavailable>".to_string());
            return Err(anyhow!("ClickHouse returned {status}: {body}"));
        }

        response
            .text()
            .await
            .context("failed to read clickhouse response text")
    }

    pub async fn query_rows<T: DeserializeOwned>(&self, query: &str) -> Result<Vec<T>> {
        let text = self.request_text(query, None).await?;
        let envelope: ClickHouseEnvelope<T> = serde_json::from_str(&text)
            .with_context(|| format!("invalid clickhouse JSON response: {text}"))?;
        Ok(envelope.data)
    }

    pub async fn ping(&self) -> Result<()> {
        let rows: Vec<Value> = self.query_rows("SELECT 1").await?;
        if rows.is_empty() {
            return Err(anyhow!("empty ping response"));
        }

        let first = rows[0].as_object().ok_or_else(|| anyhow!("invalid ping payload"))?;
        let first_value = first.values().next().and_then(Value::as_i64).unwrap_or(0);
        if first_value == 1 {
            Ok(())
        } else {
            Err(anyhow!("unexpected ping result"))
        }
    }

    pub async fn version(&self) -> Result<String> {
        let rows: Vec<Value> = self.query_rows("SELECT version() AS version").await?;
        let version = rows
            .first()
            .and_then(|row| row.get("version"))
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("missing version in payload"))?;

        Ok(version.to_string())
    }
}
