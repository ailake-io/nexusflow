use crate::config::ZendeskConnectorConfig;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use nexus_core::{retry_with_backoff, NexusError, RecordBatchBuilder, Source};
use serde_json::Value;
use std::sync::Arc;

/// Reads via the Zendesk Support API v2's cursor pagination
/// (`page[size]`/`page[after]`, `meta.has_more`/`meta.after_cursor` —
/// real, documented "Cursor Pagination" scheme, not the deprecated
/// offset-based one). The response's array key and item-count field
/// names are resource-dependent (`tickets`/`users`/`organizations`),
/// resolved via `ZendeskResource::plural()`.
///
/// Every column is projected as Utf8 — same "nested objects rendered
/// as JSON text, no second schema-discovery call" simplification
/// `nexus-connector-stripe`/`hubspot` document.
pub struct ZendeskSource {
    client: reqwest::Client,
    cfg: ZendeskConnectorConfig,
    schema: SchemaRef,
}

impl ZendeskSource {
    pub async fn connect(cfg: &ZendeskConnectorConfig) -> Result<Self, NexusError> {
        cfg.validate()?;
        let fields: Vec<Field> = cfg
            .fields
            .iter()
            .map(|f| Field::new(f, DataType::Utf8, true))
            .collect();
        Ok(Self {
            client: reqwest::Client::new(),
            cfg: cfg.clone(),
            schema: Arc::new(Schema::new(fields)),
        })
    }

    fn project(&self, object: &Value) -> Value {
        let mut out = serde_json::Map::with_capacity(self.cfg.fields.len());
        for name in &self.cfg.fields {
            let value = match object.get(name) {
                Some(Value::String(s)) => Value::String(s.clone()),
                Some(Value::Null) | None => Value::Null,
                Some(other) => Value::String(other.to_string()),
            };
            out.insert(name.clone(), value);
        }
        Value::Object(out)
    }

    async fn fetch_page(&self, after_cursor: Option<&str>) -> Result<Value, NexusError> {
        let url = format!(
            "{}/api/v2/{}.json",
            self.cfg.base_url,
            self.cfg.resource.plural()
        );
        let mut params: Vec<(String, String)> =
            vec![("page[size]".to_string(), self.cfg.page_size.to_string())];
        if let Some(cursor) = after_cursor {
            params.push(("page[after]".to_string(), cursor.to_string()));
        }

        let client = self.client.clone();
        let url = url.clone();
        let username = self.cfg.basic_auth_username();
        let api_token = self.cfg.api_token.clone();
        let timeout_seconds = self.cfg.timeout_seconds;
        let retry = self.cfg.retry.clone();

        retry_with_backoff(&retry, "zendesk list", || {
            let client = client.clone();
            let url = url.clone();
            let params = params.clone();
            let username = username.clone();
            let api_token = api_token.clone();
            async move {
                let response = tokio::time::timeout(
                    std::time::Duration::from_secs(timeout_seconds),
                    client
                        .get(&url)
                        .basic_auth(username, Some(api_token))
                        .query(&params)
                        .send(),
                )
                .await
                .map_err(|e| NexusError::Connector(format!("zendesk list request timed out: {e}")))?
                .map_err(|e| NexusError::Connector(format!("zendesk list request failed: {e}")))?;

                if response.status().as_u16() == 429 {
                    let delay = response
                        .headers()
                        .get("retry-after")
                        .and_then(|v| v.to_str().ok())
                        .and_then(|v| v.parse::<u64>().ok())
                        .unwrap_or(1);
                    tokio::time::sleep(std::time::Duration::from_secs(delay)).await;
                    return Err(NexusError::Connector(format!(
                        "zendesk list rate limited (429, retry-after={delay}s)"
                    )));
                }

                if !response.status().is_success() {
                    let status = response.status();
                    let text = response.text().await.unwrap_or_default();
                    return Err(NexusError::Connector(format!(
                        "zendesk list failed ({status}): {text}"
                    )));
                }

                response.json().await.map_err(|e| {
                    NexusError::Connector(format!("zendesk list response parse failed: {e}"))
                })
            }
        })
        .await
    }
}

impl Clone for ZendeskSource {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            cfg: self.cfg.clone(),
            schema: self.schema.clone(),
        }
    }
}

#[async_trait]
impl Source for ZendeskSource {
    async fn read_batches(
        &mut self,
    ) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError> {
        let source = self.clone();
        let schema = self.schema.clone();
        let plural = self.cfg.resource.plural();

        let stream = stream::try_unfold((None::<String>, false), move |(after, done)| {
            let source = source.clone();
            let schema = schema.clone();
            async move {
                if done {
                    return Ok(None);
                }
                let body = source.fetch_page(after.as_deref()).await?;
                let items = body.get(plural).and_then(Value::as_array).ok_or_else(|| {
                    NexusError::Connector(format!("zendesk list response missing '{plural}' array"))
                })?;
                let rows: Vec<Value> = items.iter().map(|o| source.project(o)).collect();
                let batch = RecordBatchBuilder::from_json_rows(schema, &rows)?;

                let has_more = body["meta"]["has_more"].as_bool().unwrap_or(false);
                let next_after = if has_more {
                    body["meta"]["after_cursor"].as_str().map(str::to_string)
                } else {
                    None
                };
                let next_done = next_after.is_none();
                Ok(Some((batch, (next_after, next_done))))
            }
        });

        Ok(Box::pin(stream))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
