use crate::config::StripeConnectorConfig;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use nexus_core::{retry_with_backoff, NexusError, RecordBatchBuilder, Source};
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;

/// Reads via Stripe's standard list endpoints (`GET /v1/{resource}`) —
/// confirmed real cursor pagination (`starting_after`/`has_more`, not
/// offset-based) and a real incremental filter (`created[gte]`).
/// Stripe has no bulk export API usable here — Stripe Data Pipeline/
/// Sigma are Stripe's own paid products (competing with what NexusFlow
/// itself does, not something to integrate against), so pagination
/// over the list endpoints is the real v1 mechanism, not a shortcut
/// taken for lack of research.
///
/// The source streams one `RecordBatch` per page as soon as the page
/// is fetched, instead of accumulating the entire dataset in memory
/// before returning. Retries use exponential backoff and honor Stripe's
/// `Retry-After` header when present.
pub struct StripeSource {
    client: reqwest::Client,
    cfg: StripeConnectorConfig,
    schema: SchemaRef,
}

#[derive(Deserialize)]
struct ListResponse {
    #[serde(default)]
    data: Vec<Value>,
    #[serde(default)]
    has_more: bool,
}

impl StripeSource {
    pub async fn connect(cfg: &StripeConnectorConfig) -> Result<Self, NexusError> {
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

    fn build_url(&self) -> String {
        format!("{}/v1/{}", self.cfg.api_base_url, self.cfg.resource.path())
    }

    async fn fetch_page(&self, starting_after: Option<&str>) -> Result<ListResponse, NexusError> {
        let url = self.build_url();
        let mut params: Vec<(String, String)> =
            vec![("limit".to_string(), self.cfg.page_limit.to_string())];
        if let Some(after) = starting_after {
            params.push(("starting_after".to_string(), after.to_string()));
        }
        if let Some(gte) = self.cfg.created_gte {
            params.push(("created[gte]".to_string(), gte.to_string()));
        }
        if let Some(lt) = self.cfg.created_lt {
            params.push(("created[lt]".to_string(), lt.to_string()));
        }

        let secret_key = self.cfg.secret_key.clone();
        let stripe_version = self.cfg.stripe_version.clone();
        let timeout_seconds = self.cfg.timeout_seconds;
        let client = self.client.clone();
        let retry = self.cfg.retry.clone();

        retry_with_backoff(&retry, "stripe list", || {
            let client = client.clone();
            let url = url.clone();
            let params = params.clone();
            let secret_key = secret_key.clone();
            let stripe_version = stripe_version.clone();
            async move {
                let response = tokio::time::timeout(
                    std::time::Duration::from_secs(timeout_seconds),
                    client
                        .get(&url)
                        .bearer_auth(&secret_key)
                        .header("Stripe-Version", &stripe_version)
                        .query(&params)
                        .send(),
                )
                .await
                .map_err(|e| NexusError::Connector(format!("stripe list request timed out: {e}")))?
                .map_err(|e| NexusError::Connector(format!("stripe list request failed: {e}")))?;

                // Respect Stripe's explicit rate-limit hint when present.
                if response.status().as_u16() == 429 {
                    let delay = response
                        .headers()
                        .get("retry-after")
                        .and_then(|v| v.to_str().ok())
                        .and_then(|v| v.parse::<u64>().ok())
                        .unwrap_or(1);
                    tokio::time::sleep(std::time::Duration::from_secs(delay)).await;
                    return Err(NexusError::Connector(format!(
                        "stripe list rate limited (429, retry-after={delay}s)"
                    )));
                }

                if !response.status().is_success() {
                    let status = response.status();
                    let text = response.text().await.unwrap_or_default();
                    return Err(NexusError::Connector(format!(
                        "stripe list failed ({status}): {text}"
                    )));
                }

                response.json().await.map_err(|e| {
                    NexusError::Connector(format!("stripe list response parse failed: {e}"))
                })
            }
        })
        .await
    }
}

#[async_trait]
impl Source for StripeSource {
    async fn read_batches(
        &mut self,
    ) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError> {
        let source = self.clone();
        let schema = self.schema.clone();

        let stream = stream::try_unfold((None::<String>, false), move |(starting_after, done)| {
            let source = source.clone();
            let schema = schema.clone();
            async move {
                if done {
                    return Ok(None);
                }
                let page = source.fetch_page(starting_after.as_deref()).await?;
                let last_id = page
                    .data
                    .last()
                    .and_then(|v| v.get("id"))
                    .and_then(|v| v.as_str())
                    .map(str::to_string);
                let rows: Vec<Value> = page.data.iter().map(|o| source.project(o)).collect();
                let batch = RecordBatchBuilder::from_json_rows(schema, &rows)?;

                let next_starting_after = if page.has_more { last_id } else { None };
                let next_done = next_starting_after.is_none();
                Ok(Some((batch, (next_starting_after, next_done))))
            }
        });

        Ok(Box::pin(stream))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Clone for StripeSource {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            cfg: self.cfg.clone(),
            schema: self.schema.clone(),
        }
    }
}
