use crate::auth::authenticate;
use crate::config::GoogleAdsConnectorConfig;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use nexus_core::{with_timeout, NexusError, RecordBatchBuilder, Source};
use serde::Serialize;
use serde_json::Value;
use std::sync::Arc;

/// Reads via `googleAds:searchStream` — confirmed real endpoint
/// (`POST /v{version}/customers/{id}/googleAds:searchStream`),
/// returns the whole result set in one streamed JSON array response
/// (chunked as `[{"results": [...]}, {"results": [...]}, ...]`), no
/// pagination needed (unlike GA4's `runReport`, which paginates via
/// `offset`).
///
/// `searchStream` rows are deeply nested JSON shaped by whichever
/// resource/field paths the GAQL `SELECT` clause names (e.g.
/// `{"campaign": {"name": "..."}, "metrics": {"clicks": "123"}}`) —
/// `cfg.fields` (dotted paths matching the `SELECT` clause) drives
/// both the Arrow schema and the flattening of each row into columns.
/// Every column is `Utf8`: Google Ads API values (including numeric
/// metrics like `clicks`) come back as JSON strings, same
/// safe-fallback simplification GA4/every other connector in this
/// workspace uses when there's no real type catalog.
pub struct GoogleAdsSource {
    client: reqwest::Client,
    cfg: GoogleAdsConnectorConfig,
    schema: SchemaRef,
}

#[derive(Serialize)]
struct SearchStreamRequest<'a> {
    query: &'a str,
}

#[derive(serde::Deserialize)]
struct SearchStreamChunk {
    #[serde(default)]
    results: Vec<Value>,
}

impl GoogleAdsSource {
    pub async fn connect(cfg: &GoogleAdsConnectorConfig) -> Result<Self, NexusError> {
        if cfg.fields.is_empty() {
            return Err(NexusError::Schema(
                "google-ads: fields must not be empty".into(),
            ));
        }
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
}

/// Walks a dotted field path (e.g. `campaign.name`) into a nested
/// JSON object and returns the leaf as a string — numbers/bools/
/// objects are rendered via their JSON text form rather than
/// silently dropped, missing/absent segments become `null`.
fn extract_dotted(value: &Value, path: &str) -> Value {
    let mut current = value;
    for segment in path.split('.') {
        match current.get(segment) {
            Some(v) => current = v,
            None => return Value::Null,
        }
    }
    match current {
        Value::String(s) => Value::String(s.clone()),
        Value::Null => Value::Null,
        other => Value::String(other.to_string()),
    }
}

#[async_trait]
impl Source for GoogleAdsSource {
    async fn read_batches(
        &mut self,
    ) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError> {
        let access_token =
            with_timeout(self.cfg.timeout_seconds, "google-ads authenticate", async {
                authenticate(&self.client, &self.cfg).await
            })
            .await?;

        let url = format!(
            "{}/{}/customers/{}/googleAds:searchStream",
            self.cfg.ads_api_base_url, self.cfg.api_version, self.cfg.customer_id
        );
        let body = SearchStreamRequest {
            query: &self.cfg.gaql_query,
        };

        let chunks: Vec<SearchStreamChunk> =
            with_timeout(self.cfg.timeout_seconds, "google-ads searchStream", async {
                let response = self
                    .client
                    .post(&url)
                    .header("developer-token", &self.cfg.developer_token)
                    .header("login-customer-id", &self.cfg.login_customer_id)
                    .bearer_auth(&access_token)
                    .json(&body)
                    .send()
                    .await
                    .map_err(|e| {
                        NexusError::Connector(format!(
                            "google-ads searchStream request failed: {e}"
                        ))
                    })?;

                if !response.status().is_success() {
                    let status = response.status();
                    let text = response.text().await.unwrap_or_default();
                    return Err(NexusError::Connector(format!(
                        "google-ads searchStream failed ({status}): {text}"
                    )));
                }

                response.json().await.map_err(|e| {
                    NexusError::Connector(format!(
                        "google-ads searchStream response parse failed: {e}"
                    ))
                })
            })
            .await?;

        let mut rows: Vec<Value> = Vec::new();
        for chunk in &chunks {
            for row in &chunk.results {
                let mut object = serde_json::Map::with_capacity(self.cfg.fields.len());
                for field in &self.cfg.fields {
                    object.insert(field.clone(), extract_dotted(row, field));
                }
                rows.push(Value::Object(object));
            }
        }

        let batch = RecordBatchBuilder::from_json_rows(self.schema.clone(), &rows)?;
        Ok(Box::pin(stream::iter(vec![Ok(batch)])))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
