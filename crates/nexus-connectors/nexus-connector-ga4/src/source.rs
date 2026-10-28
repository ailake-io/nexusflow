use crate::auth::authenticate;
use crate::config::Ga4ConnectorConfig;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use nexus_core::{retry_with_backoff, NexusError, RecordBatchBuilder, Source};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

/// Reads via the GA4 Data API's `runReport` — confirmed real endpoint
/// (`POST /v1beta/properties/{id}:runReport`), paginated via
/// `offset`/`limit` (not a cursor) since that's the real, documented
/// mechanism this API uses. Both dimension and metric values always
/// come back as strings in the JSON response (confirmed against the
/// API's own response shape) — every column in this connector's
/// schema is `Utf8`, no attempt to infer `Int64`/`Float64`, matching
/// the same safe-fallback simplification every other connector in
/// this workspace uses when a source has no real type catalog.
pub struct Ga4Source {
    client: reqwest::Client,
    cfg: Ga4ConnectorConfig,
    schema: SchemaRef,
    token_cache: Mutex<Option<(String, Instant)>>,
}

#[derive(Clone, Serialize)]
struct NamedRef<'a> {
    name: &'a str,
}

#[derive(Clone, Serialize)]
struct DateRange<'a> {
    #[serde(rename = "startDate")]
    start_date: &'a str,
    #[serde(rename = "endDate")]
    end_date: &'a str,
}

#[derive(Clone, Serialize)]
struct RunReportRequest<'a> {
    dimensions: Vec<NamedRef<'a>>,
    metrics: Vec<NamedRef<'a>>,
    #[serde(rename = "dateRanges")]
    date_ranges: Vec<DateRange<'a>>,
    limit: u32,
    offset: u32,
}

#[derive(Deserialize)]
struct ValueCell {
    value: String,
}

#[derive(Deserialize)]
struct ReportRow {
    #[serde(rename = "dimensionValues", default)]
    dimension_values: Vec<ValueCell>,
    #[serde(rename = "metricValues", default)]
    metric_values: Vec<ValueCell>,
}

#[derive(Deserialize)]
struct RunReportResponse {
    #[serde(default)]
    rows: Vec<ReportRow>,
    #[serde(rename = "rowCount", default)]
    row_count: u32,
}

impl Ga4Source {
    pub async fn connect(cfg: &Ga4ConnectorConfig) -> Result<Self, NexusError> {
        cfg.validate()?;
        // Duplicate names between dimensions/metrics would collide in
        // the schema — reject up front instead of building a broken
        // Arrow schema.
        let mut seen = std::collections::HashSet::new();
        for name in cfg.dimensions.iter().chain(cfg.metrics.iter()) {
            if !seen.insert(name.as_str()) {
                return Err(NexusError::Schema(format!(
                    "ga4: '{name}' appears in both dimensions and metrics"
                )));
            }
        }
        let fields: Vec<Field> = cfg
            .dimensions
            .iter()
            .chain(cfg.metrics.iter())
            .map(|name| Field::new(name, DataType::Utf8, true))
            .collect();

        Ok(Self {
            client: reqwest::Client::new(),
            cfg: cfg.clone(),
            schema: Arc::new(Schema::new(fields)),
            token_cache: Mutex::new(None),
        })
    }

    /// Returns a cached access token if still valid, otherwise exchanges
    /// the service-account JWT for a fresh one. Tokens are good for one
    /// hour; we refresh with a 5-minute safety margin to avoid edge-case
    /// expirations mid-request.
    async fn access_token(&self) -> Result<String, NexusError> {
        let now = Instant::now();
        {
            let guard = self.token_cache.lock().await;
            if let Some((token, expires_at)) = guard.as_ref() {
                if *expires_at > now + Duration::from_secs(300) {
                    return Ok(token.clone());
                }
            }
        }

        let token = authenticate(&self.client, &self.cfg).await?;
        let expires_at = now + Duration::from_secs(3600);
        *self.token_cache.lock().await = Some((token.clone(), expires_at));
        Ok(token)
    }

    async fn fetch_page(
        &self,
        access_token: &str,
        offset: u32,
    ) -> Result<RunReportResponse, NexusError> {
        let url = format!(
            "{}/v1beta/properties/{}:runReport",
            self.cfg.data_api_base_url, self.cfg.property_id
        );
        let body = RunReportRequest {
            dimensions: self
                .cfg
                .dimensions
                .iter()
                .map(|n| NamedRef { name: n })
                .collect(),
            metrics: self
                .cfg
                .metrics
                .iter()
                .map(|n| NamedRef { name: n })
                .collect(),
            date_ranges: vec![DateRange {
                start_date: &self.cfg.start_date,
                end_date: &self.cfg.end_date,
            }],
            limit: self.cfg.page_size,
            offset,
        };

        let client = self.client.clone();
        let access_token = access_token.to_string();
        let timeout_seconds = self.cfg.timeout_seconds;
        let retry = self.cfg.retry.clone();
        let property_id = self.cfg.property_id.clone();

        retry_with_backoff(&retry, "ga4 runReport", move || {
            let client = client.clone();
            let access_token = access_token.clone();
            let body = body.clone();
            let url = url.clone();
            let property_id = property_id.clone();
            async move {
                let response = tokio::time::timeout(
                    Duration::from_secs(timeout_seconds),
                    client
                        .post(&url)
                        .bearer_auth(&access_token)
                        .json(&body)
                        .send(),
                )
                .await
                .map_err(|e| {
                    NexusError::Connector(format!("ga4 runReport request timed out: {e}"))
                })?
                .map_err(|e| NexusError::Connector(format!("ga4 runReport request failed: {e}")))?;

                if !response.status().is_success() {
                    let status = response.status();
                    let text = response.text().await.unwrap_or_default();
                    return Err(NexusError::Connector(format!(
                        "ga4 runReport failed for property {property_id} ({status}): {text}"
                    )));
                }

                response.json().await.map_err(|e| {
                    NexusError::Connector(format!("ga4 runReport response parse failed: {e}"))
                })
            }
        })
        .await
    }

    fn row_to_json(&self, row: &ReportRow) -> Value {
        let mut object =
            serde_json::Map::with_capacity(self.cfg.dimensions.len() + self.cfg.metrics.len());
        for (name, cell) in self.cfg.dimensions.iter().zip(row.dimension_values.iter()) {
            object.insert(name.clone(), Value::String(cell.value.clone()));
        }
        for (name, cell) in self.cfg.metrics.iter().zip(row.metric_values.iter()) {
            object.insert(name.clone(), Value::String(cell.value.clone()));
        }
        Value::Object(object)
    }
}

#[async_trait]
impl Source for Ga4Source {
    async fn read_batches(
        &mut self,
    ) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError> {
        let access_token = self.access_token().await?;

        let mut all_rows: Vec<Value> = Vec::new();
        let mut offset = 0u32;
        loop {
            let page = self.fetch_page(&access_token, offset).await?;

            let page_len = page.rows.len() as u32;
            all_rows.extend(page.rows.iter().map(|r| self.row_to_json(r)));

            offset += self.cfg.page_size;
            if page_len < self.cfg.page_size || offset >= page.row_count {
                break;
            }
        }

        let batch = RecordBatchBuilder::from_json_rows(self.schema.clone(), &all_rows)?;
        Ok(Box::pin(stream::iter(vec![Ok(batch)])))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
