use crate::config::TiktokAdsConnectorConfig;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use nexus_core::{with_timeout, NexusError, RecordBatchBuilder, Source};
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;

/// Reads via the Reporting API's `report/integrated/get` — confirmed
/// real endpoint (TikTok for Business Marketing API v1.3), real
/// page-based pagination (`page`/`page_size`, not offset/cursor).
/// `dimensions`/`metrics` are sent as JSON-array-encoded query string
/// values (a real quirk of this API, confirmed via the official SDK —
/// not comma-separated the way GA4/Google Ads take field lists).
/// Every column is `Utf8`, same fallback every other tier-5 connector
/// in this workspace uses — TikTok Reporting API values (including
/// numeric metrics) come back as plain JSON values inside the
/// `dimensions`/`metrics` sub-objects, rendered uniformly as strings.
pub struct TiktokAdsSource {
    client: reqwest::Client,
    cfg: TiktokAdsConnectorConfig,
    schema: SchemaRef,
}

#[derive(Deserialize)]
struct ReportResponse {
    data: ReportData,
}

#[derive(Deserialize)]
struct ReportData {
    #[serde(default)]
    list: Vec<ReportRow>,
    page_info: PageInfo,
}

#[derive(Deserialize)]
struct ReportRow {
    #[serde(default)]
    dimensions: Value,
    #[serde(default)]
    metrics: Value,
}

#[derive(Deserialize)]
struct PageInfo {
    page: u32,
    total_page: u32,
}

impl TiktokAdsSource {
    pub async fn connect(cfg: &TiktokAdsConnectorConfig) -> Result<Self, NexusError> {
        if cfg.dimensions.is_empty() && cfg.metrics.is_empty() {
            return Err(NexusError::Schema(
                "tiktok-ads: dimensions and metrics must not both be empty".into(),
            ));
        }
        let fields: Vec<Field> = cfg
            .dimensions
            .iter()
            .chain(cfg.metrics.iter())
            .map(|f| Field::new(f, DataType::Utf8, true))
            .collect();
        Ok(Self {
            client: reqwest::Client::new(),
            cfg: cfg.clone(),
            schema: Arc::new(Schema::new(fields)),
        })
    }

    fn row_to_json(&self, row: &ReportRow) -> Value {
        let mut object =
            serde_json::Map::with_capacity(self.cfg.dimensions.len() + self.cfg.metrics.len());
        for name in &self.cfg.dimensions {
            object.insert(name.clone(), extract(&row.dimensions, name));
        }
        for name in &self.cfg.metrics {
            object.insert(name.clone(), extract(&row.metrics, name));
        }
        Value::Object(object)
    }
}

fn extract(source: &Value, name: &str) -> Value {
    match source.get(name) {
        Some(Value::String(s)) => Value::String(s.clone()),
        Some(Value::Null) | None => Value::Null,
        Some(other) => Value::String(other.to_string()),
    }
}

#[async_trait]
impl Source for TiktokAdsSource {
    async fn read_batches(
        &mut self,
    ) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError> {
        let url = format!(
            "{}/open_api/v1.3/report/integrated/get/",
            self.cfg.api_base_url
        );
        let dimensions_json = serde_json::to_string(&self.cfg.dimensions).map_err(|e| {
            NexusError::Connector(format!("tiktok-ads: failed to encode dimensions: {e}"))
        })?;
        let metrics_json = serde_json::to_string(&self.cfg.metrics).map_err(|e| {
            NexusError::Connector(format!("tiktok-ads: failed to encode metrics: {e}"))
        })?;

        let mut rows: Vec<Value> = Vec::new();
        let mut page = 1u32;

        loop {
            let params = [
                ("advertiser_id", self.cfg.advertiser_id.clone()),
                ("report_type", self.cfg.report_type.clone()),
                ("data_level", self.cfg.data_level.clone()),
                ("dimensions", dimensions_json.clone()),
                ("metrics", metrics_json.clone()),
                ("start_date", self.cfg.start_date.clone()),
                ("end_date", self.cfg.end_date.clone()),
                ("page", page.to_string()),
                ("page_size", self.cfg.page_size.to_string()),
            ];

            let response: ReportResponse =
                with_timeout(self.cfg.timeout_seconds, "tiktok-ads report", async {
                    let response = self
                        .client
                        .get(&url)
                        .header("Access-Token", &self.cfg.access_token)
                        .query(&params)
                        .send()
                        .await
                        .map_err(|e| {
                            NexusError::Connector(format!("tiktok-ads report request failed: {e}"))
                        })?;

                    if !response.status().is_success() {
                        let status = response.status();
                        let text = response.text().await.unwrap_or_default();
                        return Err(NexusError::Connector(format!(
                            "tiktok-ads report failed ({status}): {text}"
                        )));
                    }

                    response.json().await.map_err(|e| {
                        NexusError::Connector(format!(
                            "tiktok-ads report response parse failed: {e}"
                        ))
                    })
                })
                .await?;

            rows.extend(response.data.list.iter().map(|r| self.row_to_json(r)));

            if response.data.page_info.page >= response.data.page_info.total_page {
                break;
            }
            page += 1;
        }

        let batch = RecordBatchBuilder::from_json_rows(self.schema.clone(), &rows)?;
        Ok(Box::pin(stream::iter(vec![Ok(batch)])))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
