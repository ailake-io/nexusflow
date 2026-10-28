use crate::auth::resolve_access_token;
use crate::config::YoutubeAnalyticsConnectorConfig;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use nexus_core::{with_timeout, NexusError, RecordBatchBuilder, Source};
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;

/// Reads via `reports.query` — confirmed real endpoint (`GET
/// /v2/reports`), real index-based pagination (`startIndex`/
/// `maxResults`, not offset/cursor). **Response shape differs from
/// `nexus-connector-ga4`**: rows are positional arrays, not named
/// objects — `columnHeaders[i].name` gives the column for `row[i]`.
/// Column order in the API response isn't assumed to match
/// `cfg.dimensions`/`cfg.metrics`' declared order — each row is
/// re-projected by name into this connector's own fixed schema, not
/// read positionally by index into it.
pub struct YoutubeAnalyticsSource {
    client: reqwest::Client,
    cfg: YoutubeAnalyticsConnectorConfig,
    schema: SchemaRef,
}

#[derive(Deserialize)]
struct ColumnHeader {
    name: String,
}

#[derive(Deserialize)]
struct ReportsResponse {
    #[serde(default)]
    #[serde(rename = "columnHeaders")]
    column_headers: Vec<ColumnHeader>,
    #[serde(default)]
    rows: Vec<Vec<Value>>,
}

impl YoutubeAnalyticsSource {
    pub async fn connect(cfg: &YoutubeAnalyticsConnectorConfig) -> Result<Self, NexusError> {
        if cfg.dimensions.is_empty() && cfg.metrics.is_empty() {
            return Err(NexusError::Schema(
                "youtube-analytics: dimensions and metrics must not both be empty".into(),
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
}

fn cell_to_json(cell: &Value) -> Value {
    match cell {
        Value::String(s) => Value::String(s.clone()),
        Value::Null => Value::Null,
        other => Value::String(other.to_string()),
    }
}

#[async_trait]
impl Source for YoutubeAnalyticsSource {
    async fn read_batches(
        &mut self,
    ) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError> {
        let access_token = with_timeout(
            self.cfg.timeout_seconds,
            "youtube-analytics resolve token",
            async { resolve_access_token(&self.client, &self.cfg).await },
        )
        .await?;

        let url = format!("{}/v2/reports", self.cfg.api_base_url);
        let mut rows: Vec<Value> = Vec::new();
        let mut start_index = 1u32; // API is 1-indexed

        loop {
            let mut params: Vec<(String, String)> = vec![
                ("ids".to_string(), self.cfg.ids.clone()),
                ("startDate".to_string(), self.cfg.start_date.clone()),
                ("endDate".to_string(), self.cfg.end_date.clone()),
                ("metrics".to_string(), self.cfg.metrics.join(",")),
                ("startIndex".to_string(), start_index.to_string()),
                ("maxResults".to_string(), self.cfg.max_results.to_string()),
            ];
            if !self.cfg.dimensions.is_empty() {
                params.push(("dimensions".to_string(), self.cfg.dimensions.join(",")));
            }
            if let Some(filters) = &self.cfg.filters {
                params.push(("filters".to_string(), filters.clone()));
            }

            let page: ReportsResponse = with_timeout(
                self.cfg.timeout_seconds,
                "youtube-analytics reports.query",
                async {
                    let response = self
                        .client
                        .get(&url)
                        .bearer_auth(&access_token)
                        .query(&params)
                        .send()
                        .await
                        .map_err(|e| {
                            NexusError::Connector(format!(
                                "youtube-analytics reports.query request failed: {e}"
                            ))
                        })?;

                    if !response.status().is_success() {
                        let status = response.status();
                        let text = response.text().await.unwrap_or_default();
                        return Err(NexusError::Connector(format!(
                            "youtube-analytics reports.query failed ({status}): {text}"
                        )));
                    }

                    response.json().await.map_err(|e| {
                        NexusError::Connector(format!(
                            "youtube-analytics reports.query response parse failed: {e}"
                        ))
                    })
                },
            )
            .await?;

            let column_names: Vec<&str> = page
                .column_headers
                .iter()
                .map(|h| h.name.as_str())
                .collect();
            let page_len = page.rows.len() as u32;
            for row in &page.rows {
                let mut object = serde_json::Map::with_capacity(column_names.len());
                for (i, name) in column_names.iter().enumerate() {
                    if let Some(cell) = row.get(i) {
                        object.insert(name.to_string(), cell_to_json(cell));
                    }
                }
                rows.push(Value::Object(object));
            }

            if page_len < self.cfg.max_results {
                break;
            }
            start_index += self.cfg.max_results;
        }

        let batch = RecordBatchBuilder::from_json_rows(self.schema.clone(), &rows)?;
        Ok(Box::pin(stream::iter(vec![Ok(batch)])))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
