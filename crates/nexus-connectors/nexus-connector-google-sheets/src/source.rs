use crate::config::GoogleSheetsConnectorConfig;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use nexus_core::{retry_with_backoff, NexusError, RecordBatchBuilder, Source};
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;

#[derive(Deserialize)]
struct ValuesResponse {
    #[serde(default)]
    values: Vec<Vec<Value>>,
}

/// Reads via the Sheets API v4's `values.get`
/// (`GET /v4/spreadsheets/{id}/values/{range}`) — a whole range comes
/// back in one response, no cursor pagination (real API shape, see
/// `GoogleSheetsConnectorConfig`'s doc comment). `connect()` does the
/// fetch eagerly (needs the header row to build a schema anyway), so
/// `read_batches()` just hands back what's already in memory as a
/// single `RecordBatch`.
pub struct GoogleSheetsSource {
    schema: SchemaRef,
    rows: Vec<Value>,
}

fn cell_to_string(cell: &Value) -> Value {
    match cell {
        Value::String(s) => Value::String(s.clone()),
        Value::Null => Value::Null,
        other => Value::String(other.to_string()),
    }
}

impl GoogleSheetsSource {
    pub async fn connect(cfg: &GoogleSheetsConnectorConfig) -> Result<Self, NexusError> {
        cfg.validate()?;
        let mut url = reqwest::Url::parse(&format!(
            "{}/v4/spreadsheets/{}/values",
            cfg.base_url, cfg.spreadsheet_id
        ))
        .map_err(|e| NexusError::Connector(format!("google-sheets: invalid base_url: {e}")))?;
        url.path_segments_mut()
            .map_err(|_| NexusError::Connector("google-sheets: base_url cannot be a base".into()))?
            .push(&cfg.range);

        let client = reqwest::Client::new();
        let access_token = cfg.access_token.clone();
        let timeout_seconds = cfg.timeout_seconds;
        let response = retry_with_backoff(&cfg.retry, "google-sheets values.get", || {
            let client = client.clone();
            let url = url.clone();
            let access_token = access_token.clone();
            async move {
                let response = tokio::time::timeout(
                    std::time::Duration::from_secs(timeout_seconds),
                    client.get(url).bearer_auth(&access_token).send(),
                )
                .await
                .map_err(|e| {
                    NexusError::Connector(format!("google-sheets values.get timed out: {e}"))
                })?
                .map_err(|e| {
                    NexusError::Connector(format!("google-sheets values.get failed: {e}"))
                })?;

                if !response.status().is_success() {
                    let status = response.status();
                    let text = response.text().await.unwrap_or_default();
                    return Err(NexusError::Connector(format!(
                        "google-sheets values.get failed ({status}): {text}"
                    )));
                }

                response.json::<ValuesResponse>().await.map_err(|e| {
                    NexusError::Connector(format!(
                        "google-sheets values.get response parse failed: {e}"
                    ))
                })
            }
        })
        .await?;

        let mut values = response.values;
        let header: Vec<String> = if cfg.has_header_row && !values.is_empty() {
            values
                .remove(0)
                .iter()
                .map(|v| match v {
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                })
                .collect()
        } else {
            let width = values.iter().map(Vec::len).max().unwrap_or(0);
            (0..width).map(|i| format!("col_{i}")).collect()
        };

        let schema = Arc::new(Schema::new(
            header
                .iter()
                .map(|name| Field::new(name, DataType::Utf8, true))
                .collect::<Vec<_>>(),
        ));

        let rows: Vec<Value> = values
            .into_iter()
            .map(|row| {
                let mut object = serde_json::Map::with_capacity(header.len());
                for (i, name) in header.iter().enumerate() {
                    let cell = row.get(i).cloned().unwrap_or(Value::Null);
                    object.insert(name.clone(), cell_to_string(&cell));
                }
                Value::Object(object)
            })
            .collect();

        Ok(Self { schema, rows })
    }
}

#[async_trait]
impl Source for GoogleSheetsSource {
    async fn read_batches(
        &mut self,
    ) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError> {
        let schema = self.schema.clone();
        let rows = std::mem::take(&mut self.rows);
        let batch = RecordBatchBuilder::from_json_rows(schema, &rows);
        Ok(Box::pin(stream::once(async move { batch })))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
