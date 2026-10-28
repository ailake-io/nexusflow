use crate::config::WorkdayConnectorConfig;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use nexus_core::{retry_with_backoff, NexusError, RecordBatchBuilder, Source};
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;

#[derive(Deserialize)]
struct RaasResponse {
    #[serde(rename = "Report_Entry", default)]
    report_entry: Vec<Value>,
}

/// Reads a Workday RaaS (Report-as-a-Service) report in one shot —
/// real, documented response envelope key `Report_Entry` (an array of
/// row objects, keyed by the report's own column names). RaaS has no
/// pagination of its own (a report either returns everything or the
/// tenant's report definition itself limits/filters it) — same
/// "whole result in one response" shape `nexus-connector-google-sheets`
/// documents for the same reason (no cursor to page through).
///
/// Column names come from the report's own field prompts, not a fixed
/// `fields` config — schema is built from the first row's keys, all
/// Utf8, same dynamic-schema approach `nexus-connector-netsuite`'s
/// SuiteQL source uses for the same reason (arbitrary caller-defined
/// column set).
pub struct WorkdaySource {
    schema: SchemaRef,
    rows: Vec<Value>,
}

impl WorkdaySource {
    pub async fn connect(cfg: &WorkdayConnectorConfig) -> Result<Self, NexusError> {
        cfg.validate()?;
        let url = format!(
            "{}/ccx/service/customreport2/{}/{}/{}?format=json",
            cfg.base_url, cfg.tenant, cfg.report_owner, cfg.report_name
        );

        let client = reqwest::Client::new();
        let username = cfg.username.clone();
        let password = cfg.password.clone();
        let timeout_seconds = cfg.timeout_seconds;
        let response: RaasResponse = retry_with_backoff(&cfg.retry, "workday raas", || {
            let client = client.clone();
            let url = url.clone();
            let username = username.clone();
            let password = password.clone();
            async move {
                let response = tokio::time::timeout(
                    std::time::Duration::from_secs(timeout_seconds),
                    client.get(&url).basic_auth(username, Some(password)).send(),
                )
                .await
                .map_err(|e| NexusError::Connector(format!("workday raas timed out: {e}")))?
                .map_err(|e| NexusError::Connector(format!("workday raas failed: {e}")))?;

                if !response.status().is_success() {
                    let status = response.status();
                    let text = response.text().await.unwrap_or_default();
                    return Err(NexusError::Connector(format!(
                        "workday raas failed ({status}): {text}"
                    )));
                }
                response.json().await.map_err(|e| {
                    NexusError::Connector(format!("workday raas response parse failed: {e}"))
                })
            }
        })
        .await?;

        let columns: Vec<String> = response
            .report_entry
            .first()
            .and_then(|row| row.as_object())
            .map(|o| o.keys().cloned().collect())
            .unwrap_or_default();
        let schema = Arc::new(Schema::new(
            columns
                .iter()
                .map(|name| Field::new(name, DataType::Utf8, true))
                .collect::<Vec<_>>(),
        ));

        let rows: Vec<Value> = response
            .report_entry
            .iter()
            .map(|row| project_row(row, &columns))
            .collect();

        Ok(Self { schema, rows })
    }
}

fn project_row(object: &Value, columns: &[String]) -> Value {
    let mut out = serde_json::Map::with_capacity(columns.len());
    for name in columns {
        let value = match object.get(name) {
            Some(Value::String(s)) => Value::String(s.clone()),
            Some(Value::Null) | None => Value::Null,
            Some(other) => Value::String(other.to_string()),
        };
        out.insert(name.clone(), value);
    }
    Value::Object(out)
}

#[async_trait]
impl Source for WorkdaySource {
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
