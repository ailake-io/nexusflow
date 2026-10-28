use crate::config::ServicenowConnectorConfig;
use arrow_array::{Array, RecordBatch, StringArray};
use async_trait::async_trait;
use nexus_core::{retry_with_backoff, CheckpointCursor, NexusError, Sink};
use serde_json::Value;

/// Creates or updates one record per row — `PATCH
/// /api/now/table/{table}/{sys_id}` when the row's `sys_id` column is
/// present and non-null, `POST /api/now/table/{table}` (create)
/// otherwise. Same per-record trade-off `nexus-connector-zendesk`'s
/// sink documents (no ServiceNow bulk/import-set batching wired up
/// for v1).
///
/// All columns are read as strings (`StringArray`) — same
/// "everything's a string on the wire" simplification
/// `nexus-connector-hubspot`/`zendesk`'s sinks document.
///
/// No external checkpoint state: `commit_checkpoint` is a no-op.
pub struct ServicenowSink {
    client: reqwest::Client,
    config: ServicenowConnectorConfig,
}

impl ServicenowSink {
    pub async fn connect(config: &ServicenowConnectorConfig) -> Result<Self, NexusError> {
        config.validate()?;
        Ok(Self {
            client: reqwest::Client::new(),
            config: config.clone(),
        })
    }
}

fn cell_to_string(
    batch: &RecordBatch,
    row: usize,
    col: usize,
) -> Result<Option<String>, NexusError> {
    let column = batch.column(col);
    let arr = column
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            NexusError::Schema(
                "servicenow sink only supports Utf8 columns — cast upstream first".to_string(),
            )
        })?;
    Ok(if arr.is_null(row) {
        None
    } else {
        Some(arr.value(row).to_string())
    })
}

fn row_to_upsert(batch: &RecordBatch, row: usize) -> Result<(Option<String>, Value), NexusError> {
    let field_names: Vec<String> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    let mut fields = serde_json::Map::with_capacity(field_names.len());
    let mut sys_id = None;
    for (col, name) in field_names.iter().enumerate() {
        let value = cell_to_string(batch, row, col)?;
        if name == "sys_id" {
            sys_id = value;
        } else if let Some(value) = value {
            fields.insert(name.clone(), Value::String(value));
        }
    }
    Ok((sys_id, Value::Object(fields)))
}

#[async_trait]
impl Sink for ServicenowSink {
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<(), NexusError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let table = self.config.table.clone();

        for row in 0..batch.num_rows() {
            let (sys_id, body) = row_to_upsert(&batch, row)?;
            let url = match &sys_id {
                Some(id) => format!("{}/api/now/table/{table}/{id}", self.config.base_url),
                None => format!("{}/api/now/table/{table}", self.config.base_url),
            };
            let is_update = sys_id.is_some();

            let client = self.client.clone();
            let username = self.config.username.clone();
            let password = self.config.password.clone();
            let timeout_seconds = self.config.timeout_seconds;
            retry_with_backoff(&self.config.retry, "servicenow upsert", || {
                let client = client.clone();
                let url = url.clone();
                let username = username.clone();
                let password = password.clone();
                let body = body.clone();
                async move {
                    let request = if is_update {
                        client.patch(&url)
                    } else {
                        client.post(&url)
                    };
                    let response = tokio::time::timeout(
                        std::time::Duration::from_secs(timeout_seconds),
                        request
                            .basic_auth(username, Some(password))
                            .json(&body)
                            .send(),
                    )
                    .await
                    .map_err(|e| {
                        NexusError::Connector(format!("servicenow upsert timed out: {e}"))
                    })?
                    .map_err(|e| NexusError::Connector(format!("servicenow upsert failed: {e}")))?;

                    if !response.status().is_success() {
                        let status = response.status();
                        let text = response.text().await.unwrap_or_default();
                        return Err(NexusError::Connector(format!(
                            "servicenow upsert failed ({status}): {text}"
                        )));
                    }
                    Ok(())
                }
            })
            .await?;
        }
        Ok(())
    }

    async fn commit_checkpoint(&mut self, _cursor: CheckpointCursor) -> Result<(), NexusError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn extracts_sys_id_and_excludes_it_from_fields() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("sys_id", DataType::Utf8, true),
            Field::new("short_description", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("abc123")])),
                Arc::new(StringArray::from(vec!["Printer down"])),
            ],
        )
        .unwrap();

        let (sys_id, fields) = row_to_upsert(&batch, 0).unwrap();
        assert_eq!(sys_id, Some("abc123".to_string()));
        assert_eq!(fields["short_description"], "Printer down");
        assert!(fields.get("sys_id").is_none());
    }

    #[test]
    fn missing_sys_id_column_value_means_create() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("sys_id", DataType::Utf8, true),
            Field::new("short_description", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(StringArray::from(vec!["New ticket"])),
            ],
        )
        .unwrap();

        let (sys_id, _) = row_to_upsert(&batch, 0).unwrap();
        assert_eq!(sys_id, None);
    }
}
