use crate::config::NetsuiteConnectorConfig;
use arrow_array::{Array, RecordBatch, StringArray};
use async_trait::async_trait;
use nexus_core::{retry_with_backoff, CheckpointCursor, NexusError, Sink};
use serde_json::Value;

/// Creates or updates one record per row via the REST Record API
/// (`POST /services/rest/record/v1/{record_type}` to create, `PATCH
/// .../{record_type}/{id}` to update when the row's `id` column is
/// present and non-null) — real NetSuite REST endpoint, distinct from
/// SuiteQL (query-only, no writes). Same per-record trade-off
/// `nexus-connector-hubspot`/`zendesk`/`servicenow`/`dynamics365`'s
/// sinks document (no NetSuite CSV-import/bulk batching wired up for
/// v1).
///
/// All columns are read as strings (`StringArray`) — same
/// "everything's a string on the wire" simplification every REST
/// sink in this repo documents.
///
/// No external checkpoint state: `commit_checkpoint` is a no-op.
pub struct NetsuiteSink {
    client: reqwest::Client,
    config: NetsuiteConnectorConfig,
}

impl NetsuiteSink {
    pub async fn connect(config: &NetsuiteConnectorConfig) -> Result<Self, NexusError> {
        config.validate()?;
        config.record_type_or_err()?;
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
                "netsuite sink only supports Utf8 columns — cast upstream first".to_string(),
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
    let mut id = None;
    for (col, name) in field_names.iter().enumerate() {
        let value = cell_to_string(batch, row, col)?;
        if name == "id" {
            id = value;
        } else if let Some(value) = value {
            fields.insert(name.clone(), Value::String(value));
        }
    }
    Ok((id, Value::Object(fields)))
}

#[async_trait]
impl Sink for NetsuiteSink {
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<(), NexusError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let record_type = self.config.record_type_or_err()?.to_string();

        for row in 0..batch.num_rows() {
            let (id, body) = row_to_upsert(&batch, row)?;
            let url = match &id {
                Some(id) => format!(
                    "{}/services/rest/record/v1/{record_type}/{id}",
                    self.config.account_url
                ),
                None => format!(
                    "{}/services/rest/record/v1/{record_type}",
                    self.config.account_url
                ),
            };
            let is_update = id.is_some();

            let client = self.client.clone();
            let access_token = self.config.access_token.clone();
            let timeout_seconds = self.config.timeout_seconds;
            retry_with_backoff(&self.config.retry, "netsuite upsert", || {
                let client = client.clone();
                let url = url.clone();
                let access_token = access_token.clone();
                let body = body.clone();
                async move {
                    let request = if is_update {
                        client.patch(&url)
                    } else {
                        client.post(&url)
                    };
                    let response = tokio::time::timeout(
                        std::time::Duration::from_secs(timeout_seconds),
                        request.bearer_auth(&access_token).json(&body).send(),
                    )
                    .await
                    .map_err(|e| NexusError::Connector(format!("netsuite upsert timed out: {e}")))?
                    .map_err(|e| NexusError::Connector(format!("netsuite upsert failed: {e}")))?;

                    if !response.status().is_success() {
                        let status = response.status();
                        let text = response.text().await.unwrap_or_default();
                        return Err(NexusError::Connector(format!(
                            "netsuite upsert failed ({status}): {text}"
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
    fn extracts_id_and_excludes_it_from_fields() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("entityid", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("42")])),
                Arc::new(StringArray::from(vec!["Acme"])),
            ],
        )
        .unwrap();

        let (id, fields) = row_to_upsert(&batch, 0).unwrap();
        assert_eq!(id, Some("42".to_string()));
        assert_eq!(fields["entityid"], "Acme");
        assert!(fields.get("id").is_none());
    }

    #[test]
    fn missing_id_column_value_means_create() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("entityid", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(StringArray::from(vec!["New customer"])),
            ],
        )
        .unwrap();

        let (id, _) = row_to_upsert(&batch, 0).unwrap();
        assert_eq!(id, None);
    }
}
