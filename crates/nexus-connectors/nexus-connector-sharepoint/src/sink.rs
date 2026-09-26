use crate::config::SharepointConnectorConfig;
use arrow_array::{Array, RecordBatch, StringArray};
use async_trait::async_trait;
use nexus_core::{retry_with_backoff, CheckpointCursor, NexusError, Sink};
use serde_json::{json, Value};

/// Creates or updates one list item per row. Real Graph API asymmetry
/// worth calling out: create is
/// `POST /sites/{site}/lists/{list}/items` with body `{"fields":
/// {...}}` (wrapped), but update is
/// `PATCH /sites/{site}/lists/{list}/items/{id}/fields` with the
/// field map directly as the body (no wrapper — the URL itself
/// addresses the `fields` sub-resource). Same per-record trade-off
/// `nexus-connector-zendesk`/`servicenow`/`dynamics365`'s sinks
/// document (no Graph `$batch` multipart batching wired up for v1).
///
/// All columns are read as strings (`StringArray`) — same
/// "everything's a string on the wire" simplification every REST
/// sink in this repo documents.
///
/// No external checkpoint state: `commit_checkpoint` is a no-op.
pub struct SharepointSink {
    client: reqwest::Client,
    config: SharepointConnectorConfig,
}

impl SharepointSink {
    pub async fn connect(config: &SharepointConnectorConfig) -> Result<Self, NexusError> {
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
                "sharepoint sink only supports Utf8 columns — cast upstream first".to_string(),
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
impl Sink for SharepointSink {
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<(), NexusError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let site_id = self.config.site_id.clone();
        let list_id = self.config.list_id.clone();

        for row in 0..batch.num_rows() {
            let (id, fields) = row_to_upsert(&batch, row)?;
            let (url, body, is_update) = match &id {
                Some(id) => (
                    format!(
                        "{}/sites/{site_id}/lists/{list_id}/items/{id}/fields",
                        self.config.base_url
                    ),
                    fields,
                    true,
                ),
                None => (
                    format!(
                        "{}/sites/{site_id}/lists/{list_id}/items",
                        self.config.base_url
                    ),
                    json!({ "fields": fields }),
                    false,
                ),
            };

            let client = self.client.clone();
            let access_token = self.config.access_token.clone();
            let timeout_seconds = self.config.timeout_seconds;
            retry_with_backoff(&self.config.retry, "sharepoint upsert", || {
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
                    .map_err(|e| {
                        NexusError::Connector(format!("sharepoint upsert timed out: {e}"))
                    })?
                    .map_err(|e| NexusError::Connector(format!("sharepoint upsert failed: {e}")))?;

                    if !response.status().is_success() {
                        let status = response.status();
                        let text = response.text().await.unwrap_or_default();
                        return Err(NexusError::Connector(format!(
                            "sharepoint upsert failed ({status}): {text}"
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
            Field::new("Title", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("42")])),
                Arc::new(StringArray::from(vec!["Task"])),
            ],
        )
        .unwrap();

        let (id, fields) = row_to_upsert(&batch, 0).unwrap();
        assert_eq!(id, Some("42".to_string()));
        assert_eq!(fields["Title"], "Task");
        assert!(fields.get("id").is_none());
    }

    #[test]
    fn missing_id_column_value_means_create() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("Title", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(StringArray::from(vec!["New task"])),
            ],
        )
        .unwrap();

        let (id, _) = row_to_upsert(&batch, 0).unwrap();
        assert_eq!(id, None);
    }
}
