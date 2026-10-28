use crate::config::ZendeskConnectorConfig;
use arrow_array::{Array, RecordBatch, StringArray};
use async_trait::async_trait;
use nexus_core::{retry_with_backoff, CheckpointCursor, NexusError, Sink};
use serde_json::{json, Value};

/// Creates or updates one record per row — `PUT
/// /api/v2/{resource}/{id}.json` when the row's `id` column is
/// present and non-null, `POST /api/v2/{resource}.json` (create)
/// otherwise. Zendesk's real bulk endpoints
/// (`/api/v2/tickets/create_many.json` etc.) are async — they return
/// a job id you must poll for completion, not an immediate result —
/// so per-record synchronous calls trade throughput for not needing a
/// job-polling loop, a documented v1 simplification (same "real
/// mechanism exists but adds a state machine, deferred" precedent as
/// `nexus-connector-kinesis`'s in-memory cursor).
///
/// All columns are read as strings (`StringArray`) and sent as
/// resource fields — same "everything's a string on the wire"
/// simplification `nexus-connector-hubspot`'s sink documents.
///
/// No external checkpoint state: `commit_checkpoint` is a no-op.
pub struct ZendeskSink {
    client: reqwest::Client,
    config: ZendeskConnectorConfig,
}

impl ZendeskSink {
    pub async fn connect(config: &ZendeskConnectorConfig) -> Result<Self, NexusError> {
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
                "zendesk sink only supports Utf8 columns — cast upstream first".to_string(),
            )
        })?;
    Ok(if arr.is_null(row) {
        None
    } else {
        Some(arr.value(row).to_string())
    })
}

/// Returns (id, fields-body) for one row — `id` comes from an `id`
/// column when present and non-null, `fields` excludes it (Zendesk
/// takes the id from the URL path on update, not the body).
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
impl Sink for ZendeskSink {
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<(), NexusError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let plural = self.config.resource.plural();
        let singular = self.config.resource.singular();

        for row in 0..batch.num_rows() {
            let (id, fields) = row_to_upsert(&batch, row)?;
            let body = json!({ singular: fields });
            let url = match &id {
                Some(id) => format!("{}/api/v2/{plural}/{id}.json", self.config.base_url),
                None => format!("{}/api/v2/{plural}.json", self.config.base_url),
            };
            let is_update = id.is_some();

            let client = self.client.clone();
            let username = self.config.basic_auth_username();
            let api_token = self.config.api_token.clone();
            let timeout_seconds = self.config.timeout_seconds;
            retry_with_backoff(&self.config.retry, "zendesk upsert", || {
                let client = client.clone();
                let url = url.clone();
                let username = username.clone();
                let api_token = api_token.clone();
                let body = body.clone();
                async move {
                    let request = if is_update {
                        client.put(&url)
                    } else {
                        client.post(&url)
                    };
                    let response = tokio::time::timeout(
                        std::time::Duration::from_secs(timeout_seconds),
                        request
                            .basic_auth(username, Some(api_token))
                            .json(&body)
                            .send(),
                    )
                    .await
                    .map_err(|e| NexusError::Connector(format!("zendesk upsert timed out: {e}")))?
                    .map_err(|e| NexusError::Connector(format!("zendesk upsert failed: {e}")))?;

                    if !response.status().is_success() {
                        let status = response.status();
                        let text = response.text().await.unwrap_or_default();
                        return Err(NexusError::Connector(format!(
                            "zendesk upsert failed ({status}): {text}"
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
            Field::new("subject", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("42")])),
                Arc::new(StringArray::from(vec!["Help"])),
            ],
        )
        .unwrap();

        let (id, fields) = row_to_upsert(&batch, 0).unwrap();
        assert_eq!(id, Some("42".to_string()));
        assert_eq!(fields["subject"], "Help");
        assert!(fields.get("id").is_none());
    }

    #[test]
    fn missing_id_column_value_means_create() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("subject", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(StringArray::from(vec!["Help"])),
            ],
        )
        .unwrap();

        let (id, _) = row_to_upsert(&batch, 0).unwrap();
        assert_eq!(id, None);
    }
}
