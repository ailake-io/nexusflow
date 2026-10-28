use crate::config::HubspotConnectorConfig;
use arrow_array::{Array, RecordBatch, StringArray};
use async_trait::async_trait;
use nexus_core::{retry_with_backoff, CheckpointCursor, NexusError, Sink};
use serde_json::{json, Value};

const MAX_BATCH_SIZE: usize = 100;

/// Upserts via the CRM v3 Objects API's Batch Upsert endpoint
/// (`POST /crm/v3/objects/{object_type}/batch/upsert`, real/documented
/// — up to 100 inputs per request, matched by `id_property` rather
/// than HubSpot's internal record id). All columns in the batch are
/// read as strings (`StringArray`) and sent as HubSpot properties —
/// same "everything's a string on the wire" reality
/// `nexus-connector-stripe`'s source documents for reads, mirrored
/// here for writes: HubSpot's Properties API would be needed to learn
/// each property's real type, out of scope for v1. A batch failing
/// validation fails the whole request (no partial-row retry) — same
/// all-or-nothing contract `nexus-connector-rest`'s `WebhookSink`
/// documents.
///
/// No external checkpoint state: `commit_checkpoint` is a no-op.
pub struct HubspotSink {
    client: reqwest::Client,
    config: HubspotConnectorConfig,
}

impl HubspotSink {
    pub async fn connect(config: &HubspotConnectorConfig) -> Result<Self, NexusError> {
        config.validate()?;
        config.id_property_or_err()?;
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
                "hubspot sink only supports Utf8 columns — cast upstream first".to_string(),
            )
        })?;
    Ok(if arr.is_null(row) {
        None
    } else {
        Some(arr.value(row).to_string())
    })
}

fn batch_to_upsert_inputs(
    batch: &RecordBatch,
    id_property: &str,
) -> Result<Vec<Value>, NexusError> {
    let field_names: Vec<String> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    let id_col = batch.schema().index_of(id_property).map_err(|_| {
        NexusError::Schema(format!(
            "id_property column '{id_property}' not found in batch"
        ))
    })?;

    (0..batch.num_rows())
        .map(|row| {
            let id = cell_to_string(batch, row, id_col)?.ok_or_else(|| {
                NexusError::Schema(format!("row {row}: id_property column is null"))
            })?;
            let mut properties = serde_json::Map::with_capacity(field_names.len());
            for (col, name) in field_names.iter().enumerate() {
                if let Some(value) = cell_to_string(batch, row, col)? {
                    properties.insert(name.clone(), Value::String(value));
                }
            }
            Ok(json!({
                "idProperty": id_property,
                "id": id,
                "properties": properties,
            }))
        })
        .collect()
}

#[async_trait]
impl Sink for HubspotSink {
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<(), NexusError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let id_property = self.config.id_property_or_err()?.to_string();
        let inputs = batch_to_upsert_inputs(&batch, &id_property)?;
        let url = format!(
            "{}/crm/v3/objects/{}/batch/upsert",
            self.config.base_url, self.config.object_type
        );

        for chunk in inputs.chunks(MAX_BATCH_SIZE) {
            let body = json!({ "inputs": chunk });
            let client = self.client.clone();
            let url = url.clone();
            let access_token = self.config.access_token.clone();
            let timeout_seconds = self.config.timeout_seconds;
            retry_with_backoff(&self.config.retry, "hubspot batch upsert", || {
                let client = client.clone();
                let url = url.clone();
                let access_token = access_token.clone();
                let body = body.clone();
                async move {
                    let response = tokio::time::timeout(
                        std::time::Duration::from_secs(timeout_seconds),
                        client
                            .post(&url)
                            .bearer_auth(&access_token)
                            .json(&body)
                            .send(),
                    )
                    .await
                    .map_err(|e| {
                        NexusError::Connector(format!("hubspot batch upsert timed out: {e}"))
                    })?
                    .map_err(|e| {
                        NexusError::Connector(format!("hubspot batch upsert failed: {e}"))
                    })?;

                    if !response.status().is_success() {
                        let status = response.status();
                        let text = response.text().await.unwrap_or_default();
                        return Err(NexusError::Connector(format!(
                            "hubspot batch upsert failed ({status}): {text}"
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
    fn converts_batch_to_upsert_inputs() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("email", DataType::Utf8, false),
            Field::new("firstname", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a@b.com", "c@d.com"])),
                Arc::new(StringArray::from(vec![Some("Alice"), None])),
            ],
        )
        .unwrap();

        let inputs = batch_to_upsert_inputs(&batch, "email").unwrap();
        assert_eq!(inputs.len(), 2);
        assert_eq!(inputs[0]["id"], "a@b.com");
        assert_eq!(inputs[0]["idProperty"], "email");
        assert_eq!(inputs[0]["properties"]["firstname"], "Alice");
        assert!(inputs[1]["properties"].get("firstname").is_none());
    }

    #[test]
    fn rejects_missing_id_property_column() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "firstname",
            DataType::Utf8,
            true,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["Alice"]))]).unwrap();
        assert!(batch_to_upsert_inputs(&batch, "email").is_err());
    }
}
