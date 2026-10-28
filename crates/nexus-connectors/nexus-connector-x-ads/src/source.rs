use crate::bulk::{create_job, fetch_results, poll_until_complete};
use crate::config::XAdsConnectorConfig;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use nexus_core::{with_timeout, NexusError, RecordBatchBuilder, Source};
use serde_json::Value;
use std::sync::Arc;

/// Reads via the X Ads API's async stats job flow — confirmed real
/// pattern: `POST .../stats/jobs/accounts/:id` → job id → poll
/// `GET .../stats/jobs/accounts/:id?job_ids=...` until `status =
/// "SUCCESS"` → download+gunzip the result file (`src/bulk.rs`). Every
/// column is `Utf8`, same convention `nexus-connector-meta-ads`/
/// `nexus-connector-google-ads`/`nexus-connector-ga4` already use for
/// report APIs in this workspace.
pub struct XAdsSource {
    client: reqwest::Client,
    cfg: XAdsConnectorConfig,
    schema: SchemaRef,
}

impl XAdsSource {
    pub async fn connect(cfg: &XAdsConnectorConfig) -> Result<Self, NexusError> {
        if cfg.fields.is_empty() {
            return Err(NexusError::Schema("x-ads: fields must not be empty".into()));
        }
        if cfg.entity_ids.is_empty() || cfg.entity_ids.len() > 20 {
            return Err(NexusError::Schema(
                "x-ads: entity_ids must have between 1 and 20 entries".into(),
            ));
        }
        let fields: Vec<Field> = cfg
            .fields
            .iter()
            .map(|f| Field::new(f, DataType::Utf8, true))
            .collect();
        Ok(Self {
            client: reqwest::Client::new(),
            cfg: cfg.clone(),
            schema: Arc::new(Schema::new(fields)),
        })
    }
}

#[async_trait]
impl Source for XAdsSource {
    async fn read_batches(
        &mut self,
    ) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError> {
        let job_id = with_timeout(self.cfg.timeout_seconds, "x-ads create job", async {
            create_job(&self.client, &self.cfg).await
        })
        .await?;

        let download_url = with_timeout(self.cfg.timeout_seconds, "x-ads poll job", async {
            poll_until_complete(&self.client, &self.cfg, &job_id).await
        })
        .await?;

        let rows = with_timeout(self.cfg.timeout_seconds, "x-ads download results", async {
            fetch_results(&self.client, &download_url).await
        })
        .await?;

        let schema = self.schema.clone();
        let column_names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
        let projected: Vec<Value> = rows
            .iter()
            .map(|row| {
                let mut object = serde_json::Map::with_capacity(column_names.len());
                for name in &column_names {
                    let value = match row.get(name) {
                        Some(Value::String(s)) => Value::String(s.clone()),
                        Some(Value::Null) | None => Value::Null,
                        Some(other) => Value::String(other.to_string()),
                    };
                    object.insert(name.clone(), value);
                }
                Value::Object(object)
            })
            .collect();

        let batch = RecordBatchBuilder::from_json_rows(schema, &projected)?;
        Ok(Box::pin(stream::iter(vec![Ok(batch)])))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
