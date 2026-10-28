use crate::bulk::{create_job, fetch_results, poll_until_complete};
use crate::config::MetaAdsConnectorConfig;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use nexus_core::{with_timeout, NexusError, RecordBatchBuilder, Source};
use serde_json::Value;
use std::sync::Arc;

/// Reads via the Marketing API's async Insights job flow — confirmed
/// real pattern: `POST .../insights?async=true` → `report_run_id` →
/// poll `GET /{report_run_id}` until `async_status = "Job Completed"`
/// → `GET /{report_run_id}/insights` (paginated). Every column is
/// `Utf8` — Meta Insights values (including numeric metrics) come
/// back as JSON strings, same fallback GA4/Google Ads already use in
/// this workspace.
pub struct MetaAdsSource {
    client: reqwest::Client,
    cfg: MetaAdsConnectorConfig,
    schema: SchemaRef,
}

impl MetaAdsSource {
    pub async fn connect(cfg: &MetaAdsConnectorConfig) -> Result<Self, NexusError> {
        if cfg.fields.is_empty() {
            return Err(NexusError::Schema(
                "meta-ads: fields must not be empty".into(),
            ));
        }
        let fields: Vec<Field> = cfg
            .fields
            .iter()
            .chain(cfg.breakdowns.iter())
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
impl Source for MetaAdsSource {
    async fn read_batches(
        &mut self,
    ) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError> {
        let report_run_id = with_timeout(self.cfg.timeout_seconds, "meta-ads create job", async {
            create_job(&self.client, &self.cfg).await
        })
        .await?;

        poll_until_complete(&self.client, &self.cfg, &report_run_id).await?;

        let rows = with_timeout(self.cfg.timeout_seconds, "meta-ads fetch results", async {
            fetch_results(&self.client, &self.cfg, &report_run_id).await
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
