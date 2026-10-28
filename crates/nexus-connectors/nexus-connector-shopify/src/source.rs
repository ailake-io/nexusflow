use crate::bulk::{download_jsonl, poll_until_complete, start_bulk_query};
use crate::config::ShopifyConnectorConfig;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use nexus_core::{with_timeout, NexusError, RecordBatchBuilder, Source};
use serde_json::Value;
use std::sync::Arc;

/// Reads via `bulkOperationRunQuery` — real, confirmed async bulk-read
/// job: submit the query, poll `currentBulkOperation` until
/// `COMPLETED`, download the result as JSONL from the URL Shopify
/// hands back. Every column is `Utf8` — same "explicit `fields`, safe
/// fallback" simplification `nexus-connector-stripe`'s config already
/// documents, since there's no schema catalog for an arbitrary bulk
/// GraphQL query's shape.
pub struct ShopifySource {
    client: reqwest::Client,
    cfg: ShopifyConnectorConfig,
    schema: SchemaRef,
}

impl ShopifySource {
    pub async fn connect(cfg: &ShopifyConnectorConfig) -> Result<Self, NexusError> {
        cfg.validate()?;
        cfg.graphql_query_or_err()?;
        if cfg.fields.is_empty() {
            return Err(NexusError::Schema("shopify source requires fields".into()));
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

    fn project(&self, object: &Value) -> Value {
        let mut out = serde_json::Map::with_capacity(self.cfg.fields.len());
        for name in &self.cfg.fields {
            let value = match object.get(name) {
                Some(Value::String(s)) => Value::String(s.clone()),
                Some(Value::Null) | None => Value::Null,
                Some(other) => Value::String(other.to_string()),
            };
            out.insert(name.clone(), value);
        }
        Value::Object(out)
    }
}

#[async_trait]
impl Source for ShopifySource {
    async fn read_batches(
        &mut self,
    ) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError> {
        let query = self.cfg.graphql_query_or_err()?.to_string();

        with_timeout(
            self.cfg.timeout_seconds,
            "shopify start bulk query",
            async { start_bulk_query(&self.client, &self.cfg, &query).await },
        )
        .await?;

        let url = poll_until_complete(&self.client, &self.cfg).await?;

        let rows = match url {
            Some(url) => {
                with_timeout(self.cfg.timeout_seconds, "shopify download jsonl", async {
                    download_jsonl(&self.client, &url).await
                })
                .await?
            }
            None => Vec::new(),
        };

        let projected: Vec<Value> = rows.iter().map(|r| self.project(r)).collect();
        let batch = RecordBatchBuilder::from_json_rows(self.schema.clone(), &projected)?;
        Ok(Box::pin(stream::iter(vec![Ok(batch)])))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
