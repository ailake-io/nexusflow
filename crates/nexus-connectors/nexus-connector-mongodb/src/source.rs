use crate::config::{MongoConnectorConfig, MongoDataType, MongoFieldSpec};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use futures::stream::{self, BoxStream, TryStreamExt};
use mongodb::bson::Document;
use mongodb::{Client, Collection};
use nexus_core::{NexusError, RecordBatchBuilder, Source};
use std::sync::Arc;

/// Bridging connector for MongoDB — converts `bson::Document` into
/// `RecordBatch` via `RecordBatchBuilder`, see ARCHITECTURE.md §2/§4.1.
/// No native partitioning: degenerate single-partition case (ARCHITECTURE.md §4).
pub struct MongoSource {
    collection: Collection<Document>,
    schema: SchemaRef,
    batch_size: usize,
}

impl MongoSource {
    pub async fn connect(config: &MongoConnectorConfig) -> Result<Self, NexusError> {
        let client = Client::with_uri_str(&config.uri)
            .await
            .map_err(|e| NexusError::Connector(format!("mongo connect failed: {e}")))?;
        let collection = client
            .database(&config.database)
            .collection::<Document>(&config.collection);

        Ok(Self {
            collection,
            schema: build_schema(&config.fields),
            batch_size: config.batch_size,
        })
    }
}

fn build_schema(fields: &[MongoFieldSpec]) -> SchemaRef {
    Arc::new(Schema::new(
        fields
            .iter()
            .map(|f| {
                let data_type = match f.data_type {
                    MongoDataType::Int64 => DataType::Int64,
                    MongoDataType::Float64 => DataType::Float64,
                    MongoDataType::Boolean => DataType::Boolean,
                    MongoDataType::Utf8 => DataType::Utf8,
                };
                Field::new(&f.name, data_type, f.nullable)
            })
            .collect::<Vec<_>>(),
    ))
}

#[async_trait]
impl Source for MongoSource {
    async fn read_batches(
        &mut self,
    ) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError> {
        let mut cursor = self
            .collection
            .find(Document::new())
            .await
            .map_err(|e| NexusError::Connector(format!("mongo find failed: {e}")))?;

        let mut batches = Vec::new();
        let mut buffer = Vec::with_capacity(self.batch_size);

        while let Some(doc) = cursor
            .try_next()
            .await
            .map_err(|e| NexusError::Connector(format!("mongo cursor error: {e}")))?
        {
            let row = serde_json::to_value(&doc)
                .map_err(|e| NexusError::Serialization(format!("bson->json failed: {e}")))?;
            buffer.push(row);

            if buffer.len() >= self.batch_size {
                batches.push(RecordBatchBuilder::from_json_rows(
                    self.schema.clone(),
                    &buffer,
                )?);
                buffer.clear();
            }
        }
        if !buffer.is_empty() {
            batches.push(RecordBatchBuilder::from_json_rows(
                self.schema.clone(),
                &buffer,
            )?);
        }

        Ok(Box::pin(stream::iter(batches.into_iter().map(Ok))))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
