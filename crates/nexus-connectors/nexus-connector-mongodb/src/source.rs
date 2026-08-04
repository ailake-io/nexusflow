use crate::config::{MongoConnectorConfig, MongoDataType, MongoFieldSpec};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use futures::stream::{BoxStream, Stream};
use mongodb::bson::Document;
use mongodb::{Client, Collection, Cursor};
use nexus_core::{NexusError, RecordBatchBuilder, Source};
use serde_json::Value;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

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

/// Lazy stream returned by [`MongoSource::read_batches`]. Pulls documents from
/// the MongoDB cursor one at a time and emits `RecordBatch`es as soon as a
/// batch is full, so a huge collection does not have to be materialised in
/// memory before the downstream pipeline can start (CLAUDE.md §8.1 / M2).
struct MongoReadStream {
    cursor: Cursor<Document>,
    schema: SchemaRef,
    batch_size: usize,
    buffer: Vec<Value>,
    finished: bool,
}

impl MongoReadStream {
    fn flush_batch(&mut self) -> Result<RecordBatch, NexusError> {
        let batch = RecordBatchBuilder::from_json_rows(self.schema.clone(), &self.buffer)?;
        self.buffer.clear();
        Ok(batch)
    }
}

impl Stream for MongoReadStream {
    type Item = Result<RecordBatch, NexusError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if self.buffer.len() >= self.batch_size {
                return Poll::Ready(Some(self.flush_batch()));
            }
            if self.finished {
                return if self.buffer.is_empty() {
                    Poll::Ready(None)
                } else {
                    Poll::Ready(Some(self.flush_batch()))
                };
            }

            match Pin::new(&mut self.cursor).poll_next(cx) {
                Poll::Ready(Some(Ok(doc))) => match serde_json::to_value(&doc) {
                    Ok(row) => self.buffer.push(row),
                    Err(e) => {
                        self.finished = true;
                        return Poll::Ready(Some(Err(NexusError::Serialization(format!(
                            "bson->json failed: {e}"
                        )))));
                    }
                },
                Poll::Ready(Some(Err(e))) => {
                    self.finished = true;
                    return Poll::Ready(Some(Err(NexusError::Connector(format!(
                        "mongo cursor error: {e}"
                    )))));
                }
                Poll::Ready(None) => {
                    self.finished = true;
                }
                Poll::Pending => {
                    return if self.buffer.is_empty() {
                        Poll::Pending
                    } else {
                        Poll::Ready(Some(self.flush_batch()))
                    };
                }
            }
        }
    }
}

#[async_trait]
impl Source for MongoSource {
    async fn read_batches(
        &mut self,
    ) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError> {
        let cursor = self
            .collection
            .find(Document::new())
            .await
            .map_err(|e| NexusError::Connector(format!("mongo find failed: {e}")))?;

        Ok(Box::pin(MongoReadStream {
            cursor,
            schema: self.schema.clone(),
            batch_size: self.batch_size,
            buffer: Vec::with_capacity(self.batch_size),
            finished: false,
        }))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
