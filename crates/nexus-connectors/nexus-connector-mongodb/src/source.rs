use crate::config::{MongoConnectorConfig, MongoDataType, MongoFieldSpec};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use futures::stream::{BoxStream, Stream};
use mongodb::bson::Document;
use mongodb::{Client, Collection, Cursor};
use nexus_core::{with_timeout, NexusError, RecordBatchBuilder, Source};
use serde_json::Value;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::time::Sleep;

/// Bridging connector for MongoDB — converts `bson::Document` into
/// `RecordBatch` via `RecordBatchBuilder`, see ARCHITECTURE.md §2/§4.1.
/// No native partitioning: degenerate single-partition case (ARCHITECTURE.md §4).
pub struct MongoSource {
    collection: Collection<Document>,
    schema: SchemaRef,
    batch_size: usize,
    timeout_seconds: u64,
}

impl MongoSource {
    pub async fn connect(config: &MongoConnectorConfig) -> Result<Self, NexusError> {
        let client = with_timeout(config.timeout_seconds, "mongo connect", async {
            Client::with_uri_str(&config.uri)
                .await
                .map_err(|e| NexusError::Connector(format!("mongo connect failed: {e}")))
        })
        .await?;
        let collection = client
            .database(&config.database)
            .collection::<Document>(&config.collection);

        Ok(Self {
            collection,
            schema: build_schema(&config.fields),
            batch_size: config.batch_size,
            timeout_seconds: config.timeout_seconds,
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
    idle_timeout: Duration,
    // Reset every time the cursor yields something (Ready(Some(..)) or
    // Ready(None)) — fires only when the cursor stays Pending longer than
    // `idle_timeout` (a wedged connection), same idea as the Kafka source's
    // `poll_timeout` idle cutoff (C15).
    deadline: Pin<Box<Sleep>>,
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
                Poll::Ready(Some(Ok(doc))) => {
                    let deadline = tokio::time::Instant::now() + self.idle_timeout;
                    self.deadline.as_mut().reset(deadline);
                    match serde_json::to_value(&doc) {
                        Ok(row) => self.buffer.push(row),
                        Err(e) => {
                            self.finished = true;
                            return Poll::Ready(Some(Err(NexusError::Serialization(format!(
                                "bson->json failed: {e}"
                            )))));
                        }
                    }
                }
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
                    if self.deadline.as_mut().poll(cx).is_ready() {
                        self.finished = true;
                        return Poll::Ready(Some(Err(NexusError::Connector(format!(
                            "mongo cursor stalled for more than {}s",
                            self.idle_timeout.as_secs()
                        )))));
                    }
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
        let idle_timeout = Duration::from_secs(self.timeout_seconds);
        let cursor = with_timeout(self.timeout_seconds, "mongo find", async {
            self.collection
                .find(Document::new())
                .await
                .map_err(|e| NexusError::Connector(format!("mongo find failed: {e}")))
        })
        .await?;

        Ok(Box::pin(MongoReadStream {
            cursor,
            schema: self.schema.clone(),
            batch_size: self.batch_size,
            buffer: Vec::with_capacity(self.batch_size),
            finished: false,
            idle_timeout,
            deadline: Box::pin(tokio::time::sleep(idle_timeout)),
        }))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
