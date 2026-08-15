use crate::config::{MongoCdcConfig, MongoDataType, MongoFieldSpec};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use futures::stream::{BoxStream, Stream};
use mongodb::bson::Document;
use mongodb::change_stream::event::{ChangeStreamEvent, OperationType};
use mongodb::change_stream::ChangeStream;
use mongodb::options::FullDocumentType;
use mongodb::{Client, Collection};
use nexus_core::{with_timeout, NexusError, RecordBatchBuilder, Source, OPCODE_COLUMN};
use serde_json::Value;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::time::Sleep;

/// Native CDC source for MongoDB via Change Streams — no Debezium/Kafka in
/// front (`docs/ENTERPRISE_LICENSING.md` is unrelated; see
/// `ARCHITECTURE.md §7` for the CDC scope this belongs to). Requires MongoDB
/// running as a replica set (even a single-node one); Change Streams don't
/// work against a standalone server.
pub struct MongoCdcSource {
    collection: Collection<Document>,
    schema: SchemaRef,
    resume_token: Option<String>,
    batch_size: usize,
    timeout_seconds: u64,
    max_batch_events: u64,
}

impl MongoCdcSource {
    pub async fn connect(config: &MongoCdcConfig) -> Result<Self, NexusError> {
        let client = with_timeout(config.timeout_seconds, "mongo connect", async {
            Client::with_uri_str(&config.connection_string())
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
            resume_token: config.resume_token.clone(),
            batch_size: config.batch_size,
            timeout_seconds: config.timeout_seconds,
            max_batch_events: config.max_batch_events,
        })
    }
}

fn build_schema(fields: &[MongoFieldSpec]) -> SchemaRef {
    let mut arrow_fields: Vec<Field> = fields
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
        .collect();
    arrow_fields.push(Field::new(OPCODE_COLUMN, DataType::Utf8, false));
    Arc::new(Schema::new(arrow_fields))
}

/// Lazy stream returned by [`MongoCdcSource::read_batches`] — same
/// idle-timeout-driven buffering as `MongoReadStream` (`source.rs`), just
/// pulling from a Change Stream instead of a plain find cursor, and tagging
/// each row with `__opcode` before it goes through
/// `RecordBatchBuilder::from_json_rows`.
struct MongoCdcStream {
    change_stream: ChangeStream<ChangeStreamEvent<Document>>,
    schema: SchemaRef,
    batch_size: usize,
    buffer: Vec<Value>,
    finished: bool,
    idle_timeout: Duration,
    deadline: Pin<Box<Sleep>>,
    max_batch_events: u64,
    events_seen: u64,
}

impl MongoCdcStream {
    fn flush_batch(&mut self) -> Result<RecordBatch, NexusError> {
        let batch = RecordBatchBuilder::from_json_rows(self.schema.clone(), &self.buffer)?;
        self.buffer.clear();
        Ok(batch)
    }

    /// `None` for a change-stream event outside the I/U/D contract (schema
    /// changes, collection drop, invalidate, ...) — the caller skips it
    /// rather than treating it as a row.
    fn event_to_row(event: ChangeStreamEvent<Document>) -> Option<Value> {
        let opcode = match event.operation_type {
            OperationType::Insert => "I",
            OperationType::Update | OperationType::Replace => "U",
            OperationType::Delete => "D",
            _ => return None,
        };
        // Deletes carry no `full_document` — only `document_key` (usually
        // just `_id`), so a delete row will have every other configured
        // field come through as null. That's a real limitation of Change
        // Streams (not something `full_document_before_change` fixes
        // without extra server-side pre-image config), documented on
        // `MongoCdcConfig`.
        //
        // An update's `full_document` (`UpdateLookup`) is fetched by a
        // separate query *at decode time*, not captured atomically with the
        // update itself — MongoDB can legitimately return `null` for it
        // (observed even without a racing delete: the driver's own lookup
        // doesn't guarantee it always finds a fresh copy, e.g. read
        // preference/timing on the underlying query). Falling back to
        // `document_key` here means the row still carries its identity and
        // the `U` opcode instead of vanishing outright; a downstream sink
        // doing an upsert-by-key at least knows *that* row changed, even
        // without the field values from this particular event.
        let doc = if opcode == "D" {
            event.document_key
        } else {
            event.full_document.or(event.document_key)
        }?;
        let mut row = serde_json::to_value(doc).ok()?;
        if let Value::Object(map) = &mut row {
            map.insert(OPCODE_COLUMN.to_string(), Value::String(opcode.to_string()));
        }
        Some(row)
    }
}

impl Stream for MongoCdcStream {
    type Item = Result<RecordBatch, NexusError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if self.buffer.len() >= self.batch_size {
                return Poll::Ready(Some(self.flush_batch()));
            }
            if self.finished || self.events_seen >= self.max_batch_events {
                return if self.buffer.is_empty() {
                    Poll::Ready(None)
                } else {
                    Poll::Ready(Some(self.flush_batch()))
                };
            }

            match Pin::new(&mut self.change_stream).poll_next(cx) {
                Poll::Ready(Some(Ok(event))) => {
                    let deadline = tokio::time::Instant::now() + self.idle_timeout;
                    self.deadline.as_mut().reset(deadline);
                    if let Some(row) = Self::event_to_row(event) {
                        self.buffer.push(row);
                        self.events_seen += 1;
                    }
                }
                Poll::Ready(Some(Err(e))) => {
                    self.finished = true;
                    return Poll::Ready(Some(Err(NexusError::Connector(format!(
                        "mongo change stream error: {e}"
                    )))));
                }
                Poll::Ready(None) => {
                    self.finished = true;
                }
                Poll::Pending => {
                    // Unlike a batch source's idle timeout (a genuine stall
                    // to error out on), going idle here just means no writes
                    // are happening right now — the normal steady state of a
                    // live change stream. Flush whatever's buffered and keep
                    // waiting; only a real `Err`/`None` from the underlying
                    // stream above ends this source.
                    if self.deadline.as_mut().poll(cx).is_ready() {
                        let deadline = tokio::time::Instant::now() + self.idle_timeout;
                        self.deadline.as_mut().reset(deadline);
                        if !self.buffer.is_empty() {
                            return Poll::Ready(Some(self.flush_batch()));
                        }
                    }
                    return Poll::Pending;
                }
            }
        }
    }
}

#[async_trait]
impl Source for MongoCdcSource {
    async fn read_batches(
        &mut self,
    ) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError> {
        let idle_timeout = Duration::from_secs(self.timeout_seconds);
        let resume_token: Option<mongodb::change_stream::event::ResumeToken> = match &self
            .resume_token
        {
            Some(s) => Some(
                serde_json::from_str(s)
                    .map_err(|e| NexusError::Schema(format!("invalid mongo resume_token: {e}")))?,
            ),
            None => None,
        };
        let change_stream = with_timeout(self.timeout_seconds, "mongo watch", async {
            let mut builder = self
                .collection
                .watch()
                .full_document(FullDocumentType::UpdateLookup);
            if let Some(token) = resume_token {
                builder = builder.resume_after(token);
            }
            builder
                .await
                .map_err(|e| NexusError::Connector(format!("mongo watch failed: {e}")))
        })
        .await?;

        Ok(Box::pin(MongoCdcStream {
            change_stream,
            schema: self.schema.clone(),
            batch_size: self.batch_size,
            buffer: Vec::with_capacity(self.batch_size),
            finished: false,
            idle_timeout,
            deadline: Box::pin(tokio::time::sleep(idle_timeout)),
            max_batch_events: self.max_batch_events,
            events_seen: 0,
        }))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
