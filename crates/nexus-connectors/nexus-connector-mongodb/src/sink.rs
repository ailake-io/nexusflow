use crate::config::MongoConnectorConfig;
use crate::rows::batch_to_json_rows;
use arrow_array::RecordBatch;
use async_trait::async_trait;
use futures::stream::{self, StreamExt, TryStreamExt};
use mongodb::bson::{doc, Document};
use mongodb::{Client, Collection};
use nexus_core::{CheckpointCursor, NexusError, Opcode, Sink, OPCODE_COLUMN};

/// Number of MongoDB write operations to keep in flight at once. The driver's
/// `bulk_write` API requires MongoDB 8.0+; while we target older servers, a
/// bounded concurrent fan-out replaces the previous strictly serial
/// replace_one/delete_one loop and removes the per-row network latency as the
/// dominant bottleneck.
const WRITE_CONCURRENCY: usize = 16;

/// Idempotent by construction: every row is a `replace_one` upsert keyed on
/// `primary_key`, matching the `Sink` contract in ARCHITECTURE.md §5
/// (at-least-once delivery, retry-safe writes).
pub struct MongoSink {
    collection: Collection<Document>,
    primary_key: String,
}

impl MongoSink {
    pub async fn connect(config: &MongoConnectorConfig) -> Result<Self, NexusError> {
        let client = Client::with_uri_str(&config.uri)
            .await
            .map_err(|e| NexusError::Connector(format!("mongo connect failed: {e}")))?;
        let collection = client
            .database(&config.database)
            .collection::<Document>(&config.collection);

        Ok(Self {
            collection,
            primary_key: config.primary_key.clone(),
        })
    }
}

#[async_trait]
impl Sink for MongoSink {
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<(), NexusError> {
        let rows = batch_to_json_rows(&batch)?;
        let pk = self.primary_key.clone();
        let collection = self.collection.clone();

        // Pre-convert every row so the concurrent stage works with plain
        // futures (buffer_unordered needs Stream<Item = impl Future>, not
        // Result<Future, _>).
        let mut ops = Vec::with_capacity(rows.len());
        for mut row in rows {
            let opcode = row
                .get(OPCODE_COLUMN)
                .and_then(serde_json::Value::as_str)
                .and_then(Opcode::from_letter);
            if let Some(obj) = row.as_object_mut() {
                obj.remove(OPCODE_COLUMN);
            }

            let key_value = row.get(&pk).cloned().ok_or_else(|| {
                NexusError::Schema(format!("row missing primary key field '{pk}'"))
            })?;
            let key_bson = mongodb::bson::to_bson(&key_value)
                .map_err(|e| NexusError::Serialization(format!("json->bson failed: {e}")))?;
            let document = mongodb::bson::to_document(&row)
                .map_err(|e| NexusError::Serialization(format!("json->bson failed: {e}")))?;

            ops.push((opcode, key_bson, document));
        }

        stream::iter(ops)
            .map(|(opcode, key_bson, document)| {
                let collection = collection.clone();
                let pk = pk.clone();
                async move {
                    if opcode == Some(Opcode::Delete) {
                        collection
                            .delete_one(doc! { &pk: key_bson })
                            .await
                            .map_err(|e| {
                                NexusError::Connector(format!("mongo delete failed: {e}"))
                            })?;
                    } else {
                        collection
                            .replace_one(doc! { &pk: key_bson }, document)
                            .upsert(true)
                            .await
                            .map_err(|e| {
                                NexusError::Connector(format!("mongo upsert failed: {e}"))
                            })?;
                    }
                    Ok::<(), NexusError>(())
                }
            })
            .buffer_unordered(WRITE_CONCURRENCY)
            .try_collect::<()>()
            .await?;

        Ok(())
    }

    async fn commit_checkpoint(&mut self, _cursor: CheckpointCursor) -> Result<(), NexusError> {
        // Persisting the cursor is nexus-server's job — see ARCHITECTURE.md
        // §5. This connector's only idempotency obligation is the upsert above.
        Ok(())
    }
}
