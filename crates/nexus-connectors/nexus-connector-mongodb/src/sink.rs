use crate::config::MongoConnectorConfig;
use crate::rows::batch_to_json_rows;
use arrow_array::RecordBatch;
use async_trait::async_trait;
use mongodb::bson::{doc, Document};
use mongodb::{Client, Collection};
use nexus_core::{CheckpointCursor, NexusError, Opcode, Sink, OPCODE_COLUMN};

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

        for mut row in rows {
            // CDC rows carry an `__opcode` field (ARCHITECTURE.md §5) — a `D`
            // row must be a real delete, not silently upserted. Stripped
            // either way so it never ends up persisted in the document.
            let opcode = row
                .get(OPCODE_COLUMN)
                .and_then(serde_json::Value::as_str)
                .and_then(Opcode::from_letter);
            if let Some(obj) = row.as_object_mut() {
                obj.remove(OPCODE_COLUMN);
            }

            let key_value = row.get(&self.primary_key).cloned().ok_or_else(|| {
                NexusError::Schema(format!(
                    "row missing primary key field '{}'",
                    self.primary_key
                ))
            })?;
            let key_value = mongodb::bson::to_bson(&key_value)
                .map_err(|e| NexusError::Serialization(format!("json->bson failed: {e}")))?;

            if opcode == Some(Opcode::Delete) {
                self.collection
                    .delete_one(doc! { &self.primary_key: key_value })
                    .await
                    .map_err(|e| NexusError::Connector(format!("mongo delete failed: {e}")))?;
                continue;
            }

            let document = mongodb::bson::to_document(&row)
                .map_err(|e| NexusError::Serialization(format!("json->bson failed: {e}")))?;
            self.collection
                .replace_one(doc! { &self.primary_key: key_value }, document)
                .upsert(true)
                .await
                .map_err(|e| NexusError::Connector(format!("mongo upsert failed: {e}")))?;
        }

        Ok(())
    }

    async fn commit_checkpoint(&mut self, _cursor: CheckpointCursor) -> Result<(), NexusError> {
        // Persisting the cursor is nexus-server's job — see ARCHITECTURE.md
        // §5. This connector's only idempotency obligation is the upsert above.
        Ok(())
    }
}
