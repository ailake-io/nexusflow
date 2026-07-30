use crate::config::MongoConnectorConfig;
use crate::rows::batch_to_json_rows;
use arrow_array::RecordBatch;
use async_trait::async_trait;
use mongodb::bson::{doc, Document};
use mongodb::{Client, Collection};
use nexus_core::{CheckpointCursor, NexusError, Sink};

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

        for row in rows {
            let document = mongodb::bson::to_document(&row)
                .map_err(|e| NexusError::Serialization(format!("json->bson failed: {e}")))?;
            let key_value = document.get(&self.primary_key).cloned().ok_or_else(|| {
                NexusError::Schema(format!(
                    "row missing primary key field '{}'",
                    self.primary_key
                ))
            })?;

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
