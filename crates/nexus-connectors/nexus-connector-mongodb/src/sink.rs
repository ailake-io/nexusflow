use crate::config::MongoConnectorConfig;
use crate::rows::batch_to_documents;
use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::DataType;
use async_trait::async_trait;
use futures::stream::{self, StreamExt, TryStreamExt};
use mongodb::bson::{doc, Bson, Document};
use mongodb::options::{DeleteOneModel, ReplaceOneModel, WriteModel};
use mongodb::{Client, Collection, Namespace};
use nexus_core::{with_timeout, CheckpointCursor, NexusError, Opcode, Sink, OPCODE_COLUMN};

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
    client: Client,
    namespace: Namespace,
    primary_key: String,
    use_bulk_write: bool,
    timeout_seconds: u64,
}

impl MongoSink {
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
        let namespace = collection.namespace();

        let use_bulk_write =
            detect_bulk_write_support(&client, &config.database, config.timeout_seconds).await?;

        Ok(Self {
            client,
            namespace,
            primary_key: config.primary_key.clone(),
            use_bulk_write,
            timeout_seconds: config.timeout_seconds,
        })
    }
}

async fn detect_bulk_write_support(
    client: &Client,
    db: &str,
    timeout_seconds: u64,
) -> Result<bool, NexusError> {
    let build_info: Document = with_timeout(timeout_seconds, "mongo buildInfo", async {
        client
            .database(db)
            .run_command(doc! { "buildInfo": 1 })
            .await
            .map_err(|e| NexusError::Connector(format!("mongo buildInfo failed: {e}")))
    })
    .await?;
    let major = build_info
        .get_array("versionArray")
        .ok()
        .and_then(|arr| arr.first())
        .and_then(|v| v.as_i32())
        .unwrap_or(0);
    Ok(major >= 8)
}

fn extract_pk_bson(batch: &RecordBatch, pk: &str) -> Result<Vec<Bson>, NexusError> {
    let idx = batch
        .schema()
        .index_of(pk)
        .map_err(|_| NexusError::Schema(format!("primary key column '{pk}' not found")))?;
    let column = batch.column(idx);
    match column.data_type() {
        DataType::Int64 => {
            let arr = column
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| NexusError::Schema("primary key column is not Int64".into()))?;
            Ok((0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        Bson::Null
                    } else {
                        Bson::Int64(arr.value(i))
                    }
                })
                .collect())
        }
        DataType::Utf8 => {
            let arr = column
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| NexusError::Schema("primary key column is not Utf8".into()))?;
            Ok((0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        Bson::Null
                    } else {
                        Bson::String(arr.value(i).to_string())
                    }
                })
                .collect())
        }
        other => Err(NexusError::Schema(format!(
            "unsupported primary key type '{other:?}' for MongoDB"
        ))),
    }
}

fn extract_opcodes(batch: &RecordBatch) -> Vec<Option<Opcode>> {
    let Ok(idx) = batch.schema().index_of(OPCODE_COLUMN) else {
        return vec![None; batch.num_rows()];
    };
    let column = batch.column(idx);
    let Some(arr) = column.as_any().downcast_ref::<StringArray>() else {
        return vec![None; batch.num_rows()];
    };
    (0..arr.len())
        .map(|i| {
            if arr.is_null(i) {
                None
            } else {
                Opcode::from_letter(arr.value(i))
            }
        })
        .collect()
}

#[async_trait]
impl Sink for MongoSink {
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<(), NexusError> {
        let opcodes = extract_opcodes(&batch);
        let pk_values = extract_pk_bson(&batch, &self.primary_key)?;
        let documents = batch_to_documents(&batch)?;
        let pk = self.primary_key.clone();
        let namespace = self.namespace.clone();

        // Strip __opcode from documents (it is not part of the user's data)
        // and pair each document with its opcode and primary key.
        let mut ops = Vec::with_capacity(documents.len());
        for (i, mut document) in documents.into_iter().enumerate() {
            document.remove(OPCODE_COLUMN);
            let key_bson = pk_values.get(i).cloned().ok_or_else(|| {
                NexusError::Schema(format!("row {} missing primary key value", i))
            })?;
            ops.push((opcodes[i], key_bson, document));
        }

        if self.use_bulk_write {
            let models: Vec<WriteModel> = ops
                .into_iter()
                .map(|(opcode, key_bson, document)| {
                    if opcode == Some(Opcode::Delete) {
                        WriteModel::DeleteOne(
                            DeleteOneModel::builder()
                                .namespace(namespace.clone())
                                .filter(doc! { &pk: key_bson })
                                .build(),
                        )
                    } else {
                        WriteModel::ReplaceOne(
                            ReplaceOneModel::builder()
                                .namespace(namespace.clone())
                                .filter(doc! { &pk: key_bson })
                                .replacement(document)
                                .upsert(true)
                                .build(),
                        )
                    }
                })
                .collect();

            with_timeout(self.timeout_seconds, "mongo bulk_write", async {
                self.client
                    .bulk_write(models)
                    .ordered(false)
                    .await
                    .map_err(|e| NexusError::Connector(format!("mongo bulk_write failed: {e}")))
            })
            .await?;
        } else {
            let collection: Collection<Document> = self
                .client
                .database(&namespace.db)
                .collection(&namespace.coll);
            let timeout_seconds = self.timeout_seconds;
            stream::iter(ops)
                .map(|(opcode, key_bson, document)| {
                    let collection = collection.clone();
                    let pk = pk.clone();
                    async move {
                        if opcode == Some(Opcode::Delete) {
                            with_timeout(timeout_seconds, "mongo delete_one", async {
                                collection
                                    .delete_one(doc! { &pk: key_bson })
                                    .await
                                    .map_err(|e| {
                                        NexusError::Connector(format!("mongo delete failed: {e}"))
                                    })
                            })
                            .await?;
                        } else {
                            with_timeout(timeout_seconds, "mongo replace_one", async {
                                collection
                                    .replace_one(doc! { &pk: key_bson }, document)
                                    .upsert(true)
                                    .await
                                    .map_err(|e| {
                                        NexusError::Connector(format!("mongo upsert failed: {e}"))
                                    })
                            })
                            .await?;
                        }
                        Ok::<(), NexusError>(())
                    }
                })
                .buffer_unordered(WRITE_CONCURRENCY)
                .try_collect::<()>()
                .await?;
        }

        Ok(())
    }

    async fn commit_checkpoint(&mut self, _cursor: CheckpointCursor) -> Result<(), NexusError> {
        // Persisting the cursor is nexus-server's job — see ARCHITECTURE.md
        // §5. This connector's only idempotency obligation is the upsert above.
        Ok(())
    }
}
