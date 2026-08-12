use crate::config::MilvusConnectorConfig;
use arrow_array::{
    Array, BooleanArray, FixedSizeListArray, Float32Array, Float64Array, Int64Array, RecordBatch,
    StringArray,
};
use arrow_schema::DataType;
use async_trait::async_trait;
use milvus::client::Client;
use milvus::data::FieldColumn;
use milvus::schema::FieldSchema;
use nexus_core::{
    project_column, split_by_opcode, validate_identifier, with_timeout, CheckpointCursor,
    NexusError, Sink,
};

/// AI Lakehouse sink #4. The `milvus-sdk-rust` crate (0.1.0) has no native
/// upsert, so an upsert is implemented as delete-then-insert on the primary
/// key — same idempotency contract as every other `Sink` (ARCHITECTURE.md
/// §5). Collection must already exist (schema comes from Milvus itself via
/// `describe_collection`, not from local config) — see
/// IMPLEMENTATION_PLAN.md Marco 5.
pub struct MilvusSink {
    client: Client,
    collection: String,
    primary_key: String,
    embedding_column: String,
    timeout_seconds: u64,
}

impl MilvusSink {
    pub async fn connect(cfg: &MilvusConnectorConfig) -> Result<Self, NexusError> {
        // `primary_key` is attacker-controlled (comes from the pipeline spec),
        // so validate it before splicing it into any expression string.
        validate_identifier(&cfg.primary_key)?;

        let url = cfg.url();
        let client = with_timeout(cfg.timeout_seconds, "milvus connect", async {
            Client::new(url)
                .await
                .map_err(|e| NexusError::Connector(format!("milvus connect failed: {e}")))
        })
        .await?;
        Ok(Self {
            client,
            collection: cfg.collection_name(),
            primary_key: cfg.primary_key.clone(),
            embedding_column: cfg.embedding_column.clone(),
            timeout_seconds: cfg.timeout_seconds,
        })
    }

    fn field_column(
        &self,
        field: &FieldSchema,
        batch: &RecordBatch,
    ) -> Result<FieldColumn, NexusError> {
        let idx = batch.schema().index_of(&field.name).map_err(|_| {
            NexusError::Schema(format!("column '{}' not found in batch", field.name))
        })?;
        let column = batch.column(idx);

        if field.name == self.embedding_column {
            let list = column
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .ok_or_else(|| {
                    NexusError::Schema(format!("column '{}' is not a FixedSizeList", field.name))
                })?;
            let mut flat = Vec::with_capacity(list.len() * field.dim as usize);
            for row in 0..list.len() {
                let values = list.value(row);
                let floats = values
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .ok_or_else(|| {
                        NexusError::Schema(format!("column '{}' items are not Float32", field.name))
                    })?;
                flat.extend_from_slice(floats.values());
            }
            return Ok(FieldColumn::new(field, flat));
        }

        Ok(match batch.schema().field(idx).data_type() {
            DataType::Int64 => {
                let arr = column
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| {
                        NexusError::Schema(format!(
                            "column '{}' has unexpected array type",
                            field.name
                        ))
                    })?;
                FieldColumn::new(field, arr.values().to_vec())
            }
            DataType::Float64 => {
                let arr = column
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| {
                        NexusError::Schema(format!(
                            "column '{}' has unexpected array type",
                            field.name
                        ))
                    })?;
                FieldColumn::new(field, arr.values().to_vec())
            }
            DataType::Boolean => {
                let arr = column
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| {
                        NexusError::Schema(format!(
                            "column '{}' has unexpected array type",
                            field.name
                        ))
                    })?;
                FieldColumn::new(
                    field,
                    (0..arr.len()).map(|i| arr.value(i)).collect::<Vec<bool>>(),
                )
            }
            DataType::Utf8 => {
                let arr = column
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        NexusError::Schema(format!(
                            "column '{}' has unexpected array type",
                            field.name
                        ))
                    })?;
                FieldColumn::new(
                    field,
                    (0..arr.len())
                        .map(|i| arr.value(i).to_string())
                        .collect::<Vec<String>>(),
                )
            }
            other => {
                return Err(NexusError::Schema(format!(
                    "unsupported data type for column '{}': {other:?}",
                    field.name
                )))
            }
        })
    }

    async fn upsert(&self, batch: &RecordBatch) -> Result<(), NexusError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        // No native upsert in this SDK — delete any existing rows for these
        // primary keys first so re-inserting doesn't create duplicates.
        self.delete(batch).await?;

        let milvus_collection =
            with_timeout(self.timeout_seconds, "milvus get_collection", async {
                self.client
                    .get_collection(&self.collection)
                    .await
                    .map_err(|e| {
                        NexusError::Connector(format!("milvus get_collection failed: {e}"))
                    })
            })
            .await?;
        let field_columns = batch
            .schema()
            .fields()
            .iter()
            .map(|f| {
                let field_schema =
                    milvus_collection
                        .schema()
                        .get_field(f.name())
                        .ok_or_else(|| {
                            NexusError::Schema(format!(
                                "column '{}' not present in collection schema",
                                f.name()
                            ))
                        })?;
                self.field_column(field_schema, batch)
            })
            .collect::<Result<Vec<_>, NexusError>>()?;

        // No `.flush()` here on purpose: Milvus rate-limits flush requests
        // aggressively by default (collection-scope, ~0.1/s), and it's a
        // segment-durability operation, not a read-visibility one — the SDK
        // already tracks a per-collection session timestamp from `insert`'s
        // result and uses it as the query guarantee timestamp, so writes are
        // visible to reads through the same `Collection` handle without it.
        with_timeout(self.timeout_seconds, "milvus insert", async {
            milvus_collection
                .insert(field_columns, None)
                .await
                .map_err(|e| NexusError::Connector(format!("milvus insert failed: {e}")))
        })
        .await?;
        Ok(())
    }

    async fn delete(&self, batch: &RecordBatch) -> Result<(), NexusError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let keys = project_column(batch, &self.primary_key)?;
        let ids = keys
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                NexusError::Schema(format!(
                    "primary key column '{}' must be Int64",
                    self.primary_key
                ))
            })?;
        let id_list = (0..ids.len())
            .map(|i| ids.value(i).to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let expr = format!("{} in [{id_list}]", self.primary_key);

        let milvus_collection =
            with_timeout(self.timeout_seconds, "milvus get_collection", async {
                self.client
                    .get_collection(&self.collection)
                    .await
                    .map_err(|e| {
                        NexusError::Connector(format!("milvus get_collection failed: {e}"))
                    })
            })
            .await?;
        with_timeout(self.timeout_seconds, "milvus delete", async {
            milvus_collection
                .delete(&expr, None)
                .await
                .map_err(|e| NexusError::Connector(format!("milvus delete failed: {e}")))
        })
        .await?;
        Ok(())
    }
}

#[async_trait]
impl Sink for MilvusSink {
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<(), NexusError> {
        // CDC batches carry an `__opcode` column (ARCHITECTURE.md §5) — split
        // it so deletes are issued as real deletes instead of being silently
        // upserted. Plain (non-CDC) batches take the unchanged single
        // upsert (delete-then-insert) path.
        match split_by_opcode(&batch)? {
            None => self.upsert(&batch).await,
            Some(split) => {
                self.upsert(&split.upserts).await?;
                self.delete(&split.deletes).await?;
                Ok(())
            }
        }
    }

    async fn commit_checkpoint(&mut self, _cursor: CheckpointCursor) -> Result<(), NexusError> {
        Ok(())
    }
}
