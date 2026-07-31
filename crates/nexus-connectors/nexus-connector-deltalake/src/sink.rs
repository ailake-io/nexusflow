use crate::config::DeltaConnectorConfig;
use crate::rows::{arrow_schema_to_delta_fields, extract_pk_strings, in_predicate};
use arrow_array::RecordBatch;
use async_trait::async_trait;
use deltalake::operations::create::CreateBuilder;
use deltalake::table::builder::ensure_table_uri;
use deltalake::writer::{DeltaWriter, RecordBatchWriter};
use deltalake::{open_table, DeltaOps, DeltaTable};
use nexus_core::{split_by_opcode, CheckpointCursor, NexusError, Sink};

/// Delta Lake sink (Marco 6 — `deltalake` crate directly). Table creation
/// and appends use the lower-level `CreateBuilder`/`RecordBatchWriter` (no
/// query engine involved); CDC deletes go through `DeltaOps::delete()` with
/// a SQL `IN (...)` predicate, which needs the `datafusion` feature but not
/// a direct `datafusion` dependency of our own — the predicate is a plain
/// string, parsed internally by deltalake's bundled DataFusion.
/// Upsert = delete-then-append on the primary key, same trade-off Milvus's
/// upsert makes in Marco 5 (Delta's own `merge()` needs a `datafusion::DataFrame`
/// source, which isn't worth the extra dependency surface for this).
pub struct DeltaSink {
    table_uri: String,
    primary_key: String,
}

impl DeltaSink {
    pub fn connect(cfg: &DeltaConnectorConfig) -> Result<Self, NexusError> {
        Ok(Self {
            table_uri: cfg.table_uri.clone(),
            primary_key: cfg.primary_key.clone(),
        })
    }

    async fn open(&self) -> Result<Option<DeltaTable>, NexusError> {
        let url = ensure_table_uri(&self.table_uri)
            .map_err(|e| NexusError::Connector(format!("delta table uri invalid: {e}")))?;
        match open_table(url).await {
            Ok(table) => Ok(Some(table)),
            Err(_) => Ok(None),
        }
    }

    async fn ensure_table(&self, batch: &RecordBatch) -> Result<DeltaTable, NexusError> {
        if let Some(table) = self.open().await? {
            return Ok(table);
        }
        let fields = arrow_schema_to_delta_fields(&batch.schema())?;
        CreateBuilder::new()
            .with_location(&self.table_uri)
            .with_columns(fields)
            .await
            .map_err(|e| NexusError::Connector(format!("delta create table failed: {e}")))
    }

    async fn delete_by_pks(
        &self,
        table: DeltaTable,
        batch_for_types: &RecordBatch,
        pks: &[String],
    ) -> Result<DeltaTable, NexusError> {
        if pks.is_empty() {
            return Ok(table);
        }
        let predicate = in_predicate(batch_for_types, &self.primary_key, pks)?;
        let (table, _metrics) = DeltaOps(table)
            .delete()
            .with_predicate(predicate)
            .await
            .map_err(|e| NexusError::Connector(format!("delta delete failed: {e}")))?;
        Ok(table)
    }

    async fn upsert(&self, batch: RecordBatch) -> Result<(), NexusError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let mut table = self.ensure_table(&batch).await?;
        // Dedup: drop any pre-existing rows sharing a primary key with this
        // batch before appending, so re-upserting a key updates it instead
        // of leaving a stale duplicate row behind.
        let pks = extract_pk_strings(&batch, &self.primary_key)?;
        table = self.delete_by_pks(table, &batch, &pks).await?;

        let mut writer = RecordBatchWriter::for_table(&table)
            .map_err(|e| NexusError::Connector(format!("delta writer init failed: {e}")))?;
        writer
            .write(batch)
            .await
            .map_err(|e| NexusError::Connector(format!("delta write failed: {e}")))?;
        writer
            .flush_and_commit(&mut table)
            .await
            .map_err(|e| NexusError::Connector(format!("delta commit failed: {e}")))?;
        Ok(())
    }

    async fn delete(&self, batch: &RecordBatch) -> Result<(), NexusError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let Some(table) = self.open().await? else {
            return Ok(()); // no table yet — nothing to delete
        };
        let pks = extract_pk_strings(batch, &self.primary_key)?;
        self.delete_by_pks(table, batch, &pks).await?;
        Ok(())
    }
}

#[async_trait]
impl Sink for DeltaSink {
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<(), NexusError> {
        // CDC batches carry an `__opcode` column (ARCHITECTURE.md §5) — split
        // it so deletes are issued as real Delta deletes instead of being
        // silently kept. Plain (non-CDC) batches take the unchanged
        // all-upsert path.
        match split_by_opcode(&batch)? {
            None => self.upsert(batch).await,
            Some(split) => {
                self.upsert(split.upserts).await?;
                self.delete(&split.deletes).await?;
                Ok(())
            }
        }
    }

    async fn commit_checkpoint(&mut self, _cursor: CheckpointCursor) -> Result<(), NexusError> {
        Ok(())
    }
}
