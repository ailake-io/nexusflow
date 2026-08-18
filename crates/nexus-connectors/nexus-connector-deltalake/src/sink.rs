use crate::config::DeltaConnectorConfig;
use crate::rows::{arrow_schema_to_delta_fields, extract_pk_strings, in_predicate};
use arrow_array::RecordBatch;
use async_trait::async_trait;
use deltalake::errors::DeltaTableError;
use deltalake::operations::create::CreateBuilder;
use deltalake::table::builder::ensure_table_uri;
use deltalake::writer::{DeltaWriter, RecordBatchWriter};
use deltalake::{open_table, DeltaTable};
use nexus_core::{split_by_opcode, with_timeout, CheckpointCursor, NexusError, Sink};

/// Delta Lake sink (Marco 6 — `deltalake` crate directly). Table creation
/// and appends use the lower-level `CreateBuilder`/`RecordBatchWriter` (no
/// query engine involved); CDC deletes go through `DeltaTable::delete()` with
/// a SQL `IN (...)` predicate, which needs the `datafusion` feature but not
/// a direct `datafusion` dependency of our own — the predicate is a plain
/// string, parsed internally by deltalake's bundled DataFusion.
/// Upsert = delete-then-append on the primary key, same trade-off Milvus's
/// upsert makes in Marco 5 (Delta's own `merge()` needs a `datafusion::DataFrame`
/// source, which isn't worth the extra dependency surface for this).
pub struct DeltaSink {
    table_uri: String,
    primary_key: String,
    timeout_seconds: u64,
}

impl DeltaSink {
    pub fn connect(cfg: &DeltaConnectorConfig) -> Result<Self, NexusError> {
        Ok(Self {
            table_uri: cfg.table_uri.clone(),
            primary_key: cfg.primary_key.clone(),
            timeout_seconds: cfg.timeout_seconds,
        })
    }

    async fn open(&self) -> Result<Option<DeltaTable>, NexusError> {
        let url = ensure_table_uri(&self.table_uri)
            .map_err(|e| NexusError::Connector(format!("delta table uri invalid: {e}")))?;
        with_timeout(self.timeout_seconds, "delta open_table", async {
            match open_table(url).await {
                Ok(table) => Ok(Some(table)),
                Err(e) if Self::is_missing_table(&e) => Ok(None),
                Err(e) => Err(NexusError::Connector(format!(
                    "delta open table failed: {e}"
                ))),
            }
        })
        .await
    }

    fn is_missing_table(err: &DeltaTableError) -> bool {
        let msg = err.to_string().to_lowercase();
        // "not a table" was this crate's older wording for an empty/
        // nonexistent table directory; newer delta_kernel versions phrase
        // the same case as "Not a Delta table: ... No files in log
        // segment" — "table" alone no longer matches, so match both.
        msg.contains("not found")
            || msg.contains("does not exist")
            || msg.contains("not a table")
            || msg.contains("not a delta table")
            || msg.contains("no files in log segment")
    }

    async fn ensure_table(&self, batch: &RecordBatch) -> Result<DeltaTable, NexusError> {
        if let Some(table) = self.open().await? {
            return Ok(table);
        }
        let fields = arrow_schema_to_delta_fields(&batch.schema())?;
        with_timeout(self.timeout_seconds, "delta create_table", async {
            CreateBuilder::new()
                .with_location(&self.table_uri)
                .with_columns(fields)
                .await
                .map_err(|e| NexusError::Connector(format!("delta create table failed: {e}")))
        })
        .await
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
        with_timeout(self.timeout_seconds, "delta delete", async {
            let (table, _metrics) = table
                .delete()
                .with_predicate(predicate)
                .await
                .map_err(|e| NexusError::Connector(format!("delta delete failed: {e}")))?;
            Ok(table)
        })
        .await
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

        with_timeout(self.timeout_seconds, "delta write", async {
            let mut writer = RecordBatchWriter::for_table(&table)
                .map_err(|e| NexusError::Connector(format!("delta writer init failed: {e}")))?;
            writer
                .write(batch)
                .await
                .map_err(|e| NexusError::Connector(format!("delta write failed: {e}")))?;
            writer
                .flush_and_commit(&mut table)
                .await
                .map_err(|e| NexusError::Connector(format!("delta commit failed: {e}")))
        })
        .await?;
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
