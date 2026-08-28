use crate::config::DeltaConnectorConfig;
use crate::rows::{arrow_schema_to_delta_fields, extract_pk_strings, in_predicate};
use arrow_array::RecordBatch;
use async_trait::async_trait;
use deltalake::errors::DeltaTableError;
use deltalake::operations::create::CreateBuilder;
use deltalake::table::builder::ensure_table_uri;
use deltalake::writer::{DeltaWriter, RecordBatchWriter};
use deltalake::{open_table, DeltaTable};
use nexus_core::{
    batch_buffer::RecordBatchBuffer, split_by_opcode, with_timeout, CheckpointCursor, NexusError,
    Sink,
};

/// Default byte threshold for the buffered writer. 32 MiB keeps memory bounded
/// while still amortizing transaction overhead across many rows.
const FLUSH_THRESHOLD_BYTES: usize = 32 * 1024 * 1024;

/// Delta Lake sink (Marco 6 — `deltalake` crate directly). Table creation
/// and appends use the lower-level `CreateBuilder`/`RecordBatchWriter` (no
/// query engine involved); CDC deletes go through `DeltaTable::delete()` with
/// a SQL `IN (...)` predicate, which needs the `datafusion` feature but not
/// a direct `datafusion` dependency of our own — the predicate is a plain
/// string, parsed internally by deltalake's bundled DataFusion.
/// Upsert = delete-then-append on the primary key, same trade-off Milvus's
/// upsert makes in Marco 5 (Delta's own `merge()` needs a `datafusion::DataFrame`
/// source, which isn't worth the extra dependency surface for this).
///
/// Plain (non-CDC) batches are buffered and flushed only when the row or byte
/// threshold is reached, or at `commit_checkpoint`. This dramatically reduces
/// the number of Delta transactions for large historical loads.
pub struct DeltaSink {
    table_uri: String,
    primary_key: String,
    append_only: bool,
    timeout_seconds: u64,
    buffer: RecordBatchBuffer,
}

impl DeltaSink {
    pub fn connect(cfg: &DeltaConnectorConfig) -> Result<Self, NexusError> {
        Ok(Self {
            table_uri: cfg.table_uri.clone(),
            primary_key: cfg.primary_key.clone(),
            append_only: cfg.append_only,
            timeout_seconds: cfg.timeout_seconds,
            buffer: RecordBatchBuffer::new(cfg.flush_threshold_rows, FLUSH_THRESHOLD_BYTES),
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

    /// Maximum number of primary-key values passed to a single Delta `DELETE`
    /// predicate. DataFusion's SQL parser can overflow its stack on very large
    /// `IN (...)` lists, so we chunk the keys and issue multiple deletes.
    const DELETE_CHUNK_SIZE: usize = 1_000;

    async fn delete_by_pks(
        &self,
        mut table: DeltaTable,
        batch_for_types: &RecordBatch,
        pks: &[String],
    ) -> Result<DeltaTable, NexusError> {
        if pks.is_empty() {
            return Ok(table);
        }
        for chunk in pks.chunks(Self::DELETE_CHUNK_SIZE) {
            let predicate = in_predicate(batch_for_types, &self.primary_key, chunk)?;
            table = with_timeout(self.timeout_seconds, "delta delete", async {
                let (table, _metrics) = table
                    .delete()
                    .with_predicate(predicate)
                    .await
                    .map_err(|e| NexusError::Connector(format!("delta delete failed: {e}")))?;
                Ok::<_, NexusError>(table)
            })
            .await?;
        }
        Ok(table)
    }

    async fn write_buffered_upsert(&self, batch: RecordBatch) -> Result<(), NexusError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let mut table = self.ensure_table(&batch).await?;
        // Dedup: drop any pre-existing rows sharing a primary key with this
        // batch before appending, so re-upserting a key updates it instead
        // of leaving a stale duplicate row behind. Skipped in append-only
        // mode for large non-CDC loads where duplicates are acceptable.
        if !self.append_only {
            let pks = extract_pk_strings(&batch, &self.primary_key)?;
            table = self.delete_by_pks(table, &batch, &pks).await?;
        }

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

    async fn flush_buffer(&mut self) -> Result<(), NexusError> {
        if let Some(batch) = self
            .buffer
            .take()
            .map_err(|e| NexusError::Serialization(format!("delta batch concat failed: {e}")))?
        {
            self.write_buffered_upsert(batch).await?;
        }
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
        // silently kept. CDC traffic is applied immediately to preserve
        // ordering between upserts and deletes; plain historical loads are
        // buffered and flushed in larger chunks.
        match split_by_opcode(&batch)? {
            None => {
                if let Some(flushed) = self.buffer.push(batch).map_err(|e| {
                    NexusError::Serialization(format!("delta batch concat failed: {e}"))
                })? {
                    self.write_buffered_upsert(flushed).await?;
                }
                Ok(())
            }
            Some(split) => {
                self.flush_buffer().await?;
                self.write_buffered_upsert(split.upserts).await?;
                self.delete(&split.deletes).await?;
                Ok(())
            }
        }
    }

    async fn commit_checkpoint(&mut self, _cursor: CheckpointCursor) -> Result<(), NexusError> {
        // Flush any remaining buffered rows before the pipeline run ends.
        self.flush_buffer().await
    }
}
