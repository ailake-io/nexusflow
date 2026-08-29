use crate::bridge::to_old_batch;
use crate::config::AilakeConnectorConfig;
use crate::rows::{dedupe_keep_last_by_pk, drop_column, extract_embeddings, extract_pk_strings};
use ailake_catalog::hadoop::HadoopCatalog;
use ailake_catalog::provider::{CatalogProvider, TableIdent};
use ailake_core::schema::VectorStoragePolicy;
use ailake_core::types::VectorMetric;
use ailake_query::delete::delete_where;
use ailake_query::writer::TableWriter;
use ailake_store::local::LocalStore;
use ailake_store::store::Store;
use arrow_array::RecordBatch;
use async_trait::async_trait;
use nexus_core::{
    batch_buffer::RecordBatchBuffer, split_by_opcode, with_timeout, CheckpointCursor, NexusError,
    Sink,
};
use std::sync::Arc;

/// Default byte threshold for the buffered writer. 32 MiB keeps memory bounded
/// while still amortizing create-or-open/write/commit cycles across many rows.
const FLUSH_THRESHOLD_BYTES: usize = 32 * 1024 * 1024;

/// AI-Lake sink — writes into the self-contained Parquet+HNSW vector-native
/// Lakehouse format (github.com/ailake-io/ai-lakehouse). Backed by
/// `HadoopCatalog` + `LocalStore`: no server/container, `warehouse` is a
/// plain local directory (same embedded shape as the Marco 5 LanceDB sink).
///
/// Plain (non-CDC) batches are buffered and flushed only when the row or byte
/// threshold is reached, or at `commit_checkpoint`. This reduces the number of
/// create-or-open/write/commit cycles for large historical loads.
pub struct AilakeSink {
    catalog: Arc<dyn CatalogProvider>,
    store: Arc<dyn Store>,
    table: TableIdent,
    policy: VectorStoragePolicy,
    primary_key: String,
    embedding_column: String,
    append_only: bool,
    timeout_seconds: u64,
    buffer: RecordBatchBuffer,
}

const FORMAT_VERSION: u8 = 2;

impl AilakeSink {
    pub fn connect(cfg: &AilakeConnectorConfig) -> Result<Self, NexusError> {
        let warehouse = cfg.warehouse();
        let namespace = cfg.namespace();
        let table_name = cfg.table_name();

        let store: Arc<dyn Store> = Arc::new(LocalStore::new(warehouse));
        let catalog: Arc<dyn CatalogProvider> = Arc::new(HadoopCatalog::new(store.clone(), ""));
        let table = TableIdent::new(namespace, table_name);
        let policy = VectorStoragePolicy::default_f16(
            &cfg.embedding_column,
            cfg.dimension,
            VectorMetric::Cosine,
        );

        Ok(Self {
            catalog,
            store,
            table,
            policy,
            primary_key: cfg.primary_key.clone(),
            embedding_column: cfg.embedding_column.clone(),
            append_only: cfg.append_only,
            timeout_seconds: cfg.timeout_seconds,
            buffer: RecordBatchBuffer::new(cfg.flush_threshold_rows, FLUSH_THRESHOLD_BYTES),
        })
    }

    async fn write_buffered_upsert(&self, batch: RecordBatch) -> Result<(), NexusError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        // A key written twice while still inside the same buffer window
        // (two write_batch calls, one flush) would otherwise reach the
        // delete-then-append below as two rows for the same key — the
        // delete only masks a row from an *earlier* flush, so both would
        // survive as separate physical rows. Collapse to the last write per
        // key first so "second write replaces the first" holds regardless
        // of whether the two writes landed in the same flush or different
        // ones.
        let batch = dedupe_keep_last_by_pk(&batch, &self.primary_key)?;
        if batch.num_rows() == 0 {
            return Ok(());
        }
        // Real upsert semantics: mask any existing row sharing a primary key
        // with this batch *before* appending it, via a separate, earlier
        // commit. `ailake-catalog`/`ailake-query` >=0.1.11 scope equality
        // deletes by Iceberg sequence number — a delete only masks a data
        // file with a strictly *lower* sequence number than its own. Since
        // this delete commits before the write_batch()+commit() below, the
        // freshly-appended rows always carry a higher sequence number than
        // this delete and are therefore never masked by it, while any prior
        // row sharing the same key (necessarily an even earlier commit) is.
        //
        // Exception: the table itself may not exist yet (this sink's very
        // first write). `delete_where` unconditionally loads the table's
        // metadata.json and errors if it's missing — skip the delete entirely
        // in that case. Also skipped in append-only mode.
        if !self.append_only && self.catalog.load_table(&self.table).await.is_ok() {
            self.delete(&batch).await?;
        }
        with_timeout(self.timeout_seconds, "ailake upsert", async {
            let embeddings = extract_embeddings(&batch, &self.embedding_column)?;
            // ailake_query::writer::TableWriter appends its own vector column
            // from `embeddings` — the tabular batch handed to it must not
            // already carry one under the same name (see `drop_column`'s doc).
            let tabular = drop_column(&batch, &self.embedding_column)?;
            // Bridge across the arrow-array version boundary (see bridge.rs).
            let tabular = to_old_batch(&tabular)?;
            let mut writer = TableWriter::create_or_open(
                self.catalog.clone(),
                self.store.clone(),
                self.policy.clone(),
                self.table.clone(),
                FORMAT_VERSION,
            )
            .await
            .map_err(|e| NexusError::Connector(format!("ailake create_or_open failed: {e}")))?;
            writer
                .write_batch(&tabular, &embeddings)
                .await
                .map_err(|e| NexusError::Connector(format!("ailake write_batch failed: {e}")))?;
            writer
                .commit()
                .await
                .map_err(|e| NexusError::Connector(format!("ailake commit failed: {e}")))?;
            Ok(())
        })
        .await
    }

    async fn flush_buffer(&mut self) -> Result<(), NexusError> {
        if let Some(batch) = self
            .buffer
            .take()
            .map_err(|e| NexusError::Serialization(format!("ailake batch concat failed: {e}")))?
        {
            self.write_buffered_upsert(batch).await?;
        }
        Ok(())
    }

    async fn delete(&self, batch: &RecordBatch) -> Result<(), NexusError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let ids = extract_pk_strings(batch, &self.primary_key)?;
        let refs: Vec<&str> = ids.iter().map(String::as_str).collect();
        with_timeout(self.timeout_seconds, "ailake delete_where", async {
            delete_where(
                self.catalog.clone(),
                self.store.clone(),
                &self.table,
                &self.primary_key,
                &refs,
            )
            .await
            .map_err(|e| NexusError::Connector(format!("ailake delete_where failed: {e}")))
        })
        .await?;
        Ok(())
    }
}

#[async_trait]
impl Sink for AilakeSink {
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<(), NexusError> {
        // CDC batches carry an `__opcode` column (ARCHITECTURE.md §5) — split
        // it so deletes are issued as real Iceberg equality deletes instead
        // of being silently appended. CDC traffic is applied immediately to
        // preserve ordering between upserts and deletes; plain historical
        // loads are buffered and flushed in larger chunks.
        match split_by_opcode(&batch)? {
            None => {
                if let Some(flushed) = self.buffer.push(batch).map_err(|e| {
                    NexusError::Serialization(format!("ailake batch concat failed: {e}"))
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
        self.flush_buffer().await
    }
}
