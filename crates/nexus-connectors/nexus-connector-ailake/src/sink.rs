use crate::bridge::to_old_batch;
use crate::config::AilakeConnectorConfig;
use crate::rows::{drop_column, extract_embeddings, extract_pk_strings};
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
use nexus_core::{split_by_opcode, with_timeout, CheckpointCursor, NexusError, Sink};
use std::sync::Arc;

/// AI-Lake sink — writes into the self-contained Parquet+HNSW vector-native
/// Lakehouse format (github.com/ailake-io/ai-lakehouse). Backed by
/// `HadoopCatalog` + `LocalStore`: no server/container, `warehouse` is a
/// plain local directory (same embedded shape as the Marco 5 LanceDB sink).
/// One `TableWriter` session (create-or-open, write, commit) per
/// `write_batch` call — AI-Lake has no long-lived write-session API to
/// stream across calls the way `Sink::write_batch` is invoked.
pub struct AilakeSink {
    catalog: Arc<dyn CatalogProvider>,
    store: Arc<dyn Store>,
    table: TableIdent,
    policy: VectorStoragePolicy,
    primary_key: String,
    embedding_column: String,
    timeout_seconds: u64,
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
            timeout_seconds: cfg.timeout_seconds,
        })
    }

    async fn upsert(&self, batch: RecordBatch) -> Result<(), NexusError> {
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
        // Safe to run even when no prior row exists for these keys yet — an
        // equality delete for a value with no current match is a no-op today
        // and, by the same sequence-number rule, can never retroactively mask
        // a future row that reuses the key.
        //
        // Exception: the table itself may not exist yet (this sink's very
        // first write). `delete_where` unconditionally loads the table's
        // metadata.json and errors if it's missing — nothing to be created
        // by it (unlike `TableWriter::create_or_open` below) — so skip the
        // delete entirely in that case; there is nothing to mask anyway.
        if self.catalog.load_table(&self.table).await.is_ok() {
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
        // of being silently appended. Plain (non-CDC) batches take the
        // unchanged single-append path.
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
