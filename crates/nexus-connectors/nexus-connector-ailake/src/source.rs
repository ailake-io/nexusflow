use crate::bridge::to_new_batch;
use crate::config::AilakeConnectorConfig;
use crate::rows::{append_embedding_column, extract_pk_strings};
use ailake_catalog::hadoop::HadoopCatalog;
use ailake_catalog::provider::{CatalogProvider, TableIdent};
use ailake_catalog::read_equality_delete_values;
use ailake_file::reader::AilakeFileReader;
use ailake_store::local::LocalStore;
use ailake_store::store::Store;
use arrow_array::{BooleanArray, RecordBatch};
use arrow_schema::SchemaRef;
use arrow_select::filter::filter_record_batch;
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use nexus_core::{NexusError, Source};
use std::collections::HashSet;
use std::sync::Arc;

/// AI-Lake source — full-table scan (not a vector similarity search) used to
/// read back everything a `Sink::write_batch` committed, e.g. for a
/// pipeline's downstream stage or a round-trip test. Reads every current
/// data file directly with `AilakeFileReader` (plain Parquet + the AI-Lake
/// footer past the final `PAR1` magic — standard readers never see it), then
/// re-appends the decoded embedding as a `FixedSizeList<Float32>` column so
/// the returned batch matches what was written.
///
/// Raw per-file reads bypass `ailake_query`'s scanner (and its
/// `EqualityDeleteFilter`, which is bound to ailake's own arrow-array
/// version — see `bridge.rs`) entirely, so equality deletes committed by
/// `AilakeSink::delete` are masked out here directly by primary-key value
/// rather than via that type.
pub struct AilakeSource {
    catalog: Arc<dyn CatalogProvider>,
    store: Arc<dyn Store>,
    table: TableIdent,
    embedding_column: String,
    primary_key: String,
    dimension: u32,
    schema: SchemaRef,
}

impl AilakeSource {
    pub async fn connect(cfg: &AilakeConnectorConfig) -> Result<Self, NexusError> {
        let store: Arc<dyn Store> = Arc::new(LocalStore::new(&cfg.warehouse));
        let catalog: Arc<dyn CatalogProvider> = Arc::new(HadoopCatalog::new(store.clone(), ""));
        let table = TableIdent::new(&cfg.namespace, &cfg.table);

        // Schema is derived from the first committed data file — AI-Lake
        // tables have no separate DDL step to introspect ahead of any write,
        // same constraint as the LanceDB source in Marco 5.
        let files = catalog
            .list_files(&table, None)
            .await
            .map_err(|e| NexusError::Connector(format!("ailake list_files failed: {e}")))?;
        let first = files.first().ok_or_else(|| {
            NexusError::Connector(format!(
                "ailake table '{}.{}' has no committed data files yet — schema cannot be derived",
                cfg.namespace, cfg.table
            ))
        })?;
        let batch =
            read_file_batch(&store, &first.path, &cfg.embedding_column, cfg.dimension).await?;

        Ok(Self {
            catalog,
            store,
            table,
            embedding_column: cfg.embedding_column.clone(),
            primary_key: cfg.primary_key.clone(),
            dimension: cfg.dimension,
            schema: batch.schema(),
        })
    }
}

async fn read_file_batch(
    store: &Arc<dyn Store>,
    path: &str,
    embedding_column: &str,
    dimension: u32,
) -> Result<RecordBatch, NexusError> {
    let bytes = store
        .get(path)
        .await
        .map_err(|e| NexusError::Connector(format!("ailake read failed: {e}")))?;
    let reader = AilakeFileReader::new(bytes, embedding_column, dimension);
    let (old_raw_batch, vectors) = reader
        .read_parquet()
        .map_err(|e| NexusError::Connector(format!("ailake read_parquet failed: {e}")))?;
    let raw_batch = to_new_batch(&old_raw_batch)?;
    append_embedding_column(&raw_batch, &vectors, dimension, embedding_column)
}

/// Loads every equality-delete file active for the table and returns the set
/// of deleted primary-key values (string-normalised — same representation
/// `AilakeSink::delete` writes via `delete_where`). Non-primary-key columns
/// in a delete file are ignored: `AilakeSink` only ever issues single-column
/// identity deletes keyed by `primary_key`.
async fn deleted_primary_keys(
    catalog: &Arc<dyn CatalogProvider>,
    store: &Arc<dyn Store>,
    table: &TableIdent,
    primary_key: &str,
) -> Result<HashSet<String>, NexusError> {
    let files = catalog
        .list_equality_deletes(table, None)
        .await
        .map_err(|e| NexusError::Connector(format!("ailake list_equality_deletes failed: {e}")))?;

    let mut deleted = HashSet::new();
    for file in &files {
        let bytes = store
            .get(&file.path)
            .await
            .map_err(|e| NexusError::Connector(format!("ailake read failed: {e}")))?;
        let pairs = read_equality_delete_values(&bytes).map_err(|e| {
            NexusError::Connector(format!("ailake equality delete decode failed: {e}"))
        })?;
        for (col, val) in pairs {
            if col == primary_key {
                deleted.insert(val);
            }
        }
    }
    Ok(deleted)
}

#[async_trait]
impl Source for AilakeSource {
    async fn read_batches(
        &mut self,
    ) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError> {
        let files = self
            .catalog
            .list_files(&self.table, None)
            .await
            .map_err(|e| NexusError::Connector(format!("ailake list_files failed: {e}")))?;
        let deleted =
            deleted_primary_keys(&self.catalog, &self.store, &self.table, &self.primary_key)
                .await?;

        let mut batches = Vec::with_capacity(files.len());
        for file in files {
            let batch = read_file_batch(
                &self.store,
                &file.path,
                &self.embedding_column,
                self.dimension,
            )
            .await?;
            let batch = if deleted.is_empty() {
                batch
            } else {
                let pk_values = extract_pk_strings(&batch, &self.primary_key)?;
                let keep: Vec<bool> = pk_values.iter().map(|v| !deleted.contains(v)).collect();
                let mask = BooleanArray::from(keep);
                filter_record_batch(&batch, &mask)
                    .map_err(|e| NexusError::Schema(format!("ailake delete filter failed: {e}")))?
            };
            batches.push(Ok(batch));
        }

        Ok(Box::pin(stream::iter(batches)))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
