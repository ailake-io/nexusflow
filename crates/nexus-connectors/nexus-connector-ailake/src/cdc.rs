use crate::config::AilakeCdcConfig;
use crate::source::read_file_batch;
use ailake_catalog::hadoop::HadoopCatalog;
use ailake_catalog::provider::{CatalogProvider, DataFileEntry, EqualityDeleteFile, TableIdent};
use ailake_catalog::read_equality_delete_values;
use ailake_store::local::LocalStore;
use ailake_store::store::Store;
use arrow_array::{new_null_array, BooleanArray, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use arrow_select::filter::filter_record_batch;
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use nexus_core::{with_timeout, NexusError, Source, OPCODE_COLUMN};
use std::collections::HashSet;
use std::sync::Arc;

/// Native CDC source for AI-Lake — see `AilakeCdcConfig`'s doc comment for
/// why this one supports the full `I`/`U`/`D` set (unlike `iceberg-cdc`).
/// Inherently poll/batch, same as `deltalake-cdc`/`iceberg-cdc` — one call
/// reads everything committed after `starting_snapshot_id`, then ends.
pub struct AilakeCdcSource {
    catalog: Arc<dyn CatalogProvider>,
    store: Arc<dyn Store>,
    table: TableIdent,
    embedding_column: String,
    primary_key: String,
    dimension: u32,
    starting_snapshot_id: Option<i64>,
    schema: SchemaRef,
    timeout_seconds: u64,
}

impl AilakeCdcSource {
    pub async fn connect(cfg: &AilakeCdcConfig) -> Result<Self, NexusError> {
        with_timeout(cfg.timeout_seconds, "ailake-cdc connect", async {
            let store: Arc<dyn Store> = Arc::new(LocalStore::new(&cfg.warehouse));
            let catalog: Arc<dyn CatalogProvider> =
                Arc::new(HadoopCatalog::new(store.clone(), ""));
            let table = TableIdent::new(&cfg.namespace, &cfg.table);

            // Schema derived from the first committed file, same constraint
            // the batch `AilakeSource` already documents.
            let files = catalog
                .list_files(&table, None)
                .await
                .map_err(|e| NexusError::Connector(format!("ailake-cdc list_files failed: {e}")))?;
            let first = files.first().ok_or_else(|| {
                NexusError::Connector(format!(
                    "ailake table '{}.{}' has no committed data files yet — schema cannot be derived",
                    cfg.namespace, cfg.table
                ))
            })?;
            let batch =
                read_file_batch(&store, &first.path, &cfg.embedding_column, cfg.dimension).await?;
            // Every non-key column is forced nullable regardless of what the
            // underlying file declares: a pure-delete row (see
            // `delete_only_row`) genuinely has no value for them — we never
            // read the row being removed, only its key — so the schema must
            // allow null there or `RecordBatch::try_new` rejects the row.
            let mut fields: Vec<Field> = batch
                .schema()
                .fields()
                .iter()
                .map(|f| {
                    if f.name() == &cfg.primary_key {
                        f.as_ref().clone()
                    } else {
                        f.as_ref().clone().with_nullable(true)
                    }
                })
                .collect();
            fields.push(Field::new(OPCODE_COLUMN, DataType::Utf8, false));

            Ok(Self {
                catalog,
                store,
                table,
                embedding_column: cfg.embedding_column.clone(),
                primary_key: cfg.primary_key.clone(),
                dimension: cfg.dimension,
                starting_snapshot_id: cfg.starting_snapshot_id,
                schema: Arc::new(Schema::new(fields)),
                timeout_seconds: cfg.timeout_seconds,
            })
        })
        .await
    }
}

/// Files/deletes present "as of" `new_id` but not "as of" `old_id` — the
/// diff is exactly what changed in between, since `CatalogProvider` already
/// gives point-in-time snapshots rather than raw manifest entries (compare
/// `iceberg-cdc`, which has to walk `ManifestEntry`s by hand for the same
/// thing — this catalog's own API does the equivalent of that internally).
fn added_since<T>(old: &[T], new: Vec<T>, path_of: impl Fn(&T) -> &str) -> Vec<T> {
    let old_paths: HashSet<&str> = old.iter().map(&path_of).collect();
    new.into_iter()
        .filter(|f| !old_paths.contains(path_of(f)))
        .collect()
}

async fn deleted_keys_from(
    store: &Arc<dyn Store>,
    files: &[EqualityDeleteFile],
    primary_key: &str,
) -> Result<HashSet<String>, NexusError> {
    let mut deleted = HashSet::new();
    for file in files {
        let bytes = store
            .get(&file.path)
            .await
            .map_err(|e| NexusError::Connector(format!("ailake-cdc read failed: {e}")))?;
        let pairs = read_equality_delete_values(&bytes).map_err(|e| {
            NexusError::Connector(format!("ailake-cdc equality delete decode failed: {e}"))
        })?;
        for (col, val) in pairs {
            if col == primary_key {
                deleted.insert(val);
            }
        }
    }
    Ok(deleted)
}

/// Appends `__opcode = "I"` to every row of `batch` — call sites have
/// already excluded any row whose key is in `deleted_keys` (see
/// `read_batches`; there's no ordering info available to tell whether such
/// a row predates or postdates the delete, so the delete always wins
/// rather than risking resurrecting one).
fn tag_insert(batch: &RecordBatch, target_schema: &SchemaRef) -> Result<RecordBatch, NexusError> {
    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(StringArray::from(vec!["I"; batch.num_rows()])) as _);
    RecordBatch::try_new(target_schema.clone(), columns)
        .map_err(|e| NexusError::Schema(format!("ailake-cdc tag_insert failed: {e}")))
}

/// A synthetic row for a primary key that was deleted without also being
/// re-inserted in this window — every non-key column is null (we never had
/// its post-delete values; there's nothing left to read), same fallback
/// `mongodb-cdc` uses when `full_document` comes back empty for a delete.
fn delete_only_row(
    target_schema: &SchemaRef,
    primary_key: &str,
    pk_value: &str,
) -> Result<RecordBatch, NexusError> {
    let data_field_count = target_schema.fields().len() - 1; // minus __opcode
    let mut columns = Vec::with_capacity(target_schema.fields().len());
    for field in target_schema.fields().iter().take(data_field_count) {
        if field.name() == primary_key {
            // Matches `extract_pk_strings`' supported primary-key types.
            let array: arrow_array::ArrayRef = match field.data_type() {
                DataType::Int64 => Arc::new(arrow_array::Int64Array::from(vec![pk_value
                    .parse::<i64>()
                    .map_err(|e| {
                        NexusError::Schema(format!(
                            "ailake-cdc primary key '{pk_value}' is not a valid Int64: {e}"
                        ))
                    })?])),
                DataType::Utf8 => Arc::new(StringArray::from(vec![pk_value.to_string()])),
                other => {
                    return Err(NexusError::Schema(format!(
                        "ailake-cdc primary key column must be Int64 or Utf8, got {other:?}"
                    )))
                }
            };
            columns.push(array);
        } else {
            columns.push(new_null_array(field.data_type(), 1));
        }
    }
    columns.push(Arc::new(StringArray::from(vec!["D"])) as _);
    RecordBatch::try_new(target_schema.clone(), columns)
        .map_err(|e| NexusError::Schema(format!("ailake-cdc delete_only_row failed: {e}")))
}

#[async_trait]
impl Source for AilakeCdcSource {
    async fn read_batches(
        &mut self,
    ) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError> {
        let metadata = with_timeout(self.timeout_seconds, "ailake-cdc load_table", async {
            self.catalog
                .load_table(&self.table)
                .await
                .map_err(|e| NexusError::Connector(format!("ailake-cdc load_table failed: {e}")))
        })
        .await?;
        let current_id = metadata.current_snapshot_id;

        // `CatalogProvider`'s `Option<SnapshotId>` is an "as of" parameter
        // where `None` means "as of the latest snapshot" — the opposite of
        // what `starting_snapshot_id: None` means in *our* config ("nothing
        // read yet, take the whole history"). Only query "old" state when
        // we actually have a starting point; otherwise the "old" side is
        // the empty table, full stop.
        let old_files = match self.starting_snapshot_id {
            Some(id) => self
                .catalog
                .list_files(&self.table, Some(id))
                .await
                .map_err(|e| NexusError::Connector(format!("ailake-cdc list_files failed: {e}")))?,
            None => Vec::new(),
        };
        let new_files = self
            .catalog
            .list_files(&self.table, current_id)
            .await
            .map_err(|e| NexusError::Connector(format!("ailake-cdc list_files failed: {e}")))?;
        let added_files: Vec<DataFileEntry> =
            added_since(&old_files, new_files, |f: &DataFileEntry| f.path.as_str());

        let old_deletes = match self.starting_snapshot_id {
            Some(id) => self
                .catalog
                .list_equality_deletes(&self.table, Some(id))
                .await
                .map_err(|e| {
                    NexusError::Connector(format!("ailake-cdc list_equality_deletes failed: {e}"))
                })?,
            None => Vec::new(),
        };
        let new_deletes = self
            .catalog
            .list_equality_deletes(&self.table, current_id)
            .await
            .map_err(|e| {
                NexusError::Connector(format!("ailake-cdc list_equality_deletes failed: {e}"))
            })?;
        let added_deletes: Vec<EqualityDeleteFile> =
            added_since(&old_deletes, new_deletes, |f: &EqualityDeleteFile| {
                f.path.as_str()
            });
        let deleted_keys =
            deleted_keys_from(&self.store, &added_deletes, &self.primary_key).await?;

        let mut batches = Vec::with_capacity(added_files.len() + deleted_keys.len());
        for file in &added_files {
            let batch = with_timeout(self.timeout_seconds, "ailake-cdc read_file_batch", async {
                read_file_batch(
                    &self.store,
                    &file.path,
                    &self.embedding_column,
                    self.dimension,
                )
                .await
            })
            .await?;
            // Exclude rows whose key was also deleted in this window — see
            // `tag_insert`'s doc comment for why the delete always wins.
            let pks = crate::rows::extract_pk_strings(&batch, &self.primary_key)?;
            let keep: Vec<bool> = pks.iter().map(|pk| !deleted_keys.contains(pk)).collect();
            if keep.iter().any(|k| *k) {
                let mask = BooleanArray::from(keep);
                let filtered = filter_record_batch(&batch, &mask)
                    .map_err(|e| NexusError::Schema(format!("ailake-cdc filter failed: {e}")))?;
                if filtered.num_rows() > 0 {
                    batches.push(tag_insert(&filtered, &self.schema)?);
                }
            }
        }

        for pk in &deleted_keys {
            batches.push(delete_only_row(&self.schema, &self.primary_key, pk)?);
        }

        Ok(Box::pin(stream::iter(batches.into_iter().map(Ok))))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
