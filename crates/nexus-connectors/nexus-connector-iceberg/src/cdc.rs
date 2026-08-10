use crate::catalog;
use crate::config::IcebergCdcConfig;
use arrow_array::{RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::spec::{DataContentType, ManifestStatus, SnapshotRef};
use iceberg::table::Table;
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use nexus_core::{with_timeout, NexusError, Source, OPCODE_COLUMN};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::sync::Arc;

/// Native CDC source for Iceberg — manual manifest diffing (`iceberg`
/// 0.10.0 has no built-in incremental/CDC scan, unlike Delta's Change Data
/// Feed). Insert-only, see `IcebergCdcConfig`'s doc comment for why.
/// Inherently poll/batch, same as `deltalake-cdc`/`ailake-cdc` — one call
/// reads every snapshot committed after `starting_snapshot_id`, then ends.
pub struct IcebergCdcSource {
    cfg: IcebergCdcConfig,
    schema: SchemaRef,
}

impl IcebergCdcSource {
    pub async fn connect(cfg: &IcebergCdcConfig) -> Result<Self, NexusError> {
        let table = load_table(cfg).await?;
        let mut fields: Vec<Field> = schema_to_arrow_schema(table.metadata().current_schema())
            .map_err(|e| NexusError::Schema(format!("iceberg-cdc schema_to_arrow_schema failed: {e}")))?
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        fields.push(Field::new(OPCODE_COLUMN, DataType::Utf8, false));

        Ok(Self {
            cfg: cfg.clone(),
            schema: Arc::new(Schema::new(fields)),
        })
    }
}

async fn load_table(cfg: &IcebergCdcConfig) -> Result<Table, NexusError> {
    let catalog = catalog::connect(&IcebergCdcConfigAsBatch(cfg).into()).await?;
    let ident = TableIdent::new(NamespaceIdent::new(cfg.namespace.clone()), cfg.table.clone());
    with_timeout(cfg.timeout_seconds, "iceberg-cdc load_table", async {
        catalog
            .load_table(&ident)
            .await
            .map_err(|e| NexusError::Connector(format!("iceberg-cdc load_table failed: {e}")))
    })
    .await
}

/// `catalog::connect` takes the batch `IcebergConnectorConfig` — the two
/// configs share the same catalog/warehouse/namespace/table fields, so
/// this just borrows them rather than duplicating `catalog::connect`.
struct IcebergCdcConfigAsBatch<'a>(&'a IcebergCdcConfig);

impl From<IcebergCdcConfigAsBatch<'_>> for crate::config::IcebergConnectorConfig {
    fn from(wrapper: IcebergCdcConfigAsBatch<'_>) -> Self {
        let cfg = wrapper.0;
        crate::config::IcebergConnectorConfig {
            catalog_uri: cfg.catalog_uri.clone(),
            warehouse_location: cfg.warehouse_location.clone(),
            namespace: cfg.namespace.clone(),
            table: cfg.table.clone(),
            format_version: Default::default(),
            timeout_seconds: cfg.timeout_seconds,
        }
    }
}

/// Snapshots strictly after `starting_snapshot_id` (or the whole history if
/// `None`), oldest first — walks `parent_snapshot_id` backward from
/// `current_snapshot()` and reverses, since Iceberg only links snapshots
/// backward (no forward index).
fn snapshots_since(table: &Table, starting_snapshot_id: Option<i64>) -> Vec<SnapshotRef> {
    let metadata = table.metadata();
    let mut chain = Vec::new();
    let mut current = metadata.current_snapshot().cloned();
    while let Some(snapshot) = current {
        if Some(snapshot.snapshot_id()) == starting_snapshot_id {
            break;
        }
        let parent_id = snapshot.parent_snapshot_id();
        chain.push(snapshot);
        current = parent_id.and_then(|id| metadata.snapshot_by_id(id).cloned());
    }
    chain.reverse();
    chain
}

#[async_trait]
impl Source for IcebergCdcSource {
    async fn read_batches(
        &mut self,
    ) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError> {
        let table = load_table(&self.cfg).await?;
        let snapshots = snapshots_since(&table, self.cfg.starting_snapshot_id);

        let mut batches = Vec::new();
        for snapshot in &snapshots {
            let manifest_list = with_timeout(
                self.cfg.timeout_seconds,
                "iceberg-cdc load manifest list",
                async {
                    table
                        .manifest_list_reader(snapshot)
                        .load()
                        .await
                        .map_err(|e| {
                            NexusError::Connector(format!("iceberg-cdc manifest list load failed: {e}"))
                        })
                },
            )
            .await?;

            for manifest_file in manifest_list.entries() {
                // A snapshot's manifest list re-lists every manifest still
                // live at that point, not just the ones it introduced —
                // `fast_append` inherits the prior snapshot's manifests
                // unchanged. Skip any manifest this snapshot didn't add
                // itself, or the same `Added` data-file entries get
                // processed again for every later snapshot they're still
                // listed under.
                if manifest_file.added_snapshot_id != snapshot.snapshot_id() {
                    continue;
                }
                let manifest = manifest_file
                    .load_manifest(table.file_io())
                    .await
                    .map_err(|e| {
                        NexusError::Connector(format!("iceberg-cdc manifest load failed: {e}"))
                    })?;

                for entry in manifest.entries() {
                    // Only newly-added *data* files count as inserts — see
                    // `IcebergCdcConfig`'s doc comment on why this source
                    // never needs to look at delete-content entries or
                    // `Existing`/`Deleted` status.
                    if entry.status() != ManifestStatus::Added
                        || entry.data_file().content_type() != DataContentType::Data
                    {
                        continue;
                    }

                    let bytes = table
                        .file_io()
                        .new_input(entry.data_file().file_path())
                        .map_err(|e| {
                            NexusError::Connector(format!("iceberg-cdc file open failed: {e}"))
                        })?
                        .read()
                        .await
                        .map_err(|e| {
                            NexusError::Connector(format!("iceberg-cdc file read failed: {e}"))
                        })?;
                    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
                        .map_err(|e| {
                            NexusError::Connector(format!(
                                "iceberg-cdc parquet reader build failed: {e}"
                            ))
                        })?
                        .build()
                        .map_err(|e| {
                            NexusError::Connector(format!(
                                "iceberg-cdc parquet reader build failed: {e}"
                            ))
                        })?;
                    for batch in reader {
                        let batch = batch.map_err(|e| {
                            NexusError::Connector(format!("iceberg-cdc parquet read failed: {e}"))
                        })?;
                        batches.push(append_insert_opcode(&batch, &self.schema)?);
                    }
                }
            }
        }

        Ok(Box::pin(stream::iter(batches.into_iter().map(Ok))))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Appends a constant `__opcode = "I"` column, in the order `target_schema`
/// declares (data columns as written by the file, then `__opcode` last).
fn append_insert_opcode(
    batch: &RecordBatch,
    target_schema: &SchemaRef,
) -> Result<RecordBatch, NexusError> {
    let data_field_count = target_schema.fields().len() - 1;
    let mut columns = Vec::with_capacity(target_schema.fields().len());
    for field in target_schema.fields().iter().take(data_field_count) {
        let idx = batch.schema().index_of(field.name()).map_err(|_| {
            NexusError::Schema(format!(
                "iceberg-cdc column '{}' missing from data file",
                field.name()
            ))
        })?;
        columns.push(batch.column(idx).clone());
    }
    columns.push(Arc::new(StringArray::from(vec![
        "I";
        batch.num_rows()
    ])) as _);

    RecordBatch::try_new(target_schema.clone(), columns)
        .map_err(|e| NexusError::Schema(format!("iceberg-cdc remap failed: {e}")))
}
