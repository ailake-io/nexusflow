use crate::catalog;
use crate::config::{IcebergConnectorConfig, IcebergFormatVersion};
use arrow_array::{
    Array, BooleanArray, Int32Array, Int64Array, RecordBatch, StringArray, UInt32Array, UInt64Array,
};
use arrow_select::filter::filter;
use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};
use iceberg::arrow::{arrow_schema_to_schema_auto_assign_ids, schema_to_arrow_schema};
use iceberg::spec::{DataFileFormat, FormatVersion};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use nexus_core::{split_by_opcode, with_timeout, CheckpointCursor, NexusError, Sink};
use parquet::file::properties::WriterProperties;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};

/// Iceberg sink (Marco 6 — `iceberg` crate + `iceberg-catalog-sql`).
/// Append-only: `iceberg` 0.10.0/0.10.1's `Transaction` API only exposes
/// `fast_append()`. The higher-level row-delta / equality-delete action is
/// missing, and so are the copy-on-write primitives (`OverwriteFiles`,
/// `RewriteFiles`, `DeleteFiles`) that could otherwise be used to rewrite
/// data files without deletes. See apache/iceberg-rust#2269.
///
/// TODO: Once `RowDeltaAction` (or at least `OverwriteFiles`/`RewriteFiles`)
/// lands in iceberg-rust, replace the error below with a real CDC delete
/// path — either equality-delete files (MoR) or filtered data-file rewrites
/// (CoW). Until then, delete batches are rejected explicitly instead of being
/// silently dropped.
pub struct IcebergSink {
    cfg: IcebergConnectorConfig,
    // `DefaultFileNameGenerator`'s per-file counter starts at 0 for every
    // instance, and a fresh writer (with a fresh generator) is built on
    // every `write_batch` call — without a call-scoped-unique suffix here,
    // two calls would both try to write "data-00000.parquet" and the
    // second commit would fail ("files already referenced by table").
    write_counter: AtomicU64,
}

impl IcebergSink {
    pub fn connect(cfg: &IcebergConnectorConfig) -> Result<Self, NexusError> {
        Ok(Self {
            cfg: cfg.clone(),
            write_counter: AtomicU64::new(0),
        })
    }

    async fn append(&self, batch: RecordBatch) -> Result<(), NexusError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        with_timeout(
            self.cfg.timeout_seconds,
            "iceberg append",
            self.append_inner(batch),
        )
        .await
    }

    async fn append_inner(&self, batch: RecordBatch) -> Result<(), NexusError> {
        let catalog = catalog::connect(&self.cfg).await?;
        let namespace = NamespaceIdent::new(self.cfg.namespace());
        if !catalog
            .namespace_exists(&namespace)
            .await
            .map_err(|e| NexusError::Connector(format!("iceberg namespace_exists failed: {e}")))?
        {
            catalog
                .create_namespace(&namespace, HashMap::new())
                .await
                .map_err(|e| {
                    NexusError::Connector(format!("iceberg create_namespace failed: {e}"))
                })?;
        }
        let ident = TableIdent::new(namespace.clone(), self.cfg.table_name());
        let table = if catalog
            .table_exists(&ident)
            .await
            .map_err(|e| NexusError::Connector(format!("iceberg table_exists failed: {e}")))?
        {
            catalog
                .load_table(&ident)
                .await
                .map_err(|e| NexusError::Connector(format!("iceberg load_table failed: {e}")))?
        } else {
            let schema = arrow_schema_to_schema_auto_assign_ids(&batch.schema()).map_err(|e| {
                NexusError::Schema(format!("iceberg arrow_schema_to_schema failed: {e}"))
            })?;
            let format_version = match self.cfg.format_version {
                IcebergFormatVersion::V2 => FormatVersion::V2,
                IcebergFormatVersion::V3 => FormatVersion::V3,
            };
            let creation = TableCreation::builder()
                .name(self.cfg.table_name())
                .schema(schema)
                .format_version(format_version)
                .build();
            catalog
                .create_table(&namespace, creation)
                .await
                .map_err(|e| NexusError::Connector(format!("iceberg create_table failed: {e}")))?
        };

        let batch = if self.cfg.append_only {
            batch
        } else {
            self.dedup_against_existing(&table, batch).await?
        };
        if batch.num_rows() == 0 {
            return Ok(());
        }

        // The writer matches columns to the table's Iceberg schema by field
        // id, stamped as Parquet field-id metadata on each Arrow field — not
        // by position. Our caller's batch has no such metadata (it didn't
        // come from an Iceberg table), so rebuild it against an Arrow schema
        // derived from the table's own Iceberg schema (which carries the ids
        // `arrow_schema_to_schema_auto_assign_ids` just assigned) before
        // writing. Same columns, same order — only the schema/metadata
        // wrapper changes.
        let arrow_schema_with_ids = std::sync::Arc::new(
            schema_to_arrow_schema(table.metadata().current_schema()).map_err(|e| {
                NexusError::Schema(format!("iceberg schema_to_arrow_schema failed: {e}"))
            })?,
        );
        let batch =
            RecordBatch::try_new(arrow_schema_with_ids, batch.columns().to_vec()).map_err(|e| {
                NexusError::Schema(format!("iceberg field-id batch rebuild failed: {e}"))
            })?;

        let location_generator = DefaultLocationGenerator::new(table.metadata()).map_err(|e| {
            NexusError::Connector(format!("iceberg location generator failed: {e}"))
        })?;
        let call_id = self.write_counter.fetch_add(1, Ordering::Relaxed);
        let file_name_generator = DefaultFileNameGenerator::new(
            "data".to_string(),
            Some(call_id.to_string()),
            DataFileFormat::Parquet,
        );
        let parquet_writer_builder = ParquetWriterBuilder::new(
            WriterProperties::default(),
            table.metadata().current_schema().clone(),
        );
        let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            parquet_writer_builder,
            table.file_io().clone(),
            location_generator,
            file_name_generator,
        );
        let data_file_writer_builder = DataFileWriterBuilder::new(rolling_writer_builder);
        let mut writer = data_file_writer_builder
            .build(None)
            .await
            .map_err(|e| NexusError::Connector(format!("iceberg writer init failed: {e}")))?;
        writer
            .write(batch)
            .await
            .map_err(|e| NexusError::Connector(format!("iceberg write failed: {e}")))?;
        let data_files = writer
            .close()
            .await
            .map_err(|e| NexusError::Connector(format!("iceberg writer close failed: {e}")))?;

        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(data_files);
        let tx = action
            .apply(tx)
            .map_err(|e| NexusError::Connector(format!("iceberg apply append failed: {e}")))?;
        tx.commit(&catalog)
            .await
            .map_err(|e| NexusError::Connector(format!("iceberg commit failed: {e}")))?;
        Ok(())
    }

    /// When `primary_key` is configured, drop rows whose key already exists in
    /// the current table snapshot. This makes Iceberg appends idempotent and
    /// prevents duplicate lines on retry/resume (A01).
    ///
    /// **Cost:** scans the entire current snapshot on every write call, so it
    /// trades memory/CPU for idempotency. For very large tables prefer a
    /// dedicated merge-on-read pipeline once iceberg-rust exposes equality
    /// deletes.
    async fn dedup_against_existing(
        &self,
        table: &iceberg::table::Table,
        batch: RecordBatch,
    ) -> Result<RecordBatch, NexusError> {
        let pk_name = match self.cfg.primary_key.as_deref() {
            None | Some("") => return Ok(batch),
            Some(name) => name,
        };

        let scan =
            table.scan().select_all().build().map_err(|e| {
                NexusError::Connector(format!("iceberg dedup scan build failed: {e}"))
            })?;
        let stream = with_timeout(
            self.cfg.timeout_seconds,
            "iceberg dedup scan to_arrow",
            async {
                scan.to_arrow()
                    .await
                    .map_err(|e| NexusError::Connector(format!("iceberg dedup scan failed: {e}")))
            },
        )
        .await?;
        let batches: Vec<RecordBatch> = stream
            .map(|r| {
                r.map_err(|e| NexusError::Connector(format!("iceberg dedup scan read failed: {e}")))
            })
            .try_collect()
            .await?;

        let mut existing = HashSet::new();
        for existing_batch in &batches {
            for key in extract_key_strings(existing_batch, pk_name)? {
                existing.insert(key);
            }
        }

        filter_batch_by_pk(batch, pk_name, &existing)
    }
}

#[async_trait]
impl Sink for IcebergSink {
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<(), NexusError> {
        match split_by_opcode(&batch)? {
            None => self.append(batch).await,
            Some(split) => {
                if split.deletes.num_rows() > 0 {
                    return Err(NexusError::Connector(
                        "iceberg sink: CDC deletes are not supported — iceberg-rust 0.10.0's \
                         Transaction API has no committable row-delta/equality-delete action yet"
                            .to_string(),
                    ));
                }
                self.append(split.upserts).await
            }
        }
    }

    async fn commit_checkpoint(&mut self, _cursor: CheckpointCursor) -> Result<(), NexusError> {
        Ok(())
    }
}

/// Extract every non-null value of the named primary-key column as a `String`,
/// regardless of its Arrow type. Composite keys are not supported yet.
fn extract_key_strings(batch: &RecordBatch, pk_name: &str) -> Result<Vec<String>, NexusError> {
    let col = batch
        .column_by_name(pk_name)
        .ok_or_else(|| NexusError::Schema(format!("primary_key column '{pk_name}' not found")))?;
    Ok(string_values(col))
}

fn string_values(array: &dyn Array) -> Vec<String> {
    use arrow_array::LargeStringArray;
    if let Some(a) = array.as_any().downcast_ref::<StringArray>() {
        (0..a.len())
            .filter(|&i| a.is_valid(i))
            .map(|i| a.value(i).to_string())
            .collect()
    } else if let Some(a) = array.as_any().downcast_ref::<LargeStringArray>() {
        (0..a.len())
            .filter(|&i| a.is_valid(i))
            .map(|i| a.value(i).to_string())
            .collect()
    } else if let Some(a) = array.as_any().downcast_ref::<Int32Array>() {
        (0..a.len())
            .filter(|&i| a.is_valid(i))
            .map(|i| a.value(i).to_string())
            .collect()
    } else if let Some(a) = array.as_any().downcast_ref::<Int64Array>() {
        (0..a.len())
            .filter(|&i| a.is_valid(i))
            .map(|i| a.value(i).to_string())
            .collect()
    } else if let Some(a) = array.as_any().downcast_ref::<UInt32Array>() {
        (0..a.len())
            .filter(|&i| a.is_valid(i))
            .map(|i| a.value(i).to_string())
            .collect()
    } else if let Some(a) = array.as_any().downcast_ref::<UInt64Array>() {
        (0..a.len())
            .filter(|&i| a.is_valid(i))
            .map(|i| a.value(i).to_string())
            .collect()
    } else {
        vec![]
    }
}

/// Keep only rows whose primary-key value is **not** already in `existing`.
fn filter_batch_by_pk(
    batch: RecordBatch,
    pk_name: &str,
    existing: &HashSet<String>,
) -> Result<RecordBatch, NexusError> {
    let pk_col = batch
        .column_by_name(pk_name)
        .ok_or_else(|| NexusError::Schema(format!("primary_key column '{pk_name}' not found")))?;
    let keep: Vec<bool> = string_values(pk_col.as_ref())
        .into_iter()
        .map(|key| !existing.contains(&key))
        .collect();
    let mask = BooleanArray::from(keep);

    let filtered_columns: Vec<arrow_array::ArrayRef> = batch
        .columns()
        .iter()
        .map(|col| {
            filter(col, &mask)
                .map_err(|e| NexusError::Schema(format!("iceberg dedup filter failed: {e}")))
        })
        .collect::<Result<Vec<_>, _>>()?;

    RecordBatch::try_new(batch.schema(), filtered_columns)
        .map_err(|e| NexusError::Schema(format!("iceberg dedup batch rebuild failed: {e}")))
}
