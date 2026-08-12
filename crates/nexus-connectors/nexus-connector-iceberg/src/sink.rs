use crate::catalog;
use crate::config::{IcebergConnectorConfig, IcebergFormatVersion};
use arrow_array::RecordBatch;
use async_trait::async_trait;
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
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

/// Iceberg sink (Marco 6 — `iceberg` crate + `iceberg-catalog-sql`).
/// Append-only: `iceberg` 0.10.0's `Transaction` API has a `fast_append()`
/// action to commit new data files, but no committable row-delta /
/// equality-delete action yet — the low-level `EqualityDeleteFileWriterBuilder`
/// exists (writes a valid delete file), but nothing in the public
/// `Transaction` API wires its output into a snapshot commit. CDC delete
/// batches are therefore rejected with a clear error rather than silently
/// dropped, until iceberg-rust exposes that action.
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
