use crate::config::ParquetConnectorConfig;
use crate::rows::extract_pk_strings;
use crate::store::open_store;
use arrow_array::{BooleanArray, RecordBatch};
use arrow_schema::SchemaRef;
use arrow_select::filter::filter_record_batch;
use async_trait::async_trait;
use nexus_core::{split_by_opcode, CheckpointCursor, NexusError, Sink};
use bytes::Bytes;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, PutPayload};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::collections::HashSet;
use std::sync::Arc;

/// Pure Parquet sink (Marco 6 — `parquet` crate directly, no Delta/Iceberg
/// metadata). Plain Parquet has no update/delete of its own, so
/// `write_batch` implements CDC upsert/delete as read-filter-rewrite: read
/// every existing row group, drop rows whose primary key is being
/// upserted-over or deleted, then append the new upserts and rewrite the
/// whole file. Writes are performed through `object_store`, so the same code
/// path works for local disk and cloud storage (S3/GCS/Azure).
pub struct ParquetSink {
    store: Arc<dyn ObjectStore>,
    path: ObjectPath,
    primary_key: String,
    compression: parquet::basic::Compression,
    row_group_size: Option<usize>,
}

impl ParquetSink {
    pub fn connect(cfg: &ParquetConnectorConfig) -> Result<Self, NexusError> {
        let (store, path) = open_store(&cfg.uri()?, &cfg.storage_options())?;
        Ok(Self {
            store,
            path,
            primary_key: cfg.primary_key.clone(),
            compression: cfg.compression(),
            row_group_size: cfg.row_group_size,
        })
    }

    /// Existing row groups, or `None` if the object doesn't exist yet.
    async fn read_existing(
        &self,
    ) -> Result<Option<(SchemaRef, Vec<RecordBatch>)>, NexusError> {
        let bytes = match self.store.get(&self.path).await {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(e) => {
                return Err(NexusError::Connector(format!(
                    "parquet sink read existing failed: {e}"
                )))
            }
        };

        let bytes = bytes
            .bytes()
            .await
            .map_err(|e| NexusError::Connector(format!("parquet sink read failed: {e}")))?;

        let (schema, batches) = tokio::task::spawn_blocking(move || {
            let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes.to_vec()))
                .map_err(|e| {
                    NexusError::Connector(format!("parquet reader build failed: {e}"))
                })?;
            let schema = builder.schema().clone();
            let reader = builder
                .build()
                .map_err(|e| NexusError::Connector(format!("parquet reader build failed: {e}")))?;
            let batches = reader
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| NexusError::Connector(format!("parquet read failed: {e}")))?;
            Ok::<_, NexusError>((schema, batches))
        })
        .await
        .map_err(|e| NexusError::Connector(format!("blocking task panicked: {e}")))??;

        Ok(Some((schema, batches)))
    }

    async fn write_all(&self, schema: SchemaRef, row_groups: &[RecordBatch]) -> Result<(), NexusError> {
        let mut props_builder = WriterProperties::builder().set_compression(self.compression);
        if let Some(size) = self.row_group_size {
            props_builder = props_builder.set_max_row_group_size(size);
        }
        let props = props_builder.build();

        let row_groups = row_groups.to_vec();
        let bytes = tokio::task::spawn_blocking(move || -> Result<Vec<u8>, NexusError> {
            let mut buffer = Vec::new();
            let mut writer = ArrowWriter::try_new(&mut buffer, schema, Some(props))
                .map_err(|e| NexusError::Connector(format!("parquet writer init failed: {e}")))?;
            for batch in &row_groups {
                if batch.num_rows() == 0 {
                    continue;
                }
                writer
                    .write(batch)
                    .map_err(|e| NexusError::Connector(format!("parquet write failed: {e}")))?;
            }
            writer
                .close()
                .map_err(|e| NexusError::Connector(format!("parquet close failed: {e}")))?;
            Ok(buffer)
        })
        .await
        .map_err(|e| NexusError::Connector(format!("blocking task panicked: {e}")))??;

        self.store
            .put(&self.path, PutPayload::from(Bytes::from(bytes)))
            .await
            .map_err(|e| NexusError::Connector(format!("parquet store put failed: {e}")))?;
        Ok(())
    }

    async fn apply(&self, upserts: RecordBatch, deletes: &RecordBatch) -> Result<(), NexusError> {
        if upserts.num_rows() == 0 && deletes.num_rows() == 0 {
            return Ok(());
        }
        let upsert_pks = if upserts.num_rows() > 0 {
            extract_pk_strings(&upserts, &self.primary_key)?
        } else {
            vec![]
        };
        let delete_pks = if deletes.num_rows() > 0 {
            extract_pk_strings(deletes, &self.primary_key)?
        } else {
            vec![]
        };
        let remove: HashSet<String> = upsert_pks.into_iter().chain(delete_pks).collect();

        let existing = self.read_existing().await?;
        let schema = match &existing {
            Some((schema, _)) => schema.clone(),
            None => upserts.schema(),
        };

        let mut row_groups = Vec::new();
        if let Some((_, batches)) = existing {
            for batch in batches {
                let filtered = if remove.is_empty() {
                    batch
                } else {
                    let pk_values = extract_pk_strings(&batch, &self.primary_key)?;
                    let keep: Vec<bool> = pk_values.iter().map(|v| !remove.contains(v)).collect();
                    filter_record_batch(&batch, &BooleanArray::from(keep)).map_err(|e| {
                        NexusError::Schema(format!("parquet delete filter failed: {e}"))
                    })?
                };
                if filtered.num_rows() > 0 {
                    row_groups.push(filtered);
                }
            }
        }
        if upserts.num_rows() > 0 {
            row_groups.push(upserts);
        }

        self.write_all(schema, &row_groups).await
    }
}

#[async_trait]
impl Sink for ParquetSink {
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<(), NexusError> {
        // CDC batches carry an `__opcode` column (ARCHITECTURE.md §5) — split
        // it so deletes are issued as real row removals via the
        // read-filter-rewrite path instead of being silently kept. Plain
        // (non-CDC) batches take the unchanged all-upsert path.
        match split_by_opcode(&batch)? {
            None => {
                // Zero-row placeholder with the same schema — no deletes on
                // the plain (non-CDC) path, but `apply` wants one code path.
                let empty = batch.slice(0, 0);
                self.apply(batch, &empty).await
            }
            Some(split) => self.apply(split.upserts, &split.deletes).await,
        }
    }

    async fn commit_checkpoint(&mut self, _cursor: CheckpointCursor) -> Result<(), NexusError> {
        Ok(())
    }
}
