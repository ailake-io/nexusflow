use crate::config::ParquetConnectorConfig;
use crate::rows::extract_pk_strings;
use arrow_array::{BooleanArray, RecordBatch};
use arrow_schema::SchemaRef;
use arrow_select::filter::filter_record_batch;
use async_trait::async_trait;
use nexus_core::{split_by_opcode, CheckpointCursor, NexusError, Sink};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use std::collections::HashSet;
use std::fs::File;
use std::path::PathBuf;

/// Pure Parquet sink (Marco 6 — `parquet` crate directly, no Delta/Iceberg
/// metadata). Plain Parquet has no update/delete of its own, so
/// `write_batch` implements CDC upsert/delete as read-filter-rewrite: read
/// every existing row group, drop rows whose primary key is being
/// upserted-over or deleted, then append the new upserts and rewrite the
/// whole file. Correct and simple; O(existing rows) per call, acceptable for
/// the batch sizes this connector targets (same trade-off Milvus's
/// delete-then-insert upsert makes in Marco 5).
pub struct ParquetSink {
    path: PathBuf,
    primary_key: String,
}

impl ParquetSink {
    pub fn connect(cfg: &ParquetConnectorConfig) -> Result<Self, NexusError> {
        Ok(Self {
            path: PathBuf::from(&cfg.path),
            primary_key: cfg.primary_key.clone(),
        })
    }

    /// Existing row groups, or `None` if the file doesn't exist yet.
    fn read_existing(&self) -> Result<Option<(SchemaRef, Vec<RecordBatch>)>, NexusError> {
        if !self.path.exists() {
            return Ok(None);
        }
        let file = File::open(&self.path)
            .map_err(|e| NexusError::Connector(format!("parquet open failed: {e}")))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| NexusError::Connector(format!("parquet reader build failed: {e}")))?;
        let schema = builder.schema().clone();
        let reader = builder
            .build()
            .map_err(|e| NexusError::Connector(format!("parquet reader build failed: {e}")))?;
        let batches = reader
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| NexusError::Connector(format!("parquet read failed: {e}")))?;
        Ok(Some((schema, batches)))
    }

    fn write_all(&self, schema: SchemaRef, row_groups: &[RecordBatch]) -> Result<(), NexusError> {
        let file = File::create(&self.path)
            .map_err(|e| NexusError::Connector(format!("parquet create failed: {e}")))?;
        let mut writer = ArrowWriter::try_new(file, schema, None)
            .map_err(|e| NexusError::Connector(format!("parquet writer init failed: {e}")))?;
        for batch in row_groups {
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
        Ok(())
    }

    fn apply(&self, upserts: RecordBatch, deletes: &RecordBatch) -> Result<(), NexusError> {
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

        let existing = self.read_existing()?;
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

        self.write_all(schema, &row_groups)
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
                self.apply(batch, &empty)
            }
            Some(split) => self.apply(split.upserts, &split.deletes),
        }
    }

    async fn commit_checkpoint(&mut self, _cursor: CheckpointCursor) -> Result<(), NexusError> {
        Ok(())
    }
}
