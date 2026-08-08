use crate::config::CsvConnectorConfig;
use crate::rows::extract_pk_strings;
use crate::schema::{build_schema, delimiter_byte, primary_key_or_err};
use crate::store::open_store;
use arrow_array::{BooleanArray, RecordBatch};
use arrow_csv::{ReaderBuilder, WriterBuilder};
use arrow_schema::SchemaRef;
use arrow_select::filter::filter_record_batch;
use async_trait::async_trait;
use nexus_core::{split_by_opcode, with_timeout, CheckpointCursor, NexusError, Sink};
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, PutPayload};
use std::collections::HashSet;
use std::io::Cursor;
use std::sync::Arc;

/// Delimited text has no update/delete of its own — same trade-off
/// `nexus-connector-parquet` makes: `write_batch` reads the whole existing
/// file, drops rows whose primary key is being upserted-over or deleted,
/// appends the new upserts, and rewrites the whole object. `ObjectStore::put`
/// is a single atomic whole-object write on every backend (S3/GCS/Azure
/// replace the object in one call; the local backend stages to a temp file
/// and renames), so there's no separate temp-file dance to do here.
pub struct CsvSink {
    store: Arc<dyn ObjectStore>,
    path: ObjectPath,
    schema: SchemaRef,
    delimiter: u8,
    has_header: bool,
    primary_key: String,
    timeout_seconds: u64,
}

impl CsvSink {
    pub fn connect(cfg: &CsvConnectorConfig) -> Result<Self, NexusError> {
        let primary_key = primary_key_or_err(cfg)?;
        let (store, path) = open_store(&cfg.uri, &cfg.storage_options)?;
        Ok(Self {
            store,
            path,
            schema: build_schema(&cfg.fields),
            delimiter: delimiter_byte(cfg.delimiter)?,
            has_header: cfg.has_header,
            primary_key,
            timeout_seconds: cfg.timeout_seconds,
        })
    }

    async fn read_existing(&self) -> Result<Option<Vec<RecordBatch>>, NexusError> {
        let bytes = match self.store.get(&self.path).await {
            Ok(result) => result
                .bytes()
                .await
                .map_err(|e| NexusError::Connector(format!("csv read body failed: {e}")))?,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(e) => return Err(NexusError::Connector(format!("csv get failed: {e}"))),
        };

        let schema = self.schema.clone();
        let delimiter = self.delimiter;
        let has_header = self.has_header;
        let batches =
            tokio::task::spawn_blocking(move || -> Result<Vec<RecordBatch>, NexusError> {
                let reader = ReaderBuilder::new(schema)
                    .with_delimiter(delimiter)
                    .with_header(has_header)
                    .build(Cursor::new(bytes))
                    .map_err(|e| NexusError::Connector(format!("csv reader build failed: {e}")))?;
                reader
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| NexusError::Connector(format!("csv parse failed: {e}")))
            })
            .await
            .map_err(|e| NexusError::Connector(format!("blocking task panicked: {e}")))??;
        Ok(Some(batches))
    }

    fn write_all(&self, row_groups: &[RecordBatch]) -> Result<Vec<u8>, NexusError> {
        let mut buf = Vec::new();
        {
            let mut writer = WriterBuilder::new()
                .with_delimiter(self.delimiter)
                .with_header(self.has_header)
                .build(&mut buf);
            for batch in row_groups {
                if batch.num_rows() == 0 {
                    continue;
                }
                writer
                    .write(batch)
                    .map_err(|e| NexusError::Connector(format!("csv write failed: {e}")))?;
            }
        }
        Ok(buf)
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

        let mut row_groups = Vec::new();
        if let Some(batches) = existing {
            for batch in batches {
                let filtered = if remove.is_empty() {
                    batch
                } else {
                    let pk_values = extract_pk_strings(&batch, &self.primary_key)?;
                    let keep: Vec<bool> = pk_values.iter().map(|v| !remove.contains(v)).collect();
                    filter_record_batch(&batch, &BooleanArray::from(keep))
                        .map_err(|e| NexusError::Schema(format!("csv delete filter failed: {e}")))?
                };
                if filtered.num_rows() > 0 {
                    row_groups.push(filtered);
                }
            }
        }
        if upserts.num_rows() > 0 {
            row_groups.push(upserts);
        }

        let bytes = self.write_all(&row_groups)?;
        with_timeout(self.timeout_seconds, "csv put", async {
            self.store
                .put(&self.path, PutPayload::from(bytes))
                .await
                .map_err(|e| NexusError::Connector(format!("csv put failed: {e}")))
        })
        .await?;
        Ok(())
    }
}

#[async_trait]
impl Sink for CsvSink {
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<(), NexusError> {
        // CDC batches carry an `__opcode` column (ARCHITECTURE.md §5) — split
        // it so deletes are issued as real row removals via the
        // read-filter-rewrite path instead of being silently kept. Plain
        // (non-CDC) batches take the unchanged all-upsert path.
        match split_by_opcode(&batch)? {
            None => {
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
