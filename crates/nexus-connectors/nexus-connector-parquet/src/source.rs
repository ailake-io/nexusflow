use crate::config::ParquetConnectorConfig;
use crate::store::open_store;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, BoxStream};
use nexus_core::{NexusError, Source};
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::sync::Arc;

/// Pure Parquet source — reads every row group of the configured file(s)
/// back as `RecordBatch`es. `open_store` resolves `path` to a directory's
/// worth of files (sorted, non-recursive) or a single file; every file is
/// read/concatenated against the schema derived from the first one, erroring
/// clearly (naming the file) if a later file's schema doesn't match. Each
/// file already reflects the sink's current state (CDC deletes are applied
/// by rewriting it — see `sink.rs`), so no filtering is needed here. Works
/// for both local paths and cloud object storage.
pub struct ParquetSource {
    store: Arc<dyn ObjectStore>,
    paths: Vec<ObjectPath>,
    schema: SchemaRef,
}

impl ParquetSource {
    pub async fn connect(cfg: &ParquetConnectorConfig) -> Result<Self, NexusError> {
        let (store, paths) = open_store(&cfg.uri()?, &cfg.storage_options())?;
        let first = paths.first().ok_or_else(|| {
            NexusError::Connector("parquet: open_store returned no path".to_string())
        })?;

        // Read enough metadata to determine the schema, from the first
        // resolved file. The full file is downloaded for plain Parquet;
        // this matches the read-filter-rewrite semantics of the sink and
        // keeps the implementation simple.
        let bytes = store
            .get(first)
            .await
            .map_err(|e| {
                NexusError::Connector(format!("parquet source get '{first}' failed: {e}"))
            })?
            .bytes()
            .await
            .map_err(|e| {
                NexusError::Connector(format!("parquet source read '{first}' failed: {e}"))
            })?;

        let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes.to_vec()))
            .map_err(|e| NexusError::Connector(format!("parquet reader build failed: {e}")))?;
        let schema = builder.schema().clone();

        Ok(Self {
            store,
            paths,
            schema,
        })
    }

    async fn read_one(&self, path: &ObjectPath) -> Result<Vec<RecordBatch>, NexusError> {
        let bytes = self
            .store
            .get(path)
            .await
            .map_err(|e| NexusError::Connector(format!("parquet source get '{path}' failed: {e}")))?
            .bytes()
            .await
            .map_err(|e| {
                NexusError::Connector(format!("parquet source read '{path}' failed: {e}"))
            })?;

        let expected_schema = self.schema.clone();
        let path_owned = path.to_string();
        tokio::task::spawn_blocking(move || -> Result<Vec<RecordBatch>, NexusError> {
            let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes.to_vec()))
                .map_err(|e| {
                    NexusError::Connector(format!(
                        "parquet reader build '{path_owned}' failed: {e}"
                    ))
                })?;
            if builder.schema().as_ref() != expected_schema.as_ref() {
                return Err(NexusError::Schema(format!(
                    "parquet: '{path_owned}' schema does not match the first file's schema"
                )));
            }
            let reader: ParquetRecordBatchReader = builder.build().map_err(|e| {
                NexusError::Connector(format!("parquet reader build '{path_owned}' failed: {e}"))
            })?;
            reader.collect::<Result<Vec<_>, _>>().map_err(|e| {
                NexusError::Connector(format!("parquet read '{path_owned}' failed: {e}"))
            })
        })
        .await
        .map_err(|e| NexusError::Connector(format!("blocking task panicked: {e}")))?
    }
}

#[async_trait]
impl Source for ParquetSource {
    async fn read_batches(
        &mut self,
    ) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError> {
        let mut all_batches = Vec::new();
        for path in &self.paths {
            all_batches.extend(self.read_one(path).await?);
        }
        Ok(Box::pin(stream::iter(all_batches.into_iter().map(Ok))))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
