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

/// Pure Parquet source — reads every row group of the configured file back as
/// `RecordBatch`es. The file already reflects the sink's current state (CDC
/// deletes are applied by rewriting it — see `sink.rs`), so no filtering is
/// needed here. Works for both local paths and cloud object storage.
pub struct ParquetSource {
    store: Arc<dyn ObjectStore>,
    path: ObjectPath,
    schema: SchemaRef,
}

impl ParquetSource {
    pub async fn connect(cfg: &ParquetConnectorConfig) -> Result<Self, NexusError> {
        let (store, path) = open_store(&cfg.uri()?, &cfg.storage_options())?;

        // Read enough metadata to determine the schema. The full file is
        // downloaded for plain Parquet; this matches the read-filter-rewrite
        // semantics of the sink and keeps the implementation simple.
        let bytes = store
            .get(&path)
            .await
            .map_err(|e| NexusError::Connector(format!("parquet source get failed: {e}")))?
            .bytes()
            .await
            .map_err(|e| NexusError::Connector(format!("parquet source read failed: {e}")))?;

        let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes.to_vec()))
            .map_err(|e| NexusError::Connector(format!("parquet reader build failed: {e}")))?;
        let schema = builder.schema().clone();

        Ok(Self {
            store,
            path,
            schema,
        })
    }
}

#[async_trait]
impl Source for ParquetSource {
    async fn read_batches(
        &mut self,
    ) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError> {
        // Download the file bytes on the async executor, then build the
        // synchronous Parquet reader off of it.
        let bytes = self
            .store
            .get(&self.path)
            .await
            .map_err(|e| NexusError::Connector(format!("parquet source get failed: {e}")))?
            .bytes()
            .await
            .map_err(|e| NexusError::Connector(format!("parquet source read failed: {e}")))?;

        let reader =
            tokio::task::spawn_blocking(move || -> Result<ParquetRecordBatchReader, NexusError> {
                let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes.to_vec()))
                    .map_err(|e| {
                        NexusError::Connector(format!("parquet reader build failed: {e}"))
                    })?;
                builder
                    .build()
                    .map_err(|e| NexusError::Connector(format!("parquet reader build failed: {e}")))
            })
            .await
            .map_err(|e| NexusError::Connector(format!("blocking task panicked: {e}")))??;

        Ok(Box::pin(stream::iter(reader.map(|r| {
            r.map_err(|e| NexusError::Connector(format!("parquet read failed: {e}")))
        }))))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
