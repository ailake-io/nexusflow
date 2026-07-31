use crate::config::ParquetConnectorConfig;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use nexus_core::{NexusError, Source};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;
use std::path::PathBuf;

/// Pure Parquet source — reads every row group of `path` back as
/// `RecordBatch`es. The file already reflects the sink's current state (CDC
/// deletes are applied by rewriting it — see `sink.rs`), so no filtering is
/// needed here.
pub struct ParquetSource {
    path: PathBuf,
    schema: SchemaRef,
}

impl ParquetSource {
    pub async fn connect(cfg: &ParquetConnectorConfig) -> Result<Self, NexusError> {
        let path = PathBuf::from(&cfg.path);
        let file = File::open(&path)
            .map_err(|e| NexusError::Connector(format!("parquet open failed: {e}")))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| NexusError::Connector(format!("parquet reader build failed: {e}")))?;
        let schema = builder.schema().clone();
        Ok(Self { path, schema })
    }
}

#[async_trait]
impl Source for ParquetSource {
    async fn read_batches(
        &mut self,
    ) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError> {
        let file = File::open(&self.path)
            .map_err(|e| NexusError::Connector(format!("parquet open failed: {e}")))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| NexusError::Connector(format!("parquet reader build failed: {e}")))?;
        let reader = builder
            .build()
            .map_err(|e| NexusError::Connector(format!("parquet reader build failed: {e}")))?;
        let batches: Vec<Result<RecordBatch, NexusError>> = reader
            .map(|r| r.map_err(|e| NexusError::Connector(format!("parquet read failed: {e}"))))
            .collect();
        Ok(Box::pin(stream::iter(batches)))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
