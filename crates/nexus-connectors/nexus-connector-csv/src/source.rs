use crate::config::CsvConnectorConfig;
use crate::schema::{build_schema, delimiter_byte};
use crate::store::open_store;
use arrow_array::RecordBatch;
use arrow_csv::ReaderBuilder;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use nexus_core::{with_timeout, NexusError, Source};
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use std::io::Cursor;
use std::sync::Arc;

/// Reads a whole delimited text file back as `RecordBatch`es. Whole-file,
/// not streaming from the object store directly — `arrow-csv`'s reader
/// wants a `Read`, and `object_store::GetResult::bytes()` already has to
/// buffer the object fully for any backend that isn't a local file anyway,
/// so there's no partial-read path being given up here.
pub struct CsvSource {
    store: Arc<dyn ObjectStore>,
    path: ObjectPath,
    schema: SchemaRef,
    delimiter: u8,
    has_header: bool,
    batch_size: usize,
    timeout_seconds: u64,
}

impl CsvSource {
    pub fn connect(cfg: &CsvConnectorConfig) -> Result<Self, NexusError> {
        let (store, path) = open_store(&cfg.uri, &cfg.storage_options)?;
        Ok(Self {
            store,
            path,
            schema: build_schema(&cfg.fields),
            delimiter: delimiter_byte(cfg.delimiter)?,
            has_header: cfg.has_header,
            batch_size: cfg.batch_size,
            timeout_seconds: cfg.timeout_seconds,
        })
    }
}

#[async_trait]
impl Source for CsvSource {
    async fn read_batches(
        &mut self,
    ) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError> {
        let bytes = with_timeout(self.timeout_seconds, "csv get", async {
            self.store
                .get(&self.path)
                .await
                .map_err(|e| NexusError::Connector(format!("csv get failed: {e}")))?
                .bytes()
                .await
                .map_err(|e| NexusError::Connector(format!("csv read body failed: {e}")))
        })
        .await?;

        let schema = self.schema.clone();
        let delimiter = self.delimiter;
        let has_header = self.has_header;
        let batch_size = self.batch_size;
        let batches =
            tokio::task::spawn_blocking(move || -> Result<Vec<RecordBatch>, NexusError> {
                let reader = ReaderBuilder::new(schema)
                    .with_delimiter(delimiter)
                    .with_header(has_header)
                    .with_batch_size(batch_size)
                    .build(Cursor::new(bytes))
                    .map_err(|e| NexusError::Connector(format!("csv reader build failed: {e}")))?;
                reader
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| NexusError::Connector(format!("csv parse failed: {e}")))
            })
            .await
            .map_err(|e| NexusError::Connector(format!("blocking task panicked: {e}")))??;

        Ok(Box::pin(stream::iter(batches.into_iter().map(Ok))))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
