use crate::config::CsvConnectorConfig;
use crate::schema::{build_schema, delimiter_byte, infer_schema, quote_byte};
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

/// Reads one or more delimited text files back as `RecordBatch`es —
/// `open_store` resolves `path` to a directory's worth of files (sorted,
/// non-recursive) or a single file, and this reads/parses each in turn
/// with the same shared schema/format, concatenating the results.
/// Whole-file, not streaming from the object store directly — `arrow-csv`'s
/// reader wants a `Read`, and `object_store::GetResult::bytes()` already
/// has to buffer the object fully for any backend that isn't a local file
/// anyway, so there's no partial-read path being given up here.
pub struct CsvSource {
    store: Arc<dyn ObjectStore>,
    paths: Vec<ObjectPath>,
    schema: SchemaRef,
    delimiter: u8,
    quote: u8,
    escape: Option<u8>,
    has_header: bool,
    batch_size: usize,
    timeout_seconds: u64,
}

impl CsvSource {
    pub async fn connect(cfg: &CsvConnectorConfig) -> Result<Self, NexusError> {
        let (store, paths) = open_store(&cfg.uri()?, &cfg.storage_options())?;
        let delimiter = delimiter_byte(cfg.delimiter)?;
        let quote = quote_byte(cfg.quote)?;
        let escape = cfg.escape.map(quote_byte).transpose()?;

        let schema = if cfg.fields.is_empty() {
            let first_path = paths.first().ok_or_else(|| {
                NexusError::Connector("csv source: no files found to infer schema from".into())
            })?;
            let sample = with_timeout(cfg.timeout_seconds, "csv schema sample get", async {
                store
                    .get(first_path)
                    .await
                    .map_err(|e| {
                        NexusError::Connector(format!("csv get '{first_path}' failed: {e}"))
                    })?
                    .bytes()
                    .await
                    .map_err(|e| {
                        NexusError::Connector(format!("csv read body '{first_path}' failed: {e}"))
                    })
            })
            .await?;
            infer_schema(
                &sample,
                delimiter,
                quote,
                escape,
                cfg.has_header,
                cfg.schema_sample_rows,
            )?
        } else {
            build_schema(&cfg.fields)
        };

        Ok(Self {
            store,
            paths,
            schema,
            delimiter,
            quote,
            escape,
            has_header: cfg.has_header,
            batch_size: cfg.batch_size,
            timeout_seconds: cfg.timeout_seconds,
        })
    }

    async fn read_one(&self, path: &ObjectPath) -> Result<Vec<RecordBatch>, NexusError> {
        let bytes = with_timeout(self.timeout_seconds, "csv get", async {
            self.store
                .get(path)
                .await
                .map_err(|e| NexusError::Connector(format!("csv get '{path}' failed: {e}")))?
                .bytes()
                .await
                .map_err(|e| NexusError::Connector(format!("csv read body '{path}' failed: {e}")))
        })
        .await?;

        let schema = self.schema.clone();
        let delimiter = self.delimiter;
        let quote = self.quote;
        let escape = self.escape;
        let has_header = self.has_header;
        let batch_size = self.batch_size;
        let path_owned = path.to_string();
        tokio::task::spawn_blocking(move || -> Result<Vec<RecordBatch>, NexusError> {
            let mut builder = ReaderBuilder::new(schema)
                .with_delimiter(delimiter)
                .with_header(has_header)
                .with_batch_size(batch_size)
                .with_quote(quote);
            if let Some(escape) = escape {
                builder = builder.with_escape(escape);
            }
            let reader = builder.build(Cursor::new(bytes)).map_err(|e| {
                NexusError::Connector(format!("csv reader build '{path_owned}' failed: {e}"))
            })?;
            reader
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| NexusError::Connector(format!("csv parse '{path_owned}' failed: {e}")))
        })
        .await
        .map_err(|e| NexusError::Connector(format!("blocking task panicked: {e}")))?
    }
}

#[async_trait]
impl Source for CsvSource {
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
