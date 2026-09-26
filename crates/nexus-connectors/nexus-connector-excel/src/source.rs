use crate::config::ExcelConnectorConfig;
use crate::schema::{build_schema, infer_schema, parse_range_to_batch, resolve_sheet};
use crate::store::open_store;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use calamine::{Reader, Xlsx};
use futures::stream::{self, BoxStream};
use nexus_core::{with_timeout, NexusError, Source};
use std::io::Cursor;

/// Reads a whole `.xlsx` sheet back as a single `RecordBatch`. Whole-file,
/// not streaming — same trade-off `nexus-connector-csv`'s `CsvSource`
/// makes (a spreadsheet is a single archive, not line-delimited).
///
/// Unlike `CsvSource`, the fetch + parse happens eagerly in `connect()`
/// rather than lazily in `read_batches()`: `Source::schema()` is
/// synchronous and gets called *before* `read_batches()` by
/// `PipelineEngine` (`nexus-core/src/pipeline.rs`), and schema inference
/// (`ExcelConnectorConfig.fields` empty — see `schema.rs`) needs the
/// parsed sheet to exist already, unlike `csv` where the schema always
/// comes from the config alone.
pub struct ExcelSource {
    schema: SchemaRef,
    batch: RecordBatch,
}

impl ExcelSource {
    pub async fn connect(cfg: &ExcelConnectorConfig) -> Result<Self, NexusError> {
        let (store, path) = open_store(&cfg.uri()?, &cfg.storage_options())?;
        let bytes = with_timeout(cfg.timeout_seconds, "excel get", async {
            store
                .get(&path)
                .await
                .map_err(|e| NexusError::Connector(format!("excel get failed: {e}")))?
                .bytes()
                .await
                .map_err(|e| NexusError::Connector(format!("excel read body failed: {e}")))
        })
        .await?;

        let has_header = cfg.has_header;
        let sheet_name = cfg.sheet_name.clone();
        let sheet_index = cfg.sheet_index;
        let explicit_fields = cfg.fields.clone();
        let sample_rows = cfg.schema_sample_rows;

        let (schema, batch) =
            tokio::task::spawn_blocking(move || -> Result<(SchemaRef, RecordBatch), NexusError> {
                let mut workbook: Xlsx<_> = Xlsx::new(Cursor::new(bytes))
                    .map_err(|e| NexusError::Connector(format!("excel open failed: {e}")))?;
                let sheet_names = workbook.sheet_names().to_owned();
                let sheet =
                    resolve_sheet(&sheet_names, sheet_name.as_deref(), sheet_index)?.to_string();
                let range = workbook
                    .worksheet_range(&sheet)
                    .map_err(|e| NexusError::Connector(format!("excel sheet read failed: {e}")))?;

                let schema = if explicit_fields.is_empty() {
                    infer_schema(&range, has_header, sample_rows)
                } else {
                    build_schema(&explicit_fields)
                };
                let batch = parse_range_to_batch(&range, schema.clone(), has_header)?;
                Ok((schema, batch))
            })
            .await
            .map_err(|e| NexusError::Connector(format!("blocking task panicked: {e}")))??;

        Ok(Self { schema, batch })
    }
}

#[async_trait]
impl Source for ExcelSource {
    async fn read_batches(
        &mut self,
    ) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError> {
        Ok(Box::pin(stream::once(async { Ok(self.batch.clone()) })))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
