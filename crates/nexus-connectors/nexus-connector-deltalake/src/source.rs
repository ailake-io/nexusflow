use crate::config::DeltaConnectorConfig;
use crate::rows::{delta_schema_to_arrow, normalize_batch};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use deltalake::{open_table, open_table_with_storage_options};
use deltalake::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use deltalake::table::builder::ensure_table_uri;
use futures::stream::{self, BoxStream};
use nexus_core::{with_timeout, NexusError, Source};
use std::fs::File;

/// Delta Lake source — reads every currently active data file of the table
/// back as `RecordBatch`es. Goes straight to the table's physical Parquet
/// files via `deltalake::parquet` (re-exported, so guaranteed to be the same
/// `arrow-array`/`parquet` version deltalake itself uses — no separate
/// dependency, no version bridge needed) rather than deltalake's own query
/// engine — CDC deletes rewrite affected files by default (no deletion
/// vectors enabled), so the current file list already reflects live data.
pub struct DeltaSource {
    table_uri: String,
    schema: SchemaRef,
    timeout_seconds: u64,
}

/// Strips a `file://` scheme some log stores prepend — `File::open` needs a
/// plain filesystem path either way.
fn strip_file_scheme(uri: &str) -> &str {
    uri.strip_prefix("file://").unwrap_or(uri)
}

fn read_file_batches(
    uri: &str,
    target_schema: &SchemaRef,
) -> Result<Vec<Result<RecordBatch, NexusError>>, NexusError> {
    let file = File::open(strip_file_scheme(uri))
        .map_err(|e| NexusError::Connector(format!("delta file open failed: {e}")))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| NexusError::Connector(format!("delta parquet reader build failed: {e}")))?;
    let reader = builder
        .build()
        .map_err(|e| NexusError::Connector(format!("delta parquet reader build failed: {e}")))?;
    Ok(reader
        .map(|r| {
            let batch =
                r.map_err(|e| NexusError::Connector(format!("delta parquet read failed: {e}")))?;
            normalize_batch(batch, target_schema)
        })
        .collect())
}

impl DeltaSource {
    pub async fn connect(cfg: &DeltaConnectorConfig) -> Result<Self, NexusError> {
        let table_uri = cfg.table_uri();
        let url = ensure_table_uri(&table_uri)
            .map_err(|e| NexusError::Connector(format!("delta table uri invalid: {e}")))?;
        let storage_options = cfg.storage_options();
        let table = with_timeout(cfg.timeout_seconds, "delta open_table", async {
            if storage_options.is_empty() {
                open_table(url).await
            } else {
                open_table_with_storage_options(url, storage_options).await
            }
            .map_err(|e| NexusError::Connector(format!("delta open_table failed: {e}")))
        })
        .await?;
        // Derived from Delta's own declared table schema (metadata), not any
        // one physical file — see `delta_schema_to_arrow`'s doc for why a
        // file's physical type can't be trusted as the canonical schema.
        let delta_schema = table
            .snapshot()
            .map_err(|e| NexusError::Connector(format!("delta snapshot failed: {e}")))?
            .schema();
        let schema = delta_schema_to_arrow(&delta_schema)?;

        Ok(Self {
            table_uri,
            schema,
            timeout_seconds: cfg.timeout_seconds,
        })
    }
}

#[async_trait]
impl Source for DeltaSource {
    async fn read_batches(
        &mut self,
    ) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError> {
        // Reopen fresh — a table handle opened before a later write/delete
        // doesn't see those commits (same gotcha the Marco 5 LanceDB source
        // hit); this connector always reopens per call.
        let url = ensure_table_uri(&self.table_uri)
            .map_err(|e| NexusError::Connector(format!("delta table uri invalid: {e}")))?;
        let table = with_timeout(self.timeout_seconds, "delta open_table", async {
            open_table(url)
                .await
                .map_err(|e| NexusError::Connector(format!("delta open_table failed: {e}")))
        })
        .await?;
        let uris: Vec<String> = table
            .get_file_uris()
            .map_err(|e| NexusError::Connector(format!("delta get_file_uris failed: {e}")))?
            .collect();

        let mut batches = Vec::new();
        for uri in uris {
            batches.extend(read_file_batches(&uri, &self.schema)?);
        }
        Ok(Box::pin(stream::iter(batches)))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
