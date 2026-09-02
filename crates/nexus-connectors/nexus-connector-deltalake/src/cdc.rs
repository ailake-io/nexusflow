use crate::config::DeltaCdcConfig;
use crate::rows::delta_schema_to_arrow;
use arrow_array::{Array, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use arrow_select::filter::filter_record_batch;
use async_trait::async_trait;
use deltalake::datafusion::prelude::SessionContext;
use deltalake::delta_datafusion::cdf::scan::DeltaCdfTableProvider;
use deltalake::delta_datafusion::cdf::{CHANGE_TYPE_COL, COMMIT_VERSION_COL};
use deltalake::open_table;
use deltalake::table::builder::ensure_table_uri;
use futures::stream::{self, BoxStream};
use nexus_core::{with_timeout, NexusError, Source, OPCODE_COLUMN};
use std::sync::{Arc, Mutex};

/// Native CDC source for Delta Lake's built-in Change Data Feed (no
/// Debezium/Kafka, no polling loop of our own — `DeltaOps::load_cdf` does
/// the version-range/manifest work). Unlike the streaming Postgres/MongoDB/
/// MySQL CDC sources, this is inherently poll/batch: one call reads
/// everything committed between `starting_version` and the table's current
/// version, then the stream ends — a natural fit for a `schedule`d
/// pipeline, not a long-lived connection. See `ARCHITECTURE.md §7`.
pub struct DeltaCdcSource {
    table_uri: String,
    starting_version: u64,
    schema: SchemaRef,
    timeout_seconds: u64,
    /// Updated by `read_batches` with the highest `_commit_version` seen —
    /// `Source::position_handle`'s backing storage, used by the runner to
    /// persist the resume cursor in `CheckpointCursor.resume_state`.
    position: Arc<Mutex<Option<String>>>,
}

impl DeltaCdcSource {
    pub async fn connect(config: &DeltaCdcConfig) -> Result<Self, NexusError> {
        let table_uri = config.table_uri();
        let url = ensure_table_uri(&table_uri)
            .map_err(|e| NexusError::Connector(format!("delta-cdc table uri invalid: {e}")))?;
        let table = with_timeout(config.timeout_seconds, "delta-cdc open_table", async {
            open_table(url)
                .await
                .map_err(|e| NexusError::Connector(format!("delta-cdc open_table failed: {e}")))
        })
        .await?;
        let delta_schema = table
            .snapshot()
            .map_err(|e| NexusError::Connector(format!("delta-cdc snapshot failed: {e}")))?
            .schema();
        let mut fields: Vec<Field> = delta_schema_to_arrow(&delta_schema)?
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        fields.push(Field::new(OPCODE_COLUMN, DataType::Utf8, false));

        Ok(Self {
            table_uri,
            starting_version: config.starting_version.unwrap_or(0),
            schema: Arc::new(Schema::new(fields)),
            timeout_seconds: config.timeout_seconds,
            position: Arc::new(Mutex::new(None)),
        })
    }
}

/// `"insert"`/`"update_postimage"`/`"delete"` -> our `I`/`U`/`D` convention;
/// `"update_preimage"` (the "before" half of an update) is dropped — same
/// "only emit the after value" convention every other CDC source here
/// follows (Postgres/MongoDB/MySQL).
fn opcode_for_change_type(change_type: &str) -> Option<&'static str> {
    match change_type {
        "insert" => Some("I"),
        "update_postimage" => Some("U"),
        "delete" => Some("D"),
        _ => None,
    }
}

#[async_trait]
impl Source for DeltaCdcSource {
    async fn read_batches(
        &mut self,
    ) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError> {
        let url = ensure_table_uri(&self.table_uri)
            .map_err(|e| NexusError::Connector(format!("delta-cdc table uri invalid: {e}")))?;
        let table = with_timeout(self.timeout_seconds, "delta-cdc open_table", async {
            open_table(url)
                .await
                .map_err(|e| NexusError::Connector(format!("delta-cdc open_table failed: {e}")))
        })
        .await?;

        let builder = table
            .scan_cdf()
            .with_starting_version(self.starting_version);
        let provider = DeltaCdfTableProvider::try_new(builder)
            .map_err(|e| NexusError::Connector(format!("delta-cdc load_cdf failed: {e}")))?;

        let ctx = SessionContext::new();
        let df = ctx
            .read_table(Arc::new(provider))
            .map_err(|e| NexusError::Connector(format!("delta-cdc read_table failed: {e}")))?;
        // DataFusion does not guarantee file/partition scan order matches
        // commit order — without this, rows from a later delete can be
        // returned before an earlier insert of the same key, corrupting
        // any downstream fold-by-key logic. `_commit_version` is always
        // present on every CDF batch (see `CDC_PARTITION_SCHEMA` in
        // deltalake-core), so this sort is always valid.
        let df = df
            .sort(vec![deltalake::datafusion::logical_expr::col(
                COMMIT_VERSION_COL,
            )
            .sort(true, false)])
            .map_err(|e| NexusError::Connector(format!("delta-cdc sort failed: {e}")))?;
        let cdf_batches = with_timeout(self.timeout_seconds, "delta-cdc collect", async {
            df.collect()
                .await
                .map_err(|e| NexusError::Connector(format!("delta-cdc collect failed: {e}")))
        })
        .await?;

        let target_schema = self.schema.clone();
        let mut batches = Vec::with_capacity(cdf_batches.len());
        let mut max_commit_version: Option<u64> = None;
        for batch in cdf_batches {
            if let Ok(version_col) = batch.schema().index_of(COMMIT_VERSION_COL) {
                if let Some(versions) = batch
                    .column(version_col)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                {
                    for i in 0..versions.len() {
                        let v = versions.value(i);
                        max_commit_version = Some(max_commit_version.map_or(v, |m| m.max(v)));
                    }
                }
            }
            if let Some(mapped) = remap_cdf_batch(&batch, &target_schema)? {
                batches.push(mapped);
            }
        }

        if let Some(version) = max_commit_version {
            if let Ok(mut pos) = self.position.lock() {
                *pos = Some(version.to_string());
            }
        }

        Ok(Box::pin(stream::iter(batches.into_iter().map(Ok))))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn position_handle(&self) -> Option<Arc<Mutex<Option<String>>>> {
        Some(self.position.clone())
    }
}

/// Drops `update_preimage` rows, maps `_change_type` to `__opcode`, and
/// projects down to the table's declared columns + `__opcode` — discards
/// `_change_type`/`_commit_version`/`_commit_timestamp` (CDF's own
/// metadata columns, not part of the table schema). `None` when every row
/// in this batch was a dropped `update_preimage`.
fn remap_cdf_batch(
    batch: &RecordBatch,
    target_schema: &SchemaRef,
) -> Result<Option<RecordBatch>, NexusError> {
    let change_type_idx = batch.schema().index_of(CHANGE_TYPE_COL).map_err(|_| {
        NexusError::Schema(format!(
            "delta-cdc batch missing '{CHANGE_TYPE_COL}' column"
        ))
    })?;
    let change_types = batch
        .column(change_type_idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            NexusError::Schema(format!("delta-cdc '{CHANGE_TYPE_COL}' column is not Utf8"))
        })?;

    let mut opcodes = Vec::with_capacity(batch.num_rows());
    let mut keep = Vec::with_capacity(batch.num_rows());
    for i in 0..batch.num_rows() {
        match opcode_for_change_type(change_types.value(i)) {
            Some(op) => {
                opcodes.push(op);
                keep.push(true);
            }
            None => keep.push(false),
        }
    }
    if opcodes.is_empty() {
        return Ok(None);
    }

    let mask = arrow_array::BooleanArray::from(keep);
    let filtered = filter_record_batch(batch, &mask)
        .map_err(|e| NexusError::Schema(format!("delta-cdc filter failed: {e}")))?;

    let data_field_count = target_schema.fields().len() - 1; // minus __opcode
    let mut columns = Vec::with_capacity(target_schema.fields().len());
    for field in target_schema.fields().iter().take(data_field_count) {
        let idx = filtered.schema().index_of(field.name()).map_err(|_| {
            NexusError::Schema(format!(
                "delta-cdc column '{}' missing from CDF batch",
                field.name()
            ))
        })?;
        columns.push(filtered.column(idx).clone());
    }
    columns.push(Arc::new(StringArray::from(opcodes)));

    RecordBatch::try_new(target_schema.clone(), columns)
        .map(Some)
        .map_err(|e| NexusError::Schema(format!("delta-cdc remap failed: {e}")))
}
