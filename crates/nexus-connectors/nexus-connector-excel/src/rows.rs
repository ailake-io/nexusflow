use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use nexus_core::NexusError;

/// Reads `column_name` as primary-key strings. Copy of
/// `nexus-connector-csv`'s `extract_pk_strings` — see `store.rs`'s doc
/// comment for why this crate duplicates rather than depends on it.
pub(crate) fn extract_pk_strings(
    batch: &RecordBatch,
    column_name: &str,
) -> Result<Vec<String>, NexusError> {
    let idx = batch
        .schema()
        .index_of(column_name)
        .map_err(|_| NexusError::Schema(format!("column '{column_name}' not found")))?;
    let column = batch.column(idx);

    if let Some(arr) = column.as_any().downcast_ref::<Int64Array>() {
        return Ok((0..arr.len()).map(|i| arr.value(i).to_string()).collect());
    }
    if let Some(arr) = column.as_any().downcast_ref::<StringArray>() {
        return Ok((0..arr.len()).map(|i| arr.value(i).to_string()).collect());
    }
    Err(NexusError::Schema(format!(
        "primary key column '{column_name}' must be Int64 or Utf8"
    )))
}
