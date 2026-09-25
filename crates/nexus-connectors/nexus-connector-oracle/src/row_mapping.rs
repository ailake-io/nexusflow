use arrow_array::{Array, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::DataType;
use nexus_core::NexusError;

/// Formats a cell as a literal Oracle SQL value. Used by the sink because
/// the Oracle Instant Client ODBC driver in this environment fails to
/// execute any statement containing `?` parameter markers (it returns
/// `No Diagnostics`), so we fall back to literal values with proper escaping.
pub(crate) fn cell_to_literal(
    batch: &RecordBatch,
    row: usize,
    col: usize,
) -> Result<String, NexusError> {
    let column = batch.column(col);
    if column.is_null(row) {
        return Ok("NULL".to_string());
    }
    Ok(match column.data_type() {
        DataType::Int64 => {
            let arr = column
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| NexusError::Schema("column has unexpected array type".into()))?;
            arr.value(row).to_string()
        }
        DataType::Float64 => {
            let arr = column
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| NexusError::Schema("column has unexpected array type".into()))?;
            let s = format!("{:.17}", arr.value(row));
            let s = s.trim_end_matches('0').trim_end_matches('.');
            if s.is_empty() { "0".to_string() } else { s.to_string() }
        }
        DataType::Utf8 => {
            let arr = column
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| NexusError::Schema("column has unexpected array type".into()))?;
            format!("'{}'", arr.value(row).replace('\'', "''"))
        }
        other => {
            return Err(NexusError::Schema(format!(
                "oracle sink does not support arrow type {other:?}"
            )))
        }
    })
}
