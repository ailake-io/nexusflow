use arrow_array::{Array, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::DataType;
use nexus_core::NexusError;
use odbc_api::parameter::InputParameter;
use odbc_api::{Bit, IntoParameter, Nullable};

/// Converts a single Arrow cell into an ODBC input parameter,
/// inferring the ODBC type straight from the batch's own Arrow
/// schema — same approach every ODBC connector in this repo uses,
/// since Arrow types come from real catalog introspection
/// (`describe_table`), not a separately-configured type enum. Boolean
/// is bound as `BYTEINT` (`Bit`, 0/1) — Teradata has no native boolean
/// type, but an upstream transform could still hand this sink a
/// boolean column.
pub(crate) fn cell_to_param(
    batch: &RecordBatch,
    row: usize,
    col: usize,
) -> Result<Box<dyn InputParameter>, NexusError> {
    let column = batch.column(col);
    Ok(match column.data_type() {
        DataType::Int64 => {
            let arr = column
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| NexusError::Schema("column has unexpected array type".into()))?;
            let nullable = if arr.is_null(row) {
                Nullable::null()
            } else {
                Nullable::new(arr.value(row))
            };
            Box::new(nullable)
        }
        DataType::Float64 => {
            let arr = column
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| NexusError::Schema("column has unexpected array type".into()))?;
            let nullable = if arr.is_null(row) {
                Nullable::null()
            } else {
                Nullable::new(arr.value(row))
            };
            Box::new(nullable)
        }
        DataType::Boolean => {
            let arr = column
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| NexusError::Schema("column has unexpected array type".into()))?;
            let nullable = if arr.is_null(row) {
                Nullable::null()
            } else {
                Nullable::new(Bit(u8::from(arr.value(row))))
            };
            Box::new(nullable)
        }
        DataType::Utf8 => {
            let arr = column
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| NexusError::Schema("column has unexpected array type".into()))?;
            let opt = if arr.is_null(row) {
                None
            } else {
                Some(arr.value(row).to_owned())
            };
            Box::new(opt.into_parameter())
        }
        other => {
            return Err(NexusError::Schema(format!(
                "teradata sink does not support arrow type {other:?}"
            )))
        }
    })
}
