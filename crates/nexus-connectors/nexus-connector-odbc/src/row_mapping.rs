use arrow_array::{Array, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::DataType;
use nexus_core::NexusError;
#[cfg(feature = "legacy")]
use odbc_api::parameter::InputParameter;
#[cfg(feature = "legacy")]
use odbc_api::{Bit, IntoParameter, Nullable};
use serde_json::Value;

#[cfg(feature = "legacy")]
use crate::config::OdbcDataType;

/// Converts a single Arrow cell into an ODBC input parameter. Kept here (with
/// the rest of the row mapping) so it can be unit-tested without a driver.
/// Behind `legacy` — the only caller (`sink.rs`) is too, and `Box<dyn
/// InputParameter>` is an `odbc-api` type, which isn't even a compiled
/// dependency without the feature (see the crate's `lib.rs` doc comment).
#[cfg(feature = "legacy")]
pub(crate) fn cell_to_param(
    batch: &RecordBatch,
    row: usize,
    col: usize,
    data_type: OdbcDataType,
) -> Result<Box<dyn InputParameter>, NexusError> {
    let column = batch.column(col);
    macro_rules! downcast {
        ($ty:ty) => {
            column
                .as_any()
                .downcast_ref::<$ty>()
                .ok_or_else(|| NexusError::Schema("column has unexpected array type".into()))?
        };
    }

    Ok(match data_type {
        OdbcDataType::Int64 => {
            let arr = downcast!(Int64Array);
            let nullable = if arr.is_null(row) {
                Nullable::null()
            } else {
                Nullable::new(arr.value(row))
            };
            Box::new(nullable)
        }
        OdbcDataType::Float64 => {
            let arr = downcast!(Float64Array);
            let nullable = if arr.is_null(row) {
                Nullable::null()
            } else {
                Nullable::new(arr.value(row))
            };
            Box::new(nullable)
        }
        OdbcDataType::Boolean => {
            let arr = downcast!(BooleanArray);
            let nullable = if arr.is_null(row) {
                Nullable::null()
            } else {
                Nullable::new(Bit(u8::from(arr.value(row))))
            };
            Box::new(nullable)
        }
        OdbcDataType::Utf8 => {
            let arr = downcast!(StringArray);
            let opt = if arr.is_null(row) {
                None
            } else {
                Some(arr.value(row).to_owned())
            };
            Box::new(opt.into_parameter())
        }
    })
}

/// Reverse of `RecordBatchBuilder::from_json_rows` — turns a `RecordBatch`
/// back into row objects so the sink can bind them as ODBC parameters. Kept
/// free of `odbc-api` types so it's testable without a driver — see
/// IMPLEMENTATION_PLAN.md Marco 3. Kept for tests; the sink fast path uses
/// `batch_to_param_rows`.
#[allow(dead_code)]
pub fn batch_to_json_rows(batch: &RecordBatch) -> Result<Vec<Value>, NexusError> {
    let num_rows = batch.num_rows();
    let mut rows = vec![serde_json::Map::with_capacity(batch.num_columns()); num_rows];

    for (col_idx, field) in batch.schema().fields().iter().enumerate() {
        let column = batch.column(col_idx);
        let name = field.name();

        macro_rules! downcast {
            ($ty:ty) => {
                column.as_any().downcast_ref::<$ty>().ok_or_else(|| {
                    NexusError::Schema(format!("column '{name}' has unexpected array type"))
                })?
            };
        }

        match field.data_type() {
            DataType::Int64 => {
                let arr = downcast!(Int64Array);
                for (i, row) in rows.iter_mut().enumerate() {
                    let value = if arr.is_null(i) {
                        Value::Null
                    } else {
                        Value::from(arr.value(i))
                    };
                    row.insert(name.clone(), value);
                }
            }
            DataType::Float64 => {
                let arr = downcast!(Float64Array);
                for (i, row) in rows.iter_mut().enumerate() {
                    let value = if arr.is_null(i) {
                        Value::Null
                    } else {
                        Value::from(arr.value(i))
                    };
                    row.insert(name.clone(), value);
                }
            }
            DataType::Boolean => {
                let arr = downcast!(BooleanArray);
                for (i, row) in rows.iter_mut().enumerate() {
                    let value = if arr.is_null(i) {
                        Value::Null
                    } else {
                        Value::from(arr.value(i))
                    };
                    row.insert(name.clone(), value);
                }
            }
            DataType::Utf8 => {
                let arr = downcast!(StringArray);
                for (i, row) in rows.iter_mut().enumerate() {
                    let value = if arr.is_null(i) {
                        Value::Null
                    } else {
                        Value::from(arr.value(i))
                    };
                    row.insert(name.clone(), value);
                }
            }
            other => {
                return Err(NexusError::Schema(format!(
                    "unsupported data type for field '{name}': {other:?}"
                )));
            }
        }
    }

    Ok(rows.into_iter().map(Value::Object).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{Field, Schema};
    use serde_json::json;
    use std::sync::Arc;

    #[test]
    fn round_trips_through_record_batch_builder() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("alice"), None])),
            ],
        )
        .unwrap();

        let rows = batch_to_json_rows(&batch).unwrap();
        assert_eq!(
            rows,
            vec![
                json!({"id": 1, "name": "alice"}),
                json!({"id": 2, "name": null})
            ]
        );
    }
}
