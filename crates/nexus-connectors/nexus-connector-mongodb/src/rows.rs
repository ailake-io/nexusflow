use arrow_array::{Array, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::DataType;
use mongodb::bson::{Bson, Document};
use nexus_core::NexusError;
use serde_json::{Map, Value};

/// Reverse of `RecordBatchBuilder::from_json_rows` — turns a `RecordBatch`
/// back into row objects so the sink can hand them to `bson::to_document`.
/// Only the 4 primitive types `RecordBatchBuilder` supports round-trip.
/// Kept for tests and any JSON-oriented caller; the sink fast path uses
/// `batch_to_documents` instead.
#[allow(dead_code)]
pub fn batch_to_json_rows(batch: &RecordBatch) -> Result<Vec<Value>, NexusError> {
    let num_rows = batch.num_rows();
    let mut rows = vec![Map::with_capacity(batch.num_columns()); num_rows];

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

/// Converts a `RecordBatch` directly into MongoDB `Document`s, avoiding the
/// intermediate `serde_json::Value` allocation per cell. This is the fast path
/// used by `MongoSink`; `batch_to_json_rows` is kept for tests and any caller
/// that genuinely needs JSON.
pub fn batch_to_documents(batch: &RecordBatch) -> Result<Vec<Document>, NexusError> {
    let num_rows = batch.num_rows();
    let mut rows = vec![Document::new(); num_rows];

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
                        Bson::Null
                    } else {
                        Bson::Int64(arr.value(i))
                    };
                    row.insert(name, value);
                }
            }
            DataType::Float64 => {
                let arr = downcast!(Float64Array);
                for (i, row) in rows.iter_mut().enumerate() {
                    let value = if arr.is_null(i) {
                        Bson::Null
                    } else {
                        Bson::Double(arr.value(i))
                    };
                    row.insert(name, value);
                }
            }
            DataType::Boolean => {
                let arr = downcast!(BooleanArray);
                for (i, row) in rows.iter_mut().enumerate() {
                    let value = if arr.is_null(i) {
                        Bson::Null
                    } else {
                        Bson::Boolean(arr.value(i))
                    };
                    row.insert(name, value);
                }
            }
            DataType::Utf8 => {
                let arr = downcast!(StringArray);
                for (i, row) in rows.iter_mut().enumerate() {
                    let value = if arr.is_null(i) {
                        Bson::Null
                    } else {
                        Bson::String(arr.value(i).to_string())
                    };
                    row.insert(name, value);
                }
            }
            other => {
                return Err(NexusError::Schema(format!(
                    "unsupported data type for field '{name}': {other:?}"
                )));
            }
        }
    }

    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array as I64;
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
                Arc::new(I64::from(vec![1, 2])),
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
