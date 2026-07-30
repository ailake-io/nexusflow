use arrow_array::{Array, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::DataType;
use nexus_core::NexusError;
use serde_json::{Map, Value};

/// Reverse of `RecordBatchBuilder::from_json_rows` — turns a `RecordBatch`
/// back into row objects so the sink can bind them as ODBC parameters. Kept
/// free of `odbc-api` types so it's testable without a driver — see
/// IMPLEMENTATION_PLAN.md Marco 3.
pub fn batch_to_json_rows(batch: &RecordBatch) -> Result<Vec<Value>, NexusError> {
    let num_rows = batch.num_rows();
    let mut rows = vec![Map::new(); num_rows];

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
