use arrow_array::{
    Array, BooleanArray, FixedSizeListArray, Float32Array, Float64Array, Int64Array, RecordBatch,
    StringArray,
};
use arrow_schema::DataType;
use nexus_core::NexusError;
use serde_json::{Map, Value};

/// Turns every column of `batch` except `skip` into a JSON payload per row —
/// used as the Qdrant point payload. Same 4-primitive-type support as the
/// other bridging connectors' JSON row helpers.
pub fn batch_to_payloads(batch: &RecordBatch, skip: &[&str]) -> Result<Vec<Value>, NexusError> {
    let num_rows = batch.num_rows();
    let mut rows = vec![Map::new(); num_rows];

    for (col_idx, field) in batch.schema().fields().iter().enumerate() {
        let name = field.name();
        if skip.contains(&name.as_str()) {
            continue;
        }
        let column = batch.column(col_idx);

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

/// Reads `column_name` (a `FixedSizeList<Float32>`) as one `Vec<f32>` per row.
pub fn extract_embeddings(
    batch: &RecordBatch,
    column_name: &str,
) -> Result<Vec<Vec<f32>>, NexusError> {
    let idx = batch
        .schema()
        .index_of(column_name)
        .map_err(|_| NexusError::Schema(format!("column '{column_name}' not found")))?;
    let list = batch
        .column(idx)
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .ok_or_else(|| {
            NexusError::Schema(format!("column '{column_name}' is not a FixedSizeList"))
        })?;

    let mut embeddings = Vec::with_capacity(list.len());
    for row in 0..list.len() {
        let values = list.value(row);
        let floats = values
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| {
                NexusError::Schema(format!("column '{column_name}' items are not Float32"))
            })?;
        embeddings.push(floats.values().to_vec());
    }
    Ok(embeddings)
}

/// Reads `column_name` (an `Int64` primary key) as `u64` point IDs.
pub fn extract_ids(batch: &RecordBatch, column_name: &str) -> Result<Vec<u64>, NexusError> {
    let idx = batch
        .schema()
        .index_of(column_name)
        .map_err(|_| NexusError::Schema(format!("column '{column_name}' not found")))?;
    let arr = batch
        .column(idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| {
            NexusError::Schema(format!(
                "primary key column '{column_name}' must be Int64 for Qdrant point IDs"
            ))
        })?;

    (0..arr.len())
        .map(|i| {
            if arr.is_null(i) {
                Err(NexusError::Schema(format!(
                    "primary key column '{column_name}' cannot be null"
                )))
            } else {
                u64::try_from(arr.value(i))
                    .map_err(|_| NexusError::Schema(format!("negative primary key at row {i}")))
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{Field, Schema};
    use std::sync::Arc;

    #[test]
    fn batch_to_payloads_skips_named_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("text", DataType::Utf8, true),
            Field::new("__opcode", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec![Some("hi")])),
                Arc::new(StringArray::from(vec!["I"])),
            ],
        )
        .unwrap();

        let payloads = batch_to_payloads(&batch, &["__opcode"]).unwrap();
        assert_eq!(payloads.len(), 1);
        let obj = payloads[0].as_object().unwrap();
        assert!(!obj.contains_key("__opcode"));
        assert_eq!(obj["id"], 1);
        assert_eq!(obj["text"], "hi");
    }

    #[test]
    fn extract_ids_rejects_negative_values() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![-1]))]).unwrap();
        let err = extract_ids(&batch, "id").expect_err("negative id must be rejected");
        assert!(matches!(err, NexusError::Schema(_)));
    }
}
