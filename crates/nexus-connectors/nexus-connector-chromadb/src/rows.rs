use arrow_array::{
    Array, BooleanArray, FixedSizeListArray, Float32Array, Float64Array, Int64Array, RecordBatch,
    StringArray,
};
use arrow_schema::DataType;
use nexus_core::NexusError;
use serde_json::{Map, Value};

/// Turns every column of `batch` except `skip` into a JSON metadata object
/// per row — Chroma metadata values are limited to string/number/bool,
/// which is exactly what `RecordBatchBuilder`'s 4 supported primitive types
/// map to.
pub fn batch_to_metadata(batch: &RecordBatch, skip: &[&str]) -> Result<Vec<Value>, NexusError> {
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
                    if !arr.is_null(i) {
                        row.insert(name.clone(), Value::from(arr.value(i)));
                    }
                }
            }
            DataType::Float64 => {
                let arr = downcast!(Float64Array);
                for (i, row) in rows.iter_mut().enumerate() {
                    if !arr.is_null(i) {
                        row.insert(name.clone(), Value::from(arr.value(i)));
                    }
                }
            }
            DataType::Boolean => {
                let arr = downcast!(BooleanArray);
                for (i, row) in rows.iter_mut().enumerate() {
                    if !arr.is_null(i) {
                        row.insert(name.clone(), Value::from(arr.value(i)));
                    }
                }
            }
            DataType::Utf8 => {
                let arr = downcast!(StringArray);
                for (i, row) in rows.iter_mut().enumerate() {
                    if !arr.is_null(i) {
                        row.insert(name.clone(), Value::from(arr.value(i)));
                    }
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

/// Reads `column_name` as string vector IDs — Chroma IDs are always
/// strings, so both `Int64` and `Utf8` primary key columns are supported.
pub fn extract_ids(batch: &RecordBatch, column_name: &str) -> Result<Vec<String>, NexusError> {
    let idx = batch
        .schema()
        .index_of(column_name)
        .map_err(|_| NexusError::Schema(format!("column '{column_name}' not found")))?;
    let column = batch.column(idx);

    match batch.schema().field(idx).data_type() {
        DataType::Int64 => {
            let arr = column.as_any().downcast_ref::<Int64Array>().unwrap();
            (0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        Err(NexusError::Schema(format!(
                            "primary key column '{column_name}' cannot be null"
                        )))
                    } else {
                        Ok(arr.value(i).to_string())
                    }
                })
                .collect()
        }
        DataType::Utf8 => {
            let arr = column.as_any().downcast_ref::<StringArray>().unwrap();
            (0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        Err(NexusError::Schema(format!(
                            "primary key column '{column_name}' cannot be null"
                        )))
                    } else {
                        Ok(arr.value(i).to_string())
                    }
                })
                .collect()
        }
        other => Err(NexusError::Schema(format!(
            "primary key column '{column_name}' must be Int64 or Utf8, got {other:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{Field, Schema};
    use std::sync::Arc;

    #[test]
    fn batch_to_metadata_skips_named_columns() {
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

        let metadata = batch_to_metadata(&batch, &["__opcode"]).unwrap();
        assert_eq!(metadata.len(), 1);
        let obj = metadata[0].as_object().unwrap();
        assert!(!obj.contains_key("__opcode"));
        assert_eq!(obj["id"], 1);
        assert_eq!(obj["text"], "hi");
    }

    #[test]
    fn extract_ids_supports_int64_primary_key() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2]))]).unwrap();
        let ids = extract_ids(&batch, "id").unwrap();
        assert_eq!(ids, vec!["1".to_string(), "2".to_string()]);
    }
}
