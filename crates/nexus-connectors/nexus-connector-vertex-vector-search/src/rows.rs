use arrow_array::{Array, FixedSizeListArray, Float32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::DataType;
use nexus_core::NexusError;

/// Reads `column_name` (a `FixedSizeList<Float32>`) as one `Vec<f32>`
/// per row — same helper `nexus-connector-elasticsearch`/`weaviate`
/// (this repo) already use, local copy since it can't be a shared
/// dependency across the public/private repo boundary.
pub(crate) fn extract_embeddings(
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

/// Reads `column_name` as string datapoint IDs — both `Int64` and
/// `Utf8` primary key columns are supported.
pub(crate) fn extract_ids(
    batch: &RecordBatch,
    column_name: &str,
) -> Result<Vec<String>, NexusError> {
    let idx = batch
        .schema()
        .index_of(column_name)
        .map_err(|_| NexusError::Schema(format!("column '{column_name}' not found")))?;
    let column = batch.column(idx);

    match batch.schema().field(idx).data_type() {
        DataType::Int64 => {
            let arr = column
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    NexusError::Schema(format!(
                        "column '{column_name}' declared Int64 is not an Int64Array"
                    ))
                })?;
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
            let arr = column
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    NexusError::Schema(format!(
                        "column '{column_name}' declared Utf8 is not a StringArray"
                    ))
                })?;
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
    fn extract_ids_supports_int64_primary_key() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2]))]).unwrap();
        let ids = extract_ids(&batch, "id").unwrap();
        assert_eq!(ids, vec!["1".to_string(), "2".to_string()]);
    }
}
