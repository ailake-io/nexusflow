use arrow_array::{
    Array, BooleanArray, FixedSizeListArray, Float32Array, Float64Array, Int64Array, RecordBatch,
    StringArray,
};
use arrow_schema::DataType;
use nexus_core::NexusError;
use tokio_postgres::types::ToSql;

/// Builds the bound parameters for one row of `batch`'s `columns` (schema
/// order, typed per each field's Arrow `DataType`) — mirrors
/// `nexus-connector-odbc`'s per-row binding style, just for
/// `tokio-postgres` instead of ODBC.
pub fn row_params<'a>(
    batch: &'a RecordBatch,
    columns: &[String],
    row: usize,
) -> Result<Vec<Box<dyn ToSql + Sync + Send + 'a>>, NexusError> {
    columns
        .iter()
        .map(|name| column_value(batch, name, row))
        .collect()
}

fn column_value<'a>(
    batch: &'a RecordBatch,
    name: &str,
    row: usize,
) -> Result<Box<dyn ToSql + Sync + Send + 'a>, NexusError> {
    let idx = batch
        .schema()
        .index_of(name)
        .map_err(|_| NexusError::Schema(format!("column '{name}' not found")))?;
    let column = batch.column(idx);

    macro_rules! downcast {
        ($ty:ty) => {
            column.as_any().downcast_ref::<$ty>().ok_or_else(|| {
                NexusError::Schema(format!("column '{name}' has unexpected array type"))
            })?
        };
    }

    Ok(match batch.schema().field(idx).data_type() {
        DataType::Int64 => {
            let arr = downcast!(Int64Array);
            Box::new((!arr.is_null(row)).then(|| arr.value(row)))
                as Box<dyn ToSql + Sync + Send + 'a>
        }
        DataType::Float64 => {
            let arr = downcast!(Float64Array);
            Box::new((!arr.is_null(row)).then(|| arr.value(row)))
                as Box<dyn ToSql + Sync + Send + 'a>
        }
        DataType::Boolean => {
            let arr = downcast!(BooleanArray);
            Box::new((!arr.is_null(row)).then(|| arr.value(row)))
                as Box<dyn ToSql + Sync + Send + 'a>
        }
        DataType::Utf8 => {
            let arr = downcast!(StringArray);
            // Borrow `&'a str` straight out of the batch instead of
            // `.to_string()`-ing it — the boxed `ToSql` value only needs to
            // live as long as `batch` does, and it never outlives the
            // `write_batch` call that owns it (M1, CLAUDE.md §8.1).
            Box::new((!arr.is_null(row)).then(|| arr.value(row)))
                as Box<dyn ToSql + Sync + Send + 'a>
        }
        other => {
            return Err(NexusError::Schema(format!(
                "unsupported data type for column '{name}': {other:?}"
            )))
        }
    })
}

/// Extracts one `pgvector::Vector` per row from `column_name`, a
/// `FixedSizeList<Float32>` column — the shape `nexus-ai::embedding` appends
/// to a `RecordBatch` (see ARCHITECTURE.md §4.3).
pub fn extract_embeddings(
    batch: &RecordBatch,
    column_name: &str,
) -> Result<Vec<pgvector::Vector>, NexusError> {
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
        embeddings.push(pgvector::Vector::from(floats.values().to_vec()));
    }
    Ok(embeddings)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{Field, Schema};
    use std::sync::Arc;

    #[test]
    fn row_params_reads_typed_columns() {
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

        let params = row_params(&batch, &["id".to_string(), "name".to_string()], 1).unwrap();
        assert_eq!(params.len(), 2);
    }

    #[test]
    fn extract_embeddings_reads_fixed_size_list() {
        let field = Arc::new(Field::new("item", DataType::Float32, false));
        let values: Float32Array = vec![0.1, 0.2, 0.3, 0.4].into();
        let list = FixedSizeListArray::try_new(field, 2, Arc::new(values), None).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "embedding",
            list.data_type().clone(),
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(list)]).unwrap();

        let embeddings = extract_embeddings(&batch, "embedding").unwrap();
        assert_eq!(embeddings.len(), 2);
        assert_eq!(embeddings[0].as_slice(), &[0.1, 0.2]);
        assert_eq!(embeddings[1].as_slice(), &[0.3, 0.4]);
    }
}
