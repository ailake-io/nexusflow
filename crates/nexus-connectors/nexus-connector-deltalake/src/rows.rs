use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_cast::cast::cast;
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use deltalake::{DataType as DeltaDataType, PrimitiveType, StructField, StructType};
use nexus_core::NexusError;
use std::sync::Arc;

/// Reads `column_name` as primary-key strings — both `Int64` and `Utf8`
/// primary keys are supported (same constraint documented on
/// `nexus-connector-pinecone`/`nexus-connector-ailake`/
/// `nexus-connector-parquet`).
pub fn extract_pk_strings(
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

/// Builds a `col IN (v1, v2, ...)` SQL predicate for `delete().with_predicate(...)`,
/// quoting values when `column_name` is a string column (Delta's predicate
/// parser needs `'a'`, not `a`, for `Utf8`) and leaving them bare for numeric
/// columns.
pub fn in_predicate(
    batch: &RecordBatch,
    column_name: &str,
    values: &[String],
) -> Result<String, NexusError> {
    let idx = batch
        .schema()
        .index_of(column_name)
        .map_err(|_| NexusError::Schema(format!("column '{column_name}' not found")))?;
    let is_string = matches!(batch.schema().field(idx).data_type(), ArrowDataType::Utf8);
    let rendered: Vec<String> = if is_string {
        values.iter().map(|v| format!("'{}'", v.replace('\'', "''"))).collect()
    } else {
        values.to_vec()
    };
    Ok(format!("{column_name} IN ({})", rendered.join(", ")))
}

/// Maps an Arrow schema to Delta's `StructField`s for `CreateBuilder::with_columns` —
/// same small primitive-type set every bridging connector's `RecordBatchBuilder`
/// supports (`ARCHITECTURE.md §2`).
pub fn arrow_schema_to_delta_fields(
    schema: &ArrowSchemaRef,
) -> Result<Vec<StructField>, NexusError> {
    schema
        .fields()
        .iter()
        .map(|f| {
            let primitive = match f.data_type() {
                ArrowDataType::Int64 => PrimitiveType::Long,
                ArrowDataType::Utf8 => PrimitiveType::String,
                ArrowDataType::Float64 => PrimitiveType::Double,
                ArrowDataType::Float32 => PrimitiveType::Float,
                ArrowDataType::Boolean => PrimitiveType::Boolean,
                other => {
                    return Err(NexusError::Schema(format!(
                        "unsupported data type for field '{}': {other:?}",
                        f.name()
                    )));
                }
            };
            Ok(StructField::new(
                f.name().clone(),
                DeltaDataType::Primitive(primitive),
                f.is_nullable(),
            ))
        })
        .collect()
}

/// Reverse of `arrow_schema_to_delta_fields` — derives the canonical Arrow
/// schema from Delta's own declared table schema (metadata, not any one
/// physical file). Needed because a delete-rewrite goes through deltalake's
/// bundled DataFusion internally, which can write a rewritten file's string
/// columns back as `LargeUtf8` instead of the `Utf8` our own
/// `RecordBatchWriter` appends use directly — the two physical types
/// diverge across files in the same table even though Delta's declared
/// column type ("string") never changed. `read_file_batches` casts every
/// batch to this schema so `Source::schema()` and the batches it actually
/// yields never disagree, regardless of which files were touched by a
/// rewrite.
pub fn delta_schema_to_arrow(schema: &StructType) -> Result<ArrowSchemaRef, NexusError> {
    let fields: Vec<ArrowField> = schema
        .fields()
        .map(|f| {
            let arrow_type = match &f.data_type {
                DeltaDataType::Primitive(PrimitiveType::Long) => ArrowDataType::Int64,
                DeltaDataType::Primitive(PrimitiveType::String) => ArrowDataType::Utf8,
                DeltaDataType::Primitive(PrimitiveType::Double) => ArrowDataType::Float64,
                DeltaDataType::Primitive(PrimitiveType::Float) => ArrowDataType::Float32,
                DeltaDataType::Primitive(PrimitiveType::Boolean) => ArrowDataType::Boolean,
                other => {
                    return Err(NexusError::Schema(format!(
                        "unsupported delta data type for field '{}': {other:?}",
                        f.name
                    )));
                }
            };
            Ok(ArrowField::new(f.name.clone(), arrow_type, f.nullable))
        })
        .collect::<Result<_, NexusError>>()?;
    Ok(Arc::new(ArrowSchema::new(fields)))
}

/// Casts `batch` to `target_schema` column-by-column (e.g. `LargeUtf8` ->
/// `Utf8`) — see `delta_schema_to_arrow`'s doc for why this is needed.
/// A no-op per column when the physical type already matches.
pub fn normalize_batch(
    batch: RecordBatch,
    target_schema: &ArrowSchemaRef,
) -> Result<RecordBatch, NexusError> {
    let columns: Vec<ArrayRef> = target_schema
        .fields()
        .iter()
        .map(|target_field| {
            let idx = batch.schema().index_of(target_field.name()).map_err(|_| {
                NexusError::Schema(format!(
                    "column '{}' missing from batch",
                    target_field.name()
                ))
            })?;
            let col = batch.column(idx);
            if col.data_type() == target_field.data_type() {
                Ok(col.clone())
            } else {
                cast(col, target_field.data_type()).map_err(|e| {
                    NexusError::Schema(format!(
                        "cast failed for column '{}': {e}",
                        target_field.name()
                    ))
                })
            }
        })
        .collect::<Result<_, NexusError>>()?;
    RecordBatch::try_new(target_schema.clone(), columns)
        .map_err(|e| NexusError::Schema(format!("normalize_batch failed: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{Field, Schema};
    use std::sync::Arc;

    #[test]
    fn delta_schema_to_arrow_maps_supported_types() {
        let delta_schema = StructType::try_new(vec![
            StructField::new("id", DeltaDataType::Primitive(PrimitiveType::Long), false),
            StructField::new("name", DeltaDataType::Primitive(PrimitiveType::String), true),
        ])
        .unwrap();
        let arrow_schema = delta_schema_to_arrow(&delta_schema).unwrap();
        assert_eq!(
            arrow_schema.field_with_name("id").unwrap().data_type(),
            &ArrowDataType::Int64
        );
        assert_eq!(
            arrow_schema.field_with_name("name").unwrap().data_type(),
            &ArrowDataType::Utf8
        );
    }

    #[test]
    fn normalize_batch_casts_large_utf8_to_utf8() {
        let source_schema = Arc::new(Schema::new(vec![Field::new(
            "status",
            ArrowDataType::LargeUtf8,
            false,
        )]));
        let batch = RecordBatch::try_new(
            source_schema,
            vec![Arc::new(arrow_array::LargeStringArray::from(vec!["paid"]))],
        )
        .unwrap();
        let target_schema = Arc::new(Schema::new(vec![Field::new(
            "status",
            ArrowDataType::Utf8,
            false,
        )]));
        let normalized = normalize_batch(batch, &target_schema).unwrap();
        assert_eq!(normalized.schema().field(0).data_type(), &ArrowDataType::Utf8);
        let col = normalized
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(col.value(0), "paid");
    }

    #[test]
    fn extract_pk_strings_supports_int64_and_utf8() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", ArrowDataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2]))]).unwrap();
        assert_eq!(
            extract_pk_strings(&batch, "id").unwrap(),
            vec!["1".to_string(), "2".to_string()]
        );
    }

    #[test]
    fn in_predicate_quotes_string_columns_only() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", ArrowDataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
        let pred = in_predicate(&batch, "id", &["1".to_string(), "2".to_string()]).unwrap();
        assert_eq!(pred, "id IN (1, 2)");

        let schema = Arc::new(Schema::new(vec![Field::new("id", ArrowDataType::Utf8, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["a"]))]).unwrap();
        let pred = in_predicate(&batch, "id", &["a".to_string(), "b".to_string()]).unwrap();
        assert_eq!(pred, "id IN ('a', 'b')");
    }

    #[test]
    fn arrow_schema_to_delta_fields_maps_supported_types() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", ArrowDataType::Int64, false),
            Field::new("name", ArrowDataType::Utf8, true),
        ]));
        let fields = arrow_schema_to_delta_fields(&schema).unwrap();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].name, "id");
        assert!(!fields[0].nullable);
        assert_eq!(fields[1].name, "name");
        assert!(fields[1].nullable);
    }

    #[test]
    fn arrow_schema_to_delta_fields_rejects_unsupported_type() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            ArrowDataType::Date32,
            false,
        )]));
        let err = arrow_schema_to_delta_fields(&schema).expect_err("Date32 must be rejected");
        assert!(matches!(err, NexusError::Schema(_)));
    }
}
