use arrow_schema::{DataType, SchemaRef};
use nexus_core::{quote_identifier, NexusError};

/// Oracle stores unquoted identifiers as uppercase. We uppercase table/column
/// names in generated SQL so they match tables created without quoted names.
/// Quoted lowercase identifiers are a known v1 limitation.
pub(crate) fn oracle_identifier(name: &str) -> Result<String, NexusError> {
    nexus_core::validate_identifier(name)?;
    Ok(name.to_uppercase())
}

/// Kept free of `odbc-api` types so it's testable without a driver —
/// same rule `nexus-connector-odbc`'s `sql.rs` (public repo) documents.
pub(crate) fn build_select_sql(table: &str, columns: &[String]) -> Result<String, NexusError> {
    let quoted_table = quote_identifier(table)?;
    let quoted_columns = columns
        .iter()
        .map(|c| quote_identifier(c))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(format!(
        "SELECT {} FROM {quoted_table}",
        quoted_columns.join(", ")
    ))
}

/// A target table that doesn't exist yet is created from `schema`'s
/// columns/types before the first write, instead of failing with a bare
/// `ORA-00942: table or view does not exist` — same posture as
/// `PostgresSink::connect`'s `build_create_table_sql` (public repo), just
/// called lazily from the first `write_batch` (this sink's `connect`
/// doesn't receive a schema, only the generic enterprise sink-builder
/// macro's `cfg: serde_json::Value` closure does). Oracle has no
/// `CREATE TABLE IF NOT EXISTS` before 23c, so the caller is expected to
/// swallow `ORA-00955` ("name is already used by an existing object")
/// instead of checking existence up front.
pub(crate) fn build_create_table_sql(
    table: &str,
    primary_key: &str,
    schema: &SchemaRef,
) -> Result<String, NexusError> {
    let quoted_table = oracle_identifier(table)?;
    let columns = schema
        .fields()
        .iter()
        .map(|f| {
            let name = oracle_identifier(f.name())?;
            let sql_type = arrow_type_to_oracle(f.data_type());
            let pk_suffix = if f.name() == primary_key {
                " PRIMARY KEY"
            } else {
                ""
            };
            Ok(format!("{name} {sql_type}{pk_suffix}"))
        })
        .collect::<Result<Vec<_>, NexusError>>()?;
    Ok(format!(
        "CREATE TABLE {quoted_table} ({})",
        columns.join(", ")
    ))
}

/// Arrow type -> Oracle column type. Anything not explicitly matched falls
/// back to `VARCHAR2(4000)` — same "never lose the value" posture
/// `arrow_type_to_postgres` (public repo) documents for its `TEXT`
/// fallback.
fn arrow_type_to_oracle(data_type: &DataType) -> &'static str {
    match data_type {
        DataType::Int8 | DataType::Int16 | DataType::UInt8 | DataType::UInt16 => "NUMBER(10)",
        DataType::Int32 | DataType::UInt32 | DataType::Int64 | DataType::UInt64 => "NUMBER(19)",
        DataType::Float16 | DataType::Float32 => "BINARY_FLOAT",
        DataType::Float64 => "BINARY_DOUBLE",
        DataType::Boolean => "NUMBER(1)",
        DataType::Date32 | DataType::Date64 => "DATE",
        DataType::Timestamp(_, _) => "TIMESTAMP",
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => "NUMBER",
        _ => "VARCHAR2(4000)",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn select_sql_lists_columns_in_order() {
        let sql = build_select_sql("events", &["id".into(), "name".into()]).unwrap();
        assert_eq!(sql, "SELECT \"id\", \"name\" FROM \"events\"");
    }

    #[test]
    fn oracle_identifier_uppercases_and_validates() {
        assert_eq!(oracle_identifier("events").unwrap(), "EVENTS");
        assert!(oracle_identifier("events\"; DROP TABLE users; --").is_err());
    }

    #[test]
    fn rejects_sql_injection_in_column_name() {
        let err = build_select_sql(
            "events",
            &["id".to_string(), "x\"; DROP TABLE users; --".to_string()],
        )
        .expect_err("malicious column name must be rejected");
        assert!(matches!(err, NexusError::Schema(_)));
    }

    #[test]
    fn create_table_sql_maps_types_and_marks_primary_key() {
        use arrow_schema::{Field, Schema};
        use std::sync::Arc;

        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Float64, true),
        ]));
        let sql = build_create_table_sql("events", "id", &schema).unwrap();
        assert_eq!(
            sql,
            "CREATE TABLE EVENTS (ID NUMBER(19) PRIMARY KEY, NAME VARCHAR2(4000), SCORE BINARY_DOUBLE)"
        );
    }

    #[test]
    fn create_table_sql_rejects_sql_injection_in_table_name() {
        use arrow_schema::{Field, Schema};
        use std::sync::Arc;

        let schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let err = build_create_table_sql("events\"; DROP TABLE users; --", "id", &schema)
            .expect_err("malicious table name must be rejected");
        assert!(matches!(err, NexusError::Schema(_)));
    }
}
