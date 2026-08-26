use crate::config::{OdbcDataType, OdbcFieldSpec};
use nexus_core::{quote_identifier, NexusError};

/// Connector-type -> SQL column type for `CREATE TABLE`, using SQL-92
/// standard type names rather than any one driver's dialect — the most
/// portable choice available across the wildly different databases ODBC
/// fronts (see `OdbcConnectorConfig::fields` doc comment on why read-side
/// introspection here is already best-effort; this is the same trade-off
/// applied to DDL). `VARCHAR(255)` rather than bare `TEXT`/`VARCHAR`: some
/// drivers (older Oracle/SQL Server dialects) require an explicit length.
/// If a given driver rejects these specific types or the length limit is
/// too small for real data, create the table manually instead — this is a
/// convenience for the common case, not a guarantee for every backend.
fn odbc_column_type(data_type: OdbcDataType) -> &'static str {
    match data_type {
        OdbcDataType::Int64 => "INTEGER",
        OdbcDataType::Float64 => "DOUBLE PRECISION",
        OdbcDataType::Boolean => "BOOLEAN",
        OdbcDataType::Utf8 => "VARCHAR(255)",
    }
}

/// `CREATE TABLE IF NOT EXISTS` from the connector's declared `fields` —
/// same "auto-create instead of failing on the first write" convenience as
/// `nexus-connector-postgres`/`nexus-connector-sqlite`/`nexus-connector-mysql`'s
/// equivalent, with the portability caveat in `odbc_column_type`'s doc
/// comment. `IF NOT EXISTS` itself isn't universally supported by every
/// ODBC-fronted database either — best-effort like everything else here.
pub fn build_create_table_sql(
    table: &str,
    primary_key: &str,
    fields: &[OdbcFieldSpec],
) -> Result<String, NexusError> {
    let quoted_table = quote_identifier(table)?;
    let columns = fields
        .iter()
        .map(|f| {
            let quoted_name = quote_identifier(&f.name)?;
            let sql_type = odbc_column_type(f.data_type);
            let pk_suffix = if f.name == primary_key {
                " PRIMARY KEY"
            } else {
                ""
            };
            Ok(format!("{quoted_name} {sql_type}{pk_suffix}"))
        })
        .collect::<Result<Vec<_>, NexusError>>()?;
    Ok(format!(
        "CREATE TABLE IF NOT EXISTS {quoted_table} ({})",
        columns.join(", ")
    ))
}

/// Kept free of `odbc-api` types so it's testable without a driver — see
/// IMPLEMENTATION_PLAN.md Marco 3 (mockable unit tests per connector).
pub fn build_select_sql(table: &str, fields: &[OdbcFieldSpec]) -> Result<String, NexusError> {
    let quoted_table = quote_identifier(table)?;
    let columns = fields
        .iter()
        .map(|f| quote_identifier(&f.name))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(format!("SELECT {} FROM {quoted_table}", columns.join(", ")))
}

/// Row values are bound in this order: every non-`primary_key` field (schema
/// order), then `primary_key` last for the `WHERE` clause — matches
/// [`update_param_order`].
pub fn build_update_sql(
    table: &str,
    primary_key: &str,
    fields: &[OdbcFieldSpec],
) -> Result<String, NexusError> {
    let quoted_table = quote_identifier(table)?;
    let quoted_primary_key = quote_identifier(primary_key)?;
    let assignments = fields
        .iter()
        .filter(|f| f.name != primary_key)
        .map(|f| quote_identifier(&f.name).map(|q| format!("{q} = ?")))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(format!(
        "UPDATE {quoted_table} SET {} WHERE {quoted_primary_key} = ?",
        assignments.join(", ")
    ))
}

/// Field order used to bind `build_update_sql`'s parameters.
pub fn update_param_order<'a>(
    primary_key: &str,
    fields: &'a [OdbcFieldSpec],
) -> Vec<&'a OdbcFieldSpec> {
    let mut order: Vec<&OdbcFieldSpec> = fields.iter().filter(|f| f.name != primary_key).collect();
    if let Some(pk_field) = fields.iter().find(|f| f.name == primary_key) {
        order.push(pk_field);
    }
    order
}

pub fn build_delete_sql(table: &str, primary_key: &str) -> Result<String, NexusError> {
    let quoted_table = quote_identifier(table)?;
    let quoted_primary_key = quote_identifier(primary_key)?;
    Ok(format!(
        "DELETE FROM {quoted_table} WHERE {quoted_primary_key} = ?"
    ))
}

/// Row values are bound in schema order — matches `fields` directly.
pub fn build_insert_sql(table: &str, fields: &[OdbcFieldSpec]) -> Result<String, NexusError> {
    let quoted_table = quote_identifier(table)?;
    let columns = fields
        .iter()
        .map(|f| quote_identifier(&f.name))
        .collect::<Result<Vec<_>, _>>()?;
    let placeholders = vec!["?"; columns.len()].join(", ");

    Ok(format!(
        "INSERT INTO {quoted_table} ({}) VALUES ({placeholders})",
        columns.join(", ")
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::OdbcDataType;

    fn fields() -> Vec<OdbcFieldSpec> {
        vec![
            OdbcFieldSpec {
                name: "id".into(),
                data_type: OdbcDataType::Int64,
                nullable: false,
            },
            OdbcFieldSpec {
                name: "name".into(),
                data_type: OdbcDataType::Utf8,
                nullable: true,
            },
        ]
    }

    #[test]
    fn select_sql_lists_configured_columns_in_order() {
        let sql = build_select_sql("events", &fields()).unwrap();
        assert_eq!(sql, "SELECT \"id\", \"name\" FROM \"events\"");
    }

    #[test]
    fn update_sql_excludes_primary_key_from_assignments() {
        let sql = build_update_sql("events", "id", &fields()).unwrap();
        assert_eq!(sql, "UPDATE \"events\" SET \"name\" = ? WHERE \"id\" = ?");
    }

    #[test]
    fn update_param_order_puts_primary_key_last() {
        let fields = fields();
        let order = update_param_order("id", &fields);
        let names: Vec<&str> = order.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(names, vec!["name", "id"]);
    }

    #[test]
    fn delete_sql_targets_primary_key() {
        let sql = build_delete_sql("events", "id").unwrap();
        assert_eq!(sql, "DELETE FROM \"events\" WHERE \"id\" = ?");
    }

    #[test]
    fn insert_sql_matches_schema_order() {
        let sql = build_insert_sql("events", &fields()).unwrap();
        assert_eq!(
            sql,
            "INSERT INTO \"events\" (\"id\", \"name\") VALUES (?, ?)"
        );
    }

    #[test]
    fn rejects_sql_injection_in_table_name() {
        let err = build_select_sql("events\"; DROP TABLE users; --", &fields())
            .expect_err("malicious table name must be rejected");
        assert!(matches!(err, NexusError::Schema(_)));
    }
}
