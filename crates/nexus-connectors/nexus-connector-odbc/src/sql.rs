use crate::config::OdbcFieldSpec;
use nexus_core::{quote_identifier, NexusError};

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
