use nexus_core::{quote_identifier, NexusError};

/// Kept free of `odbc-api` types so it's testable without a driver —
/// same rule the Oracle connector's `sql.rs` (this repo) and
/// `nexus-connector-odbc`'s `sql.rs` (public repo) document.
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

/// HANA's native `UPSERT ... WITH PRIMARY KEY` — real, documented
/// syntax (SAP Help Portal), simpler than Oracle's `MERGE ... USING
/// (SELECT ... FROM DUAL)` trick since HANA has upsert built in
/// directly. The primary key column must be included in the column
/// list (HANA requirement), which `columns` (schema order from the
/// incoming `RecordBatch`) always satisfies as long as the batch
/// itself carries the primary key column.
pub(crate) fn build_upsert_sql(table: &str, columns: &[String]) -> Result<String, NexusError> {
    let quoted_table = quote_identifier(table)?;
    let quoted_columns = columns
        .iter()
        .map(|c| quote_identifier(c))
        .collect::<Result<Vec<_>, _>>()?;
    let placeholders = vec!["?"; quoted_columns.len()].join(", ");

    Ok(format!(
        "UPSERT {quoted_table} ({}) VALUES ({placeholders}) WITH PRIMARY KEY",
        quoted_columns.join(", ")
    ))
}

pub(crate) fn build_delete_sql(table: &str, primary_key: &str) -> Result<String, NexusError> {
    let quoted_table = quote_identifier(table)?;
    let quoted_pk = quote_identifier(primary_key)?;
    Ok(format!("DELETE FROM {quoted_table} WHERE {quoted_pk} = ?"))
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
    fn upsert_sql_uses_with_primary_key() {
        let sql = build_upsert_sql("events", &["id".to_string(), "name".to_string()]).unwrap();
        assert_eq!(
            sql,
            "UPSERT \"events\" (\"id\", \"name\") VALUES (?, ?) WITH PRIMARY KEY"
        );
    }

    #[test]
    fn delete_sql_targets_primary_key() {
        assert_eq!(
            build_delete_sql("events", "id").unwrap(),
            "DELETE FROM \"events\" WHERE \"id\" = ?"
        );
    }

    #[test]
    fn rejects_sql_injection_in_table_name() {
        let err = build_upsert_sql("events\"; DROP TABLE users; --", &["id".to_string()])
            .expect_err("malicious table name must be rejected");
        assert!(matches!(err, NexusError::Schema(_)));
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
}
