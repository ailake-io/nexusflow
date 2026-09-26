use nexus_core::{quote_identifier, NexusError};

/// Kept free of `odbc-api` types so it's testable without a driver —
/// same rule every ODBC connector in this repo documents.
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

/// Teradata has no single-statement `UPSERT`/`MERGE` the way HANA
/// does — the real, documented idiom (Teradata SQL Data Manipulation
/// Language manual) is a combined `UPDATE ... ELSE INSERT ...`
/// request: the `UPDATE` runs first, and only if it affects zero rows
/// does the `INSERT` run, both against the same primary index value,
/// in the same request. **Not confirmed against a real installation
/// in this session** — same honesty flag every ODBC connector's
/// `driver.rs`/`sql.rs` in this repo carries.
///
/// Parameter order returned by this builder (caller must bind in this
/// exact order): every non-primary-key column for the `SET` clause,
/// then the primary key for `UPDATE`'s `WHERE`, then every column
/// (primary key included) in `columns`' original order for the
/// `INSERT`'s `VALUES`.
pub(crate) fn build_upsert_sql(
    table: &str,
    columns: &[String],
    primary_key: &str,
) -> Result<String, NexusError> {
    let quoted_table = quote_identifier(table)?;
    let quoted_pk = quote_identifier(primary_key)?;
    let non_pk_columns: Vec<&String> = columns.iter().filter(|c| *c != primary_key).collect();
    let set_clause = non_pk_columns
        .iter()
        .map(|c| Ok(format!("{} = ?", quote_identifier(c)?)))
        .collect::<Result<Vec<_>, NexusError>>()?
        .join(", ");
    let quoted_columns = columns
        .iter()
        .map(|c| quote_identifier(c))
        .collect::<Result<Vec<_>, _>>()?;
    let placeholders = vec!["?"; quoted_columns.len()].join(", ");

    Ok(format!(
        "UPDATE {quoted_table} SET {set_clause} WHERE {quoted_pk} = ? \
         ELSE INSERT INTO {quoted_table} ({}) VALUES ({placeholders})",
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
    fn upsert_sql_uses_update_else_insert() {
        let sql =
            build_upsert_sql("events", &["id".to_string(), "name".to_string()], "id").unwrap();
        assert_eq!(
            sql,
            "UPDATE \"events\" SET \"name\" = ? WHERE \"id\" = ? \
             ELSE INSERT INTO \"events\" (\"id\", \"name\") VALUES (?, ?)"
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
        let err = build_upsert_sql("events\"; DROP TABLE users; --", &["id".to_string()], "id")
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
