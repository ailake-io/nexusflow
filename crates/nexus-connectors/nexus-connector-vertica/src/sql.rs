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

/// Vertica supports a real standard-SQL `MERGE` statement (Vertica SQL
/// Reference, "MERGE Statement") — unlike Teradata, no two-statement
/// `UPDATE ... ELSE INSERT` workaround needed. **Not confirmed against
/// a real installation in this session** — same honesty flag every
/// ODBC connector's `driver.rs`/`sql.rs` in this repo carries.
///
/// Binds each column exactly once, in `columns`' original order — the
/// `USING (SELECT ? AS col, ...)` subquery aliases every bound value
/// once as `src.col`, then both `WHEN MATCHED` and `WHEN NOT MATCHED`
/// branches reference `src.col` instead of re-binding, unlike
/// Teradata's `UPDATE ... ELSE INSERT` idiom (which binds the
/// non-primary-key columns and the primary key twice each).
pub(crate) fn build_upsert_sql(
    table: &str,
    columns: &[String],
    primary_key: &str,
) -> Result<String, NexusError> {
    let quoted_table = quote_identifier(table)?;
    let quoted_pk = quote_identifier(primary_key)?;
    let quoted_columns = columns
        .iter()
        .map(|c| quote_identifier(c))
        .collect::<Result<Vec<_>, _>>()?;

    let select_list = quoted_columns
        .iter()
        .map(|c| format!("? AS {c}"))
        .collect::<Vec<_>>()
        .join(", ");
    let set_clause = columns
        .iter()
        .filter(|c| *c != primary_key)
        .map(|c| {
            let quoted = quote_identifier(c)?;
            Ok(format!("{quoted} = src.{quoted}"))
        })
        .collect::<Result<Vec<_>, NexusError>>()?
        .join(", ");
    let insert_values = quoted_columns
        .iter()
        .map(|c| format!("src.{c}"))
        .collect::<Vec<_>>()
        .join(", ");

    Ok(format!(
        "MERGE INTO {quoted_table} AS tgt \
         USING (SELECT {select_list}) AS src \
         ON tgt.{quoted_pk} = src.{quoted_pk} \
         WHEN MATCHED THEN UPDATE SET {set_clause} \
         WHEN NOT MATCHED THEN INSERT ({}) VALUES ({insert_values})",
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
    fn upsert_sql_uses_merge() {
        let sql =
            build_upsert_sql("events", &["id".to_string(), "name".to_string()], "id").unwrap();
        assert_eq!(
            sql,
            "MERGE INTO \"events\" AS tgt \
             USING (SELECT ? AS \"id\", ? AS \"name\") AS src \
             ON tgt.\"id\" = src.\"id\" \
             WHEN MATCHED THEN UPDATE SET \"name\" = src.\"name\" \
             WHEN NOT MATCHED THEN INSERT (\"id\", \"name\") VALUES (src.\"id\", src.\"name\")"
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
