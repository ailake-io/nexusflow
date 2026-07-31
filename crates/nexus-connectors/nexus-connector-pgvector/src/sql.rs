use nexus_core::{quote_identifier, NexusError};

/// `columns` are the non-embedding data columns (schema order, primary key
/// included) — the embedding itself is always the last bound parameter.
/// Mirrors `nexus-connector-postgres`'s upsert, plus the vector column.
pub fn build_upsert_sql(
    table: &str,
    primary_key: &str,
    columns: &[String],
    embedding_column: &str,
) -> Result<String, NexusError> {
    let quoted_table = quote_identifier(table)?;
    let quoted_primary_key = quote_identifier(primary_key)?;
    let quoted_embedding_column = quote_identifier(embedding_column)?;
    let quoted_columns = columns
        .iter()
        .map(|c| quote_identifier(c))
        .collect::<Result<Vec<_>, _>>()?;

    let all_columns: Vec<String> = quoted_columns
        .iter()
        .cloned()
        .chain(std::iter::once(quoted_embedding_column.clone()))
        .collect();
    let placeholders: Vec<String> = (1..=all_columns.len()).map(|i| format!("${i}")).collect();

    let mut updates: Vec<String> = columns
        .iter()
        .zip(quoted_columns.iter())
        .filter(|(raw, _)| raw.as_str() != primary_key)
        .map(|(_, quoted)| format!("{quoted} = EXCLUDED.{quoted}"))
        .collect();
    updates.push(format!(
        "{quoted_embedding_column} = EXCLUDED.{quoted_embedding_column}"
    ));

    Ok(format!(
        "INSERT INTO {quoted_table} ({cols}) VALUES ({vals}) ON CONFLICT ({quoted_primary_key}) DO UPDATE SET {upd}",
        cols = all_columns.join(", "),
        vals = placeholders.join(", "),
        upd = updates.join(", "),
    ))
}

pub fn build_delete_sql(table: &str, primary_key: &str) -> Result<String, NexusError> {
    let quoted_table = quote_identifier(table)?;
    let quoted_primary_key = quote_identifier(primary_key)?;
    Ok(format!(
        "DELETE FROM {quoted_table} WHERE {quoted_primary_key} = $1"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upsert_sql_binds_embedding_as_last_placeholder() {
        let sql = build_upsert_sql(
            "docs",
            "id",
            &["id".to_string(), "text".to_string()],
            "embedding",
        )
        .unwrap();

        assert_eq!(
            sql,
            "INSERT INTO \"docs\" (\"id\", \"text\", \"embedding\") VALUES ($1, $2, $3) \
             ON CONFLICT (\"id\") DO UPDATE SET \"text\" = EXCLUDED.\"text\", \"embedding\" = EXCLUDED.\"embedding\""
        );
    }

    #[test]
    fn delete_sql_targets_primary_key() {
        let sql = build_delete_sql("docs", "id").unwrap();
        assert_eq!(sql, "DELETE FROM \"docs\" WHERE \"id\" = $1");
    }

    #[test]
    fn rejects_sql_injection_in_table_name() {
        let err = build_upsert_sql(
            "docs\"; DROP TABLE users; --",
            "id",
            &["id".to_string()],
            "embedding",
        )
        .expect_err("malicious table name must be rejected");
        assert!(matches!(err, NexusError::Schema(_)));
    }
}
