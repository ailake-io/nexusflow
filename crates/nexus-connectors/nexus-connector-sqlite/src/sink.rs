use crate::config::SqliteConnectorConfig;
use crate::driver::open_connection;
use adbc_core::{Connection as _, Statement as _};
use adbc_driver_manager::ManagedConnection;
use arrow_array::RecordBatch;
use async_trait::async_trait;
use nexus_core::{
    project_column, quote_identifier, split_by_opcode, with_timeout, CheckpointCursor, NexusError,
    Sink,
};

pub struct SqliteSink {
    connection: ManagedConnection,
    upsert_sql: String,
    delete_sql: String,
    primary_key: String,
    timeout_seconds: u64,
}

impl SqliteSink {
    /// `columns` must match the column order of every `RecordBatch` passed to
    /// `write_batch` — ADBC binds parameters positionally.
    pub async fn connect(
        cfg: &SqliteConnectorConfig,
        columns: &[String],
    ) -> Result<Self, NexusError> {
        let uri = cfg.connection_url();
        let connection = with_timeout(cfg.timeout_seconds, "sqlite connect", async {
            tokio::task::spawn_blocking(move || open_connection(&uri))
                .await
                .map_err(|e| NexusError::Connector(format!("blocking task panicked: {e}")))?
        })
        .await?;
        let upsert_sql = build_upsert_sql(&cfg.table, &cfg.primary_key, columns)?;
        let delete_sql = build_delete_sql(&cfg.table, &cfg.primary_key)?;
        Ok(Self {
            connection,
            upsert_sql,
            delete_sql,
            primary_key: cfg.primary_key.clone(),
            timeout_seconds: cfg.timeout_seconds,
        })
    }
}

/// SQLite's `INSERT ... ON CONFLICT DO UPDATE` (3.24+) gives the same
/// idempotency contract as Postgres's — see ARCHITECTURE.md §5. Placeholders
/// are plain `?` (SQLite driver quirk, unlike Postgres's numbered `$1`).
fn build_upsert_sql(
    table: &str,
    primary_key: &str,
    columns: &[String],
) -> Result<String, NexusError> {
    let quoted_table = quote_identifier(table)?;
    let quoted_primary_key = quote_identifier(primary_key)?;
    let quoted_columns = columns
        .iter()
        .map(|c| quote_identifier(c))
        .collect::<Result<Vec<_>, _>>()?;

    let placeholders = vec!["?"; quoted_columns.len()];
    let updates: Vec<String> = columns
        .iter()
        .zip(quoted_columns.iter())
        .filter(|(raw, _)| raw.as_str() != primary_key)
        .map(|(_, quoted)| format!("{quoted} = EXCLUDED.{quoted}"))
        .collect();

    Ok(format!(
        "INSERT INTO {quoted_table} ({cols}) VALUES ({vals}) ON CONFLICT ({quoted_primary_key}) DO UPDATE SET {upd}",
        cols = quoted_columns.join(", "),
        vals = placeholders.join(", "),
        upd = updates.join(", "),
    ))
}

impl SqliteSink {
    async fn execute(&self, sql: &str, batch: RecordBatch) -> Result<(), NexusError> {
        let mut connection = self.connection.clone();
        let sql = sql.to_string();

        with_timeout(self.timeout_seconds, "sqlite execute", async {
            tokio::task::spawn_blocking(move || -> Result<(), NexusError> {
                let mut statement = connection
                    .new_statement()
                    .map_err(|e| NexusError::Connector(e.to_string()))?;
                statement
                    .set_sql_query(&sql)
                    .map_err(|e| NexusError::Connector(e.to_string()))?;
                statement
                    .prepare()
                    .map_err(|e| NexusError::Connector(e.to_string()))?;
                statement
                    .bind(batch)
                    .map_err(|e| NexusError::Connector(e.to_string()))?;
                statement
                    .execute_update()
                    .map_err(|e| NexusError::Connector(e.to_string()))?;
                Ok(())
            })
            .await
            .map_err(|e| NexusError::Connector(format!("blocking task panicked: {e}")))?
        })
        .await?;

        Ok(())
    }
}

fn build_delete_sql(table: &str, primary_key: &str) -> Result<String, NexusError> {
    let quoted_table = quote_identifier(table)?;
    let quoted_primary_key = quote_identifier(primary_key)?;
    Ok(format!(
        "DELETE FROM {quoted_table} WHERE {quoted_primary_key} = ?"
    ))
}

#[async_trait]
impl Sink for SqliteSink {
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<(), NexusError> {
        // CDC batches carry an `__opcode` column (ARCHITECTURE.md §5) — split
        // it so deletes are issued as real `DELETE`s instead of being
        // silently upserted. Plain (non-CDC) batches take the unchanged
        // single upsert path.
        match split_by_opcode(&batch)? {
            None => self.execute(&self.upsert_sql.clone(), batch).await,
            Some(split) => {
                if split.upserts.num_rows() > 0 {
                    self.execute(&self.upsert_sql.clone(), split.upserts)
                        .await?;
                }
                if split.deletes.num_rows() > 0 {
                    let keys = project_column(&split.deletes, &self.primary_key)?;
                    self.execute(&self.delete_sql.clone(), keys).await?;
                }
                Ok(())
            }
        }
    }

    async fn commit_checkpoint(&mut self, _cursor: CheckpointCursor) -> Result<(), NexusError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upsert_sql_uses_plain_question_mark_placeholders() {
        let sql = build_upsert_sql(
            "events",
            "id",
            &["id".to_string(), "name".to_string(), "score".to_string()],
        )
        .unwrap();

        assert_eq!(
            sql,
            "INSERT INTO \"events\" (\"id\", \"name\", \"score\") VALUES (?, ?, ?) \
             ON CONFLICT (\"id\") DO UPDATE SET \"name\" = EXCLUDED.\"name\", \"score\" = EXCLUDED.\"score\""
        );
    }

    #[test]
    fn rejects_sql_injection_in_table_name() {
        let err = build_upsert_sql("events\"; DROP TABLE users; --", "id", &["id".to_string()])
            .expect_err("malicious table name must be rejected");
        assert!(matches!(err, NexusError::Schema(_)));
    }

    #[test]
    fn delete_sql_targets_primary_key() {
        let sql = build_delete_sql("events", "id").unwrap();
        assert_eq!(sql, "DELETE FROM \"events\" WHERE \"id\" = ?");
    }
}
