use crate::config::PostgresConnectorConfig;
use crate::driver::open_connection;
use adbc_core::{Connection as _, Statement as _};
use adbc_driver_manager::ManagedConnection;
use arrow_array::RecordBatch;
use async_trait::async_trait;
use nexus_core::quote_identifier;
use nexus_core::{
    project_column, split_by_opcode, with_timeout, CheckpointCursor, NexusError, Sink,
};

pub struct PostgresSink {
    connection: ManagedConnection,
    upsert_sql: String,
    delete_sql: String,
    primary_key: String,
    timeout_seconds: u64,
}

impl PostgresSink {
    /// `columns` must match the column order of every `RecordBatch` passed to
    /// `write_batch` — ADBC binds parameters positionally.
    pub async fn connect(
        cfg: &PostgresConnectorConfig,
        columns: &[String],
    ) -> Result<Self, NexusError> {
        let uri = cfg.uri.clone();
        let connection = with_timeout(cfg.timeout_seconds, "postgres connect", async {
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

/// `INSERT ... ON CONFLICT (pk) DO UPDATE` — the idempotency contract every
/// Sink must satisfy (ARCHITECTURE.md §5: checkpointing guarantees
/// at-least-once, so retried batches must not duplicate rows).
///
/// `table`, `primary_key` and every entry in `columns` come from the pipeline
/// spec (attacker-controlled request body) and get spliced into SQL text —
/// ADBC's `bind` only covers row *values*, not identifiers. Every one of them
/// is validated and quoted via `quote_identifier` before that happens; this
/// is the only place allowed to build the upsert SQL.
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

    let placeholders: Vec<String> = (1..=quoted_columns.len())
        .map(|i| format!("${i}"))
        .collect();
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

fn build_delete_sql(table: &str, primary_key: &str) -> Result<String, NexusError> {
    let quoted_table = quote_identifier(table)?;
    let quoted_primary_key = quote_identifier(primary_key)?;
    Ok(format!(
        "DELETE FROM {quoted_table} WHERE {quoted_primary_key} = $1"
    ))
}

impl PostgresSink {
    async fn execute(&self, sql: &str, batch: RecordBatch) -> Result<(), NexusError> {
        let mut connection = self.connection.clone();
        let sql = sql.to_string();

        with_timeout(self.timeout_seconds, "postgres execute", async {
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

#[async_trait]
impl Sink for PostgresSink {
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
        // Persisting the cursor is nexus-server's job (SQLite checkpoint
        // store) — see IMPLEMENTATION_PLAN.md Marco 1. The connector's only
        // idempotency obligation is the upsert above.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upsert_sql_updates_every_column_except_the_primary_key() {
        let sql = build_upsert_sql(
            "events",
            "id",
            &["id".to_string(), "name".to_string(), "score".to_string()],
        )
        .unwrap();

        assert_eq!(
            sql,
            "INSERT INTO \"events\" (\"id\", \"name\", \"score\") VALUES ($1, $2, $3) \
             ON CONFLICT (\"id\") DO UPDATE SET \"name\" = EXCLUDED.\"name\", \"score\" = EXCLUDED.\"score\""
        );
    }

    #[test]
    fn delete_sql_targets_primary_key() {
        let sql = build_delete_sql("events", "id").unwrap();
        assert_eq!(sql, "DELETE FROM \"events\" WHERE \"id\" = $1");
    }

    #[test]
    fn rejects_sql_injection_in_delete_table_name() {
        let err = build_delete_sql("events\"; DROP TABLE users; --", "id")
            .expect_err("malicious table name must be rejected");
        assert!(matches!(err, NexusError::Schema(_)));
    }

    #[test]
    fn rejects_sql_injection_in_table_name() {
        let err = build_upsert_sql("events\"; DROP TABLE users; --", "id", &["id".to_string()])
            .expect_err("malicious table name must be rejected");
        assert!(matches!(err, NexusError::Schema(_)));
    }

    #[test]
    fn rejects_sql_injection_in_column_name() {
        let err = build_upsert_sql(
            "events",
            "id",
            &["id".to_string(), "score); DROP TABLE users; --".to_string()],
        )
        .expect_err("malicious column name must be rejected");
        assert!(matches!(err, NexusError::Schema(_)));
    }
}
