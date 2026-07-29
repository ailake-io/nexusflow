use crate::config::PostgresConnectorConfig;
use crate::driver::open_connection;
use adbc_core::{Connection as _, Statement as _};
use adbc_driver_manager::ManagedConnection;
use arrow_array::RecordBatch;
use async_trait::async_trait;
use nexus_core::{CheckpointCursor, NexusError, Sink};

pub struct PostgresSink {
    connection: ManagedConnection,
    upsert_sql: String,
}

impl PostgresSink {
    /// `columns` must match the column order of every `RecordBatch` passed to
    /// `write_batch` — ADBC binds parameters positionally.
    pub fn connect(cfg: &PostgresConnectorConfig, columns: &[String]) -> Result<Self, NexusError> {
        let connection = open_connection(&cfg.uri)?;
        let upsert_sql = build_upsert_sql(&cfg.table, &cfg.primary_key, columns);
        Ok(Self {
            connection,
            upsert_sql,
        })
    }
}

/// `INSERT ... ON CONFLICT (pk) DO UPDATE` — the idempotency contract every
/// Sink must satisfy (ARCHITECTURE.md §5: checkpointing guarantees
/// at-least-once, so retried batches must not duplicate rows).
fn build_upsert_sql(table: &str, primary_key: &str, columns: &[String]) -> String {
    let placeholders: Vec<String> = (1..=columns.len()).map(|i| format!("${i}")).collect();
    let updates: Vec<String> = columns
        .iter()
        .filter(|c| c.as_str() != primary_key)
        .map(|c| format!("{c} = EXCLUDED.{c}"))
        .collect();

    format!(
        "INSERT INTO {table} ({cols}) VALUES ({vals}) ON CONFLICT ({primary_key}) DO UPDATE SET {upd}",
        cols = columns.join(", "),
        vals = placeholders.join(", "),
        upd = updates.join(", "),
    )
}

#[async_trait]
impl Sink for PostgresSink {
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<(), NexusError> {
        let mut connection = self.connection.clone();
        let sql = self.upsert_sql.clone();

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
        .map_err(|e| NexusError::Connector(format!("blocking task panicked: {e}")))??;

        Ok(())
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
        );

        assert_eq!(
            sql,
            "INSERT INTO events (id, name, score) VALUES ($1, $2, $3) \
             ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, score = EXCLUDED.score"
        );
    }
}
