use crate::config::ClickHouseConnectorConfig;
use crate::driver::open_connection;
use adbc_core::{Connection as _, Statement as _};
use adbc_driver_manager::ManagedConnection;
use arrow_array::RecordBatch;
use async_trait::async_trait;
use nexus_core::quote_identifier;
use nexus_core::{split_by_opcode, with_timeout, CheckpointCursor, NexusError, Sink};

pub struct ClickHouseSink {
    connection: ManagedConnection,
    insert_sql: String,
    timeout_seconds: u64,
}

impl ClickHouseSink {
    /// `columns` must match the column order of every `RecordBatch` passed to
    /// `write_batch` — ADBC binds parameters positionally.
    pub async fn connect(
        cfg: &ClickHouseConnectorConfig,
        columns: &[String],
    ) -> Result<Self, NexusError> {
        let uri = cfg.connection_string();
        let connection = with_timeout(cfg.timeout_seconds, "clickhouse connect", async {
            tokio::task::spawn_blocking(move || open_connection(&uri))
                .await
                .map_err(|e| NexusError::Connector(format!("blocking task panicked: {e}")))?
        })
        .await?;
        let insert_sql = build_insert_sql(&cfg.table, columns)?;
        Ok(Self {
            connection,
            insert_sql,
            timeout_seconds: cfg.timeout_seconds,
        })
    }
}

/// Plain `INSERT INTO table (...) VALUES (...)` — ClickHouse has no
/// lightweight `ON CONFLICT`/`MERGE` equivalent (`ALTER TABLE ... UPDATE`/
/// `DELETE` are heavyweight async mutations, unsuitable for per-batch
/// writes). This sink is append-only by design: idempotent dedup on
/// re-delivery is the user's responsibility via a `ReplacingMergeTree`/
/// `CollapsingMergeTree` table engine, ClickHouse's own idiomatic mechanism
/// for this — not something this sink can or should paper over.
///
/// `table` and every entry in `columns` come from the pipeline spec
/// (attacker-controlled request body) and get spliced into SQL text — ADBC's
/// `bind` only covers row *values*, not identifiers. Every one of them is
/// validated and quoted via `quote_identifier` before that happens; this is
/// the only place allowed to build the insert SQL.
fn build_insert_sql(table: &str, columns: &[String]) -> Result<String, NexusError> {
    let quoted_table = quote_identifier(table)?;
    let quoted_columns = columns
        .iter()
        .map(|c| quote_identifier(c))
        .collect::<Result<Vec<_>, _>>()?;
    let placeholders: Vec<String> = (1..=quoted_columns.len())
        .map(|i| format!("${i}"))
        .collect();

    Ok(format!(
        "INSERT INTO {quoted_table} ({cols}) VALUES ({vals})",
        cols = quoted_columns.join(", "),
        vals = placeholders.join(", "),
    ))
}

impl ClickHouseSink {
    async fn execute(&self, batch: RecordBatch) -> Result<(), NexusError> {
        let mut connection = self.connection.clone();
        let sql = self.insert_sql.clone();

        with_timeout(self.timeout_seconds, "clickhouse execute", async {
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
impl Sink for ClickHouseSink {
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<(), NexusError> {
        // CDC batches carry an `__opcode` column (ARCHITECTURE.md §5). This
        // sink has no DELETE path (see build_insert_sql's doc comment) — a
        // batch that actually contains deletes is rejected loudly instead of
        // silently upserting or dropping rows.
        match split_by_opcode(&batch)? {
            None => self.execute(batch).await,
            Some(split) => {
                if split.deletes.num_rows() > 0 {
                    return Err(NexusError::Connector(format!(
                        "clickhouse sink received {} delete row(s) via CDC __opcode, but \
                         ClickHouse has no lightweight DELETE — this connector is append-only \
                         (use a ReplacingMergeTree/CollapsingMergeTree table engine for dedup \
                         instead of routing deletes through this sink)",
                        split.deletes.num_rows()
                    )));
                }
                if split.upserts.num_rows() > 0 {
                    self.execute(split.upserts).await?;
                }
                Ok(())
            }
        }
    }

    async fn commit_checkpoint(&mut self, _cursor: CheckpointCursor) -> Result<(), NexusError> {
        // Persisting the cursor is nexus-server's job (SQLite checkpoint
        // store) — same as every other Sink.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_sql_lists_every_column_as_a_positional_placeholder() {
        let sql = build_insert_sql(
            "events",
            &["id".to_string(), "name".to_string(), "score".to_string()],
        )
        .unwrap();

        assert_eq!(
            sql,
            "INSERT INTO \"events\" (\"id\", \"name\", \"score\") VALUES ($1, $2, $3)"
        );
    }

    #[test]
    fn rejects_sql_injection_in_table_name() {
        let err = build_insert_sql("events\"; DROP TABLE users; --", &["id".to_string()])
            .expect_err("malicious table name must be rejected");
        assert!(matches!(err, NexusError::Schema(_)));
    }

    #[test]
    fn rejects_sql_injection_in_column_name() {
        let err = build_insert_sql(
            "events",
            &["id".to_string(), "score); DROP TABLE users; --".to_string()],
        )
        .expect_err("malicious column name must be rejected");
        assert!(matches!(err, NexusError::Schema(_)));
    }
}
