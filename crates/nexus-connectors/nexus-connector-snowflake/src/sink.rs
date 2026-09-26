use crate::config::SnowflakeConnectorConfig;
use crate::driver::open_connection;
use adbc_core::options::{IngestMode, OptionStatement};
use adbc_core::{Connection as _, Optionable as _, Statement as _};
use adbc_driver_manager::ManagedConnection;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use nexus_core::quote_identifier;
use nexus_core::{
    project_column, retry_with_backoff, split_by_opcode, with_timeout, CheckpointCursor,
    NexusError, Sink,
};

/// Snowflake has no `ON CONFLICT` (Postgres) — upsert here is `MERGE INTO`
/// (standard SQL, Snowflake supports it natively). Bind placeholders are
/// `?` (Snowflake's own SQL uses ODBC/JDBC-style positional placeholders,
/// not Postgres's `$N`) — **unverified against a real driver/account
/// yet**, same caveat as source.rs's `get_table_schema` note; revisit once
/// there's a trial account to test against.
///
/// For plain (non-CDC) inserts, the sink uses the ADBC driver's native
/// bulk-ingest mode (`adbc.ingest.target_table` with mode `append`, backed
/// by Snowflake's own `COPY INTO`). This avoids the `MERGE INTO` overhead
/// for large backfills. CDC batches still go through `MERGE`/`DELETE` so
/// opcodes are handled correctly.
pub struct SnowflakeSink {
    connection: ManagedConnection,
    table: String,
    primary_key: String,
    timeout_seconds: u64,
    retry: nexus_core::RetryConfig,
}

impl SnowflakeSink {
    pub async fn connect(cfg: &SnowflakeConnectorConfig) -> Result<Self, NexusError> {
        cfg.validate()?;
        let primary_key = cfg
            .primary_key
            .clone()
            .ok_or_else(|| NexusError::Schema("snowflake sink requires primary_key".into()))?;
        quote_identifier(&cfg.table)?;
        quote_identifier(&primary_key)?;

        let retry = cfg.retry.clone();
        let connection = retry_with_backoff(&retry, "snowflake connect", || {
            let cfg = cfg.clone();
            async move {
                with_timeout(cfg.timeout_seconds, "snowflake connect", async {
                    tokio::task::spawn_blocking(move || open_connection(&cfg))
                        .await
                        .map_err(|e| {
                            NexusError::Connector(format!("blocking task panicked: {e}"))
                        })?
                })
                .await
            }
        })
        .await?;

        Ok(Self {
            connection,
            table: cfg.table.clone(),
            primary_key,
            timeout_seconds: cfg.timeout_seconds,
            retry: cfg.retry.clone(),
        })
    }

    fn columns_of(schema: &SchemaRef) -> Vec<String> {
        schema.fields().iter().map(|f| f.name().clone()).collect()
    }
}

/// `MERGE INTO target USING (SELECT ? AS col, ...) AS source ON
/// target.pk = source.pk WHEN MATCHED THEN UPDATE ... WHEN NOT MATCHED
/// THEN INSERT ...` — `table`/`primary_key`/`columns` come from the
/// pipeline spec (attacker-controlled request body) and get spliced into
/// SQL text; every one is validated and quoted via `quote_identifier`
/// before that happens, same rule `nexus-connector-postgres`'s
/// `build_upsert_sql` documents.
fn build_merge_sql(
    table: &str,
    primary_key: &str,
    columns: &[String],
) -> Result<String, NexusError> {
    let quoted_table = quote_identifier(table)?;
    let quoted_pk = quote_identifier(primary_key)?;
    let quoted_columns = columns
        .iter()
        .map(|c| quote_identifier(c))
        .collect::<Result<Vec<_>, _>>()?;

    let source_cols: Vec<String> = quoted_columns.iter().map(|c| format!("? AS {c}")).collect();
    let updates: Vec<String> = columns
        .iter()
        .zip(quoted_columns.iter())
        .filter(|(raw, _)| raw.as_str() != primary_key)
        .map(|(_, quoted)| format!("target.{quoted} = source.{quoted}"))
        .collect();
    let insert_cols = quoted_columns.join(", ");
    let insert_vals: Vec<String> = quoted_columns
        .iter()
        .map(|c| format!("source.{c}"))
        .collect();

    Ok(format!(
        "MERGE INTO {quoted_table} AS target USING (SELECT {source}) AS source \
         ON target.{quoted_pk} = source.{quoted_pk} \
         WHEN MATCHED THEN UPDATE SET {upd} \
         WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})",
        source = source_cols.join(", "),
        upd = updates.join(", "),
        insert_vals = insert_vals.join(", "),
    ))
}

fn build_delete_sql(table: &str, primary_key: &str) -> Result<String, NexusError> {
    let quoted_table = quote_identifier(table)?;
    let quoted_pk = quote_identifier(primary_key)?;
    Ok(format!("DELETE FROM {quoted_table} WHERE {quoted_pk} = ?"))
}

impl SnowflakeSink {
    async fn execute(&self, sql: String, batch: RecordBatch) -> Result<(), NexusError> {
        let timeout_seconds = self.timeout_seconds;
        let retry = self.retry.clone();
        retry_with_backoff(&retry, "snowflake execute", || {
            let mut connection = self.connection.clone();
            let sql = sql.clone();
            let batch = batch.clone();
            async move {
                with_timeout(timeout_seconds, "snowflake execute", async {
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
                .await
            }
        })
        .await
    }

    /// Fast-path bulk ingest for plain inserts. Uses ADBC's
    /// `adbc.ingest.target_table` option with `append` mode, which the
    /// Snowflake driver implements via `COPY INTO`.
    async fn bulk_ingest(&self, batch: RecordBatch) -> Result<(), NexusError> {
        let timeout_seconds = self.timeout_seconds;
        let retry = self.retry.clone();
        let table = self.table.clone();
        retry_with_backoff(&retry, "snowflake bulk ingest", || {
            let mut connection = self.connection.clone();
            let batch = batch.clone();
            let table = table.clone();
            async move {
                with_timeout(timeout_seconds, "snowflake bulk ingest", async {
                    tokio::task::spawn_blocking(move || -> Result<(), NexusError> {
                        let mut statement = connection
                            .new_statement()
                            .map_err(|e| NexusError::Connector(e.to_string()))?;
                        statement
                            .set_option(OptionStatement::TargetTable, table.into())
                            .map_err(|e| NexusError::Connector(e.to_string()))?;
                        statement
                            .set_option(OptionStatement::IngestMode, IngestMode::Append.into())
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
                .await
            }
        })
        .await
    }

    async fn apply(&self, upserts: RecordBatch, deletes: &RecordBatch) -> Result<(), NexusError> {
        if upserts.num_rows() > 0 {
            let columns = Self::columns_of(&upserts.schema());
            let sql = build_merge_sql(&self.table, &self.primary_key, &columns)?;
            self.execute(sql, upserts).await?;
        }
        if deletes.num_rows() > 0 {
            let sql = build_delete_sql(&self.table, &self.primary_key)?;
            let keys = project_column(deletes, &self.primary_key)?;
            self.execute(sql, keys).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl Sink for SnowflakeSink {
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<(), NexusError> {
        // CDC batches carry an `__opcode` column (ARCHITECTURE.md §5,
        // public repo) — split it so deletes are issued as real `DELETE`s
        // instead of being silently merged in. Plain (non-CDC) batches use
        // the ADBC bulk-ingest fast path.
        match split_by_opcode(&batch)? {
            None => self.bulk_ingest(batch).await,
            Some(split) => self.apply(split.upserts, &split.deletes).await,
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
    fn merge_sql_updates_every_column_except_the_primary_key() {
        let sql = build_merge_sql(
            "events",
            "id",
            &["id".to_string(), "name".to_string(), "score".to_string()],
        )
        .unwrap();

        assert_eq!(
            sql,
            "MERGE INTO \"events\" AS target USING (SELECT ? AS \"id\", ? AS \"name\", ? AS \"score\") AS source \
             ON target.\"id\" = source.\"id\" \
             WHEN MATCHED THEN UPDATE SET target.\"name\" = source.\"name\", target.\"score\" = source.\"score\" \
             WHEN NOT MATCHED THEN INSERT (\"id\", \"name\", \"score\") VALUES (source.\"id\", source.\"name\", source.\"score\")"
        );
    }

    #[test]
    fn delete_sql_targets_primary_key() {
        let sql = build_delete_sql("events", "id").unwrap();
        assert_eq!(sql, "DELETE FROM \"events\" WHERE \"id\" = ?");
    }

    #[test]
    fn rejects_sql_injection_in_table_name() {
        let err = build_merge_sql("events\"; DROP TABLE users; --", "id", &["id".to_string()])
            .expect_err("malicious table name must be rejected");
        assert!(matches!(err, NexusError::Schema(_)));
    }

    #[test]
    fn rejects_sql_injection_in_column_name() {
        let err = build_merge_sql(
            "events",
            "id",
            &["id".to_string(), "score); DROP TABLE users; --".to_string()],
        )
        .expect_err("malicious column name must be rejected");
        assert!(matches!(err, NexusError::Schema(_)));
    }

    #[test]
    fn rejects_sql_injection_in_delete_table_name() {
        let err = build_delete_sql("events\"; DROP TABLE users; --", "id")
            .expect_err("malicious table name must be rejected");
        assert!(matches!(err, NexusError::Schema(_)));
    }
}
