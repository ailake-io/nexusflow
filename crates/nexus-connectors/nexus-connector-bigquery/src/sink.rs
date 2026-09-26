use crate::config::BigqueryConnectorConfig;
use crate::driver::open_connection;
use crate::quoting::{qualified_table, quote_backtick};
use adbc_core::{Connection as _, Statement as _};
use adbc_driver_manager::ManagedConnection;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use nexus_core::{
    project_column, retry_with_backoff, split_by_opcode, with_timeout, CheckpointCursor,
    NexusError, Sink,
};

/// BigQuery has no `ON CONFLICT` — upsert here is `MERGE INTO` (standard
/// SQL DML, BigQuery supports it natively). Bind placeholders are `?`
/// (BigQuery Standard SQL also accepts named `@param` placeholders, but
/// ADBC drivers generally use positional `?` — same unverified-against-
/// the-real-driver caveat `nexus-connector-snowflake`'s sink.rs has).
///
/// The ADBC driver's own `StatementOptions` are query-job-oriented
/// (`destination_table`, `write_disposition`, priority, etc. — see
/// driver.rs's doc comment) rather than a generic bulk-ingest extension
/// like Snowflake's `OptionStatement::TargetTable`/`IngestMode`.
/// BigQuery is a full SQL engine regardless, so plain parameterized DML
/// (`set_sql_query` + `prepare` + `bind` + `execute_update`) still works
/// the same way it does for Postgres/Snowflake — there's just no bulk-load
/// fast path to reach for here the way there is for Snowflake.
///
/// Columns are derived from each incoming `RecordBatch`'s own schema
/// (not passed at `connect()` time) — same reasoning as
/// `SnowflakeSink`/`ExcelSink`: the plugin `SinkBuilder`
/// (`nexus-core::registry`) only carries the config JSON, never a column
/// list.
pub struct BigquerySink {
    connection: ManagedConnection,
    table: String,
    primary_key: String,
    timeout_seconds: u64,
    retry: nexus_core::RetryConfig,
}

impl BigquerySink {
    pub async fn connect(cfg: &BigqueryConnectorConfig) -> Result<Self, NexusError> {
        cfg.validate()?;
        let primary_key = cfg
            .primary_key
            .clone()
            .ok_or_else(|| NexusError::Schema("bigquery sink requires primary_key".into()))?;
        let table = qualified_table(cfg)?;
        quote_backtick(&primary_key)?;

        let retry = cfg.retry.clone();
        let connection = retry_with_backoff(&retry, "bigquery connect", || {
            let cfg = cfg.clone();
            async move {
                with_timeout(cfg.timeout_seconds, "bigquery connect", async {
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
            table,
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
/// THEN INSERT ...` — `table` is already the validated+quoted
/// fully-qualified form (see `qualified_table`); `primary_key`/`columns`
/// come from the pipeline spec / upstream schema (attacker-controlled),
/// spliced into SQL text; every one is validated and quoted via
/// `quote_backtick` before that happens.
fn build_merge_sql(
    qualified_table: &str,
    primary_key: &str,
    columns: &[String],
) -> Result<String, NexusError> {
    let quoted_pk = quote_backtick(primary_key)?;
    let quoted_columns = columns
        .iter()
        .map(|c| quote_backtick(c))
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
        "MERGE INTO {qualified_table} AS target USING (SELECT {source}) AS source \
         ON target.{quoted_pk} = source.{quoted_pk} \
         WHEN MATCHED THEN UPDATE SET {upd} \
         WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})",
        source = source_cols.join(", "),
        upd = updates.join(", "),
        insert_vals = insert_vals.join(", "),
    ))
}

fn build_delete_sql(qualified_table: &str, primary_key: &str) -> Result<String, NexusError> {
    let quoted_pk = quote_backtick(primary_key)?;
    Ok(format!(
        "DELETE FROM {qualified_table} WHERE {quoted_pk} = ?"
    ))
}

impl BigquerySink {
    async fn execute(&self, sql: String, batch: RecordBatch) -> Result<(), NexusError> {
        let timeout_seconds = self.timeout_seconds;
        let retry = self.retry.clone();
        retry_with_backoff(&retry, "bigquery execute", || {
            let mut connection = self.connection.clone();
            let sql = sql.clone();
            let batch = batch.clone();
            async move {
                with_timeout(timeout_seconds, "bigquery execute", async {
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
impl Sink for BigquerySink {
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<(), NexusError> {
        // CDC batches carry an `__opcode` column (ARCHITECTURE.md §5,
        // public repo) — split it so deletes are issued as real `DELETE`s
        // instead of being silently merged in. Plain (non-CDC) batches
        // take the unchanged single MERGE path; BigQuery's ADBC driver
        // does not expose a generic bulk-ingest fast path.
        match split_by_opcode(&batch)? {
            None => {
                let empty = batch.slice(0, 0);
                self.apply(batch, &empty).await
            }
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
            "`proj.ds.events`",
            "id",
            &["id".to_string(), "name".to_string(), "score".to_string()],
        )
        .unwrap();

        assert_eq!(
            sql,
            "MERGE INTO `proj.ds.events` AS target USING (SELECT ? AS `id`, ? AS `name`, ? AS `score`) AS source \
             ON target.`id` = source.`id` \
             WHEN MATCHED THEN UPDATE SET target.`name` = source.`name`, target.`score` = source.`score` \
             WHEN NOT MATCHED THEN INSERT (`id`, `name`, `score`) VALUES (source.`id`, source.`name`, source.`score`)"
        );
    }

    #[test]
    fn delete_sql_targets_primary_key() {
        let sql = build_delete_sql("`proj.ds.events`", "id").unwrap();
        assert_eq!(sql, "DELETE FROM `proj.ds.events` WHERE `id` = ?");
    }

    #[test]
    fn rejects_sql_injection_in_column_name() {
        let err = build_merge_sql(
            "`proj.ds.events`",
            "id",
            &["id".to_string(), "score`; DROP TABLE users; --".to_string()],
        )
        .expect_err("malicious column name must be rejected");
        assert!(matches!(err, NexusError::Schema(_)));
    }

    #[test]
    fn rejects_sql_injection_in_primary_key() {
        let err = build_delete_sql("`proj.ds.events`", "id`; DROP TABLE users; --")
            .expect_err("malicious primary key must be rejected");
        assert!(matches!(err, NexusError::Schema(_)));
    }
}
