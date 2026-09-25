use crate::config::DatabricksConnectorConfig;
use crate::driver::open_connection;
use crate::quoting::{qualified_table, quote_backtick};
use adbc_core::{Connection as _, Statement as _};
use adbc_driver_manager::ManagedConnection;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use nexus_core::{
    project_column, retry_with_backoff, split_by_opcode, with_timeout, CheckpointCursor, NexusError,
    Sink,
};

/// Databricks SQL supports `MERGE INTO` natively (same as Snowflake) —
/// upsert here follows `nexus-connector-snowflake`'s `SnowflakeSink`
/// exactly, swapping double-quote for backtick and a 2-part for a
/// 3-part qualified table name (Unity Catalog: catalog.schema.table).
/// Bind placeholders are `?` — **unverified against a real driver/
/// workspace yet**, same caveat `source.rs`'s `get_table_schema` note
/// carries; revisit once there's a real workspace to test against.
///
/// v1 always goes through MERGE, even for a plain (non-CDC) batch with
/// no deletes — same "correctness first" choice Snowflake's sink
/// documents; a native bulk-ingest fast path (if the Databricks ADBC
/// driver exposes one) is a follow-up, not attempted here.
pub struct DatabricksSink {
    connection: ManagedConnection,
    qualified_table: String,
    primary_key: String,
    timeout_seconds: u64,
    retry: nexus_core::RetryConfig,
}

impl DatabricksSink {
    pub async fn connect(cfg: &DatabricksConnectorConfig) -> Result<Self, NexusError> {
        let primary_key = cfg
            .primary_key
            .clone()
            .ok_or_else(|| NexusError::Schema("databricks sink requires primary_key".into()))?;
        let qualified = qualified_table(cfg)?;
        quote_backtick(&primary_key)?;

        let retry = cfg.retry.clone();
        let connection = retry_with_backoff(&retry, "databricks connect", || {
            let cfg = cfg.clone();
            async move {
                with_timeout(cfg.timeout_seconds, "databricks connect", async {
                    tokio::task::spawn_blocking(move || open_connection(&cfg))
                        .await
                        .map_err(|e| NexusError::Connector(format!("blocking task panicked: {e}")))?
                })
                .await
            }
        })
        .await?;

        Ok(Self {
            connection,
            qualified_table: qualified,
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
/// THEN INSERT ...` — `qualified_table`/`primary_key`/`columns` come
/// from the pipeline spec (attacker-controlled request body) and get
/// spliced into SQL text; every identifier is validated and backtick-
/// quoted before that happens, same rule `nexus-connector-snowflake`'s
/// `build_merge_sql` documents.
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

impl DatabricksSink {
    async fn execute(&self, sql: String, batch: RecordBatch) -> Result<(), NexusError> {
        let timeout_seconds = self.timeout_seconds;
        let retry = self.retry.clone();
        retry_with_backoff(&retry, "databricks execute", || {
            let mut connection = self.connection.clone();
            let sql = sql.clone();
            let batch = batch.clone();
            async move {
                with_timeout(timeout_seconds, "databricks execute", async {
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
            let sql = build_merge_sql(&self.qualified_table, &self.primary_key, &columns)?;
            self.execute(sql, upserts).await?;
        }
        if deletes.num_rows() > 0 {
            let sql = build_delete_sql(&self.qualified_table, &self.primary_key)?;
            let keys = project_column(deletes, &self.primary_key)?;
            self.execute(sql, keys).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl Sink for DatabricksSink {
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<(), NexusError> {
        // CDC batches carry an `__opcode` column (ARCHITECTURE.md §5,
        // public repo) — split it so deletes are issued as real
        // `DELETE`s instead of being silently merged in. Plain
        // (non-CDC) batches take the unchanged single MERGE path.
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
            "`main`.`default`.`events`",
            "id",
            &["id".to_string(), "name".to_string(), "score".to_string()],
        )
        .unwrap();

        assert_eq!(
            sql,
            "MERGE INTO `main`.`default`.`events` AS target USING (SELECT ? AS `id`, ? AS `name`, ? AS `score`) AS source \
             ON target.`id` = source.`id` \
             WHEN MATCHED THEN UPDATE SET target.`name` = source.`name`, target.`score` = source.`score` \
             WHEN NOT MATCHED THEN INSERT (`id`, `name`, `score`) VALUES (source.`id`, source.`name`, source.`score`)"
        );
    }

    #[test]
    fn delete_sql_targets_primary_key() {
        let sql = build_delete_sql("`main`.`default`.`events`", "id").unwrap();
        assert_eq!(sql, "DELETE FROM `main`.`default`.`events` WHERE `id` = ?");
    }

    #[test]
    fn rejects_sql_injection_in_column_name() {
        let err = build_merge_sql(
            "`main`.`default`.`events`",
            "id",
            &["id".to_string(), "score); DROP TABLE users; --".to_string()],
        )
        .expect_err("malicious column name must be rejected");
        assert!(matches!(err, NexusError::Schema(_)));
    }
}
