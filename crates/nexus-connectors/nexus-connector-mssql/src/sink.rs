use crate::config::MssqlConnectorConfig;
use crate::driver::open_connection;
use adbc_core::options::{IngestMode, OptionStatement};
use adbc_core::Optionable;
use adbc_core::{Connection as _, Statement as _};
use adbc_driver_manager::ManagedConnection;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, SchemaRef};
use async_trait::async_trait;
use nexus_core::quote_identifier;
use nexus_core::{
    project_column, retry_with_backoff, split_by_opcode, with_timeout, CheckpointCursor, NexusError,
    Sink,
};

/// T-SQL sink with two execution paths:
///
/// 1. **Fast bulk-ingest path** — used for normal batches without CDC op-codes.
///    Uses ADBC's bulk-ingestion API (`Statement.set_option(TargetTable, ...)` +
///    `IngestMode::Append`) which routes to the driver's `INSERT BULK`
///    implementation on SQL Server. This is orders of magnitude faster than
///    row-by-row parameterized `INSERT` and avoids the `?`/`@p1` placeholder
///    dialect problem entirely. Large batches are split into 10k-row chunks to
///    avoid very long-running operations and to give checkpointing finer
///    granularity.
///
/// 2. **`MERGE INTO` path** — used only when the batch carries CDC op-codes
///    (upserts + deletes). SQL Server's Go driver expects named parameters
///    (`@p1`, `@p2`, ...) instead of `?`, so the `VALUES` derived table in the
///    `USING` clause uses that style.
///
/// Columns are derived from each incoming `RecordBatch`'s own schema — same
/// standard every enterprise connector in this repo follows.
pub struct MssqlSink {
    connection: ManagedConnection,
    table: String,
    primary_key: String,
    timeout_seconds: u64,
    retry: nexus_core::RetryConfig,
    table_created: bool,
}

impl MssqlSink {
    pub async fn connect(cfg: &MssqlConnectorConfig) -> Result<Self, NexusError> {
        let primary_key = cfg
            .primary_key
            .clone()
            .ok_or_else(|| NexusError::Schema("mssql sink requires primary_key".into()))?;
        quote_identifier(&cfg.table)?;
        quote_identifier(&primary_key)?;

        let retry = cfg.retry.clone();
        let connection = retry_with_backoff(&retry, "mssql connect", || {
            let cfg = cfg.clone();
            async move {
                with_timeout(cfg.timeout_seconds, "mssql connect", async {
                    tokio::task::spawn_blocking(move || open_connection(&cfg.connection_string()))
                        .await
                        .map_err(|e| NexusError::Connector(format!("blocking task panicked: {e}")))?
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
            table_created: false,
        })
    }

    fn columns_of(schema: &SchemaRef) -> Vec<String> {
        schema.fields().iter().map(|f| f.name().clone()).collect()
    }
}

/// `MERGE INTO target AS tgt USING (VALUES (@p1, @p2, @p3)) AS src (col1, col2, col3)
/// ON tgt.pk = src.pk WHEN MATCHED THEN UPDATE ... WHEN NOT MATCHED THEN
/// INSERT ...` — `table`/`primary_key`/`columns` come from the pipeline spec /
/// upstream schema (attacker-controlled); every one is validated and quoted via
/// `quote_identifier` before that happens, same rule every `build_merge_sql` in
/// this repo documents.
fn build_merge_sql(table: &str, primary_key: &str, columns: &[String]) -> Result<String, NexusError> {
    let quoted_table = quote_identifier(table)?;
    let quoted_pk = quote_identifier(primary_key)?;
    let quoted_columns = columns
        .iter()
        .map(|c| quote_identifier(c))
        .collect::<Result<Vec<_>, _>>()?;

    let placeholders: Vec<String> = (1..=columns.len())
        .map(|i| format!("@p{i}"))
        .collect();
    let source_decl = quoted_columns.join(", ");

    let updates: Vec<String> = columns
        .iter()
        .zip(quoted_columns.iter())
        .filter(|(raw, _)| raw.as_str() != primary_key)
        .map(|(_, quoted)| format!("tgt.{quoted} = src.{quoted}"))
        .collect();
    let insert_cols = quoted_columns.join(", ");
    let insert_vals: Vec<String> = quoted_columns
        .iter()
        .map(|c| format!("src.{c}"))
        .collect();

    Ok(format!(
        "MERGE INTO {quoted_table} AS tgt \
         USING (VALUES ({placeholders})) AS src ({source_decl}) \
         ON (tgt.{quoted_pk} = src.{quoted_pk}) \
         WHEN MATCHED THEN UPDATE SET {upd} \
         WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals});",
        placeholders = placeholders.join(", "),
        upd = updates.join(", "),
        insert_vals = insert_vals.join(", ")
    ))
}

fn build_delete_sql(table: &str, primary_key: &str) -> Result<String, NexusError> {
    let quoted_table = quote_identifier(table)?;
    let quoted_pk = quote_identifier(primary_key)?;
    Ok(format!("DELETE FROM {quoted_table} WHERE {quoted_pk} = @p1"))
}

/// A target table that doesn't exist yet is created from `schema`'s
/// columns/types before the first write, instead of failing with a bare
/// "Invalid object name" — same posture as `PostgresSink::connect`'s
/// `build_create_table_sql` (public repo). Unlike Oracle, T-SQL's
/// `OBJECT_ID(...) IS NULL` check makes this idempotent in one statement,
/// no "catch the already-exists error" needed.
fn build_create_table_sql(
    table: &str,
    primary_key: &str,
    schema: &SchemaRef,
) -> Result<String, NexusError> {
    let quoted_table = quote_identifier(table)?;
    let columns = schema
        .fields()
        .iter()
        .map(|f| {
            let quoted_name = quote_identifier(f.name())?;
            let sql_type = arrow_type_to_mssql(f.data_type());
            let pk_suffix = if f.name() == primary_key { " PRIMARY KEY" } else { "" };
            Ok(format!("{quoted_name} {sql_type}{pk_suffix}"))
        })
        .collect::<Result<Vec<_>, NexusError>>()?;
    Ok(format!(
        "IF OBJECT_ID(N'{quoted_table}', N'U') IS NULL CREATE TABLE {quoted_table} ({})",
        columns.join(", ")
    ))
}

/// Arrow type -> T-SQL column type. Anything not explicitly matched falls
/// back to `NVARCHAR(MAX)` — same "never lose the value" posture
/// `arrow_type_to_postgres` (public repo) documents for its `TEXT`
/// fallback.
fn arrow_type_to_mssql(data_type: &DataType) -> &'static str {
    match data_type {
        DataType::Int8 | DataType::Int16 => "SMALLINT",
        DataType::Int32 | DataType::UInt8 | DataType::UInt16 => "INT",
        DataType::Int64 | DataType::UInt32 | DataType::UInt64 => "BIGINT",
        DataType::Float16 | DataType::Float32 => "REAL",
        DataType::Float64 => "FLOAT",
        DataType::Boolean => "BIT",
        DataType::Date32 | DataType::Date64 => "DATE",
        DataType::Timestamp(_, _) => "DATETIME2",
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => "DECIMAL(38, 10)",
        _ => "NVARCHAR(MAX)",
    }
}

impl MssqlSink {
    async fn execute(&self, sql: String, batch: RecordBatch) -> Result<(), NexusError> {
        let timeout_seconds = self.timeout_seconds;
        let retry = self.retry.clone();
        retry_with_backoff(&retry, "mssql execute", || {
            let mut connection = self.connection.clone();
            let sql = sql.clone();
            let batch = batch.clone();
            async move {
                with_timeout(timeout_seconds, "mssql execute", async {
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

    async fn bulk_ingest(&self, batch: RecordBatch) -> Result<(), NexusError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        // Chunk large batches to avoid long-running bulk operations and to
        // surface any driver issues with very large single-batch ingests.
        const CHUNK_SIZE: usize = 10_000;
        let rows = batch.num_rows();
        for start in (0..rows).step_by(CHUNK_SIZE) {
            let end = (start + CHUNK_SIZE).min(rows);
            let chunk = batch.slice(start, end - start);
            self.bulk_ingest_chunk(chunk).await?;
        }
        Ok(())
    }

    async fn bulk_ingest_chunk(&self, chunk: RecordBatch) -> Result<(), NexusError> {
        if chunk.num_rows() == 0 {
            return Ok(());
        }
        let timeout_seconds = self.timeout_seconds;
        let retry = self.retry.clone();
        let table = self.table.clone();
        retry_with_backoff(&retry, "mssql bulk ingest", || {
            let mut connection = self.connection.clone();
            let chunk = chunk.clone();
            let table = table.clone();
            async move {
                with_timeout(timeout_seconds, "mssql bulk ingest", async {
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
                            .bind(chunk)
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

    async fn ensure_table_exists(&self, schema: &SchemaRef) -> Result<(), NexusError> {
        let sql = build_create_table_sql(&self.table, &self.primary_key, schema)?;
        let timeout_seconds = self.timeout_seconds;
        let retry = self.retry.clone();
        retry_with_backoff(&retry, "mssql ensure table exists", || {
            let mut connection = self.connection.clone();
            let sql = sql.clone();
            async move {
                with_timeout(timeout_seconds, "mssql ensure table exists", async {
                    tokio::task::spawn_blocking(move || -> Result<(), NexusError> {
                        let mut statement = connection
                            .new_statement()
                            .map_err(|e| NexusError::Connector(e.to_string()))?;
                        statement
                            .set_sql_query(&sql)
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
impl Sink for MssqlSink {
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<(), NexusError> {
        if !self.table_created {
            self.ensure_table_exists(&batch.schema()).await?;
            self.table_created = true;
        }
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

        assert!(sql.starts_with("MERGE INTO \"events\" AS tgt"));
        assert!(sql.contains("USING (VALUES (@p1, @p2, @p3)) AS src"));
        assert!(sql.contains("ON (tgt.\"id\" = src.\"id\")"));
        assert!(sql.contains("WHEN MATCHED THEN UPDATE SET tgt.\"name\" = src.\"name\", tgt.\"score\" = src.\"score\""));
        assert!(sql.contains("WHEN NOT MATCHED THEN INSERT (\"id\", \"name\", \"score\") VALUES (src.\"id\", src.\"name\", src.\"score\");"));
    }

    #[test]
    fn delete_sql_targets_primary_key() {
        let sql = build_delete_sql("events", "id").unwrap();
        assert_eq!(sql, "DELETE FROM \"events\" WHERE \"id\" = @p1");
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
    fn create_table_sql_maps_types_and_marks_primary_key() {
        use arrow_schema::{Field, Schema};
        use std::sync::Arc;

        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Float64, true),
        ]));
        let sql = build_create_table_sql("events", "id", &schema).unwrap();
        assert_eq!(
            sql,
            "IF OBJECT_ID(N'\"events\"', N'U') IS NULL CREATE TABLE \"events\" (\"id\" BIGINT PRIMARY KEY, \"name\" NVARCHAR(MAX), \"score\" FLOAT)"
        );
    }

    #[test]
    fn create_table_sql_rejects_sql_injection_in_table_name() {
        use arrow_schema::{Field, Schema};
        use std::sync::Arc;

        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let err = build_create_table_sql("events\"; DROP TABLE users; --", "id", &schema)
            .expect_err("malicious table name must be rejected");
        assert!(matches!(err, NexusError::Schema(_)));
    }

    #[test]
    fn rejects_sql_injection_in_delete_table_name() {
        let err = build_delete_sql("events\"; DROP TABLE users; --", "id")
            .expect_err("malicious table name must be rejected");
        assert!(matches!(err, NexusError::Schema(_)));
    }
}
