use crate::config::DuckdbConnectorConfig;
use crate::driver::open_connection;
use adbc_core::{Connection as _, Statement as _};
use adbc_driver_manager::ManagedConnection;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, SchemaRef};
use async_trait::async_trait;
use nexus_core::{
    project_column, quote_identifier, split_by_opcode, with_timeout, CheckpointCursor, NexusError,
    Sink,
};

pub struct DuckdbSink {
    connection: ManagedConnection,
    upsert_sql: String,
    delete_sql: String,
    primary_key: String,
    timeout_seconds: u64,
}

impl DuckdbSink {
    /// `schema` must match the column order of every `RecordBatch` passed to
    /// `write_batch` — ADBC binds parameters positionally. Also drives
    /// `CREATE TABLE IF NOT EXISTS` — same reasoning as
    /// `SqliteSink::connect`'s doc comment: a target table that doesn't
    /// exist yet is created from `schema`, not left to fail on the first
    /// upsert.
    pub async fn connect(
        cfg: &DuckdbConnectorConfig,
        schema: &SchemaRef,
    ) -> Result<Self, NexusError> {
        let uri = cfg.connection_url();
        let table = cfg.table.clone();
        let primary_key = cfg.primary_key.clone();
        let create_table_sql = build_create_table_sql(&table, &primary_key, schema)?;
        let connection = with_timeout(cfg.timeout_seconds, "duckdb connect", async {
            tokio::task::spawn_blocking(move || -> Result<ManagedConnection, NexusError> {
                let mut connection = open_connection(&uri)?;
                let mut statement = connection
                    .new_statement()
                    .map_err(|e| NexusError::Connector(e.to_string()))?;
                statement
                    .set_sql_query(&create_table_sql)
                    .map_err(|e| NexusError::Connector(e.to_string()))?;
                statement
                    .execute_update()
                    .map_err(|e| NexusError::Connector(format!("create table failed: {e}")))?;
                Ok(connection)
            })
            .await
            .map_err(|e| NexusError::Connector(format!("blocking task panicked: {e}")))?
        })
        .await?;
        let columns: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
        let upsert_sql = build_upsert_sql(&cfg.table, &cfg.primary_key, &columns)?;
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

/// Arrow type -> DuckDB SQL type. Unlike SQLite (which only has type
/// affinities), DuckDB has real native types, so this maps closer to
/// Postgres's equivalent than SQLite's — anything not explicitly matched
/// falls back to `VARCHAR` (never lose the value).
fn arrow_type_to_duckdb(data_type: &DataType) -> &'static str {
    match data_type {
        DataType::Int8 => "TINYINT",
        DataType::Int16 => "SMALLINT",
        DataType::Int32 => "INTEGER",
        DataType::Int64 => "BIGINT",
        DataType::UInt8 => "UTINYINT",
        DataType::UInt16 => "USMALLINT",
        DataType::UInt32 => "UINTEGER",
        DataType::UInt64 => "UBIGINT",
        DataType::Boolean => "BOOLEAN",
        DataType::Float16 | DataType::Float32 => "FLOAT",
        DataType::Float64 => "DOUBLE",
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => "DECIMAL",
        DataType::Date32 | DataType::Date64 => "DATE",
        DataType::Timestamp(_, _) => "TIMESTAMP",
        _ => "VARCHAR",
    }
}

/// `CREATE TABLE IF NOT EXISTS` from an Arrow schema — see
/// `DuckdbSink::connect`'s doc comment.
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
            let sql_type = arrow_type_to_duckdb(f.data_type());
            let pk_suffix = if f.name() == primary_key {
                " PRIMARY KEY"
            } else {
                ""
            };
            Ok(format!("{quoted_name} {sql_type}{pk_suffix}"))
        })
        .collect::<Result<Vec<_>, NexusError>>()?;
    Ok(format!(
        "CREATE TABLE IF NOT EXISTS {quoted_table} ({})",
        columns.join(", ")
    ))
}

/// DuckDB's `INSERT ... ON CONFLICT DO UPDATE` (present since 0.9, same
/// `EXCLUDED` syntax as Postgres/SQLite) gives the same idempotency contract
/// as those two — see ARCHITECTURE.md §5. Placeholders are plain `?`, same
/// driver quirk as SQLite's equivalent.
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

fn build_delete_sql(table: &str, primary_key: &str) -> Result<String, NexusError> {
    let quoted_table = quote_identifier(table)?;
    let quoted_primary_key = quote_identifier(primary_key)?;
    Ok(format!(
        "DELETE FROM {quoted_table} WHERE {quoted_primary_key} = ?"
    ))
}

impl DuckdbSink {
    async fn execute(&self, sql: &str, batch: RecordBatch) -> Result<(), NexusError> {
        let mut connection = self.connection.clone();
        let sql = sql.to_string();
        let timeout_seconds = self.timeout_seconds;

        with_timeout(timeout_seconds, "duckdb execute", async {
            tokio::task::spawn_blocking(move || -> Result<(), NexusError> {
                Self::execute_sql(&mut connection, "BEGIN TRANSACTION")?;
                let result = (|| -> Result<(), NexusError> {
                    // The DuckDB ADBC driver only supports binding one row per
                    // execution ("Binding multiple rows at once is not
                    // supported yet" in StatementExecuteQuery), unlike the
                    // SQLite/Postgres drivers which accept a whole batch.
                    // Bind and execute each row separately inside the
                    // transaction above; each `bind` replaces the driver's
                    // ingestion stream, so reusing one prepared statement is
                    // safe.
                    let mut statement = connection
                        .new_statement()
                        .map_err(|e| NexusError::Connector(e.to_string()))?;
                    statement
                        .set_sql_query(&sql)
                        .map_err(|e| NexusError::Connector(e.to_string()))?;
                    statement
                        .prepare()
                        .map_err(|e| NexusError::Connector(e.to_string()))?;
                    for row in 0..batch.num_rows() {
                        statement
                            .bind(batch.slice(row, 1))
                            .map_err(|e| NexusError::Connector(e.to_string()))?;
                        statement
                            .execute_update()
                            .map_err(|e| NexusError::Connector(e.to_string()))?;
                    }
                    Ok(())
                })();
                match result {
                    Ok(()) => Self::execute_sql(&mut connection, "COMMIT"),
                    Err(e) => {
                        let _ = Self::execute_sql(&mut connection, "ROLLBACK");
                        Err(e)
                    }
                }
            })
            .await
            .map_err(|e| NexusError::Connector(format!("blocking task panicked: {e}")))?
        })
        .await
    }

    fn execute_sql(connection: &mut ManagedConnection, sql: &str) -> Result<(), NexusError> {
        let mut statement = connection
            .new_statement()
            .map_err(|e| NexusError::Connector(e.to_string()))?;
        statement
            .set_sql_query(sql)
            .map_err(|e| NexusError::Connector(e.to_string()))?;
        statement
            .execute_update()
            .map_err(|e| NexusError::Connector(format!("duckdb {sql} failed: {e}")))?;
        Ok(())
    }
}

#[async_trait]
impl Sink for DuckdbSink {
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
    use arrow_schema::{Field, Schema};
    use std::sync::Arc;

    #[test]
    fn create_table_sql_marks_the_primary_key_and_maps_types() {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("active", DataType::Boolean, true),
            Field::new("score", DataType::Float64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let sql = build_create_table_sql("events", "id", &schema).unwrap();
        assert_eq!(
            sql,
            "CREATE TABLE IF NOT EXISTS \"events\" (\"id\" BIGINT PRIMARY KEY, \"active\" BOOLEAN, \"score\" DOUBLE, \"name\" VARCHAR)"
        );
    }

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
