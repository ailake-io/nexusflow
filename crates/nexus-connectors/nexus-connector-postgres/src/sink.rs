use crate::config::PostgresConnectorConfig;
use crate::driver::open_connection;
use adbc_core::{Connection as _, Statement as _};
use adbc_driver_manager::ManagedStatement;
use arrow_array::{Array, RecordBatch, StringArray};
use arrow_cast::cast;
use arrow_schema::{DataType, SchemaRef};
use async_trait::async_trait;
use futures::{pin_mut, SinkExt};
use nexus_core::quote_identifier;
use nexus_core::{
    project_column, split_by_opcode, with_timeout, CheckpointCursor, NexusError, Sink,
};
use std::sync::Arc;
use tokio_postgres::Client;

pub struct PostgresSink {
    upsert_statement: ManagedStatement,
    delete_statement: ManagedStatement,
    copy_client: Arc<Client>,
    table: String,
    schema: SchemaRef,
    primary_key: String,
    timeout_seconds: u64,
}

impl PostgresSink {
    /// `schema` must match the column order of every `RecordBatch` passed to
    /// `write_batch`. Also drives `CREATE TABLE IF NOT EXISTS`: a target table
    /// that doesn't exist yet is created from `schema`'s columns/types before
    /// the first write, instead of failing with a bare "relation does not exist".
    /// A table that already exists is left alone (no `ALTER TABLE`
    /// reconciliation).
    pub async fn connect(
        cfg: &PostgresConnectorConfig,
        schema: &SchemaRef,
    ) -> Result<Self, NexusError> {
        let uri = cfg.connection_string();
        let table = cfg.table.clone();
        let primary_key = cfg.primary_key.clone();
        let schema = schema.clone();
        let create_table_sql = build_create_table_sql(&table, &primary_key, &schema)?;

        // Clones for the ADBC setup closure.
        let table_for_adbc = table.clone();
        let schema_for_adbc = schema.clone();

        // Direct tokio-postgres connection for COPY-based bulk loads.
        let pg_config = build_pg_config(cfg)?;
        let (copy_client, copy_connection) = pg_config
            .connect(tokio_postgres::NoTls)
            .await
            .map_err(|e| NexusError::Connector(format!("postgres direct connect failed: {e}")))?;
        tokio::spawn(async move {
            if let Err(e) = copy_connection.await {
                tracing::error!("postgres copy connection closed: {e}");
            }
        });

        let (upsert_statement, delete_statement) =
            with_timeout(cfg.timeout_seconds, "postgres connect", async {
                tokio::task::spawn_blocking(move || -> Result<_, NexusError> {
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

                    let upsert_sql =
                        build_upsert_sql(&table_for_adbc, &primary_key, &schema_for_adbc)?;
                    let delete_sql = build_delete_sql(&table_for_adbc, &primary_key)?;

                    let mut upsert_statement = connection
                        .new_statement()
                        .map_err(|e| NexusError::Connector(e.to_string()))?;
                    upsert_statement
                        .set_sql_query(&upsert_sql)
                        .map_err(|e| NexusError::Connector(e.to_string()))?;
                    upsert_statement
                        .prepare()
                        .map_err(|e| NexusError::Connector(e.to_string()))?;

                    let mut delete_statement = connection
                        .new_statement()
                        .map_err(|e| NexusError::Connector(e.to_string()))?;
                    delete_statement
                        .set_sql_query(&delete_sql)
                        .map_err(|e| NexusError::Connector(e.to_string()))?;
                    delete_statement
                        .prepare()
                        .map_err(|e| NexusError::Connector(e.to_string()))?;

                    Ok((upsert_statement, delete_statement))
                })
                .await
                .map_err(|e| NexusError::Connector(format!("blocking task panicked: {e}")))?
            })
            .await?;

        Ok(Self {
            upsert_statement,
            delete_statement,
            copy_client: Arc::new(copy_client),
            table,
            schema,
            primary_key: cfg.primary_key.clone(),
            timeout_seconds: cfg.timeout_seconds,
        })
    }
}

fn build_pg_config(cfg: &PostgresConnectorConfig) -> Result<tokio_postgres::Config, NexusError> {
    if let Some(uri) = cfg.uri.as_deref().filter(|s| !s.is_empty()) {
        return uri
            .parse::<tokio_postgres::Config>()
            .map_err(|e| NexusError::Connector(format!("invalid postgres uri: {e}")));
    }
    let mut config = tokio_postgres::Config::new();
    config
        .host(&cfg.host)
        .port(cfg.port)
        .user(&cfg.username)
        .password(&cfg.password)
        .dbname(&cfg.database);
    Ok(config)
}

/// Arrow type -> Postgres column type. Anything not explicitly matched
/// falls back to `TEXT` — same "never lose the value" posture as the
/// bridging connectors' schema inference, just applied to DDL instead of a
/// RecordBatch: a Postgres column that can hold anything is safer than a
/// `CREATE TABLE` that fails outright over an unrecognized Arrow type.
fn arrow_type_to_postgres(data_type: &DataType) -> &'static str {
    match data_type {
        DataType::Int8 | DataType::Int16 => "SMALLINT",
        DataType::Int32 | DataType::UInt8 | DataType::UInt16 => "INTEGER",
        DataType::Int64 | DataType::UInt32 | DataType::UInt64 => "BIGINT",
        DataType::Float16 | DataType::Float32 => "REAL",
        DataType::Float64 => "DOUBLE PRECISION",
        DataType::Boolean => "BOOLEAN",
        DataType::Date32 | DataType::Date64 => "DATE",
        DataType::Timestamp(_, _) => "TIMESTAMP",
        DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
            let _ = (precision, scale);
            "NUMERIC"
        }
        _ => "TEXT",
    }
}

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
            let sql_type = arrow_type_to_postgres(f.data_type());
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

/// `INSERT ... ON CONFLICT (pk) DO UPDATE` — used for CDC upserts where the
/// batch sizes are typically small and the idempotency contract must hold.
fn build_upsert_sql(
    table: &str,
    primary_key: &str,
    schema: &SchemaRef,
) -> Result<String, NexusError> {
    let quoted_table = quote_identifier(table)?;
    let quoted_primary_key = quote_identifier(primary_key)?;
    let columns: Vec<_> = schema.fields().iter().collect();
    let quoted_columns: Vec<_> = columns
        .iter()
        .map(|f| quote_identifier(f.name()))
        .collect::<Result<Vec<_>, _>>()?;

    let placeholders: Vec<String> = (1..=columns.len()).map(|i| format!("${i}")).collect();

    let updates: Vec<String> = columns
        .iter()
        .zip(quoted_columns.iter())
        .filter(|(f, _)| f.name().as_str() != primary_key)
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

/// Escapes a value for PostgreSQL COPY TEXT format.
fn copy_escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\t', "\\t")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
}

/// Converts a `RecordBatch` into PostgreSQL COPY TEXT format bytes.
fn batch_to_copy_text(batch: &RecordBatch) -> Result<Vec<u8>, NexusError> {
    let num_rows = batch.num_rows();
    if num_rows == 0 {
        return Ok(Vec::new());
    }

    // Cast every column to Utf8 so we can format each value as text.
    let string_columns: Vec<StringArray> = batch
        .columns()
        .iter()
        .map(|col| {
            let string_arr = cast(col.as_ref(), &DataType::Utf8)
                .map_err(|e| NexusError::Connector(format!("cast to utf8 failed: {e}")))?;
            string_arr
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    NexusError::Schema("cast to Utf8 did not produce StringArray".into())
                })
                .cloned()
        })
        .collect::<Result<Vec<_>, NexusError>>()?;

    let mut buf = Vec::with_capacity(num_rows * 64);
    for row in 0..num_rows {
        for (col_idx, col) in string_columns.iter().enumerate() {
            if col_idx > 0 {
                buf.push(b'\t');
            }
            if col.is_null(row) {
                buf.extend_from_slice(b"\\N");
            } else {
                let escaped = copy_escape(col.value(row));
                buf.extend_from_slice(escaped.as_bytes());
            }
        }
        buf.push(b'\n');
    }
    Ok(buf)
}

impl PostgresSink {
    /// Bulk-loads a non-CDC batch using `COPY ... FROM STDIN`.
    async fn execute_copy(&self, batch: &RecordBatch) -> Result<(), NexusError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let quoted_table = quote_identifier(&self.table)?;
        let quoted_columns: Vec<_> = self
            .schema
            .fields()
            .iter()
            .map(|f| quote_identifier(f.name()))
            .collect::<Result<Vec<_>, _>>()?;
        let copy_sql = format!(
            "COPY {quoted_table} ({}) FROM STDIN WITH (FORMAT text)",
            quoted_columns.join(", ")
        );

        let data = batch_to_copy_text(batch)?;
        let client = self.copy_client.clone();

        with_timeout(self.timeout_seconds, "postgres copy", async move {
            let sink = client
                .copy_in(&copy_sql)
                .await
                .map_err(|e| NexusError::Connector(format!("copy_in failed: {e}")))?;
            pin_mut!(sink);
            sink.send(bytes::Bytes::from(data))
                .await
                .map_err(|e| NexusError::Connector(format!("copy send failed: {e}")))?;
            sink.finish()
                .await
                .map_err(|e| NexusError::Connector(format!("copy finish failed: {e}")))?;
            Ok::<(), NexusError>(())
        })
        .await
    }

    /// Executes the prepared single-row upsert statement. Used for CDC upserts.
    async fn execute_upsert(&self, batch: RecordBatch) -> Result<(), NexusError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let mut statement = self.upsert_statement.clone();
        let timeout_seconds = self.timeout_seconds;

        with_timeout(timeout_seconds, "postgres upsert", async {
            tokio::task::spawn_blocking(move || -> Result<(), NexusError> {
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

    /// Executes the prepared single-row delete statement. CDC delete batches
    /// are normally small, so row-by-row execution is fine.
    async fn execute_delete(&self, batch: RecordBatch) -> Result<(), NexusError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let mut statement = self.delete_statement.clone();
        let timeout_seconds = self.timeout_seconds;

        with_timeout(timeout_seconds, "postgres delete", async {
            tokio::task::spawn_blocking(move || -> Result<(), NexusError> {
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
}

#[async_trait]
impl Sink for PostgresSink {
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<(), NexusError> {
        // CDC batches carry an `__opcode` column (ARCHITECTURE.md §5) — split
        // it so deletes are issued as real `DELETE`s instead of being
        // silently upserted. Plain (non-CDC) batches take the COPY bulk path.
        match split_by_opcode(&batch)? {
            None => self.execute_copy(&batch).await,
            Some(split) => {
                if split.upserts.num_rows() > 0 {
                    self.execute_upsert(split.upserts).await?;
                }
                if split.deletes.num_rows() > 0 {
                    let keys = project_column(&split.deletes, &self.primary_key)?;
                    self.execute_delete(keys).await?;
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
    use arrow_array::Int64Array;
    use arrow_schema::{Field, Schema};
    use std::sync::Arc;

    #[test]
    fn create_table_sql_marks_the_primary_key_and_maps_types() {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Float64, true),
        ]));
        let sql = build_create_table_sql("events", "id", &schema).unwrap();
        assert_eq!(
            sql,
            "CREATE TABLE IF NOT EXISTS \"events\" (\"id\" BIGINT PRIMARY KEY, \"name\" TEXT, \"score\" DOUBLE PRECISION)"
        );
    }

    #[test]
    fn create_table_sql_rejects_sql_injection_in_table_name() {
        let schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let err = build_create_table_sql("events\"; DROP TABLE users; --", "id", &schema)
            .expect_err("malicious table name must be rejected");
        assert!(matches!(err, NexusError::Schema(_)));
    }

    #[test]
    fn upsert_sql_updates_every_column_except_the_primary_key() {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Float64, true),
        ]));
        let sql = build_upsert_sql("events", "id", &schema).unwrap();

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
        let schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let err = build_upsert_sql("events\"; DROP TABLE users; --", "id", &schema)
            .expect_err("malicious table name must be rejected");
        assert!(matches!(err, NexusError::Schema(_)));
    }

    #[test]
    fn rejects_sql_injection_in_column_name() {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("score); DROP TABLE users; --", DataType::Float64, true),
        ]));
        let err = build_upsert_sql("events", "id", &schema)
            .expect_err("malicious column name must be rejected");
        assert!(matches!(err, NexusError::Schema(_)));
    }

    #[test]
    fn batch_to_copy_text_formats_rows() {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let id = Arc::new(Int64Array::from(vec![1, 2])) as Arc<dyn Array>;
        let name = Arc::new(StringArray::from(vec![Some("a\tb"), None])) as Arc<dyn Array>;
        let batch = RecordBatch::try_new(schema, vec![id, name]).unwrap();

        let text = String::from_utf8(batch_to_copy_text(&batch).unwrap()).unwrap();
        assert_eq!(text, "1\ta\\tb\n2\t\\N\n");
    }
}
