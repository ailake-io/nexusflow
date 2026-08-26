use crate::config::PostgresConnectorConfig;
use crate::driver::open_connection;
use adbc_core::{Connection as _, Statement as _};
use adbc_driver_manager::ManagedConnection;
use adbc_driver_manager::ManagedStatement;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, SchemaRef};
use async_trait::async_trait;
use nexus_core::quote_identifier;
use nexus_core::{
    project_column, split_by_opcode, with_timeout, CheckpointCursor, NexusError, Sink,
};

/// Limite conservador de parâmetros por query no PostgreSQL. O protocolo usa
/// Int16 para contar placeholders, então o limite teórico é 32.767. Usamos
/// 30.000 para sobrar margem para planos grandes e evitar estouro em versões
/// antigas / drivers. (`u16::MAX` = 65535, mas o parser do Postgres limita a
/// 32767 parâmetros posicionais.)
const MAX_PARAMS_PER_QUERY: usize = 30_000;

pub struct PostgresSink {
    connection: ManagedConnection,
    upsert_statement: ManagedStatement,
    delete_statement: ManagedStatement,
    table: String,
    primary_key: String,
    columns: Vec<String>,
    timeout_seconds: u64,
}

impl PostgresSink {
    /// `schema` must match the column order of every `RecordBatch` passed to
    /// `write_batch` — ADBC binds parameters positionally. Also drives
    /// `CREATE TABLE IF NOT EXISTS`: a target table that doesn't exist yet
    /// is created from `schema`'s columns/types before the first upsert,
    /// instead of failing with a bare "relation does not exist". A table
    /// that already exists is left alone (no `ALTER TABLE` reconciliation —
    /// out of scope, same as every other connector's sink).
    pub async fn connect(
        cfg: &PostgresConnectorConfig,
        schema: &SchemaRef,
    ) -> Result<Self, NexusError> {
        let uri = cfg.connection_string();
        let table = cfg.table.clone();
        let primary_key = cfg.primary_key.clone();
        let schema = schema.clone();
        let create_table_sql = build_create_table_sql(&table, &primary_key, &schema)?;
        let (connection, upsert_statement, delete_statement, table, columns) =
            with_timeout(cfg.timeout_seconds, "postgres connect", async {
                tokio::task::spawn_blocking(
                    move || -> Result<_, NexusError> {
                        let mut connection = open_connection(&uri)?;

                        // CREATE TABLE IF NOT EXISTS, depois prepara as
                        // statements fixas (uma linha) usadas para CDC,
                        // deletes e batches pequenos.
                        let mut statement = connection
                            .new_statement()
                            .map_err(|e| NexusError::Connector(e.to_string()))?;
                        statement
                            .set_sql_query(&create_table_sql)
                            .map_err(|e| NexusError::Connector(e.to_string()))?;
                        statement
                            .execute_update()
                            .map_err(|e| NexusError::Connector(format!("create table failed: {e}")))?;

                        let columns: Vec<String> =
                            schema.fields().iter().map(|f| f.name().clone()).collect();
                        let upsert_sql = build_upsert_sql(&table, &primary_key, &columns, 1)?;
                        let delete_sql = build_delete_sql(&table, &primary_key)?;

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

                        Ok((
                            connection,
                            upsert_statement,
                            delete_statement,
                            table,
                            columns,
                        ))
                    },
                )
                .await
                .map_err(|e| NexusError::Connector(format!("blocking task panicked: {e}")))?
            })
            .await?;

        Ok(Self {
            connection,
            upsert_statement,
            delete_statement,
            table,
            primary_key: cfg.primary_key.clone(),
            columns,
            timeout_seconds: cfg.timeout_seconds,
        })
    }
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
            // Can't format a dynamic "NUMERIC(p,s)" through this fn's
            // `&'static str` return type — the 3 precision/scale
            // combinations that matter in practice for the connector's own
            // supported types don't need one; anything else still gets a
            // safe, lossless NUMERIC via the catch-all below.
            let _ = (precision, scale);
            "NUMERIC"
        }
        _ => "TEXT",
    }
}

/// `CREATE TABLE IF NOT EXISTS` from an Arrow schema — see
/// `PostgresSink::connect`'s doc comment. `primary_key` must be one of
/// `schema`'s field names (checked at the DAG-validation layer, same as
/// every other connector's `primary_key`); if it somehow isn't, the
/// `PRIMARY KEY` constraint is simply never added rather than erroring
/// here — Postgres itself will reject the later upsert's `ON CONFLICT`
/// clause with a clear error instead.
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

/// `INSERT ... ON CONFLICT (pk) DO UPDATE` — the idempotency contract every
/// Sink must satisfy (ARCHITECTURE.md §5: checkpointing guarantees
/// at-least-once, so retried batches must not duplicate rows).
///
/// `table`, `primary_key` and every entry in `columns` come from the pipeline
/// spec (attacker-controlled request body) and get spliced into SQL text —
/// ADBC's `bind` only covers row *values*, not identifiers. Every one of them
/// is validated and quoted via `quote_identifier` before that happens; this
/// is the only place allowed to build the upsert SQL.
///
/// `rows` controla quantos grupos de placeholders são gerados. `rows == 1`
/// produz a SQL single-row usada para CDC/deletes/batches pequenos;
/// `rows > 1` produz um bulk `INSERT ... VALUES (...), (...), ... ON CONFLICT`.
fn build_upsert_sql(
    table: &str,
    primary_key: &str,
    columns: &[String],
    rows: usize,
) -> Result<String, NexusError> {
    let quoted_table = quote_identifier(table)?;
    let quoted_primary_key = quote_identifier(primary_key)?;
    let quoted_columns: Vec<_> = columns
        .iter()
        .map(|c| quote_identifier(c))
        .collect::<Result<Vec<_>, _>>()?;

    let num_cols = quoted_columns.len();
    if num_cols == 0 {
        return Err(NexusError::Schema(
            "upsert requires at least one column".into(),
        ));
    }

    let updates: Vec<String> = columns
        .iter()
        .zip(quoted_columns.iter())
        .filter(|(raw, _)| raw.as_str() != primary_key)
        .map(|(_, quoted)| format!("{quoted} = EXCLUDED.{quoted}"))
        .collect();

    let mut placeholder = 1usize;
    let mut value_groups = Vec::with_capacity(rows);
    for _ in 0..rows {
        let group: Vec<String> = (0..num_cols)
            .map(|_| {
                let p = placeholder;
                placeholder += 1;
                format!("${p}")
            })
            .collect();
        value_groups.push(format!("({})", group.join(", ")));
    }

    Ok(format!(
        "INSERT INTO {quoted_table} ({cols}) VALUES {vals} ON CONFLICT ({quoted_primary_key}) DO UPDATE SET {upd}",
        cols = quoted_columns.join(", "),
        vals = value_groups.join(", "),
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

/// Número máximo de linhas que cabem em uma query dado o número de colunas,
/// respeitando `MAX_PARAMS_PER_QUERY`.
fn max_rows_per_upsert(num_cols: usize) -> usize {
    if num_cols == 0 {
        return 0;
    }
    MAX_PARAMS_PER_QUERY / num_cols
}

impl PostgresSink {
    /// Executa a statement preparada fixa (single-row) usando uma cópia da
    /// statement. Usada para deletes e para batches pequenos/CDC.
    async fn execute_prepared(&self, statement: &ManagedStatement, batch: RecordBatch) -> Result<(), NexusError> {
        let mut statement = statement.clone();
        let timeout_seconds = self.timeout_seconds;

        with_timeout(timeout_seconds, "postgres execute", async {
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

    /// Executa um bulk upsert, dividindo o batch em chunks que respeitem o
    /// limite de parâmetros do PostgreSQL. Para cada chunk constrói uma SQL
    /// dinâmica multi-row, prepara, binda e executa.
    async fn execute_bulk_upsert(&self, batch: RecordBatch) -> Result<(), NexusError> {
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return Ok(());
        }

        let num_cols = batch.num_columns();
        if num_rows == 1 {
            // Caminho rápido: uma única linha usa a statement já preparada.
            return self.execute_prepared(&self.upsert_statement, batch).await;
        }

        let max_rows = max_rows_per_upsert(num_cols).max(1);

        let schema = batch.schema();
        let columns: Vec<_> = (0..num_cols).map(|i| batch.column(i).clone()).collect();
        let mut connection = self.connection.clone();
        let table = self.table.clone();
        let primary_key = self.primary_key.clone();
        let column_names = self.columns.clone();
        let timeout_seconds = self.timeout_seconds;

        with_timeout(timeout_seconds, "postgres bulk upsert", async {
            tokio::task::spawn_blocking(move || -> Result<(), NexusError> {
                let mut offset = 0usize;
                while offset < num_rows {
                    let end = (offset + max_rows).min(num_rows);
                    let chunk_len = end - offset;

                    let chunk_columns: Vec<_> = columns
                        .iter()
                        .map(|col| col.slice(offset, chunk_len))
                        .collect();
                    let chunk_batch = RecordBatch::try_new(schema.clone(), chunk_columns)
                        .map_err(|e| NexusError::Connector(format!("chunk batch failed: {e}")))?;

                    let sql = build_upsert_sql(&table, &primary_key, &column_names, chunk_len)?;

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
                        .bind(chunk_batch)
                        .map_err(|e| NexusError::Connector(e.to_string()))?;
                    statement
                        .execute_update()
                        .map_err(|e| NexusError::Connector(e.to_string()))?;

                    offset = end;
                }
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
        // silently upserted. Plain (non-CDC) batches take the bulk upsert
        // path.
        match split_by_opcode(&batch)? {
            None => self.execute_bulk_upsert(batch).await,
            Some(split) => {
                if split.upserts.num_rows() > 0 {
                    // CDC upserts normalmente são micro-batches; bulk ainda
                    // funciona e permanece idempotente.
                    self.execute_bulk_upsert(split.upserts).await?;
                }
                if split.deletes.num_rows() > 0 {
                    let keys = project_column(&split.deletes, &self.primary_key)?;
                    self.execute_prepared(&self.delete_statement, keys).await?;
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
        let sql = build_upsert_sql(
            "events",
            "id",
            &["id".to_string(), "name".to_string(), "score".to_string()],
            1,
        )
        .unwrap();

        assert_eq!(
            sql,
            "INSERT INTO \"events\" (\"id\", \"name\", \"score\") VALUES ($1, $2, $3) \
             ON CONFLICT (\"id\") DO UPDATE SET \"name\" = EXCLUDED.\"name\", \"score\" = EXCLUDED.\"score\""
        );
    }

    #[test]
    fn upsert_sql_builds_bulk_values() {
        let sql = build_upsert_sql(
            "events",
            "id",
            &["id".to_string(), "name".to_string()],
            3,
        )
        .unwrap();

        assert_eq!(
            sql,
            "INSERT INTO \"events\" (\"id\", \"name\") VALUES ($1, $2), ($3, $4), ($5, $6) \
             ON CONFLICT (\"id\") DO UPDATE SET \"name\" = EXCLUDED.\"name\""
        );
    }

    #[test]
    fn upsert_sql_with_zero_columns_fails() {
        let err = build_upsert_sql("events", "id", &[], 1).expect_err("zero columns must fail");
        assert!(matches!(err, NexusError::Schema(_)));
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
        let err = build_upsert_sql(
            "events\"; DROP TABLE users; --",
            "id",
            &["id".to_string()],
            1,
        )
        .expect_err("malicious table name must be rejected");
        assert!(matches!(err, NexusError::Schema(_)));
    }

    #[test]
    fn rejects_sql_injection_in_column_name() {
        let err = build_upsert_sql(
            "events",
            "id",
            &["id".to_string(), "score); DROP TABLE users; --".to_string()],
            1,
        )
        .expect_err("malicious column name must be rejected");
        assert!(matches!(err, NexusError::Schema(_)));
    }

    #[test]
    fn max_rows_per_upsert_respects_param_limit() {
        assert_eq!(max_rows_per_upsert(1), MAX_PARAMS_PER_QUERY);
        assert_eq!(max_rows_per_upsert(3), MAX_PARAMS_PER_QUERY / 3);
        assert_eq!(max_rows_per_upsert(0), 0);
    }
}
