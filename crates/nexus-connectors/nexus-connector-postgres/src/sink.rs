use crate::config::PostgresConnectorConfig;
use crate::driver::open_connection;
use adbc_core::{Connection as _, Statement as _};
use adbc_driver_manager::ManagedStatement;
use arrow_array::{Array, ListArray, RecordBatch, StringArray};
use arrow_cast::cast;
use arrow_schema::{DataType, Field, SchemaRef};
use async_trait::async_trait;
use nexus_core::quote_identifier;
use nexus_core::{
    project_column, split_by_opcode, with_timeout, CheckpointCursor, NexusError, Sink,
};
use std::sync::Arc;

pub struct PostgresSink {
    upsert_statement: ManagedStatement,
    delete_statement: ManagedStatement,
    primary_key: String,
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
        let (upsert_statement, delete_statement) =
            with_timeout(cfg.timeout_seconds, "postgres connect", async {
                tokio::task::spawn_blocking(
                    move || -> Result<_, NexusError> {
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

                        let columns: Vec<String> =
                            schema.fields().iter().map(|f| f.name().clone()).collect();
                        let upsert_sql = build_upsert_sql(&table, &primary_key, &columns)?;
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

                        Ok((upsert_statement, delete_statement))
                    },
                )
                .await
                .map_err(|e| NexusError::Connector(format!("blocking task panicked: {e}")))?
            })
            .await?;

        Ok(Self {
            upsert_statement,
            delete_statement,
            primary_key: cfg.primary_key.clone(),
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

/// `INSERT ... ON CONFLICT (pk) DO UPDATE` using `UNNEST` so a single
/// prepared statement can ingest an arbitrary number of rows per batch.
/// ADBC binds each parameter as a PostgreSQL array; `UNNEST` turns those
/// arrays back into rows. All values are sent as `text[]` and Postgres
/// coerces them to the target column types on insert, matching the
/// "never lose the value" fallback posture.
fn build_upsert_sql(
    table: &str,
    primary_key: &str,
    columns: &[String],
) -> Result<String, NexusError> {
    let quoted_table = quote_identifier(table)?;
    let quoted_primary_key = quote_identifier(primary_key)?;
    let quoted_columns: Vec<_> = columns
        .iter()
        .map(|c| quote_identifier(c))
        .collect::<Result<Vec<_>, _>>()?;

    let unnest_args: Vec<String> = (1..=columns.len())
        .map(|i| format!("${i}::text[]"))
        .collect();
    let updates: Vec<String> = columns
        .iter()
        .zip(quoted_columns.iter())
        .filter(|(raw, _)| raw.as_str() != primary_key)
        .map(|(_, quoted)| format!("{quoted} = EXCLUDED.{quoted}"))
        .collect();

    Ok(format!(
        "INSERT INTO {quoted_table} ({cols}) SELECT * FROM UNNEST({args}) ON CONFLICT ({quoted_primary_key}) DO UPDATE SET {upd}",
        cols = quoted_columns.join(", "),
        args = unnest_args.join(", "),
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

/// Converts any Arrow array into a single-element `ListArray<StringArray>`,
/// where the list contains every original value formatted as text. Used to
/// build parameters for `INSERT ... SELECT * FROM UNNEST($1::text[], ...)`.
fn array_to_text_list(arr: &Arc<dyn Array>) -> Result<Arc<dyn Array>, NexusError> {
    let string_arr = cast(arr.as_ref(), &DataType::Utf8)
        .map_err(|e| NexusError::Connector(format!("cast to utf8 failed: {e}")))?;
    let string_arr = string_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("cast to Utf8 returns StringArray");

    let item_field = Arc::new(Field::new("item", DataType::Utf8, true));
    let offsets = arrow_buffer::OffsetBuffer::from_lengths([string_arr.len()]);
    let list_arr = ListArray::new(item_field, offsets, Arc::new(string_arr.clone()), None);
    Ok(Arc::new(list_arr))
}

impl PostgresSink {
    /// Executes the prepared UNNEST upsert with a batch whose columns have
    /// been converted to single-element text lists.
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

    /// Executes the prepared single-row delete statement. ACDC delete batches
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

/// Builds a schema whose fields have the same names as `schema` but whose
/// types are `List(Utf8)`, matching the arrays produced by `array_to_text_list`.
fn text_list_schema(schema: &SchemaRef) -> SchemaRef {
    let item_field = Arc::new(Field::new("item", DataType::Utf8, true));
    let fields: Vec<Arc<Field>> = schema
        .fields()
        .iter()
        .map(|f| Arc::new(Field::new(f.name(), DataType::List(item_field.clone()), f.is_nullable())))
        .collect();
    Arc::new(arrow_schema::Schema::new(fields))
}

#[async_trait]
impl Sink for PostgresSink {
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<(), NexusError> {
        // CDC batches carry an `__opcode` column (ARCHITECTURE.md §5) — split
        // it so deletes are issued as real `DELETE`s instead of being
        // silently upserted. Plain (non-CDC) batches take the UNNEST bulk
        // upsert path.
        match split_by_opcode(&batch)? {
            None => {
                let list_columns: Result<Vec<_>, _> = batch
                    .columns()
                    .iter()
                    .map(|col| array_to_text_list(col))
                    .collect();
                let upsert_batch = RecordBatch::try_new(
                    text_list_schema(&batch.schema()),
                    list_columns?,
                )
                .map_err(|e| NexusError::Connector(format!("upsert batch rebuild failed: {e}")))?;
                self.execute_upsert(upsert_batch).await
            }
            Some(split) => {
                if split.upserts.num_rows() > 0 {
                    let list_columns: Result<Vec<_>, _> = split
                        .upserts
                        .columns()
                        .iter()
                        .map(|col| array_to_text_list(col))
                        .collect();
                    let upsert_batch = RecordBatch::try_new(
                        text_list_schema(&split.upserts.schema()),
                        list_columns?,
                    )
                    .map_err(|e| NexusError::Connector(format!("cdc upsert batch rebuild failed: {e}")))?;
                    self.execute_upsert(upsert_batch).await?;
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
    fn upsert_sql_uses_unnest_with_text_arrays() {
        let sql = build_upsert_sql(
            "events",
            "id",
            &["id".to_string(), "name".to_string(), "score".to_string()],
        )
        .unwrap();

        assert_eq!(
            sql,
            "INSERT INTO \"events\" (\"id\", \"name\", \"score\") SELECT * FROM UNNEST($1::text[], $2::text[], $3::text[]) \
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

    #[test]
    fn array_to_text_list_builds_single_element_list() {
        let arr = Arc::new(Int64Array::from(vec![1, 2, 3])) as Arc<dyn Array>;
        let list = array_to_text_list(&arr).unwrap();
        let list = list
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("returns ListArray");
        assert_eq!(list.len(), 1);
        assert_eq!(list.value_length(0), 3);
    }
}
