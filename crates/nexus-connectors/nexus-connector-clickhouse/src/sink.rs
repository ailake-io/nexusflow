use crate::config::ClickHouseConnectorConfig;
use crate::driver::open_connection;
use adbc_core::{Connection as _, Statement as _};
use adbc_driver_manager::ManagedConnection;
use arrow_array::{Array, RecordBatch, StringArray};
use arrow_cast::cast;
use arrow_schema::DataType;
use async_trait::async_trait;
use nexus_core::quote_identifier;
use nexus_core::{split_by_opcode, with_timeout, CheckpointCursor, NexusError, Sink};
use std::sync::Arc;

pub struct ClickHouseSink {
    connection: ManagedConnection,
    table: String,
    columns: Vec<String>,
    timeout_seconds: u64,
}

impl ClickHouseSink {
    /// `columns` must match the column order of every `RecordBatch` passed to
    /// `write_batch`.
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
        Ok(Self {
            connection,
            table: cfg.table.clone(),
            columns: columns.to_vec(),
            timeout_seconds: cfg.timeout_seconds,
        })
    }
}

/// Builds the `INSERT INTO table (cols)` prefix. The full statement appends a
/// multi-row `VALUES (...), (...), ...` clause built per batch.
fn build_insert_prefix(table: &str, columns: &[String]) -> Result<String, NexusError> {
    let quoted_table = quote_identifier(table)?;
    let quoted_columns = columns
        .iter()
        .map(|c| quote_identifier(c))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(format!(
        "INSERT INTO {quoted_table} ({cols}) VALUES ",
        cols = quoted_columns.join(", ")
    ))
}

/// Formats any Arrow array as a vector of SQL literal strings. Values are cast
/// to Utf8 first, then escaped for ClickHouse SQL. Numeric/boolean types are
/// emitted without quotes; everything else is quoted as a string.
fn column_to_sql_literals(
    column: &Arc<dyn Array>,
    data_type: &DataType,
) -> Result<Vec<String>, NexusError> {
    let string_arr = cast(column.as_ref(), &DataType::Utf8)
        .map_err(|e| NexusError::Connector(format!("cast to utf8 failed: {e}")))?;
    let string_arr = string_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("cast to Utf8 returns StringArray");

    let is_numeric = matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Boolean
    );

    Ok((0..string_arr.len())
        .map(|i| {
            if string_arr.is_null(i) {
                "NULL".to_string()
            } else if is_numeric {
                string_arr.value(i).to_string()
            } else {
                format!("'{}'", clickhouse_escape(string_arr.value(i)))
            }
        })
        .collect())
}

fn clickhouse_escape(value: &str) -> String {
    value.replace('\\', "\\\\").replace('\'', "\\'")
}

/// Builds a multi-row `INSERT ... VALUES (...), (...)` statement from a batch.
fn build_insert_sql(
    prefix: &str,
    batch: &RecordBatch,
    columns: &[String],
) -> Result<String, NexusError> {
    if batch.num_rows() == 0 {
        return Ok(String::new());
    }

    let schema = batch.schema();
    let literals_per_column: Vec<Vec<String>> = columns
        .iter()
        .map(|name| {
            let idx = schema
                .index_of(name)
                .map_err(|_| NexusError::Schema(format!("column '{name}' not found in batch")))?;
            let field = schema.field(idx);
            column_to_sql_literals(batch.column(idx), field.data_type())
        })
        .collect::<Result<Vec<_>, NexusError>>()?;

    let mut rows = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let values: Vec<String> = literals_per_column
            .iter()
            .map(|col| col[row].clone())
            .collect();
        rows.push(format!("({})", values.join(", ")));
    }

    Ok(format!("{prefix}{}", rows.join(", ")))
}

impl ClickHouseSink {
    async fn execute(&self, batch: RecordBatch) -> Result<(), NexusError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let prefix = build_insert_prefix(&self.table, &self.columns)?;
        let sql = build_insert_sql(&prefix, &batch, &self.columns)?;
        if sql.is_empty() {
            return Ok(());
        }

        let mut connection = self.connection.clone();
        let timeout_seconds = self.timeout_seconds;

        with_timeout(timeout_seconds, "clickhouse execute", async {
            tokio::task::spawn_blocking(move || -> Result<(), NexusError> {
                let mut statement = connection
                    .new_statement()
                    .map_err(|e| NexusError::Connector(e.to_string()))?;
                statement
                    .set_sql_query(&sql)
                    .map_err(|e| NexusError::Connector(e.to_string()))?;
                statement
                    .execute_update()
                    .map_err(|e| NexusError::Connector(format!("clickhouse insert failed: {e}")))?;
                Ok(())
            })
            .await
            .map_err(|e| NexusError::Connector(format!("blocking task panicked: {e}")))?
        })
        .await
    }
}

#[async_trait]
impl Sink for ClickHouseSink {
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<(), NexusError> {
        // CDC batches carry an `__opcode` column (ARCHITECTURE.md §5). This
        // sink has no DELETE path — a batch that actually contains deletes is
        // rejected loudly instead of silently upserting or dropping rows.
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
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;
    use arrow_schema::{Field, Schema};

    #[test]
    fn insert_prefix_lists_columns() {
        let prefix = build_insert_prefix("events", &["id".to_string(), "name".to_string()]).unwrap();
        assert_eq!(prefix, "INSERT INTO \"events\" (\"id\", \"name\") VALUES ");
    }

    #[test]
    fn build_insert_sql_formats_multi_row_values() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let id = Arc::new(Int64Array::from(vec![1, 2])) as Arc<dyn Array>;
        let name = Arc::new(StringArray::from(vec![Some("a"), None])) as Arc<dyn Array>;
        let batch = RecordBatch::try_new(schema, vec![id, name]).unwrap();

        let sql = build_insert_sql(
            "INSERT INTO t (id, name) VALUES ",
            &batch,
            &["id".to_string(), "name".to_string()],
        )
        .unwrap();
        assert_eq!(sql, "INSERT INTO t (id, name) VALUES (1, 'a'), (2, NULL)");
    }

    #[test]
    fn rejects_sql_injection_in_table_name() {
        let err = build_insert_prefix("events\"; DROP TABLE users; --", &["id".to_string()])
            .expect_err("malicious table name must be rejected");
        assert!(matches!(err, NexusError::Schema(_)));
    }

    #[test]
    fn rejects_sql_injection_in_column_name() {
        let err = build_insert_prefix(
            "events",
            &["id".to_string(), "score); DROP TABLE users; --".to_string()],
        )
        .expect_err("malicious column name must be rejected");
        assert!(matches!(err, NexusError::Schema(_)));
    }
}
