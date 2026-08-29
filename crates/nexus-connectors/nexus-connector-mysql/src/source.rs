use crate::config::{MySqlCdcDataType, MySqlCdcFieldSpec, MySqlConnectorConfig};
use crate::rows::{build_select_sql, quote_ident, row_to_json};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use futures::stream::{BoxStream, Stream};
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Error as MySqlError, Row};
use nexus_core::{with_timeout, NexusError, RecordBatchBuilder, Source};
use serde_json::Value;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::time::Sleep;

/// Bridging connector for MySQL — no ADBC driver exists upstream (see
/// `config.rs`'s module doc), so this converts rows into `RecordBatch` via
/// `RecordBatchBuilder`, same posture as `mongodb`. No native partitioning:
/// degenerate single-partition case (ARCHITECTURE.md §4).
pub struct MySqlSource {
    conn: Conn,
    select_sql: String,
    schema: SchemaRef,
    fields: Vec<MySqlCdcFieldSpec>,
    batch_size: usize,
    timeout_seconds: u64,
}

impl MySqlSource {
    pub async fn connect(config: &MySqlConnectorConfig) -> Result<Self, NexusError> {
        let mut conn = with_timeout(config.timeout_seconds, "mysql connect", async {
            Conn::from_url(config.connection_string())
                .await
                .map_err(|e| NexusError::Connector(format!("mysql connect failed: {e}")))
        })
        .await?;

        let fields = if config.fields.is_empty() {
            discover_fields(&mut conn, &config.table, config.timeout_seconds).await?
        } else {
            config.fields.clone()
        };

        Ok(Self {
            conn,
            select_sql: build_select_sql(&config.table, &fields)?,
            schema: build_schema(&fields),
            fields,
            batch_size: config.batch_size,
            timeout_seconds: config.timeout_seconds,
        })
    }
}

/// Reads `table`'s real column metadata from MySQL's own catalog
/// (`SHOW COLUMNS FROM table`) — shared by the batch source's
/// `MySqlConnectorConfig::fields` fallback and `mysql-cdc`'s (see
/// `cdc.rs`), since binlog events carry column values positionally but
/// never names (`binlog_row_metadata=FULL` is off by default — see
/// `MySqlCdcSourceConfig::fields` doc comment), so CDC needs this same
/// query to know what to call each position. `SHOW COLUMNS` returns rows
/// in table-definition order, which is exactly the order the binlog's
/// positional values arrive in.
pub(crate) async fn discover_fields(
    conn: &mut Conn,
    table: &str,
    timeout_seconds: u64,
) -> Result<Vec<MySqlCdcFieldSpec>, NexusError> {
    let quoted_table = quote_ident(table)?;
    let rows: Vec<Row> = with_timeout(timeout_seconds, "mysql show columns", async {
        conn.query(format!("SHOW COLUMNS FROM {quoted_table}"))
            .await
            .map_err(|e| NexusError::Connector(format!("mysql SHOW COLUMNS failed: {e}")))
    })
    .await?;

    rows.into_iter()
        .map(|mut row| {
            let name: String = row
                .take("Field")
                .ok_or_else(|| NexusError::Connector("SHOW COLUMNS: missing Field".into()))?;
            let sql_type: String = row
                .take("Type")
                .ok_or_else(|| NexusError::Connector("SHOW COLUMNS: missing Type".into()))?;
            let nullable: String = row
                .take("Null")
                .ok_or_else(|| NexusError::Connector("SHOW COLUMNS: missing Null".into()))?;
            Ok(MySqlCdcFieldSpec {
                name,
                data_type: mysql_type_to_data_type(&sql_type),
                nullable: nullable.eq_ignore_ascii_case("yes"),
            })
        })
        .collect()
}

/// Narrows a MySQL column type string (as `SHOW COLUMNS`' `Type` column
/// reports it, e.g. `"int(11)"`, `"varchar(255)"`, `"decimal(10,2)"`) down
/// to this connector's 4 supported Arrow types — matched on the prefix
/// before any `(...)` precision/length suffix. Anything not recognized
/// falls back to `Utf8`, same "never lose the value" principle as the
/// other connectors' inference.
pub(crate) fn mysql_type_to_data_type(sql_type: &str) -> MySqlCdcDataType {
    let base = sql_type.split('(').next().unwrap_or(sql_type).trim();
    match base {
        "tinyint" if sql_type.contains("(1)") => MySqlCdcDataType::Boolean,
        "tinyint" | "smallint" | "mediumint" | "int" | "integer" | "bigint" | "year" => {
            MySqlCdcDataType::Int64
        }
        "float" | "double" | "decimal" | "numeric" => MySqlCdcDataType::Float64,
        "bool" | "boolean" => MySqlCdcDataType::Boolean,
        _ => MySqlCdcDataType::Utf8,
    }
}

fn build_schema(fields: &[MySqlCdcFieldSpec]) -> SchemaRef {
    Arc::new(Schema::new(
        fields
            .iter()
            .map(|f| {
                let data_type = match f.data_type {
                    MySqlCdcDataType::Int64 => DataType::Int64,
                    MySqlCdcDataType::Float64 => DataType::Float64,
                    MySqlCdcDataType::Boolean => DataType::Boolean,
                    MySqlCdcDataType::Utf8 => DataType::Utf8,
                };
                Field::new(&f.name, data_type, f.nullable)
            })
            .collect::<Vec<_>>(),
    ))
}

/// Lazy stream returned by [`MySqlSource::read_batches`]. Pulls rows from the
/// MySQL result set one at a time and emits `RecordBatch`es as soon as a
/// batch is full, so a huge table does not have to be materialised in memory
/// before the downstream pipeline can start (CLAUDE.md §8.1 / M2).
struct MySqlReadStream<'a> {
    rows: Pin<Box<dyn Stream<Item = Result<Row, MySqlError>> + Send + 'a>>,
    fields: Vec<MySqlCdcFieldSpec>,
    schema: SchemaRef,
    batch_size: usize,
    buffer: Vec<Value>,
    finished: bool,
    idle_timeout: Duration,
    // Reset every time the stream yields something — fires only when it
    // stays Pending longer than `idle_timeout` (a wedged connection), same
    // idea as the MongoDB source's cursor idle cutoff (C15).
    deadline: Pin<Box<Sleep>>,
}

impl MySqlReadStream<'_> {
    fn flush_batch(&mut self) -> Result<RecordBatch, NexusError> {
        let batch = RecordBatchBuilder::from_json_rows(self.schema.clone(), &self.buffer)?;
        self.buffer.clear();
        Ok(batch)
    }
}

impl Stream for MySqlReadStream<'_> {
    type Item = Result<RecordBatch, NexusError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if self.buffer.len() >= self.batch_size {
                return Poll::Ready(Some(self.flush_batch()));
            }
            if self.finished {
                return if self.buffer.is_empty() {
                    Poll::Ready(None)
                } else {
                    Poll::Ready(Some(self.flush_batch()))
                };
            }

            match self.rows.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(row))) => {
                    let deadline = tokio::time::Instant::now() + self.idle_timeout;
                    self.deadline.as_mut().reset(deadline);
                    match row_to_json(&row, &self.fields) {
                        Ok(value) => self.buffer.push(value),
                        Err(e) => {
                            self.finished = true;
                            return Poll::Ready(Some(Err(e)));
                        }
                    }
                }
                Poll::Ready(Some(Err(e))) => {
                    self.finished = true;
                    return Poll::Ready(Some(Err(NexusError::Connector(format!(
                        "mysql row stream error: {e}"
                    )))));
                }
                Poll::Ready(None) => {
                    self.finished = true;
                }
                Poll::Pending => {
                    if self.deadline.as_mut().poll(cx).is_ready() {
                        self.finished = true;
                        return Poll::Ready(Some(Err(NexusError::Connector(format!(
                            "mysql row stream stalled for more than {}s",
                            self.idle_timeout.as_secs()
                        )))));
                    }
                    return if self.buffer.is_empty() {
                        Poll::Pending
                    } else {
                        Poll::Ready(Some(self.flush_batch()))
                    };
                }
            }
        }
    }
}

#[async_trait]
impl Source for MySqlSource {
    async fn read_batches(
        &mut self,
    ) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError> {
        let idle_timeout = Duration::from_secs(self.timeout_seconds);
        let sql = self.select_sql.clone();
        let row_stream = with_timeout(self.timeout_seconds, "mysql query", async {
            self.conn
                .query_stream::<Row, _>(sql)
                .await
                .map_err(|e| NexusError::Connector(format!("mysql query failed: {e}")))
        })
        .await?;

        Ok(Box::pin(MySqlReadStream {
            rows: Box::pin(row_stream),
            fields: self.fields.clone(),
            schema: self.schema.clone(),
            batch_size: self.batch_size,
            buffer: Vec::with_capacity(self.batch_size),
            finished: false,
            idle_timeout,
            deadline: Box::pin(tokio::time::sleep(idle_timeout)),
        }))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
