use crate::config::TeradataConnectorConfig;
use crate::driver::connection_string;
use crate::schema::{build_schema, describe_table, TeradataColumn};
use crate::sql::build_select_sql;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use nexus_core::{retry_with_backoff, with_timeout, NexusError, RecordBatchBuilder, Source};
use odbc_api::{Bit, ConnectionOptions, Cursor, Environment, Nullable};
use serde_json::Value;
use std::time::Duration;
use tokio::sync::mpsc::Sender;

const CHANNEL_CAPACITY: usize = 4;
const BATCH_SIZE: usize = 1000;

/// `odbc-api` handles (`Environment`/`Connection`/`Cursor`) aren't
/// `Send` — same constraint every ODBC connector in this repo
/// documents. Every ODBC call runs inside `spawn_blocking`, never
/// held across an `.await`.
///
/// `connect()` opens one short-lived connection just to run
/// `describe_table`; `read_batches()` opens a second, separate
/// connection for the actual `SELECT` — same two-connections
/// trade-off Oracle/HANA accept for the same reason.
pub struct TeradataSource {
    config: TeradataConnectorConfig,
    schema: SchemaRef,
    columns: Vec<TeradataColumn>,
}

impl TeradataSource {
    pub async fn connect(config: &TeradataConnectorConfig) -> Result<Self, NexusError> {
        config.validate()?;
        let cfg = config.clone();
        let retry = config.retry.clone();
        let timeout_seconds = config.timeout_seconds;
        let columns = retry_with_backoff(&retry, "teradata describe_table", move || {
            let cfg = cfg.clone();
            async move {
                with_timeout(timeout_seconds, "teradata describe_table", async {
                    tokio::task::spawn_blocking(
                        move || -> Result<Vec<TeradataColumn>, NexusError> {
                            let env = Environment::new()
                                .map_err(|e| NexusError::Connector(format!("teradata env: {e}")))?;
                            let conn = env
                                .connect_with_connection_string(
                                    &connection_string(&cfg),
                                    ConnectionOptions::default(),
                                )
                                .map_err(|e| {
                                    NexusError::Connector(format!("teradata connect: {e}"))
                                })?;
                            describe_table(&conn, &cfg.table)
                        },
                    )
                    .await
                    .map_err(|e| NexusError::Connector(format!("blocking task panicked: {e}")))?
                })
                .await
            }
        })
        .await?;

        let schema = build_schema(&columns);
        Ok(Self {
            config: config.clone(),
            schema,
            columns,
        })
    }
}

fn fetch_all_inner(
    config: &TeradataConnectorConfig,
    columns: &[TeradataColumn],
    schema: &SchemaRef,
    tx: &Sender<Result<RecordBatch, NexusError>>,
) -> Result<(), NexusError> {
    let env =
        Environment::new().map_err(|e| NexusError::Connector(format!("teradata env: {e}")))?;
    let conn = env
        .connect_with_connection_string(&connection_string(config), ConnectionOptions::default())
        .map_err(|e| NexusError::Connector(format!("teradata connect: {e}")))?;

    let column_names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
    let sql = build_select_sql(&config.table, &column_names)?;
    let mut cursor = conn
        .execute(&sql, (), None)
        .map_err(|e| NexusError::Connector(format!("teradata query failed: {e}")))?
        .ok_or_else(|| NexusError::Connector("teradata SELECT returned no result set".into()))?;

    let mut buffer: Vec<Value> = Vec::with_capacity(BATCH_SIZE);
    let send_batch = |buffer: &mut Vec<Value>| -> Result<(), NexusError> {
        let batch = RecordBatchBuilder::from_json_rows(schema.clone(), buffer)?;
        buffer.clear();
        tx.blocking_send(Ok(batch))
            .map_err(|_| NexusError::Connector("teradata reader: receiver dropped".into()))
    };

    while let Some(mut row) = cursor
        .next_row()
        .map_err(|e| NexusError::Connector(format!("teradata fetch failed: {e}")))?
    {
        let mut object = serde_json::Map::new();
        for (idx, column) in columns.iter().enumerate() {
            let col = (idx + 1) as u16;
            let value = read_column(&mut row, col, &column.arrow_type)?;
            object.insert(column.name.clone(), value);
        }
        buffer.push(Value::Object(object));

        if buffer.len() >= BATCH_SIZE {
            send_batch(&mut buffer)?;
        }
    }
    if !buffer.is_empty() {
        send_batch(&mut buffer)?;
    }

    Ok(())
}

fn read_column(
    row: &mut odbc_api::CursorRow<'_>,
    col: u16,
    arrow_type: &arrow_schema::DataType,
) -> Result<Value, NexusError> {
    use arrow_schema::DataType;
    let value = match arrow_type {
        DataType::Int64 => {
            let mut target = Nullable::<i64>::null();
            row.get_data(col, &mut target)
                .map_err(|e| NexusError::Connector(format!("teradata get_data failed: {e}")))?;
            target.into_opt().map(Value::from).unwrap_or(Value::Null)
        }
        DataType::Float64 => {
            let mut target = Nullable::<f64>::null();
            row.get_data(col, &mut target)
                .map_err(|e| NexusError::Connector(format!("teradata get_data failed: {e}")))?;
            target.into_opt().map(Value::from).unwrap_or(Value::Null)
        }
        DataType::Boolean => {
            let mut target = Nullable::<Bit>::null();
            row.get_data(col, &mut target)
                .map_err(|e| NexusError::Connector(format!("teradata get_data failed: {e}")))?;
            target
                .into_opt()
                .map(|b| Value::from(b.as_bool()))
                .unwrap_or(Value::Null)
        }
        _ => {
            let mut buf = Vec::new();
            let has_value = row
                .get_text(col, &mut buf)
                .map_err(|e| NexusError::Connector(format!("teradata get_text failed: {e}")))?;
            if has_value {
                Value::String(
                    String::from_utf8(buf).map_err(|e| NexusError::Serialization(e.to_string()))?,
                )
            } else {
                Value::Null
            }
        }
    };
    Ok(value)
}

#[async_trait]
impl Source for TeradataSource {
    async fn read_batches(
        &mut self,
    ) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError> {
        let config = self.config.clone();
        let columns = self.columns.clone();
        let schema = self.schema.clone();
        let retry = config.retry.clone();
        let idle_timeout = Duration::from_secs(config.timeout_seconds);

        let rx = retry_with_backoff(&retry, "teradata source fetch", move || {
            let config = config.clone();
            let columns = columns.clone();
            let schema = schema.clone();
            async move {
                let (tx, rx) =
                    tokio::sync::mpsc::channel::<Result<RecordBatch, NexusError>>(CHANNEL_CAPACITY);
                tokio::task::spawn_blocking(move || {
                    if let Err(e) = fetch_all_inner(&config, &columns, &schema, &tx) {
                        let _ = tx.blocking_send(Err(e));
                    }
                });
                Ok::<_, NexusError>(rx)
            }
        })
        .await?;

        Ok(Box::pin(stream::unfold(rx, move |mut rx| async move {
            match tokio::time::timeout(idle_timeout, rx.recv()).await {
                Ok(Some(item)) => Some((item, rx)),
                Ok(None) => None,
                Err(_) => Some((
                    Err(NexusError::Connector(format!(
                        "teradata cursor stalled for more than {}s",
                        idle_timeout.as_secs()
                    ))),
                    rx,
                )),
            }
        })))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
