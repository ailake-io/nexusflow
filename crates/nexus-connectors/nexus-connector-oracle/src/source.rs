use crate::config::OracleConnectorConfig;
use crate::driver::connection_string;
use crate::schema::{build_schema, describe_table, OracleColumn};
use crate::sql::build_select_sql;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use nexus_core::{retry_with_backoff, with_timeout, NexusError, RecordBatchBuilder, Source};
use odbc_api::{ConnectionOptions, Cursor, Environment, Nullable};
use serde_json::Value;
use std::time::Duration;
use tokio::sync::mpsc::Sender;

const CHANNEL_CAPACITY: usize = 4;
const BATCH_SIZE: usize = 1000;

/// `odbc-api` handles (`Environment`/`Connection`/`Cursor`) aren't
/// `Send`, so — same constraint `nexus-connector-odbc` (public repo)
/// documents — every ODBC call runs inside `spawn_blocking`, never
/// held across an `.await`.
///
/// `connect()` opens one short-lived connection just to run
/// `describe_table` (needs a live connection to query
/// `ALL_TAB_COLUMNS`); `read_batches()` opens a second, separate
/// connection for the actual `SELECT`. Two connections per source
/// instance instead of one — the simplest correct option given the
/// non-`Send` constraint, not a perf-tuned design.
pub struct OracleSource {
    config: OracleConnectorConfig,
    schema: SchemaRef,
    columns: Vec<OracleColumn>,
}

impl OracleSource {
    pub async fn connect(config: &OracleConnectorConfig) -> Result<Self, NexusError> {
        config.validate()?;
        let cfg = config.clone();
        let retry = config.retry.clone();
        let timeout_seconds = config.timeout_seconds;
        let columns = retry_with_backoff(&retry, "oracle describe_table", move || {
            let cfg = cfg.clone();
            async move {
                with_timeout(timeout_seconds, "oracle describe_table", async {
                    tokio::task::spawn_blocking(move || -> Result<Vec<OracleColumn>, NexusError> {
                        let env = Environment::new()
                            .map_err(|e| NexusError::Connector(format!("oracle env: {e}")))?;
                        let conn = env
                            .connect_with_connection_string(
                                &connection_string(&cfg),
                                ConnectionOptions::default(),
                            )
                            .map_err(|e| NexusError::Connector(format!("oracle connect: {e}")))?;
                        describe_table(&conn, &cfg.table)
                    })
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
    config: &OracleConnectorConfig,
    columns: &[OracleColumn],
    schema: &SchemaRef,
    tx: &Sender<Result<RecordBatch, NexusError>>,
) -> Result<(), NexusError> {
    let env = Environment::new().map_err(|e| NexusError::Connector(format!("oracle env: {e}")))?;
    let conn = env
        .connect_with_connection_string(&connection_string(config), ConnectionOptions::default())
        .map_err(|e| NexusError::Connector(format!("oracle connect: {e}")))?;

    let column_names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
    let sql = build_select_sql(&config.table, &column_names)?;
    let mut cursor = conn
        .execute(&sql, (), None)
        .map_err(|e| NexusError::Connector(format!("oracle query failed: {e}")))?
        .ok_or_else(|| NexusError::Connector("oracle SELECT returned no result set".into()))?;

    let mut buffer: Vec<Value> = Vec::with_capacity(BATCH_SIZE);
    let send_batch = |buffer: &mut Vec<Value>| -> Result<(), NexusError> {
        let batch = RecordBatchBuilder::from_json_rows(schema.clone(), buffer)?;
        buffer.clear();
        tx.blocking_send(Ok(batch))
            .map_err(|_| NexusError::Connector("oracle reader: receiver dropped".into()))
    };

    while let Some(mut row) = cursor
        .next_row()
        .map_err(|e| NexusError::Connector(format!("oracle fetch failed: {e}")))?
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
                .map_err(|e| NexusError::Connector(format!("oracle get_data failed: {e}")))?;
            target.into_opt().map(Value::from).unwrap_or(Value::Null)
        }
        DataType::Float64 => {
            let mut target = Nullable::<f64>::null();
            row.get_data(col, &mut target)
                .map_err(|e| NexusError::Connector(format!("oracle get_data failed: {e}")))?;
            target.into_opt().map(Value::from).unwrap_or(Value::Null)
        }
        _ => {
            let mut buf = Vec::new();
            let has_value = row
                .get_text(col, &mut buf)
                .map_err(|e| NexusError::Connector(format!("oracle get_text failed: {e}")))?;
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
impl Source for OracleSource {
    async fn read_batches(
        &mut self,
    ) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError> {
        let config = self.config.clone();
        let columns = self.columns.clone();
        let schema = self.schema.clone();
        let retry = config.retry.clone();
        let idle_timeout = Duration::from_secs(config.timeout_seconds);

        let rx = retry_with_backoff(&retry, "oracle source fetch", move || {
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
                        "oracle cursor stalled for more than {}s",
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
