use crate::config::{OdbcConnectorConfig, OdbcDataType, OdbcFieldSpec};
use crate::sql::build_select_sql;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use nexus_core::{NexusError, RecordBatchBuilder, Source};
use odbc_api::{Bit, ConnectionOptions, Cursor, Environment, Nullable};
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::Sender;

/// Batches in flight between the blocking ODBC cursor thread and the async
/// stream consumer. Small on purpose: this is read-ahead, not the primary
/// buffer — a full `Sender::blocking_send` stalls the cursor thread once it
/// fills up, so a slow consumer holds back ODBC fetches instead of the
/// whole table piling up in memory before the first batch is even yielded
/// (M2, CLAUDE.md §8.1; same bounded-channel backpressure design as
/// `PipelineEngine::run_partition`, ARCHITECTURE.md §4.2).
const CHANNEL_CAPACITY: usize = 4;

/// Bridging connector for legacy databases via ODBC — see ARCHITECTURE.md
/// §2/§4.1. Row-wise cursor access (`Cursor::next_row`), not columnar
/// buffer binding: this is the generic/portable tier, not a perf fast-path.
pub struct OdbcSource {
    config: OdbcConnectorConfig,
    schema: SchemaRef,
}

impl OdbcSource {
    pub fn connect(config: &OdbcConnectorConfig) -> Result<Self, NexusError> {
        Ok(Self {
            config: config.clone(),
            schema: build_schema(&config.fields),
        })
    }
}

fn build_schema(fields: &[OdbcFieldSpec]) -> SchemaRef {
    Arc::new(Schema::new(
        fields
            .iter()
            .map(|f| {
                let data_type = match f.data_type {
                    OdbcDataType::Int64 => DataType::Int64,
                    OdbcDataType::Float64 => DataType::Float64,
                    OdbcDataType::Boolean => DataType::Boolean,
                    OdbcDataType::Utf8 => DataType::Utf8,
                };
                Field::new(&f.name, data_type, f.nullable)
            })
            .collect::<Vec<_>>(),
    ))
}

/// Runs entirely inside one blocking closure: `odbc-api` handles wrap raw
/// (non-`Send`) pointers, so the `Environment`/`Connection`/`Cursor` must
/// never cross an `.await` — see the module-level note in `sink.rs`. Any
/// error (connect, query, or a row failing to decode) is reported as one
/// `Err` item on `tx` rather than a return value, since this runs detached
/// inside `spawn_blocking` — see `read_batches` below.
fn fetch_all(
    config: &OdbcConnectorConfig,
    schema: &SchemaRef,
    tx: &Sender<Result<RecordBatch, NexusError>>,
) {
    if let Err(e) = fetch_all_inner(config, schema, tx) {
        let _ = tx.blocking_send(Err(e));
    }
}

fn fetch_all_inner(
    config: &OdbcConnectorConfig,
    schema: &SchemaRef,
    tx: &Sender<Result<RecordBatch, NexusError>>,
) -> Result<(), NexusError> {
    let env = Environment::new().map_err(|e| NexusError::Connector(format!("odbc env: {e}")))?;
    let conn = env
        .connect_with_connection_string(&config.connection_string, ConnectionOptions::default())
        .map_err(|e| NexusError::Connector(format!("odbc connect: {e}")))?;

    let sql = build_select_sql(&config.table, &config.fields)?;
    let mut cursor = conn
        .execute(&sql, (), None)
        .map_err(|e| NexusError::Connector(format!("odbc query failed: {e}")))?
        .ok_or_else(|| NexusError::Connector("odbc SELECT returned no result set".into()))?;

    let mut buffer: Vec<Value> = Vec::with_capacity(config.batch_size);
    let send_batch = |buffer: &mut Vec<Value>| -> Result<(), NexusError> {
        let batch = RecordBatchBuilder::from_json_rows(schema.clone(), buffer)?;
        buffer.clear();
        tx.blocking_send(Ok(batch))
            .map_err(|_| NexusError::Connector("odbc reader: receiver dropped".into()))
    };

    while let Some(mut row) = cursor
        .next_row()
        .map_err(|e| NexusError::Connector(format!("odbc fetch failed: {e}")))?
    {
        let mut object = serde_json::Map::new();
        for (idx, field) in config.fields.iter().enumerate() {
            let col = (idx + 1) as u16;
            let value = read_column(&mut row, col, field.data_type)?;
            object.insert(field.name.clone(), value);
        }
        buffer.push(Value::Object(object));

        if buffer.len() >= config.batch_size {
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
    data_type: OdbcDataType,
) -> Result<Value, NexusError> {
    let value = match data_type {
        OdbcDataType::Int64 => {
            let mut target = Nullable::<i64>::null();
            row.get_data(col, &mut target)
                .map_err(|e| NexusError::Connector(format!("odbc get_data failed: {e}")))?;
            target.into_opt().map(Value::from).unwrap_or(Value::Null)
        }
        OdbcDataType::Float64 => {
            let mut target = Nullable::<f64>::null();
            row.get_data(col, &mut target)
                .map_err(|e| NexusError::Connector(format!("odbc get_data failed: {e}")))?;
            target.into_opt().map(Value::from).unwrap_or(Value::Null)
        }
        OdbcDataType::Boolean => {
            let mut target = Nullable::<Bit>::null();
            row.get_data(col, &mut target)
                .map_err(|e| NexusError::Connector(format!("odbc get_data failed: {e}")))?;
            target
                .into_opt()
                .map(|b| Value::from(b.as_bool()))
                .unwrap_or(Value::Null)
        }
        OdbcDataType::Utf8 => {
            let mut buf = Vec::new();
            let has_value = row
                .get_text(col, &mut buf)
                .map_err(|e| NexusError::Connector(format!("odbc get_text failed: {e}")))?;
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
impl Source for OdbcSource {
    async fn read_batches(
        &mut self,
    ) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError> {
        let config = self.config.clone();
        let schema = self.schema.clone();
        let (tx, rx) =
            tokio::sync::mpsc::channel::<Result<RecordBatch, NexusError>>(CHANNEL_CAPACITY);

        // Fire-and-forget: the blocking cursor thread reports its own
        // errors through `tx` (see `fetch_all`), and a panic just drops the
        // sender, which ends the stream with `None` like a clean EOF. There
        // is no separate result to join back here — that's the whole point
        // of streaming the batches out as they're produced instead of
        // collecting them first.
        tokio::task::spawn_blocking(move || fetch_all(&config, &schema, &tx));

        // Idle cutoff, not a total-scan timeout: `deadline` resets every
        // time a batch actually arrives, so a large-but-healthy scan never
        // trips it — only a cursor thread wedged on the driver does (C15).
        // Only unblocks this async side; the blocking `fetch_all` call and
        // its OS thread keep running regardless (no cross-thread
        // cancellation for raw ODBC handles, same trade-off as the sink).
        let idle_timeout = Duration::from_secs(self.config.timeout_seconds);
        Ok(Box::pin(stream::unfold(rx, move |mut rx| async move {
            match tokio::time::timeout(idle_timeout, rx.recv()).await {
                Ok(Some(item)) => Some((item, rx)),
                Ok(None) => None,
                Err(_) => Some((
                    Err(NexusError::Connector(format!(
                        "odbc cursor stalled for more than {}s",
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
