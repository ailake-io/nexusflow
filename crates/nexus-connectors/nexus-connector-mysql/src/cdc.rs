use crate::config::{MySqlCdcConfig, MySqlCdcDataType, MySqlCdcFieldSpec};
use crate::source::discover_fields;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use futures::stream::BoxStream;
use mysql_async::Conn;
use mysql_cdc::binlog_client::BinlogClient;
use mysql_cdc::binlog_options::BinlogOptions;
use mysql_cdc::events::binlog_event::BinlogEvent;
use mysql_cdc::events::row_events::mysql_value::MySqlValue;
use mysql_cdc::events::row_events::row_data::RowData;
use mysql_cdc::replica_options::ReplicaOptions;
use nexus_core::{NexusError, RecordBatchBuilder, Source, OPCODE_COLUMN};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;

/// How long `read_batches`' stream waits for the next row before flushing
/// whatever's buffered — same "idle is normal, not a stall" reasoning as
/// the Postgres native CDC source.
const FLUSH_ON_IDLE: Duration = Duration::from_millis(500);
const BATCH_SIZE: usize = 500;
const CHANNEL_CAPACITY: usize = 1000;

/// Native CDC source for MySQL/MariaDB via binlog replication — no
/// Debezium/Kafka in front. See `ARCHITECTURE.md §7`.
pub struct MySqlCdcSource {
    config: MySqlCdcConfig,
    schema: SchemaRef,
    /// Updated from the replication thread (`run_replication_loop`) as
    /// `"{binlog_filename}:{next_event_position}"` after each event —
    /// `Source::position_handle`'s backing storage. See that trait method's
    /// doc comment for why this is an `Arc<Mutex<..>>` handle instead of a
    /// plain getter.
    position: Arc<Mutex<Option<String>>>,
}

impl MySqlCdcSource {
    pub async fn connect(config: &MySqlCdcConfig) -> Result<Self, NexusError> {
        let fields = if config.fields.is_empty() {
            // Binlog row events carry column values positionally, never by
            // name, unless the server has `binlog_row_metadata=FULL` set
            // (off by default — see `MySqlCdcFieldSpec` doc comment on the
            // config struct). A plain SQL connection to the same server
            // doesn't have that limitation, so discovery goes through
            // `SHOW COLUMNS` instead of the replication stream itself.
            let mut conn = Conn::from_url(config.connection_string())
                .await
                .map_err(|e| {
                    NexusError::Connector(format!("mysql-cdc schema discovery connect failed: {e}"))
                })?;
            let discovered = discover_fields(&mut conn, &config.table, 30).await?;
            conn.disconnect().await.ok();
            discovered
        } else {
            config.fields.clone()
        };

        Ok(Self {
            config: MySqlCdcConfig {
                fields: fields.clone(),
                ..config.clone()
            },
            schema: build_schema(&fields),
            position: Arc::new(Mutex::new(None)),
        })
    }
}

fn build_schema(fields: &[MySqlCdcFieldSpec]) -> SchemaRef {
    let mut arrow_fields: Vec<Field> = fields
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
        .collect();
    arrow_fields.push(Field::new(OPCODE_COLUMN, DataType::Utf8, false));
    Arc::new(Schema::new(arrow_fields))
}

fn mysql_value_to_json(cell: Option<&MySqlValue>, target: MySqlCdcDataType) -> Value {
    let Some(value) = cell else {
        return Value::Null;
    };
    match (target, value) {
        (MySqlCdcDataType::Int64, MySqlValue::TinyInt(n)) => Value::from(*n as i64),
        (MySqlCdcDataType::Int64, MySqlValue::SmallInt(n)) => Value::from(*n as i64),
        (MySqlCdcDataType::Int64, MySqlValue::MediumInt(n)) => Value::from(*n as i64),
        (MySqlCdcDataType::Int64, MySqlValue::Int(n)) => Value::from(*n as i64),
        (MySqlCdcDataType::Int64, MySqlValue::BigInt(n)) => Value::from(*n as i64),
        (MySqlCdcDataType::Float64, MySqlValue::Float(f)) => Value::from(*f as f64),
        (MySqlCdcDataType::Float64, MySqlValue::Double(f)) => Value::from(*f),
        // A DECIMAL/NUMERIC column (e.g. money amounts) decodes off the
        // binlog as `MySqlValue::Decimal(String)`, not `Float`/`Double` —
        // real bug found testing mysql-cdc end to end this session: mapping
        // such a column to `float64` (the natural choice, and the only
        // numeric option besides `int64`) silently produced `null` for
        // every row, falling through to the catch-all below with no error.
        (MySqlCdcDataType::Float64, MySqlValue::Decimal(s)) => {
            s.parse::<f64>().map(Value::from).unwrap_or(Value::Null)
        }
        (MySqlCdcDataType::Boolean, MySqlValue::TinyInt(n)) => Value::from(*n != 0),
        (MySqlCdcDataType::Utf8, MySqlValue::String(s)) => Value::String(s.clone()),
        (MySqlCdcDataType::Utf8, MySqlValue::Decimal(s)) => Value::String(s.clone()),
        // TEXT/VARCHAR columns come through as raw bytes rather than
        // `MySqlValue::String` whenever the table's charset metadata isn't
        // available to the decoder (`binlog_row_metadata=FULL` is a MySQL
        // 8.0.1+ opt-in, off by default) — this connector doesn't require
        // that setting, so it must accept `Blob` for a `Utf8`-typed field
        // too. Invalid UTF-8 (a column that's genuinely binary, not text)
        // becomes null rather than a lossy/mangled string.
        (MySqlCdcDataType::Utf8, MySqlValue::Blob(bytes)) => String::from_utf8(bytes.clone())
            .map(Value::String)
            .unwrap_or(Value::Null),
        _ => Value::Null,
    }
}

/// Maps a row's cells onto `fields` **positionally** (see `MySqlCdcConfig`'s
/// doc comment on why — no column names on the wire) and tags it with the
/// opcode.
fn row_to_json(cells: &[Option<MySqlValue>], fields: &[MySqlCdcFieldSpec], opcode: &str) -> Value {
    let mut map = serde_json::Map::with_capacity(fields.len() + 1);
    for (i, field) in fields.iter().enumerate() {
        let value = mysql_value_to_json(cells.get(i).and_then(|c| c.as_ref()), field.data_type);
        map.insert(field.name.clone(), value);
    }
    map.insert(OPCODE_COLUMN.to_string(), Value::String(opcode.to_string()));
    Value::Object(map)
}

/// Runs on a dedicated OS thread — `mysql_cdc`'s `replicate()` is a
/// blocking iterator, not an async stream (it does its own blocking socket
/// I/O), so it can't run directly on a tokio task without stalling the
/// runtime. `tx.blocking_send` is the sync-to-async bridge; the receiving
/// side in `read_batches` is a normal async `mpsc::Receiver`.
fn run_replication_loop(
    config: MySqlCdcConfig,
    tx: mpsc::Sender<Result<Value, NexusError>>,
    ready_tx: tokio::sync::oneshot::Sender<Result<(), NexusError>>,
    position: Arc<Mutex<Option<String>>>,
) {
    let binlog = match (&config.binlog_filename, config.binlog_position) {
        (Some(filename), Some(position)) => {
            BinlogOptions::from_position(filename.clone(), position)
        }
        _ => BinlogOptions::from_end(),
    };
    let options = ReplicaOptions {
        hostname: config.host.clone(),
        port: config.port,
        username: config.username.clone(),
        password: config.password.clone(),
        // No default database on the *connection* — a replication user
        // typically only has the global REPLICATION SLAVE/CLIENT grants
        // (not per-database access), and this source doesn't need one:
        // `config.database` is used purely client-side, to match against
        // each `TableMapEvent`'s own database name.
        database: None,
        server_id: config.server_id,
        blocking: true,
        binlog,
        ..Default::default()
    };
    let mut client = BinlogClient::new(options);

    let events = match client.replicate() {
        Ok(events) => events,
        Err(e) => {
            let _ = ready_tx.send(Err(NexusError::Connector(format!(
                "mysql-cdc replicate failed: {e:?}"
            ))));
            return;
        }
    };
    // `client.replicate()` only returns once the connection, handshake and
    // `COM_BINLOG_DUMP` registration (including resolving `from_end`'s
    // actual position) have all completed — signal the caller *now*, not
    // when the thread was merely spawned. `read_batches` waits for this
    // before returning the stream, so a caller's writes right after that
    // await can't land before the position this connector starts reading
    // from is fixed (otherwise `from_end` could resolve to *after* those
    // writes, silently missing them — this raced and failed in testing
    // before this handshake existed).
    if ready_tx.send(Ok(())).is_err() {
        return; // caller already gave up
    }

    // table_id -> (database, table), refreshed from every TableMapEvent —
    // row events only carry the numeric id, this is how we know which ones
    // are for `config.table`.
    let mut tables: HashMap<u64, (String, String)> = HashMap::new();
    // Current binlog file, tracked from RotateEvent — the wire protocol
    // doesn't repeat the filename on every event (only `next_event_position`,
    // an offset *within* whatever file is current), and a real "fake"
    // RotateEvent is guaranteed to arrive right at the start of replication
    // (see mysql_cdc's own docs on RotateEvent), so this seeds itself
    // correctly even when `config.binlog_filename` wasn't set (fresh start).
    let mut current_filename = config.binlog_filename.clone();

    for result in events {
        let (header, event) = match result {
            Ok(pair) => pair,
            Err(e) => {
                let _ = tx.blocking_send(Err(NexusError::Connector(format!(
                    "mysql-cdc stream error: {e:?}"
                ))));
                return;
            }
        };

        let is_target = |table_id: u64, tables: &HashMap<u64, (String, String)>| {
            tables
                .get(&table_id)
                .is_some_and(|(db, table)| *db == config.database && *table == config.table)
        };

        let mut rows: Vec<Value> = Vec::new();
        match &event {
            BinlogEvent::RotateEvent(r) => {
                current_filename = Some(r.binlog_filename.clone());
            }
            BinlogEvent::TableMapEvent(tm) => {
                tables.insert(
                    tm.table_id,
                    (tm.database_name.clone(), tm.table_name.clone()),
                );
            }
            BinlogEvent::WriteRowsEvent(w) if is_target(w.table_id, &tables) => {
                rows.extend(
                    w.rows
                        .iter()
                        .map(|r: &RowData| row_to_json(&r.cells, &config.fields, "I")),
                );
            }
            BinlogEvent::UpdateRowsEvent(u) if is_target(u.table_id, &tables) => {
                rows.extend(
                    u.rows
                        .iter()
                        .map(|r| row_to_json(&r.after_update.cells, &config.fields, "U")),
                );
            }
            BinlogEvent::DeleteRowsEvent(d) if is_target(d.table_id, &tables) => {
                rows.extend(
                    d.rows
                        .iter()
                        .map(|r: &RowData| row_to_json(&r.cells, &config.fields, "D")),
                );
            }
            _ => {}
        }

        for row in rows {
            if tx.blocking_send(Ok(row)).is_err() {
                // Receiver dropped (pipeline stopped reading) — stop the thread.
                return;
            }
        }

        if let Some(filename) = &current_filename {
            if let Ok(mut guard) = position.lock() {
                *guard = Some(format!("{filename}:{}", header.next_event_position));
            } else {
                tracing::error!("MySQL CDC position mutex poisoned; checkpoint lost");
            }
        }

        client.commit(&header, &event);
    }
}

#[async_trait]
impl Source for MySqlCdcSource {
    async fn read_batches(
        &mut self,
    ) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError> {
        let (tx, rx) = mpsc::channel::<Result<Value, NexusError>>(CHANNEL_CAPACITY);
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let config = self.config.clone();
        let position = self.position.clone();
        std::thread::spawn(move || run_replication_loop(config, tx, ready_tx, position));
        ready_rx.await.map_err(|_| {
            NexusError::Connector("mysql-cdc replication thread died before starting".into())
        })??;

        let schema = self.schema.clone();
        let max_batch_events = self.config.max_batch_events;
        let stream = futures::stream::unfold(
            (rx, Vec::<Value>::new(), false, 0u64),
            move |(mut rx, mut buffer, mut finished, mut events_seen)| {
                let schema = schema.clone();
                async move {
                    loop {
                        if finished || events_seen >= max_batch_events {
                            return if buffer.is_empty() {
                                None
                            } else {
                                let batch =
                                    RecordBatchBuilder::from_json_rows(schema.clone(), &buffer);
                                Some((batch, (rx, Vec::new(), finished, events_seen)))
                            };
                        }
                        if buffer.len() >= BATCH_SIZE {
                            let batch = RecordBatchBuilder::from_json_rows(schema.clone(), &buffer);
                            return Some((batch, (rx, Vec::new(), finished, events_seen)));
                        }
                        match tokio::time::timeout(FLUSH_ON_IDLE, rx.recv()).await {
                            Ok(Some(Ok(row))) => {
                                buffer.push(row);
                                events_seen += 1;
                            }
                            Ok(Some(Err(e))) => {
                                finished = true;
                                return Some((Err(e), (rx, buffer, finished, events_seen)));
                            }
                            Ok(None) => {
                                finished = true;
                            }
                            Err(_elapsed) => {
                                if !buffer.is_empty() {
                                    let batch =
                                        RecordBatchBuilder::from_json_rows(schema.clone(), &buffer);
                                    return Some((batch, (rx, Vec::new(), finished, events_seen)));
                                }
                                // Idle, nothing buffered — keep waiting.
                            }
                        }
                    }
                }
            },
        );

        Ok(Box::pin(stream))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn position_handle(&self) -> Option<Arc<Mutex<Option<String>>>> {
        Some(self.position.clone())
    }
}
