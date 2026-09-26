use crate::config::OracleCdcConfig;
use crate::driver::connection_string_parts;
use crate::redo_parser::{opcode_letter, parse_redo};
use crate::schema::{build_schema, describe_table, OracleColumn};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use nexus_core::{
    quote_identifier, with_timeout, NexusError, RecordBatchBuilder, Source, OPCODE_COLUMN,
};
use odbc_api::{Connection, ConnectionOptions, Cursor, Environment, Nullable};
use serde_json::Value;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc::Sender;

const CHANNEL_CAPACITY: usize = 4;

/// Native CDC source for Oracle via LogMiner (`DBMS_LOGMNR`). Same
/// "consume, don't configure" posture as `mssql-cdc`/`mysql-cdc`: the
/// source table isn't altered by this connector, but LogMiner reading
/// full-row after-images does require `ALTER TABLE ... ADD
/// SUPPLEMENTAL LOG DATA (ALL) COLUMNS` enabled on it beforehand — a
/// real prerequisite, not a v1 simplification, documented in the
/// README.
///
/// `CONTINUOUS_MINE` was desupported in Oracle 19c+, so this polls a
/// bounded SCN window each cycle (`CURRENT_SCN` → `START_LOGMNR` →
/// `V$LOGMNR_CONTENTS` → `END_LOGMNR`) instead of streaming — same
/// shape as `mssql-cdc`'s LSN-window polling.
///
/// Unlike `mssql-cdc` (ADBC connections are `Send`, cloned per poll),
/// `odbc-api` handles aren't `Send` — same constraint
/// `OracleSource`/`OracleSink` (`source.rs`/`sink.rs`) already
/// document. So the whole poll loop runs inside a single
/// `spawn_blocking`, owning one connection for the source's entire
/// lifetime, streaming batches out over an `mpsc` channel — the same
/// non-`Send`-workaround shape `OracleSource::read_batches` uses, just
/// looping forever instead of running once.
pub struct OracleCdcSource {
    config: OracleCdcConfig,
    schema: SchemaRef,
    /// Updated from `poll_loop_inner` (same thread, no async involved) as
    /// the poll advances — `Source::position_handle`'s backing storage.
    /// See that trait method's doc comment for why this is an
    /// `Arc<Mutex<..>>` handle instead of a plain getter.
    position: Arc<Mutex<Option<String>>>,
}

impl OracleCdcSource {
    pub async fn connect(config: &OracleCdcConfig) -> Result<Self, NexusError> {
        quote_identifier(&config.table)?;
        quote_identifier(&config.username)?;

        let cfg = config.clone();
        let columns = with_timeout(config.timeout_seconds, "oracle-cdc describe_table", async {
            tokio::task::spawn_blocking(move || -> Result<Vec<OracleColumn>, NexusError> {
                let env = Environment::new()
                    .map_err(|e| NexusError::Connector(format!("oracle-cdc env: {e}")))?;
                let conn = env
                    .connect_with_connection_string(
                        &connection_string(&cfg),
                        ConnectionOptions::default(),
                    )
                    .map_err(|e| NexusError::Connector(format!("oracle-cdc connect: {e}")))?;
                describe_table(&conn, &cfg.table)
            })
            .await
            .map_err(|e| NexusError::Connector(format!("blocking task panicked: {e}")))?
        })
        .await?;

        let business_schema = build_schema(&columns);
        let mut fields: Vec<Field> = business_schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        fields.push(Field::new(OPCODE_COLUMN, DataType::Utf8, false));
        let schema: SchemaRef = Arc::new(Schema::new(fields));

        Ok(Self {
            config: config.clone(),
            schema,
            position: Arc::new(Mutex::new(None)),
        })
    }
}

fn connection_string(cfg: &OracleCdcConfig) -> String {
    connection_string_parts(
        &cfg.host,
        cfg.port,
        &cfg.service_name,
        &cfg.username,
        &cfg.password,
    )
}

#[async_trait]
impl Source for OracleCdcSource {
    async fn read_batches(
        &mut self,
    ) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError> {
        let config = self.config.clone();
        let schema = self.schema.clone();
        let position = self.position.clone();
        let (tx, rx) =
            tokio::sync::mpsc::channel::<Result<RecordBatch, NexusError>>(CHANNEL_CAPACITY);

        tokio::task::spawn_blocking(move || poll_loop(&config, &schema, &tx, &position));

        // No idle timeout here (unlike `OracleSource`'s batch read):
        // long gaps between polls are the normal, expected state for
        // CDC (waiting for new changes), not a stall.
        Ok(Box::pin(stream::unfold(rx, move |mut rx| async move {
            rx.recv().await.map(|item| (item, rx))
        })))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn position_handle(&self) -> Option<Arc<Mutex<Option<String>>>> {
        Some(self.position.clone())
    }
}

fn poll_loop(
    config: &OracleCdcConfig,
    schema: &SchemaRef,
    tx: &Sender<Result<RecordBatch, NexusError>>,
    position: &Arc<Mutex<Option<String>>>,
) {
    if let Err(e) = poll_loop_inner(config, schema, tx, position) {
        let _ = tx.blocking_send(Err(e));
    }
}

/// `events_seen`/`max_batch_events` didn't exist before this fix — this
/// loop ran forever inside a single `spawn_blocking` call (no cutoff at
/// all, not even a dead config field like `mssql-cdc` had). Without a
/// natural end, the checkpoint commit that only fires when a source's
/// stream ends (`PipelineEngine::run_partition`) never got a chance to
/// run, and `Source::position_handle`'s final read never happened
/// either — terminating once `max_batch_events` rows have been
/// delivered lets the scheduler's normal re-invoke cycle commit
/// progress regularly instead of running one micro-batch forever.
fn poll_loop_inner(
    config: &OracleCdcConfig,
    schema: &SchemaRef,
    tx: &Sender<Result<RecordBatch, NexusError>>,
    position: &Arc<Mutex<Option<String>>>,
) -> Result<(), NexusError> {
    let env =
        Environment::new().map_err(|e| NexusError::Connector(format!("oracle-cdc env: {e}")))?;
    let conn = env
        .connect_with_connection_string(&connection_string(config), ConnectionOptions::default())
        .map_err(|e| NexusError::Connector(format!("oracle-cdc connect: {e}")))?;

    let seg_owner = config.username.to_uppercase();
    let table_upper = config.table.to_uppercase();
    let poll_interval = Duration::from_secs(config.poll_interval_seconds);

    let mut from_scn = match config.start_scn {
        Some(scn) => scn,
        None => fetch_current_scn(&conn)?,
    };
    let mut events_seen = 0u64;

    loop {
        if events_seen >= config.max_batch_events {
            return Ok(());
        }

        let to_scn = fetch_current_scn(&conn)?;
        if to_scn <= from_scn {
            std::thread::sleep(poll_interval);
            continue;
        }

        start_logmnr(&conn, from_scn, to_scn)?;
        let rows = fetch_logmnr_contents(&conn, &seg_owner, &table_upper, from_scn, to_scn);
        end_logmnr(&conn);
        let rows = rows?;

        if !rows.is_empty() {
            let mut buffer: Vec<Value> = Vec::with_capacity(rows.len());
            for (operation, sql_redo) in &rows {
                let mut object = parse_redo(operation, sql_redo)?;
                object.insert(
                    OPCODE_COLUMN.to_string(),
                    Value::String(opcode_letter(operation)?.to_string()),
                );
                buffer.push(Value::Object(object));
            }
            events_seen += buffer.len() as u64;
            let batch = RecordBatchBuilder::from_json_rows(schema.clone(), &buffer)?;
            tx.blocking_send(Ok(batch))
                .map_err(|_| NexusError::Connector("oracle-cdc: receiver dropped".into()))?;
        }

        from_scn = to_scn;
        *position
            .lock()
            .map_err(|_| NexusError::Connector("oracle-cdc: position mutex poisoned".into()))? =
            Some(from_scn.to_string());
        std::thread::sleep(poll_interval);
    }
}

fn fetch_current_scn(conn: &Connection<'_>) -> Result<i64, NexusError> {
    let mut cursor = conn
        .execute("SELECT CURRENT_SCN FROM V$DATABASE", (), None)
        .map_err(|e| NexusError::Connector(format!("oracle-cdc CURRENT_SCN query failed: {e}")))?
        .ok_or_else(|| {
            NexusError::Connector("oracle-cdc: V$DATABASE returned no result set".into())
        })?;

    let mut row = cursor
        .next_row()
        .map_err(|e| NexusError::Connector(format!("oracle-cdc CURRENT_SCN fetch failed: {e}")))?
        .ok_or_else(|| NexusError::Connector("oracle-cdc: V$DATABASE returned no rows".into()))?;
    let mut scn = Nullable::<i64>::null();
    row.get_data(1, &mut scn)
        .map_err(|e| NexusError::Connector(format!("oracle-cdc CURRENT_SCN read failed: {e}")))?;
    scn.into_opt()
        .ok_or_else(|| NexusError::Connector("oracle-cdc: CURRENT_SCN was NULL".into()))
}

/// `from_scn`/`to_scn` come from `fetch_current_scn` (always numeric,
/// never externally supplied text), so inlining them as literals in
/// the anonymous PL/SQL block is safe.
fn start_logmnr(conn: &Connection<'_>, from_scn: i64, to_scn: i64) -> Result<(), NexusError> {
    let sql = format!(
        "BEGIN DBMS_LOGMNR.START_LOGMNR(STARTSCN => {from_scn}, ENDSCN => {to_scn}, \
         OPTIONS => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG); END;"
    );
    conn.execute(&sql, (), None)
        .map_err(|e| NexusError::Connector(format!("oracle-cdc START_LOGMNR failed: {e}")))?;
    Ok(())
}

/// Best-effort cleanup — errors here aren't surfaced (the poll's own
/// query result, if any, already succeeded or failed on its own
/// merits by the time this runs).
fn end_logmnr(conn: &Connection<'_>) {
    let _ = conn.execute("BEGIN DBMS_LOGMNR.END_LOGMNR; END;", (), None);
}

/// `seg_owner`/`table` are validated via `quote_identifier` in
/// `connect()` before reaching here (uppercased afterward, which
/// doesn't introduce any character `quote_identifier` didn't already
/// allow); `from_scn`/`to_scn` are numeric, not externally-supplied
/// text — safe to inline as literals.
fn fetch_logmnr_contents(
    conn: &Connection<'_>,
    seg_owner: &str,
    table: &str,
    from_scn: i64,
    to_scn: i64,
) -> Result<Vec<(String, String)>, NexusError> {
    let sql = format!(
        "SELECT OPERATION, SQL_REDO FROM V$LOGMNR_CONTENTS \
         WHERE SEG_OWNER = '{seg_owner}' AND TABLE_NAME = '{table}' \
         AND OPERATION IN ('INSERT','UPDATE','DELETE') \
         AND SCN > {from_scn} AND SCN <= {to_scn} ORDER BY SCN"
    );

    let mut cursor = conn
        .execute(&sql, (), None)
        .map_err(|e| {
            NexusError::Connector(format!("oracle-cdc V$LOGMNR_CONTENTS query failed: {e}"))
        })?
        .ok_or_else(|| {
            NexusError::Connector("oracle-cdc: V$LOGMNR_CONTENTS returned no result set".into())
        })?;

    let mut rows = Vec::new();
    while let Some(mut row) = cursor.next_row().map_err(|e| {
        NexusError::Connector(format!("oracle-cdc V$LOGMNR_CONTENTS fetch failed: {e}"))
    })? {
        let mut op_buf = Vec::new();
        row.get_text(1, &mut op_buf)
            .map_err(|e| NexusError::Connector(format!("oracle-cdc OPERATION read failed: {e}")))?;
        let operation =
            String::from_utf8(op_buf).map_err(|e| NexusError::Serialization(e.to_string()))?;

        let mut redo_buf = Vec::new();
        row.get_text(2, &mut redo_buf)
            .map_err(|e| NexusError::Connector(format!("oracle-cdc SQL_REDO read failed: {e}")))?;
        let sql_redo =
            String::from_utf8(redo_buf).map_err(|e| NexusError::Serialization(e.to_string()))?;

        rows.push((operation, sql_redo));
    }

    Ok(rows)
}
