use crate::config::MssqlCdcConfig;
use crate::driver::open_connection;
use adbc_core::{Connection as _, Statement as _};
use adbc_driver_manager::ManagedConnection;
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Int32Array, RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use arrow_select::filter::filter_record_batch;
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use nexus_core::{quote_identifier, with_timeout, NexusError, Source, OPCODE_COLUMN};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// SQL Server's own metadata column on every CDC change-table result —
/// not user-controlled, so not run through `quote_identifier` (it
/// contains `$`, which that validator would reject — this is a
/// hardcoded literal referencing a real SQL Server system column, not
/// an externally-supplied identifier).
const CDC_OPERATION_COLUMN: &str = "__$operation";

/// Native CDC source for SQL Server via its own capture tables — no
/// Debezium/Kafka, same "consume, don't configure" posture the public
/// repo's Postgres/MySQL native CDC connectors already have: CDC must
/// already be enabled on the database/table
/// (`sys.sp_cdc_enable_db`/`sys.sp_cdc_enable_table`) before this
/// connector runs.
///
/// Unlike MySQL's binlog (blocking replication socket) or the
/// Oracle/HANA connectors' ODBC handles (not `Send`), this is SQL
/// polling over a normal ADBC connection (`Send`) — no dedicated OS
/// thread needed, just an async loop with `tokio::time::sleep`
/// between polls.
///
/// **Azure Synapse dedicated SQL pools have no equivalent** —
/// confirmed via research: there's no `cdc.fn_cdc_get_all_changes_*`
/// on Synapse. The "CDC into Synapse" pattern documented for Azure
/// Data Factory is just ADF reading a *source's* own CDC and writing
/// to Synapse as a destination — exactly what this source (as the
/// pipeline source) plus the `synapse` sink already do together in a
/// normal NexusFlow pipeline. This connector only targets real SQL
/// Server.
pub struct MssqlCdcSource {
    connection: ManagedConnection,
    capture_instance: String,
    table_columns: Vec<String>,
    schema: SchemaRef,
    poll_interval: Duration,
    timeout_seconds: u64,
    from_lsn: Vec<u8>,
    max_batch_events: u64,
    /// Updated as the stream advances — `Source::position_handle`'s
    /// backing storage. See that trait method's doc comment for why this
    /// is an `Arc<Mutex<..>>` handle instead of a plain getter.
    position: Arc<Mutex<Option<String>>>,
}

impl MssqlCdcSource {
    pub async fn connect(cfg: &MssqlCdcConfig) -> Result<Self, NexusError> {
        let capture_instance = cfg.capture_instance();
        quote_identifier(&capture_instance)?;
        quote_identifier(&cfg.table)?;

        let uri = cfg.connection_string();
        let table = cfg.table.clone();
        let (connection, table_schema) =
            with_timeout(cfg.timeout_seconds, "mssql-cdc connect", async {
                tokio::task::spawn_blocking(
                    move || -> Result<(ManagedConnection, arrow_schema::Schema), NexusError> {
                        let connection = open_connection(&uri)?;
                        let schema = connection
                            .get_table_schema(None, None, &table)
                            .map_err(|e| NexusError::Schema(e.to_string()))?;
                        Ok((connection, schema))
                    },
                )
                .await
                .map_err(|e| NexusError::Connector(format!("blocking task panicked: {e}")))?
            })
            .await?;

        let table_columns: Vec<String> = table_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        let mut fields: Vec<Field> = table_schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        fields.push(Field::new(OPCODE_COLUMN, DataType::Utf8, false));
        let schema: SchemaRef = Arc::new(Schema::new(fields));

        let from_lsn = match &cfg.start_lsn {
            Some(hex) => parse_lsn_hex(hex)?,
            None => {
                let mut conn_for_lsn = connection.clone();
                let capture_instance_for_lsn = capture_instance.clone();
                with_timeout(cfg.timeout_seconds, "mssql-cdc get_min_lsn", async {
                    tokio::task::spawn_blocking(move || {
                        fetch_min_lsn(&mut conn_for_lsn, &capture_instance_for_lsn)
                    })
                    .await
                    .map_err(|e| NexusError::Connector(format!("blocking task panicked: {e}")))?
                })
                .await?
            }
        };

        Ok(Self {
            connection,
            capture_instance,
            table_columns,
            schema,
            poll_interval: Duration::from_secs(cfg.poll_interval_seconds),
            timeout_seconds: cfg.timeout_seconds,
            from_lsn,
            max_batch_events: cfg.max_batch_events,
            position: Arc::new(Mutex::new(None)),
        })
    }
}

#[async_trait]
impl Source for MssqlCdcSource {
    async fn read_batches(
        &mut self,
    ) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError> {
        let connection = self.connection.clone();
        let capture_instance = self.capture_instance.clone();
        let table_columns = self.table_columns.clone();
        let schema = self.schema.clone();
        let poll_interval = self.poll_interval;
        let timeout_seconds = self.timeout_seconds;
        let from_lsn = self.from_lsn.clone();
        let max_batch_events = self.max_batch_events;
        let position = self.position.clone();

        // `events_seen` didn't exist before this fix — `max_batch_events`
        // was a real config field nothing ever read, so this stream never
        // returned `None` in healthy operation (only a plain "no rows yet,
        // sleep and retry" loop, forever). Without a natural end, neither
        // this connector's own checkpoint commit nor `position_handle`'s
        // final read (both driven by "the source's stream ended") ever
        // fired — same root cause as the postgres-cdc WAL-growth bug, just
        // showing up differently here. Terminating once `max_batch_events`
        // rows have been delivered (same cutoff shape mysql-cdc/mongodb-cdc
        // already use) lets the scheduler's normal re-invoke cycle commit
        // progress regularly instead of running one micro-batch forever.
        let stream = stream::unfold((from_lsn, 0u64), move |(from_lsn, events_seen)| {
            let connection = connection.clone();
            let capture_instance = capture_instance.clone();
            let table_columns = table_columns.clone();
            let schema = schema.clone();
            let position = position.clone();
            async move {
                if events_seen >= max_batch_events {
                    return None;
                }
                let mut cursor = from_lsn;
                loop {
                    match poll_once(
                        &connection,
                        &capture_instance,
                        &table_columns,
                        &schema,
                        &cursor,
                        timeout_seconds,
                    )
                    .await
                    {
                        Ok((Some(batch), next_lsn)) if batch.num_rows() > 0 => {
                            match position.lock() {
                                Ok(mut guard) => *guard = Some(lsn_hex_literal(&next_lsn)),
                                Err(_) => {
                                    return Some((
                                        Err(NexusError::Connector(
                                            "mssql-cdc: position mutex poisoned".into(),
                                        )),
                                        (next_lsn, events_seen),
                                    ));
                                }
                            }
                            let events_seen = events_seen + batch.num_rows() as u64;
                            return Some((Ok(batch), (next_lsn, events_seen)));
                        }
                        Ok((_, next_lsn)) => {
                            cursor = next_lsn;
                            tokio::time::sleep(poll_interval).await;
                        }
                        Err(e) => return Some((Err(e), (cursor, events_seen))),
                    }
                }
            }
        });

        Ok(Box::pin(stream))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn position_handle(&self) -> Option<Arc<Mutex<Option<String>>>> {
        Some(self.position.clone())
    }
}

/// One poll cycle: get the current max LSN, pull changes in
/// `(from_lsn, to_lsn]`, transform them, and return the next
/// `from_lsn`. Everything blocking runs in one `spawn_blocking`
/// closure since the ADBC statement calls aren't async.
async fn poll_once(
    connection: &ManagedConnection,
    capture_instance: &str,
    table_columns: &[String],
    schema: &SchemaRef,
    from_lsn: &[u8],
    timeout_seconds: u64,
) -> Result<(Option<RecordBatch>, Vec<u8>), NexusError> {
    let mut connection = connection.clone();
    let capture_instance = capture_instance.to_string();
    let table_columns = table_columns.to_vec();
    let schema = schema.clone();
    let from_lsn = from_lsn.to_vec();

    with_timeout(timeout_seconds, "mssql-cdc poll", async {
        tokio::task::spawn_blocking(
            move || -> Result<(Option<RecordBatch>, Vec<u8>), NexusError> {
                let to_lsn = fetch_max_lsn(&mut connection)?;
                if to_lsn == from_lsn {
                    return Ok((None, from_lsn));
                }

                let sql = build_cdc_query(&table_columns, &capture_instance, &from_lsn, &to_lsn)?;
                let batch = run_scalar_query(&mut connection, &sql)?;
                let transformed = batch
                    .as_ref()
                    .map(|b| transform_cdc_batch(b, &schema))
                    .transpose()?;

                let next_from_lsn = fetch_increment_lsn(&mut connection, &to_lsn)?;
                Ok((transformed, next_from_lsn))
            },
        )
        .await
        .map_err(|e| NexusError::Connector(format!("blocking task panicked: {e}")))?
    })
    .await
}

/// `SELECT sys.fn_cdc_get_min_lsn('<capture_instance>')` — the earliest
/// LSN with tracked changes still available. `capture_instance` is
/// validated via `quote_identifier`'s charset before this, so
/// embedding it inside single quotes as a T-SQL string literal is safe
/// (no injection-capable character can pass that validation).
fn fetch_min_lsn(
    connection: &mut ManagedConnection,
    capture_instance: &str,
) -> Result<Vec<u8>, NexusError> {
    let sql = format!("SELECT sys.fn_cdc_get_min_lsn('{capture_instance}')");
    run_scalar_query(connection, &sql)?
        .and_then(|batch| lsn_from_batch(&batch))
        .ok_or_else(|| {
            NexusError::Connector(format!(
                "mssql-cdc: sys.fn_cdc_get_min_lsn('{capture_instance}') returned no LSN — is CDC enabled for this capture instance?"
            ))
        })
}

fn fetch_max_lsn(connection: &mut ManagedConnection) -> Result<Vec<u8>, NexusError> {
    run_scalar_query(connection, "SELECT sys.fn_cdc_get_max_lsn()")?
        .and_then(|batch| lsn_from_batch(&batch))
        .ok_or_else(|| {
            NexusError::Connector("mssql-cdc: sys.fn_cdc_get_max_lsn() returned no LSN".into())
        })
}

fn fetch_increment_lsn(
    connection: &mut ManagedConnection,
    lsn: &[u8],
) -> Result<Vec<u8>, NexusError> {
    let sql = format!("SELECT sys.fn_cdc_increment_lsn({})", lsn_hex_literal(lsn));
    run_scalar_query(connection, &sql)?
        .and_then(|batch| lsn_from_batch(&batch))
        .ok_or_else(|| {
            NexusError::Connector("mssql-cdc: sys.fn_cdc_increment_lsn returned no LSN".into())
        })
}

/// LSN columns are `binary(10)` in SQL Server — assumed to arrive as
/// Arrow `Binary` here. **Not confirmed against a real driver call**
/// (no live SQL Server instance available this session): if the ADBC
/// Foundry driver maps `binary(10)` to `FixedSizeBinary(10)` instead,
/// this downcast fails loudly with a clear error rather than silently
/// misreading bytes — a real verification item, documented in the
/// README alongside the other unconfirmed driver specifics.
fn lsn_from_batch(batch: &RecordBatch) -> Option<Vec<u8>> {
    let column = batch.column(0);
    let arr = column.as_any().downcast_ref::<BinaryArray>()?;
    if arr.is_empty() || arr.is_null(0) {
        None
    } else {
        Some(arr.value(0).to_vec())
    }
}

fn lsn_hex_literal(lsn: &[u8]) -> String {
    let mut out = String::with_capacity(2 + lsn.len() * 2);
    out.push_str("0x");
    for b in lsn {
        out.push_str(&format!("{b:02X}"));
    }
    out
}

/// Inverse of `lsn_hex_literal` — parses `MssqlCdcConfig.start_lsn` (or
/// any hex string in that same `0x...`/bare-hex shape) back into raw
/// bytes.
fn parse_lsn_hex(hex: &str) -> Result<Vec<u8>, NexusError> {
    let hex = hex.strip_prefix("0x").unwrap_or(hex);
    if !hex.len().is_multiple_of(2) {
        return Err(NexusError::Schema(format!(
            "mssql-cdc: start_lsn '{hex}' has an odd number of hex digits"
        )));
    }
    (0..hex.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&hex[i..i + 2], 16).map_err(|e| {
                NexusError::Schema(format!("mssql-cdc: invalid start_lsn hex digit: {e}"))
            })
        })
        .collect()
}

/// `table_columns`/`capture_instance` are validated via
/// `quote_identifier` before reaching here (table columns come from
/// `get_table_schema`'s own real column names, `capture_instance` from
/// `connect()`). LSNs are hex literals built by `lsn_hex_literal`, not
/// externally-supplied text.
fn build_cdc_query(
    table_columns: &[String],
    capture_instance: &str,
    from_lsn: &[u8],
    to_lsn: &[u8],
) -> Result<String, NexusError> {
    quote_identifier(capture_instance)?;
    let quoted_columns = table_columns
        .iter()
        .map(|c| quote_identifier(c))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(format!(
        "SELECT {}, [{CDC_OPERATION_COLUMN}] FROM cdc.fn_cdc_get_all_changes_{capture_instance}({}, {}, N'all')",
        quoted_columns.join(", "),
        lsn_hex_literal(from_lsn),
        lsn_hex_literal(to_lsn),
    ))
}

/// Runs `sql` and returns the first (and only expected) `RecordBatch`
/// from the reader, or `None` if the result set was empty.
fn run_scalar_query(
    connection: &mut ManagedConnection,
    sql: &str,
) -> Result<Option<RecordBatch>, NexusError> {
    let mut statement = connection
        .new_statement()
        .map_err(|e| NexusError::Connector(e.to_string()))?;
    statement
        .set_sql_query(sql)
        .map_err(|e| NexusError::Connector(e.to_string()))?;
    let reader = statement
        .execute()
        .map_err(|e| NexusError::Connector(e.to_string()))?;

    let mut batches: Vec<RecordBatch> = reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| NexusError::Serialization(e.to_string()))?;
    if batches.is_empty() {
        return Ok(None);
    }
    if batches.len() == 1 {
        return Ok(Some(batches.remove(0)));
    }
    arrow_select::concat::concat_batches(&batches[0].schema(), &batches)
        .map(Some)
        .map_err(|e| NexusError::Schema(format!("mssql-cdc: failed to concat result batches: {e}")))
}

/// `__$operation`: `1 = delete, 2 = insert, 3 = value before update, 4
/// = value after update` — confirmed via Microsoft Learn
/// (`cdc.fn_cdc_get_all_changes_<capture_instance>`, 2026-08-19).
/// `3` (before-update) is dropped — only the after-image (`4`) is
/// kept, same "final state only" simplification `mysql-cdc`'s row
/// events already make in this workspace.
fn opcode_letter(op: i32) -> Option<&'static str> {
    match op {
        1 => Some("D"),
        2 => Some("I"),
        4 => Some("U"),
        _ => None,
    }
}

/// Assumed `int` (Arrow `Int32`) for `__$operation` — SQL Server's
/// `int` type is 4 bytes, so this is the expected mapping, but **not
/// confirmed against a real driver call** this session; fails loudly
/// (not silently) if the assumption is wrong.
fn transform_cdc_batch(
    batch: &RecordBatch,
    business_schema: &SchemaRef,
) -> Result<RecordBatch, NexusError> {
    let op_idx = batch.schema().index_of(CDC_OPERATION_COLUMN).map_err(|_| {
        NexusError::Schema(format!(
            "mssql-cdc: expected column {CDC_OPERATION_COLUMN} in change-table result, not found"
        ))
    })?;
    let op_col = batch
        .column(op_idx)
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| {
            NexusError::Schema(format!(
                "mssql-cdc: {CDC_OPERATION_COLUMN} column has unexpected array type (expected int32) — \
                 SQL Server's `int` type is assumed to map to Arrow Int32 by this ADBC driver, unconfirmed \
                 against a real instance"
            ))
        })?;

    let mask: BooleanArray = (0..op_col.len())
        .map(|i| {
            if op_col.is_null(i) {
                None
            } else {
                Some(opcode_letter(op_col.value(i)).is_some())
            }
        })
        .collect();

    let filtered = filter_record_batch(batch, &mask)
        .map_err(|e| NexusError::Schema(format!("mssql-cdc: row filter failed: {e}")))?;

    let filtered_op_idx = filtered
        .schema()
        .index_of(CDC_OPERATION_COLUMN)
        .map_err(|e| {
            NexusError::Schema(format!(
                "mssql-cdc: {CDC_OPERATION_COLUMN} missing after filter: {e}"
            ))
        })?;
    let filtered_op_col = filtered
        .column(filtered_op_idx)
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| {
            NexusError::Schema(format!(
                "mssql-cdc: {CDC_OPERATION_COLUMN} type changed after filter"
            ))
        })?;

    let opcodes: StringArray = (0..filtered_op_col.len())
        .map(|i| opcode_letter(filtered_op_col.value(i)))
        .collect();

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(filtered.num_columns());
    for (i, field) in filtered.schema().fields().iter().enumerate() {
        if field.name() != CDC_OPERATION_COLUMN {
            columns.push(filtered.column(i).clone());
        }
    }
    columns.push(Arc::new(opcodes));

    RecordBatch::try_new(business_schema.clone(), columns)
        .map_err(|e| NexusError::Schema(format!("mssql-cdc: record batch build failed: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn opcode_maps_delete_insert_update_after() {
        assert_eq!(opcode_letter(1), Some("D"));
        assert_eq!(opcode_letter(2), Some("I"));
        assert_eq!(opcode_letter(4), Some("U"));
    }

    #[test]
    fn opcode_drops_before_update_and_unknown() {
        assert_eq!(opcode_letter(3), None);
        assert_eq!(opcode_letter(99), None);
    }

    #[test]
    fn lsn_hex_literal_formats_bytes_uppercase() {
        assert_eq!(lsn_hex_literal(&[0x00, 0x01, 0xAB, 0xFF]), "0x0001ABFF");
    }

    #[test]
    fn parse_lsn_hex_round_trips_with_lsn_hex_literal() {
        let bytes = vec![0x00, 0x01, 0xAB, 0xFF];
        assert_eq!(parse_lsn_hex(&lsn_hex_literal(&bytes)).unwrap(), bytes);
    }

    #[test]
    fn parse_lsn_hex_accepts_bare_hex_without_0x_prefix() {
        assert_eq!(
            parse_lsn_hex("0001ABFF").unwrap(),
            vec![0x00, 0x01, 0xAB, 0xFF]
        );
    }

    #[test]
    fn parse_lsn_hex_rejects_odd_length() {
        assert!(parse_lsn_hex("0x0").is_err());
    }

    #[test]
    fn parse_lsn_hex_rejects_invalid_digit() {
        assert!(parse_lsn_hex("0xZZ").is_err());
    }

    #[test]
    fn build_cdc_query_shape() {
        let sql = build_cdc_query(
            &["id".to_string(), "name".to_string()],
            "dbo_Orders",
            &[0x00],
            &[0x01],
        )
        .unwrap();
        assert_eq!(
            sql,
            "SELECT \"id\", \"name\", [__$operation] FROM cdc.fn_cdc_get_all_changes_dbo_Orders(0x00, 0x01, N'all')"
        );
    }

    #[test]
    fn build_cdc_query_rejects_sql_injection_in_capture_instance() {
        let err = build_cdc_query(
            &["id".to_string()],
            "x; DROP TABLE users; --",
            &[0x00],
            &[0x01],
        )
        .expect_err("malicious capture_instance must be rejected");
        assert!(matches!(err, NexusError::Schema(_)));
    }

    #[test]
    fn build_cdc_query_rejects_sql_injection_in_column_name() {
        let err = build_cdc_query(
            &["id".to_string(), "x\"; DROP TABLE users; --".to_string()],
            "dbo_Orders",
            &[0x00],
            &[0x01],
        )
        .expect_err("malicious column name must be rejected");
        assert!(matches!(err, NexusError::Schema(_)));
    }
}
