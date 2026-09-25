use crate::config::OracleConnectorConfig;
use crate::driver::connection_string;
use crate::row_mapping::cell_to_literal;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use nexus_core::{split_by_opcode, CheckpointCursor, NexusError, RetryConfig, Sink};
use odbc_api::{Connection, ConnectionOptions, Environment};
use std::sync::mpsc;
use std::time::Duration;

/// `odbc-api` handles aren't `Send` — same constraint
/// `nexus-connector-odbc` (public repo) documents, solved the same way:
/// a dedicated OS thread owns the ODBC environment/connection for the
/// sink's lifetime, `write_batch` ships batches to it over a
/// synchronous channel and waits on a oneshot response.
///
/// Upsert is `MERGE INTO ... USING (SELECT ... FROM DUAL)` (Oracle's
/// real syntax, see `sql.rs`) instead of the public connector's
/// portable update-then-insert-fallback — Oracle's dialect is known
/// here, no need for the generic-driver compromise. CDC delete opcodes
/// are handled with a real `DELETE`, unlike the Salesforce connector's
/// v1 limitation — this is plain SQL over ODBC, no external-ID
/// restriction applies.
pub struct OracleSink {
    tx: mpsc::Sender<BatchRequest>,
    timeout_seconds: u64,
}

struct BatchRequest {
    batch: RecordBatch,
    response: tokio::sync::oneshot::Sender<Result<(), NexusError>>,
}

impl OracleSink {
    pub async fn connect(config: &OracleConnectorConfig) -> Result<Self, NexusError> {
        config.validate()?;
        config.primary_key_or_err()?;
        let timeout_seconds = config.timeout_seconds;
        let config = config.clone();
        let (tx, rx) = mpsc::channel::<BatchRequest>();

        std::thread::Builder::new()
            .name("nexus-oracle-sink".to_string())
            .spawn(move || {
                if let Err(e) = run_worker(config, rx) {
                    tracing::error!("oracle sink worker terminated with error: {e}");
                }
            })
            .map_err(|e| NexusError::Connector(format!("failed to spawn oracle worker: {e}")))?;

        Ok(Self {
            tx,
            timeout_seconds,
        })
    }
}

fn retry_sync<T, F>(retry: &RetryConfig, op_name: &str, mut op: F) -> Result<T, NexusError>
where
    F: FnMut() -> Result<T, NexusError>,
{
    let mut last_err = None;
    for attempt in 0..=retry.retries {
        match op() {
            Ok(value) => return Ok(value),
            Err(err) => {
                if attempt == retry.retries || !nexus_core::is_transient_error(&err) {
                    return Err(err);
                }
                let delay = Duration::from_secs(retry.retry_backoff_seconds) * 2u32.pow(attempt);
                tracing::warn!(
                    "{op_name} failed (attempt {}/{}): {err}, retrying in {:?}",
                    attempt + 1,
                    retry.retries + 1,
                    delay
                );
                std::thread::sleep(delay);
                last_err = Some(err);
            }
        }
    }
    Err(last_err.unwrap_or_else(|| NexusError::Connector(format!("{op_name} retry exhausted"))))
}

fn run_worker(config: OracleConnectorConfig, rx: mpsc::Receiver<BatchRequest>) -> Result<(), NexusError> {
    tracing::info!("oracle sink worker starting");
    let env = Environment::new().map_err(|e| NexusError::Connector(format!("oracle env: {e}")))?;
    tracing::info!("oracle environment created");
    let conn: Connection<'_> = retry_sync(&config.retry, "oracle sink connect", || {
        tracing::info!("oracle connecting with connection string");
        env.connect_with_connection_string(&connection_string(&config), ConnectionOptions::default())
            .map_err(|e| NexusError::Connector(format!("oracle connect: {e}")))
    })?;
    tracing::info!("oracle connection established");
    conn.set_autocommit(false)
        .map_err(|e| NexusError::Connector(format!("oracle set_autocommit(false): {e}")))?;
    tracing::info!("oracle autocommit disabled");

    let mut table_created = false;
    for req in rx {
        if !table_created {
            if let Err(e) = ensure_table_exists(&config, &conn, &req.batch.schema()) {
                let _ = req.response.send(Err(e));
                continue;
            }
            table_created = true;
        }
        let result = apply_batch(&config, &conn, &req.batch);
        let _ = req.response.send(result);
    }
    Ok(())
}

/// Creates `config.table` from `schema` if it doesn't exist yet. Oracle has
/// no `CREATE TABLE IF NOT EXISTS` (pre-23c), so this just tries the DDL and
/// swallows `ORA-00955` ("name is already used by an existing object") —
/// any other error (bad connection, insufficient privilege, ...) propagates.
/// Called once, lazily, from the first `write_batch` — `OracleSink::connect`
/// doesn't receive a schema (see `sql::build_create_table_sql`'s doc
/// comment).
fn ensure_table_exists(
    config: &OracleConnectorConfig,
    conn: &Connection<'_>,
    schema: &SchemaRef,
) -> Result<(), NexusError> {
    let primary_key = config.primary_key_or_err()?;
    let sql = crate::sql::build_create_table_sql(&config.table, primary_key, schema)?;
    match conn.execute(&sql, (), None) {
        Ok(_) => conn
            .commit()
            .map_err(|e| NexusError::Connector(format!("oracle commit failed: {e}"))),
        Err(e) => {
            if e.to_string().contains("ORA-00955") {
                let _ = conn.rollback();
                Ok(())
            } else {
                let _ = conn.rollback();
                Err(NexusError::Connector(format!("oracle create table failed: {e}")))
            }
        }
    }
}

fn apply_batch(
    config: &OracleConnectorConfig,
    conn: &Connection<'_>,
    batch: &RecordBatch,
) -> Result<(), NexusError> {
    let result: Result<(), NexusError> = (|| match split_by_opcode(batch)? {
        None => upsert(config, conn, batch),
        Some(split) => {
            upsert(config, conn, &split.upserts)?;
            delete(config, conn, &split.deletes)
        }
    })();

    match result {
        Ok(()) => conn
            .commit()
            .map_err(|e| NexusError::Connector(format!("oracle commit failed: {e}"))),
        Err(e) => {
            let _ = conn.rollback();
            Err(e)
        }
    }
}

/// Process the batch in chunks of this many rows per `MERGE` statement.
/// The Oracle Instant Client ODBC driver in this environment rejects `?`
/// parameter markers, so we build literal-valued SQL. A single batched
/// `MERGE ... USING (SELECT ... UNION ALL ...)` is dramatically faster
/// than one statement per row.
const ORACLE_MERGE_BATCH_SIZE: usize = 1000;

fn upsert(config: &OracleConnectorConfig, conn: &Connection<'_>, batch: &RecordBatch) -> Result<(), NexusError> {
    if batch.num_rows() == 0 {
        return Ok(());
    }
    let primary_key = config.primary_key_or_err()?;
    let columns: Vec<String> = batch.schema().fields().iter().map(|f| f.name().clone()).collect();
    let pk_col = batch.schema().index_of(primary_key).map_err(|_| {
        NexusError::Schema(format!("primary key column '{primary_key}' not found in batch"))
    })?;
    let non_pk_cols: Vec<usize> = (0..columns.len()).filter(|c| *c != pk_col).collect();
    let table = crate::sql::oracle_identifier(&config.table)?;
    let pk_name = crate::sql::oracle_identifier(primary_key)?;
    let col_names: Vec<String> = columns
        .iter()
        .map(|c| crate::sql::oracle_identifier(c))
        .collect::<Result<Vec<_>, _>>()?;

    let num_rows = batch.num_rows();
    tracing::info!("oracle upsert {} rows in batches of {}", num_rows, ORACLE_MERGE_BATCH_SIZE);

    let mut row = 0;
    while row < num_rows {
        let end = (row + ORACLE_MERGE_BATCH_SIZE).min(num_rows);
        let mut selects = Vec::with_capacity(end - row);
        for r in row..end {
            let parts: Vec<String> = (0..columns.len())
                .map(|c| cell_to_literal(batch, r, c))
                .collect::<Result<Vec<_>, _>>()?;
            selects.push(format!(
                "SELECT {} FROM DUAL",
                parts.iter().zip(col_names.iter()).map(|(v, c)| format!("{v} AS {c}")).collect::<Vec<_>>().join(", ")
            ));
        }

        let updates: Vec<String> = non_pk_cols
            .iter()
            .map(|&col| format!("tgt.{0} = src.{0}", col_names[col]))
            .collect();
        let insert_cols = col_names.join(", ");
        let insert_vals: Vec<String> = col_names.iter().map(|c| format!("src.{c}")).collect();

        let sql = format!(
            "MERGE INTO {table} tgt USING ({}) src \
             ON (src.{pk_name} = tgt.{pk_name}) \
             WHEN MATCHED THEN UPDATE SET {upd} \
             WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})",
            selects.join(" UNION ALL "),
            upd = updates.join(", "),
            insert_vals = insert_vals.join(", "),
        );

        if row == 0 {
            tracing::info!("oracle first batched merge sql length: {}", sql.len());
        }

        retry_sync(&config.retry, "oracle batched merge", || -> Result<(), NexusError> {
            conn.execute(&sql, (), None)
                .map_err(|e| NexusError::Connector(format!("oracle batched merge failed: {e}")))?;
            Ok(())
        })?;

        row = end;
    }
    Ok(())
}

fn delete(config: &OracleConnectorConfig, conn: &Connection<'_>, batch: &RecordBatch) -> Result<(), NexusError> {
    if batch.num_rows() == 0 {
        return Ok(());
    }
    let primary_key = config.primary_key_or_err()?;
    let pk_col = batch.schema().index_of(primary_key).map_err(|_| {
        NexusError::Schema(format!("primary key column '{primary_key}' not found in batch"))
    })?;
    let table = crate::sql::oracle_identifier(&config.table)?;
    let pk_name = crate::sql::oracle_identifier(primary_key)?;

    for row in 0..batch.num_rows() {
        let pk_literal = cell_to_literal(batch, row, pk_col)?;
        let sql = format!("DELETE FROM {table} WHERE {pk_name} = {pk_literal}");
        retry_sync(&config.retry, "oracle delete", || -> Result<(), NexusError> {
            conn.execute(&sql, (), None)
                .map_err(|e| NexusError::Connector(format!("oracle delete failed: {e}")))?;
            Ok(())
        })?;
    }
    Ok(())
}

#[async_trait]
impl Sink for OracleSink {
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<(), NexusError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tx
            .send(BatchRequest { batch, response: tx })
            .map_err(|_| NexusError::Connector("oracle sink worker has terminated".to_string()))?;

        match tokio::time::timeout(std::time::Duration::from_secs(self.timeout_seconds), rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(e)) => Err(NexusError::Connector(format!("oracle worker response dropped: {e}"))),
            Err(_) => Err(NexusError::Connector(format!(
                "oracle worker did not respond within {}s (driver call still running on its own thread)",
                self.timeout_seconds
            ))),
        }
    }

    async fn commit_checkpoint(&mut self, _cursor: CheckpointCursor) -> Result<(), NexusError> {
        Ok(())
    }
}
