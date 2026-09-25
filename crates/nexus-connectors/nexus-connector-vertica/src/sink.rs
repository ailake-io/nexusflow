use crate::config::VerticaConnectorConfig;
use crate::driver::connection_string;
use crate::row_mapping::cell_to_param;
use crate::sql::{build_delete_sql, build_upsert_sql};
use arrow_array::RecordBatch;
use async_trait::async_trait;
use nexus_core::{split_by_opcode, CheckpointCursor, NexusError, RetryConfig, Sink};
use odbc_api::parameter::InputParameter;
use odbc_api::{Connection, ConnectionOptions, Environment};
use std::sync::mpsc;
use std::time::Duration;

/// `odbc-api` handles aren't `Send` — same constraint every ODBC
/// connector in this repo documents, solved the same way: a dedicated
/// OS thread owns the ODBC environment/connection for the sink's
/// lifetime, `write_batch` ships batches to it over a synchronous
/// channel and waits on a oneshot response.
///
/// Upsert is a real `MERGE` statement (see `sql.rs`) — CDC delete
/// opcodes get a real `DELETE`, same as Oracle/HANA/Teradata.
pub struct VerticaSink {
    tx: mpsc::Sender<BatchRequest>,
    timeout_seconds: u64,
}

struct BatchRequest {
    batch: RecordBatch,
    response: tokio::sync::oneshot::Sender<Result<(), NexusError>>,
}

impl VerticaSink {
    pub async fn connect(config: &VerticaConnectorConfig) -> Result<Self, NexusError> {
        config.validate()?;
        config.primary_key_or_err()?;
        let timeout_seconds = config.timeout_seconds;
        let config = config.clone();
        let (tx, rx) = mpsc::channel::<BatchRequest>();

        std::thread::Builder::new()
            .name("nexus-vertica-sink".to_string())
            .spawn(move || {
                if let Err(e) = run_worker(config, rx) {
                    tracing::error!("vertica sink worker terminated with error: {e}");
                }
            })
            .map_err(|e| NexusError::Connector(format!("failed to spawn vertica worker: {e}")))?;

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

fn run_worker(
    config: VerticaConnectorConfig,
    rx: mpsc::Receiver<BatchRequest>,
) -> Result<(), NexusError> {
    let env = Environment::new().map_err(|e| NexusError::Connector(format!("vertica env: {e}")))?;
    let conn: Connection<'_> = retry_sync(&config.retry, "vertica sink connect", || {
        env.connect_with_connection_string(
            &connection_string(&config),
            ConnectionOptions::default(),
        )
        .map_err(|e| NexusError::Connector(format!("vertica connect: {e}")))
    })?;
    conn.set_autocommit(false)
        .map_err(|e| NexusError::Connector(format!("vertica set_autocommit(false): {e}")))?;

    for req in rx {
        let result = apply_batch(&config, &conn, &req.batch);
        let _ = req.response.send(result);
    }
    Ok(())
}

fn apply_batch(
    config: &VerticaConnectorConfig,
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
            .map_err(|e| NexusError::Connector(format!("vertica commit failed: {e}"))),
        Err(e) => {
            let _ = conn.rollback();
            Err(e)
        }
    }
}

fn upsert(
    config: &VerticaConnectorConfig,
    conn: &Connection<'_>,
    batch: &RecordBatch,
) -> Result<(), NexusError> {
    if batch.num_rows() == 0 {
        return Ok(());
    }
    let primary_key = config.primary_key_or_err()?;
    let columns: Vec<String> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    let sql = build_upsert_sql(&config.table, &columns, primary_key)?;

    let mut stmt = conn
        .preallocate()
        .map_err(|e| NexusError::Connector(format!("vertica preallocate upsert: {e}")))?;
    for row in 0..batch.num_rows() {
        let params: Vec<Box<dyn InputParameter>> = (0..columns.len())
            .map(|col| cell_to_param(batch, row, col))
            .collect::<Result<_, _>>()?;
        retry_sync(
            &config.retry,
            "vertica upsert",
            || -> Result<(), NexusError> {
                stmt.execute(&sql, params.as_slice())
                    .map_err(|e| NexusError::Connector(format!("vertica upsert failed: {e}")))?;
                Ok(())
            },
        )?;
    }
    Ok(())
}

fn delete(
    config: &VerticaConnectorConfig,
    conn: &Connection<'_>,
    batch: &RecordBatch,
) -> Result<(), NexusError> {
    if batch.num_rows() == 0 {
        return Ok(());
    }
    let primary_key = config.primary_key_or_err()?;
    let pk_col = batch.schema().index_of(primary_key).map_err(|_| {
        NexusError::Schema(format!(
            "primary key column '{primary_key}' not found in batch"
        ))
    })?;
    let sql = build_delete_sql(&config.table, primary_key)?;

    let mut stmt = conn
        .preallocate()
        .map_err(|e| NexusError::Connector(format!("vertica preallocate delete: {e}")))?;
    for row in 0..batch.num_rows() {
        let param = cell_to_param(batch, row, pk_col)?;
        retry_sync(
            &config.retry,
            "vertica delete",
            || -> Result<(), NexusError> {
                stmt.execute(&sql, std::slice::from_ref(&param))
                    .map_err(|e| NexusError::Connector(format!("vertica delete failed: {e}")))?;
                Ok(())
            },
        )?;
    }
    Ok(())
}

#[async_trait]
impl Sink for VerticaSink {
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<(), NexusError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tx
            .send(BatchRequest {
                batch,
                response: tx,
            })
            .map_err(|_| NexusError::Connector("vertica sink worker has terminated".to_string()))?;

        match tokio::time::timeout(std::time::Duration::from_secs(self.timeout_seconds), rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(e)) => Err(NexusError::Connector(format!(
                "vertica worker response dropped: {e}"
            ))),
            Err(_) => Err(NexusError::Connector(format!(
                "vertica worker did not respond within {}s (driver call still running on its own thread)",
                self.timeout_seconds
            ))),
        }
    }

    async fn commit_checkpoint(&mut self, _cursor: CheckpointCursor) -> Result<(), NexusError> {
        Ok(())
    }
}
