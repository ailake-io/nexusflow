use crate::config::SnowflakeConnectorConfig;
use crate::driver::open_connection;
use adbc_core::{Connection as _, Statement as _};
use adbc_driver_manager::{ManagedConnection, ManagedStatement};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use nexus_core::{quote_identifier, retry_with_backoff, with_timeout, NexusError, Source};
use std::sync::Arc;

/// Same lifetime quirk `nexus-connector-postgres`'s `StatementBoundReader`
/// documents: the ADBC reader isn't independent of the `Statement` it came
/// from despite `Statement::execute`'s `'static` bound — dropping the
/// statement early invalidates the reader. Bundle them together.
struct StatementBoundReader {
    _statement: ManagedStatement,
    reader: Box<dyn arrow_array::RecordBatchReader + Send>,
}

impl Iterator for StatementBoundReader {
    type Item = Result<RecordBatch, arrow_schema::ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.reader.next()
    }
}

impl arrow_array::RecordBatchReader for StatementBoundReader {
    fn schema(&self) -> SchemaRef {
        self.reader.schema()
    }
}

/// Whole-table read, no partitioning — v1 simplification, same posture as
/// this repo's own `ExcelSource`. `nexus-connector-postgres` partitions
/// by primary-key range for parallelism; the Snowflake ADBC driver
/// exposes native result-set partitioning via `execute_partitions`
/// instead (ADBC's `Incremental`/multi-partition statement options,
/// see driver.rs's `StatementOptions` comment) — that's the "right" way
/// to parallelize this connector later, not porting Postgres's manual
/// PK-range logic.
pub struct SnowflakeSource {
    connection: ManagedConnection,
    table: String,
    schema: SchemaRef,
    timeout_seconds: u64,
    retry: nexus_core::RetryConfig,
}

impl SnowflakeSource {
    pub async fn connect(cfg: &SnowflakeConnectorConfig) -> Result<Self, NexusError> {
        cfg.validate()?;
        quote_identifier(&cfg.table)?;

        let retry = cfg.retry.clone();
        let (connection, schema) = retry_with_backoff(&retry, "snowflake connect", || {
            let cfg = cfg.clone();
            async move {
                with_timeout(cfg.timeout_seconds, "snowflake connect", async {
                    tokio::task::spawn_blocking(
                        move || -> Result<(ManagedConnection, arrow_schema::Schema), NexusError> {
                            let connection = open_connection(&cfg)?;
                            // NOTE (unverified — needs a real account to
                            // confirm): Snowflake identifiers are commonly
                            // uppercased unless quoted at creation time, and
                            // ADBC's get_table_schema catalog/db_schema args
                            // may need to be Some(&cfg.database)/
                            // Some(&cfg.schema) explicitly rather than None,
                            // unlike Postgres where None searches the
                            // connection's default search_path. Revisit once
                            // testing against a trial account (see
                            // ROADMAP/plan notes).
                            let schema = connection
                                .get_table_schema(None, None, &cfg.table)
                                .map_err(|e| NexusError::Schema(e.to_string()))?;
                            Ok((connection, schema))
                        },
                    )
                    .await
                    .map_err(|e| NexusError::Connector(format!("blocking task panicked: {e}")))?
                })
                .await
            }
        })
        .await?;

        Ok(Self {
            connection,
            table: cfg.table.clone(),
            schema: Arc::new(schema),
            timeout_seconds: cfg.timeout_seconds,
            retry: cfg.retry.clone(),
        })
    }
}

#[async_trait]
impl Source for SnowflakeSource {
    async fn read_batches(
        &mut self,
    ) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError> {
        let query = format!("SELECT * FROM {}", quote_identifier(&self.table)?);

        let timeout_seconds = self.timeout_seconds;
        let retry = self.retry.clone();
        let reader = retry_with_backoff(&retry, "snowflake query", || {
            let mut connection = self.connection.clone();
            let query = query.clone();
            async move {
                with_timeout(timeout_seconds, "snowflake query", async {
                    tokio::task::spawn_blocking(
                        move || -> Result<Box<dyn arrow_array::RecordBatchReader + Send>, NexusError> {
                            let mut statement = connection
                                .new_statement()
                                .map_err(|e| NexusError::Connector(e.to_string()))?;
                            statement
                                .set_sql_query(&query)
                                .map_err(|e| NexusError::Connector(e.to_string()))?;
                            let reader = statement
                                .execute()
                                .map_err(|e| NexusError::Connector(e.to_string()))?;
                            Ok(Box::new(StatementBoundReader {
                                _statement: statement,
                                reader,
                            })
                                as Box<dyn arrow_array::RecordBatchReader + Send>)
                        },
                    )
                    .await
                    .map_err(|e| NexusError::Connector(format!("blocking task panicked: {e}")))?
                })
                .await
            }
        })
        .await?;

        Ok(Box::pin(stream::iter(reader.map(|r| {
            r.map_err(|e| NexusError::Serialization(e.to_string()))
        }))))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
