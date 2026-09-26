use crate::config::MssqlConnectorConfig;
use crate::driver::open_connection;
use adbc_core::{Connection as _, Statement as _};
use adbc_driver_manager::{ManagedConnection, ManagedStatement};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use nexus_core::{quote_identifier, retry_with_backoff, with_timeout, NexusError, Source};
use std::sync::Arc;

/// Same lifetime quirk `nexus-connector-redshift`'s (this repo) and
/// `nexus-connector-postgres`'s (public repo) `StatementBoundReader`
/// document — the ADBC reader isn't independent of the `Statement` it
/// came from, so both are bundled together.
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

/// Whole-table read, no partitioning — same v1 simplification every
/// other `AdbcNative` connector in this repo makes.
///
/// `get_table_schema` carries the same "not yet confirmed against a
/// real server" flag Snowflake/BigQuery already have — the ADBC
/// Driver Foundry's `mssql` driver is real and free (confirmed
/// installing it this session), but unlike Redshift (proven wire
/// protocol Postgres already validates) this is a newer, community
/// driver with no live SQL Server/Synapse instance to test against
/// here.
pub struct MssqlSource {
    connection: ManagedConnection,
    table: String,
    schema: SchemaRef,
    timeout_seconds: u64,
    retry: nexus_core::RetryConfig,
}

impl MssqlSource {
    pub async fn connect(cfg: &MssqlConnectorConfig) -> Result<Self, NexusError> {
        quote_identifier(&cfg.table)?;

        let retry = cfg.retry.clone();
        let (connection, schema) = retry_with_backoff(&retry, "mssql connect", || {
            let cfg = cfg.clone();
            async move {
                with_timeout(cfg.timeout_seconds, "mssql connect", async {
                    tokio::task::spawn_blocking(
                        move || -> Result<(ManagedConnection, arrow_schema::Schema), NexusError> {
                            let uri = cfg.connection_string();
                            let connection = open_connection(&uri)?;
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
impl Source for MssqlSource {
    async fn read_batches(
        &mut self,
    ) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError> {
        let query = format!("SELECT * FROM {}", quote_identifier(&self.table)?);

        let timeout_seconds = self.timeout_seconds;
        let retry = self.retry.clone();
        let reader = retry_with_backoff(&retry, "mssql query", || {
            let mut connection = self.connection.clone();
            let query = query.clone();
            async move {
                with_timeout(timeout_seconds, "mssql query", async {
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
                            }) as Box<dyn arrow_array::RecordBatchReader + Send>)
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
