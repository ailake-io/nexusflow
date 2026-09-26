use crate::config::RedshiftConnectorConfig;
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
/// documents (public repo) — the ADBC reader isn't independent of the
/// `Statement` it came from, so both are bundled together.
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
/// other connector in this repo makes (Excel/Snowflake/BigQuery).
/// `nexus-connector-postgres` partitions by primary-key range for
/// parallelism; that same approach would work here too (same wire
/// protocol), just not built yet — lower priority than getting a
/// working connector shipped.
///
/// Unlike Snowflake/BigQuery, `get_table_schema` isn't a documented
/// uncertainty here: this is the exact same driver and wire protocol
/// `nexus-connector-postgres` already validates against a real Postgres
/// server, and Redshift being wire-compatible means the same call
/// should behave identically — the only real unknown for this connector
/// is whether `MERGE INTO` (the sink's upsert SQL) round-trips
/// correctly against a real Redshift cluster, not basic connectivity.
pub struct RedshiftSource {
    connection: ManagedConnection,
    table: String,
    schema: SchemaRef,
    timeout_seconds: u64,
    retry: nexus_core::RetryConfig,
}

impl RedshiftSource {
    pub async fn connect(cfg: &RedshiftConnectorConfig) -> Result<Self, NexusError> {
        quote_identifier(&cfg.table)?;

        let retry = cfg.retry.clone();
        let (connection, schema) = retry_with_backoff(&retry, "redshift connect", || {
            let cfg = cfg.clone();
            async move {
                with_timeout(cfg.timeout_seconds, "redshift connect", async {
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
impl Source for RedshiftSource {
    async fn read_batches(
        &mut self,
    ) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError> {
        let query = format!("SELECT * FROM {}", quote_identifier(&self.table)?);

        let timeout_seconds = self.timeout_seconds;
        let retry = self.retry.clone();
        let reader = retry_with_backoff(&retry, "redshift query", || {
            let mut connection = self.connection.clone();
            let query = query.clone();
            async move {
                with_timeout(timeout_seconds, "redshift query", async {
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
