use crate::config::BigqueryConnectorConfig;
use crate::driver::open_connection;
use crate::quoting::qualified_table;
use adbc_core::{Connection as _, Statement as _};
use adbc_driver_manager::{ManagedConnection, ManagedStatement};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use nexus_core::{retry_with_backoff, with_timeout, NexusError, Source};
use std::sync::Arc;

/// Same lifetime quirk `nexus-connector-postgres`'s (and
/// `nexus-connector-snowflake`'s) `StatementBoundReader` documents: the
/// ADBC reader isn't independent of the `Statement` it came from despite
/// `Statement::execute`'s `'static` bound — dropping the statement early
/// invalidates the reader.
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

/// Whole-table read, no partitioning — same v1 simplification as
/// `SnowflakeSource`/`ExcelSource`. BigQuery's own `StatementOptions`
/// (query-job destination table, write_disposition, priority, etc. — see
/// driver.rs's `bigquery_options` doc comment) are geared toward
/// materializing a *large* query result into a table, which would be the
/// "right" way to parallelize this connector later — not worth building
/// before there's a real large-result-set case to justify it.
pub struct BigquerySource {
    connection: ManagedConnection,
    query: String,
    schema: SchemaRef,
    timeout_seconds: u64,
    retry: nexus_core::RetryConfig,
}

impl BigquerySource {
    pub async fn connect(cfg: &BigqueryConnectorConfig) -> Result<Self, NexusError> {
        cfg.validate()?;
        let table = qualified_table(cfg)?;
        let query = format!("SELECT * FROM {table}");

        let retry = cfg.retry.clone();
        let query_for_schema = query.clone();
        let (connection, schema) = retry_with_backoff(&retry, "bigquery connect", || {
            let cfg = cfg.clone();
            let query_for_schema = query_for_schema.clone();
            async move {
                with_timeout(cfg.timeout_seconds, "bigquery connect", async {
                    tokio::task::spawn_blocking(
                        move || -> Result<(ManagedConnection, arrow_schema::Schema), NexusError> {
                            let mut connection = open_connection(&cfg)?;
                            // NOTE (unverified — needs a real GCP project to
                            // confirm): using a throwaway prepared statement
                            // just to read the schema, rather than
                            // `get_table_schema`, since BigQuery table
                            // metadata lookup semantics via ADBC haven't been
                            // confirmed against the real driver yet (same
                            // caveat as Snowflake's source.rs). Revisit once
                            // testing against a real project.
                            let mut statement = connection
                                .new_statement()
                                .map_err(|e| NexusError::Connector(e.to_string()))?;
                            statement
                                .set_sql_query(&query_for_schema)
                                .map_err(|e| NexusError::Connector(e.to_string()))?;
                            let schema = statement
                                .execute_schema()
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
            query,
            schema: Arc::new(schema),
            timeout_seconds: cfg.timeout_seconds,
            retry: cfg.retry.clone(),
        })
    }
}

#[async_trait]
impl Source for BigquerySource {
    async fn read_batches(
        &mut self,
    ) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError> {
        let query = self.query.clone();

        let timeout_seconds = self.timeout_seconds;
        let retry = self.retry.clone();
        let reader = retry_with_backoff(&retry, "bigquery query", || {
            let mut connection = self.connection.clone();
            let query = query.clone();
            async move {
                with_timeout(timeout_seconds, "bigquery query", async {
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
