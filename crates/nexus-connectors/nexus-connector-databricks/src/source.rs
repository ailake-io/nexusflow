use crate::config::DatabricksConnectorConfig;
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

/// Same lifetime quirk `nexus-connector-postgres`'s (public repo)
/// `StatementBoundReader` documents, reused verbatim from `nexus-
/// connector-snowflake`: the ADBC reader isn't independent of the
/// `Statement` it came from despite `Statement::execute`'s `'static`
/// bound — dropping the statement early invalidates the reader.
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

/// Whole-table read, no partitioning — v1 simplification, same posture
/// `nexus-connector-snowflake` documents (ADBC's native partitioned-read
/// options are the "right" way to parallelize this later, not porting
/// Postgres's manual PK-range logic).
pub struct DatabricksSource {
    connection: ManagedConnection,
    qualified_table: String,
    schema: SchemaRef,
    timeout_seconds: u64,
    retry: nexus_core::RetryConfig,
}

impl DatabricksSource {
    pub async fn connect(cfg: &DatabricksConnectorConfig) -> Result<Self, NexusError> {
        let qualified = qualified_table(cfg)?;

        let retry = cfg.retry.clone();
        let (connection, schema) = retry_with_backoff(&retry, "databricks connect", || {
            let cfg = cfg.clone();
            async move {
                with_timeout(cfg.timeout_seconds, "databricks connect", async {
                    tokio::task::spawn_blocking(
                        move || -> Result<(ManagedConnection, arrow_schema::Schema), NexusError> {
                            let connection = open_connection(&cfg)?;
                            // NOTE (unverified — needs a real workspace to
                            // confirm): Unity Catalog has real catalog/schema
                            // metadata, unlike Postgres's `search_path`-based
                            // default lookup, so this passes both explicitly
                            // rather than `None`/`None` the way
                            // nexus-connector-postgres does. Revisit once
                            // tested against a real Databricks workspace.
                            let schema = connection
                                .get_table_schema(
                                    Some(&cfg.catalog),
                                    Some(&cfg.schema),
                                    &cfg.table,
                                )
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
            qualified_table: qualified,
            schema: Arc::new(schema),
            timeout_seconds: cfg.timeout_seconds,
            retry: cfg.retry.clone(),
        })
    }
}

#[async_trait]
impl Source for DatabricksSource {
    async fn read_batches(
        &mut self,
    ) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError> {
        let query = format!("SELECT * FROM {}", self.qualified_table);

        // ADBC calls are blocking FFI; run off the async executor and
        // yield each batch as it's produced instead of collecting the
        // whole table before the downstream pipeline starts.
        let timeout_seconds = self.timeout_seconds;
        let retry = self.retry.clone();
        let reader = retry_with_backoff(&retry, "databricks query", || {
            let mut connection = self.connection.clone();
            let query = query.clone();
            async move {
                with_timeout(timeout_seconds, "databricks query", async {
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
