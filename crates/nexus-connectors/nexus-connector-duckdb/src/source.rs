use crate::config::DuckdbConnectorConfig;
use crate::driver::open_connection;
use adbc_core::{Connection as _, Statement as _};
use adbc_driver_manager::{ManagedConnection, ManagedStatement};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use nexus_core::{quote_identifier, with_timeout, NexusError, Source};
use std::sync::Arc;

/// Same rationale as `nexus-connector-sqlite`'s equivalent: the reader
/// returned by `Statement::execute` isn't independent of the `Statement` it
/// came from, so both must be kept alive together.
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

/// No partition range here — same choice as `nexus-connector-sqlite`: a
/// single embedded-file connection gains nothing from range-partitioned
/// parallel reads the way a networked server does.
pub struct DuckdbSource {
    connection: ManagedConnection,
    table: String,
    schema: SchemaRef,
    timeout_seconds: u64,
}

impl DuckdbSource {
    pub async fn connect(cfg: &DuckdbConnectorConfig) -> Result<Self, NexusError> {
        quote_identifier(&cfg.table)?;

        let uri = cfg.connection_url();
        let table = cfg.table.clone();
        let (connection, schema) = with_timeout(cfg.timeout_seconds, "duckdb connect", async {
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

        Ok(Self {
            connection,
            table: cfg.table.clone(),
            schema: Arc::new(schema),
            timeout_seconds: cfg.timeout_seconds,
        })
    }
}

#[async_trait]
impl Source for DuckdbSource {
    async fn read_batches(
        &mut self,
    ) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError> {
        let table = quote_identifier(&self.table)?;
        let mut connection = self.connection.clone();

        let reader = with_timeout(self.timeout_seconds, "duckdb query", async {
            tokio::task::spawn_blocking(
                move || -> Result<Box<dyn arrow_array::RecordBatchReader + Send>, NexusError> {
                    let mut statement = connection
                        .new_statement()
                        .map_err(|e| NexusError::Connector(e.to_string()))?;
                    statement
                        .set_sql_query(format!("SELECT * FROM {table}"))
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
        .await?;

        Ok(Box::pin(stream::iter(reader.map(|r| {
            r.map_err(|e| NexusError::Serialization(e.to_string()))
        }))))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
