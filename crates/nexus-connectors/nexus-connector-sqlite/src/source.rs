use crate::config::SqliteConnectorConfig;
use crate::driver::open_connection;
use adbc_core::{Connection as _, Statement as _};
use adbc_driver_manager::ManagedConnection;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use nexus_core::{quote_identifier, NexusError, Source};
use std::sync::Arc;

/// No partition range here — unlike `PostgresSource`, this connector isn't
/// wired into Marco 1's partitioned engine, only into Marco 2's transform
/// fan-in, which drains every source fully anyway (`ARCHITECTURE.md §6`).
pub struct SqliteSource {
    connection: ManagedConnection,
    table: String,
    schema: SchemaRef,
}

impl SqliteSource {
    pub fn connect(cfg: &SqliteConnectorConfig) -> Result<Self, NexusError> {
        quote_identifier(&cfg.table)?;

        let connection = open_connection(&cfg.uri)?;
        let schema = connection
            .get_table_schema(None, None, &cfg.table)
            .map_err(|e| NexusError::Schema(e.to_string()))?;

        Ok(Self {
            connection,
            table: cfg.table.clone(),
            schema: Arc::new(schema),
        })
    }
}

#[async_trait]
impl Source for SqliteSource {
    async fn read_batches(
        &mut self,
    ) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError> {
        let table = quote_identifier(&self.table)?;
        let mut connection = self.connection.clone();

        // ADBC calls are blocking FFI; run off the async executor and yield
        // each batch as it is produced instead of collecting the whole table.
        let reader = tokio::task::spawn_blocking(
            move || -> Result<Box<dyn arrow_array::RecordBatchReader + Send>, NexusError> {
                let mut statement = connection
                    .new_statement()
                    .map_err(|e| NexusError::Connector(e.to_string()))?;
                statement
                    .set_sql_query(format!("SELECT * FROM {table}"))
                    .map_err(|e| NexusError::Connector(e.to_string()))?;
                statement
                    .execute()
                    .map_err(|e| NexusError::Connector(e.to_string()))
            },
        )
        .await
        .map_err(|e| NexusError::Connector(format!("blocking task panicked: {e}")))??;

        Ok(Box::pin(stream::iter(reader.map(|r| {
            r.map_err(|e| NexusError::Serialization(e.to_string()))
        }))))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
