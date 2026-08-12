use crate::catalog;
use crate::config::IcebergConnectorConfig;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::StreamExt;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use nexus_core::{with_timeout, NexusError, Source};
use std::sync::Arc;

/// Iceberg source — full-table scan via `Table::scan().select_all()`, using
/// iceberg-rust's own query engine (not a raw file read like the Parquet/
/// Delta Marco 6 sources) since Iceberg's manifest/snapshot indirection
/// makes "list current data files" a real query, not a directory listing.
pub struct IcebergSource {
    cfg: IcebergConnectorConfig,
    schema: SchemaRef,
}

impl IcebergSource {
    pub async fn connect(cfg: &IcebergConnectorConfig) -> Result<Self, NexusError> {
        let catalog = catalog::connect(cfg).await?;
        let ident = TableIdent::new(
            NamespaceIdent::new(cfg.namespace()),
            cfg.table_name(),
        );
        let table = with_timeout(cfg.timeout_seconds, "iceberg load_table", async {
            catalog
                .load_table(&ident)
                .await
                .map_err(|e| NexusError::Connector(format!("iceberg load_table failed: {e}")))
        })
        .await?;
        let schema = Arc::new(
            schema_to_arrow_schema(table.metadata().current_schema()).map_err(|e| {
                NexusError::Schema(format!("iceberg schema_to_arrow_schema failed: {e}"))
            })?,
        );
        Ok(Self {
            cfg: cfg.clone(),
            schema,
        })
    }
}

#[async_trait]
impl Source for IcebergSource {
    async fn read_batches(
        &mut self,
    ) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError> {
        // Reopen fresh — same "must reload to see committed state" gotcha
        // the Marco 5 LanceDB source and the Marco 6 Delta source hit.
        let catalog = catalog::connect(&self.cfg).await?;
        let ident = TableIdent::new(
            NamespaceIdent::new(self.cfg.namespace()),
            self.cfg.table_name(),
        );
        let table = with_timeout(self.cfg.timeout_seconds, "iceberg load_table", async {
            catalog
                .load_table(&ident)
                .await
                .map_err(|e| NexusError::Connector(format!("iceberg load_table failed: {e}")))
        })
        .await?;
        let scan = table
            .scan()
            .select_all()
            .build()
            .map_err(|e| NexusError::Connector(format!("iceberg scan build failed: {e}")))?;
        let stream = with_timeout(self.cfg.timeout_seconds, "iceberg scan to_arrow", async {
            scan.to_arrow()
                .await
                .map_err(|e| NexusError::Connector(format!("iceberg scan to_arrow failed: {e}")))
        })
        .await?;
        let mapped = stream.map(|r| {
            r.map_err(|e| NexusError::Connector(format!("iceberg scan read failed: {e}")))
        });
        Ok(Box::pin(mapped))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
