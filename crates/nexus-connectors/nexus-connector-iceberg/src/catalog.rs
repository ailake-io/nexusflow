use crate::config::IcebergConnectorConfig;
use iceberg::io::LocalFsStorageFactory;
use iceberg::CatalogBuilder;
use iceberg_catalog_sql::{SqlBindStyle, SqlCatalog, SqlCatalogBuilder};
use nexus_core::NexusError;
use std::collections::HashMap;
use std::sync::Arc;

/// Builds a fresh `SqlCatalog` handle pointed at `cfg`'s SQLite metadata
/// file + local warehouse directory. Cheap enough to call per operation —
/// mirrors the "always reopen to see committed state" pattern the other
/// Marco 5/6 connectors use (LanceDB, AI-Lake, Delta).
pub async fn connect(cfg: &IcebergConnectorConfig) -> Result<SqlCatalog, NexusError> {
    SqlCatalogBuilder::default()
        .uri(&cfg.catalog_uri)
        .warehouse_location(&cfg.warehouse_location)
        .sql_bind_style(SqlBindStyle::QMark)
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load("nexus", HashMap::new())
        .await
        .map_err(|e| NexusError::Connector(format!("iceberg catalog load failed: {e}")))
}
