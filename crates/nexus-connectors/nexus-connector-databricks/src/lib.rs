//! Databricks (SQL Warehouse / Unity Catalog) connector —
//! `ConnectorCapability::AdbcNative`, same family as `nexus-connector-
//! snowflake`/`nexus-connector-bigquery`. Uses the ADBC Driver Foundry's
//! dedicated Databricks driver (`dbc install databricks`, no account
//! needed — see `driver.rs`), same `adbc_driver_manager` crate the
//! public repo's `nexus-connector-postgres`/`nexus-connector-sqlite`
//! already use.
//!
//! Registers the same two ways `nexus-connector-excel` does — see that
//! crate's `lib.rs` doc comment for the full rationale.

mod config;
mod driver;
mod quoting;
mod sink;
mod source;

pub use config::{DatabricksAuth, DatabricksConnectorConfig};
pub use sink::DatabricksSink;
pub use source::DatabricksSource;

use futures::future::BoxFuture;
use nexus_core::{ConnectorCapability, NexusError, Sink, Source};

nexus_core::submit_connector!(
    "databricks",
    ConnectorCapability::AdbcNative,
    DatabricksConnectorConfig
);

fn parse_and_validate(cfg: serde_json::Value) -> Result<DatabricksConnectorConfig, NexusError> {
    let parsed: DatabricksConnectorConfig = serde_json::from_value(cfg)
        .map_err(|e| NexusError::Serialization(e.to_string()))?;
    parsed.validate()?;
    Ok(parsed)
}

fn validate_databricks_config(cfg: &serde_json::Value) -> Result<(), NexusError> {
    parse_and_validate(cfg.clone()).map(|_| ())
}

nexus_core::submit_source_builder!(
    "databricks",
    validate_databricks_config,
    |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Source>, NexusError>> {
        Box::pin(async move {
            let parsed = parse_and_validate(cfg)?;
            let source = DatabricksSource::connect(&parsed).await?;
            Ok(Box::new(source) as Box<dyn Source>)
        })
    }
);

nexus_core::submit_sink_builder!(
    "databricks",
    validate_databricks_config,
    |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Sink>, NexusError>> {
        Box::pin(async move {
            let parsed = parse_and_validate(cfg)?;
            let sink = DatabricksSink::connect(&parsed).await?;
            Ok(Box::new(sink) as Box<dyn Sink>)
        })
    }
);
