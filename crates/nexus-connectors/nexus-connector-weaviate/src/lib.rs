//! Weaviate connector — nineteenth crate, second of the tier-6
//! vector/search group. `Bridged` (REST + JSON, no ADBC/ODBC driver
//! exists for this). Sink only — mirrors
//! `nexus-connector-chromadb`/`elasticsearch` (this repo/public
//! repo): the class (collection) must already exist, this connector
//! only writes rows.
//!
//! Registers the same two ways `nexus-connector-excel` does — see that
//! crate's `lib.rs` doc comment for the full rationale.

mod config;
mod rows;
mod sink;

pub use config::WeaviateConnectorConfig;
pub use sink::WeaviateSink;

use futures::future::BoxFuture;
use nexus_core::{ConnectorCapability, NexusError, Sink};

nexus_core::submit_connector!(
    "weaviate",
    ConnectorCapability::Bridged,
    WeaviateConnectorConfig
);

fn parse_and_validate(cfg: serde_json::Value) -> Result<WeaviateConnectorConfig, NexusError> {
    let parsed: WeaviateConnectorConfig = serde_json::from_value(cfg)
        .map_err(|e| NexusError::Serialization(e.to_string()))?;
    parsed.validate()?;
    Ok(parsed)
}

fn validate_weaviate_config(cfg: &serde_json::Value) -> Result<(), NexusError> {
    parse_and_validate(cfg.clone()).map(|_| ())
}

nexus_core::submit_sink_builder!(
    "weaviate",
    validate_weaviate_config,
    |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Sink>, NexusError>> {
        Box::pin(async move {
            let parsed = parse_and_validate(cfg)?;
            let sink = WeaviateSink::connect(&parsed).await?;
            Ok(Box::new(sink) as Box<dyn Sink>)
        })
    }
);
