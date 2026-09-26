//! Starburst connector — a commercial fork of Trino with no free ADBC/ODBC
//! driver of its own (JDBC/ODBC live behind the Starburst customer portal),
//! so this crate talks HTTP directly against Trino's publicly-documented
//! client-facing protocol (`client.rs`) instead. Source only: it's a
//! query-federation engine, not storage, so `INSERT` depends on whatever
//! catalog the cluster operator configured server-side.
//!
//! Migrated from the enterprise repo to OSS (Fase 32, 2026-09-25) —
//! registered via `submit_source_builder!` (the plugin-extension path
//! meant for out-of-workspace crates, `registry.rs`'s doc comment) rather
//! than a native match arm in `nexus-server::connectors`, since that's how
//! it worked before the move; still correct here (the registry is a
//! fallback, not enterprise-only), just not yet converted to match every
//! other in-tree connector's convention.

mod client;
mod config;
mod rows;
mod source;

pub use config::StarburstConnectorConfig;
pub use source::{PartitionRange, StarburstSource};

use futures::future::BoxFuture;
use nexus_core::{ConnectorCapability, NexusError, Source};

nexus_core::submit_connector!(
    "starburst",
    ConnectorCapability::Bridged,
    StarburstConnectorConfig
);

fn validate_starburst_config(cfg: &serde_json::Value) -> Result<(), NexusError> {
    serde_json::from_value::<StarburstConnectorConfig>(cfg.clone())
        .map(|_| ())
        .map_err(|e| NexusError::Serialization(e.to_string()))
}

nexus_core::submit_source_builder!(
    "starburst",
    validate_starburst_config,
    |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Source>, NexusError>> {
        Box::pin(async move {
            let parsed: StarburstConnectorConfig = serde_json::from_value(cfg)
                .map_err(|e| NexusError::Serialization(e.to_string()))?;
            let source = StarburstSource::connect(&parsed, None).await?;
            Ok(Box::new(source) as Box<dyn Source>)
        })
    }
);
