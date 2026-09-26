//! Vertica connector — `Bridged` (ODBC, no ADBC/pure-Rust Vertica
//! driver we can ship, same story as Oracle/HANA/Teradata in this
//! repo). Same ODBC-batch skeleton as `nexus-connector-hana`, swapped
//! for Vertica's own catalog view (`v_catalog.columns`) and its real
//! `MERGE` statement (simpler than Teradata's `UPDATE ... ELSE
//! INSERT` workaround — Vertica supports standard-SQL `MERGE`
//! natively).
//!
//! Registers the same two ways `nexus-connector-excel` does — see that
//! crate's `lib.rs` doc comment for the full rationale.

mod config;
mod driver;
mod row_mapping;
mod schema;
mod sink;
mod source;
mod sql;

pub use config::VerticaConnectorConfig;
pub use sink::VerticaSink;
pub use source::VerticaSource;

use futures::future::BoxFuture;
use nexus_core::{ConnectorCapability, NexusError, Sink, Source};

nexus_core::submit_connector!(
    "vertica",
    ConnectorCapability::Bridged,
    VerticaConnectorConfig
);

fn validate_vertica_config(cfg: &serde_json::Value) -> Result<(), NexusError> {
    serde_json::from_value::<VerticaConnectorConfig>(cfg.clone())
        .map(|_| ())
        .map_err(|e| NexusError::Serialization(e.to_string()))
}

nexus_core::submit_source_builder!(
    "vertica",
    validate_vertica_config,
    |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Source>, NexusError>> {
        Box::pin(async move {
            let parsed: VerticaConnectorConfig = serde_json::from_value(cfg)
                .map_err(|e| NexusError::Serialization(e.to_string()))?;
            parsed.validate()?;
            let source = VerticaSource::connect(&parsed).await?;
            Ok(Box::new(source) as Box<dyn Source>)
        })
    }
);

nexus_core::submit_sink_builder!(
    "vertica",
    validate_vertica_config,
    |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Sink>, NexusError>> {
        Box::pin(async move {
            let parsed: VerticaConnectorConfig = serde_json::from_value(cfg)
                .map_err(|e| NexusError::Serialization(e.to_string()))?;
            parsed.validate()?;
            let sink = VerticaSink::connect(&parsed).await?;
            Ok(Box::new(sink) as Box<dyn Sink>)
        })
    }
);
