//! SAP HANA connector — eighth crate, third `Bridged` one: no ADBC or
//! pure-Rust HANA driver we can ship, same story as Oracle. Connects
//! via ODBC (`odbc-api`) + the official SAP HANA Client ODBC driver
//! (`HDBODBC`). v1 covers HANA (SQL) only — BAPI/IDoc via RFC needs
//! SAP-licensed SDKs (JCo/NCo/PyRFC/NW RFC SDK) that can't be
//! redistributed and is a much larger, separate effort; see README's
//! "Config do conector SAP HANA" section.
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

pub use config::HanaConnectorConfig;
pub use sink::HanaSink;
pub use source::HanaSource;

use futures::future::BoxFuture;
use nexus_core::{ConnectorCapability, NexusError, Sink, Source};

nexus_core::submit_connector!("hana", ConnectorCapability::Bridged, HanaConnectorConfig);

fn validate_hana_config(cfg: &serde_json::Value) -> Result<(), NexusError> {
    serde_json::from_value::<HanaConnectorConfig>(cfg.clone())
        .map(|_| ())
        .map_err(|e| NexusError::Serialization(e.to_string()))
}

nexus_core::submit_source_builder!(
    "hana",
    validate_hana_config,
    |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Source>, NexusError>> {
        Box::pin(async move {
            let parsed: HanaConnectorConfig = serde_json::from_value(cfg)
                .map_err(|e| NexusError::Serialization(e.to_string()))?;
            parsed.validate()?;
            let source = HanaSource::connect(&parsed).await?;
            Ok(Box::new(source) as Box<dyn Source>)
        })
    }
);

nexus_core::submit_sink_builder!(
    "hana",
    validate_hana_config,
    |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Sink>, NexusError>> {
        Box::pin(async move {
            let parsed: HanaConnectorConfig = serde_json::from_value(cfg)
                .map_err(|e| NexusError::Serialization(e.to_string()))?;
            parsed.validate()?;
            let sink = HanaSink::connect(&parsed).await?;
            Ok(Box::new(sink) as Box<dyn Sink>)
        })
    }
);
