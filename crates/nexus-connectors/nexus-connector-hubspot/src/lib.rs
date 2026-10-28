//! HubSpot connector — CRM v3 Objects API. `Bridged` (REST + JSON, no
//! ADBC/ODBC driver exists for this API). Source + sink.
//!
//! Registers the same two ways `nexus-connector-excel` does — see that
//! crate's `lib.rs` doc comment for the full rationale.

mod config;
mod sink;
mod source;

pub use config::HubspotConnectorConfig;
pub use sink::HubspotSink;
pub use source::HubspotSource;

use futures::future::BoxFuture;
use nexus_core::{ConnectorCapability, NexusError, Sink, Source};

nexus_core::submit_connector!(
    "hubspot",
    ConnectorCapability::Bridged,
    HubspotConnectorConfig
);

fn parse_and_validate(cfg: serde_json::Value) -> Result<HubspotConnectorConfig, NexusError> {
    let parsed: HubspotConnectorConfig =
        serde_json::from_value(cfg).map_err(|e| NexusError::Serialization(e.to_string()))?;
    parsed.validate()?;
    Ok(parsed)
}

fn validate_hubspot_config(cfg: &serde_json::Value) -> Result<(), NexusError> {
    parse_and_validate(cfg.clone()).map(|_| ())
}

nexus_core::submit_source_builder!(
    "hubspot",
    validate_hubspot_config,
    |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Source>, NexusError>> {
        Box::pin(async move {
            let parsed = parse_and_validate(cfg)?;
            let source = HubspotSource::connect(&parsed).await?;
            Ok(Box::new(source) as Box<dyn Source>)
        })
    }
);

nexus_core::submit_sink_builder!(
    "hubspot",
    validate_hubspot_config,
    |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Sink>, NexusError>> {
        Box::pin(async move {
            let parsed = parse_and_validate(cfg)?;
            let sink = HubspotSink::connect(&parsed).await?;
            Ok(Box::new(sink) as Box<dyn Sink>)
        })
    }
);
