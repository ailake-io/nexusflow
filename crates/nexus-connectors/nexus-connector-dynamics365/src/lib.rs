//! Dynamics 365 connector — Dataverse Web API (OData v4). `Bridged`
//! (REST + JSON, no ADBC/ODBC driver exists for this API). Source +
//! sink.
//!
//! Registers the same two ways `nexus-connector-excel` does — see that
//! crate's `lib.rs` doc comment for the full rationale.

mod config;
mod sink;
mod source;

pub use config::Dynamics365ConnectorConfig;
pub use sink::Dynamics365Sink;
pub use source::Dynamics365Source;

use futures::future::BoxFuture;
use nexus_core::{ConnectorCapability, NexusError, Sink, Source};

nexus_core::submit_connector!(
    "dynamics365",
    ConnectorCapability::Bridged,
    Dynamics365ConnectorConfig
);

fn parse_and_validate(cfg: serde_json::Value) -> Result<Dynamics365ConnectorConfig, NexusError> {
    let parsed: Dynamics365ConnectorConfig =
        serde_json::from_value(cfg).map_err(|e| NexusError::Serialization(e.to_string()))?;
    parsed.validate()?;
    Ok(parsed)
}

fn validate_dynamics365_config(cfg: &serde_json::Value) -> Result<(), NexusError> {
    parse_and_validate(cfg.clone()).map(|_| ())
}

nexus_core::submit_source_builder!(
    "dynamics365",
    validate_dynamics365_config,
    |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Source>, NexusError>> {
        Box::pin(async move {
            let parsed = parse_and_validate(cfg)?;
            let source = Dynamics365Source::connect(&parsed).await?;
            Ok(Box::new(source) as Box<dyn Source>)
        })
    }
);

nexus_core::submit_sink_builder!(
    "dynamics365",
    validate_dynamics365_config,
    |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Sink>, NexusError>> {
        Box::pin(async move {
            let parsed = parse_and_validate(cfg)?;
            let sink = Dynamics365Sink::connect(&parsed).await?;
            Ok(Box::new(sink) as Box<dyn Sink>)
        })
    }
);
