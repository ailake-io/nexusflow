//! Zendesk connector — Support API v2 (tickets/users/organizations).
//! `Bridged` (REST + JSON, no ADBC/ODBC driver exists for this API).
//! Source + sink.
//!
//! Registers the same two ways `nexus-connector-excel` does — see that
//! crate's `lib.rs` doc comment for the full rationale.

mod config;
mod sink;
mod source;

pub use config::{ZendeskConnectorConfig, ZendeskResource};
pub use sink::ZendeskSink;
pub use source::ZendeskSource;

use futures::future::BoxFuture;
use nexus_core::{ConnectorCapability, NexusError, Sink, Source};

nexus_core::submit_connector!(
    "zendesk",
    ConnectorCapability::Bridged,
    ZendeskConnectorConfig
);

fn parse_and_validate(cfg: serde_json::Value) -> Result<ZendeskConnectorConfig, NexusError> {
    let parsed: ZendeskConnectorConfig =
        serde_json::from_value(cfg).map_err(|e| NexusError::Serialization(e.to_string()))?;
    parsed.validate()?;
    Ok(parsed)
}

fn validate_zendesk_config(cfg: &serde_json::Value) -> Result<(), NexusError> {
    parse_and_validate(cfg.clone()).map(|_| ())
}

nexus_core::submit_source_builder!(
    "zendesk",
    validate_zendesk_config,
    |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Source>, NexusError>> {
        Box::pin(async move {
            let parsed = parse_and_validate(cfg)?;
            let source = ZendeskSource::connect(&parsed).await?;
            Ok(Box::new(source) as Box<dyn Source>)
        })
    }
);

nexus_core::submit_sink_builder!(
    "zendesk",
    validate_zendesk_config,
    |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Sink>, NexusError>> {
        Box::pin(async move {
            let parsed = parse_and_validate(cfg)?;
            let sink = ZendeskSink::connect(&parsed).await?;
            Ok(Box::new(sink) as Box<dyn Sink>)
        })
    }
);
