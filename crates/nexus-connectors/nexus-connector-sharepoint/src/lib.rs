//! SharePoint connector — SharePoint List items via Microsoft Graph
//! (`/sites/{site_id}/lists/{list_id}/items`). `Bridged` (REST +
//! JSON, no ADBC/ODBC driver exists for this API). Source + sink —
//! see `config.rs`'s doc comment for how this differs from a document
//! library (files, `nexus-connector-dropbox`/`google-drive` shape).
//!
//! Registers the same two ways `nexus-connector-excel` does — see that
//! crate's `lib.rs` doc comment for the full rationale.

mod config;
mod sink;
mod source;

pub use config::SharepointConnectorConfig;
pub use sink::SharepointSink;
pub use source::SharepointSource;

use futures::future::BoxFuture;
use nexus_core::{ConnectorCapability, NexusError, Sink, Source};

nexus_core::submit_connector!(
    "sharepoint",
    ConnectorCapability::Bridged,
    SharepointConnectorConfig
);

fn parse_and_validate(cfg: serde_json::Value) -> Result<SharepointConnectorConfig, NexusError> {
    let parsed: SharepointConnectorConfig =
        serde_json::from_value(cfg).map_err(|e| NexusError::Serialization(e.to_string()))?;
    parsed.validate()?;
    Ok(parsed)
}

fn validate_sharepoint_config(cfg: &serde_json::Value) -> Result<(), NexusError> {
    parse_and_validate(cfg.clone()).map(|_| ())
}

nexus_core::submit_source_builder!(
    "sharepoint",
    validate_sharepoint_config,
    |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Source>, NexusError>> {
        Box::pin(async move {
            let parsed = parse_and_validate(cfg)?;
            let source = SharepointSource::connect(&parsed).await?;
            Ok(Box::new(source) as Box<dyn Source>)
        })
    }
);

nexus_core::submit_sink_builder!(
    "sharepoint",
    validate_sharepoint_config,
    |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Sink>, NexusError>> {
        Box::pin(async move {
            let parsed = parse_and_validate(cfg)?;
            let sink = SharepointSink::connect(&parsed).await?;
            Ok(Box::new(sink) as Box<dyn Sink>)
        })
    }
);
