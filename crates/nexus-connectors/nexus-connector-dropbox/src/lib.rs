//! Dropbox connector — reads/writes delimited text files (CSV/TSV/
//! custom-delimiter) in a Dropbox folder via the Dropbox API v2.
//! `Bridged` (REST + JSON/raw bytes, no ADBC/ODBC driver exists for
//! this API). Source + sink — same file-format contract as the
//! public repo's `nexus-connector-csv`, see `config.rs`'s doc comment
//! for how the two differ (API-fetched vs local/`object_store`).
//!
//! Registers the same two ways `nexus-connector-excel` does — see that
//! crate's `lib.rs` doc comment for the full rationale.

mod client;
mod config;
mod schema;
mod sink;
mod source;

pub use config::{DropboxConnectorConfig, DropboxDataType, DropboxFieldSpec};
pub use sink::DropboxSink;
pub use source::DropboxSource;

use futures::future::BoxFuture;
use nexus_core::{ConnectorCapability, NexusError, Sink, Source};

nexus_core::submit_connector!(
    "dropbox",
    ConnectorCapability::Bridged,
    DropboxConnectorConfig
);

fn parse_and_validate(cfg: serde_json::Value) -> Result<DropboxConnectorConfig, NexusError> {
    let parsed: DropboxConnectorConfig =
        serde_json::from_value(cfg).map_err(|e| NexusError::Serialization(e.to_string()))?;
    parsed.validate()?;
    Ok(parsed)
}

fn validate_dropbox_config(cfg: &serde_json::Value) -> Result<(), NexusError> {
    parse_and_validate(cfg.clone()).map(|_| ())
}

nexus_core::submit_source_builder!(
    "dropbox",
    validate_dropbox_config,
    |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Source>, NexusError>> {
        Box::pin(async move {
            let parsed = parse_and_validate(cfg)?;
            let source = DropboxSource::connect(&parsed).await?;
            Ok(Box::new(source) as Box<dyn Source>)
        })
    }
);

nexus_core::submit_sink_builder!(
    "dropbox",
    validate_dropbox_config,
    |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Sink>, NexusError>> {
        Box::pin(async move {
            let parsed = parse_and_validate(cfg)?;
            let sink = DropboxSink::connect(&parsed).await?;
            Ok(Box::new(sink) as Box<dyn Sink>)
        })
    }
);
