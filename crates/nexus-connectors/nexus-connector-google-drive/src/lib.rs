//! Google Drive connector — reads/writes delimited text files (CSV/
//! TSV/custom-delimiter) in a Drive folder via the Drive API v3.
//! `Bridged` (REST + JSON/raw bytes, no ADBC/ODBC driver exists for
//! this API). Source + sink — same file-format contract as the
//! public repo's `nexus-connector-csv` and `nexus-connector-dropbox`,
//! see `config.rs`'s doc comment for how they differ. Distinct from
//! `nexus-connector-google-sheets`: this reads/writes delimited text
//! *files*, not a live Sheet's cell grid.
//!
//! Registers the same two ways `nexus-connector-excel` does — see that
//! crate's `lib.rs` doc comment for the full rationale.

mod client;
mod config;
mod schema;
mod sink;
mod source;

pub use config::{GoogleDriveConnectorConfig, GoogleDriveDataType, GoogleDriveFieldSpec};
pub use sink::GoogleDriveSink;
pub use source::GoogleDriveSource;

use futures::future::BoxFuture;
use nexus_core::{ConnectorCapability, NexusError, Sink, Source};

nexus_core::submit_connector!(
    "google-drive",
    ConnectorCapability::Bridged,
    GoogleDriveConnectorConfig
);

fn parse_and_validate(cfg: serde_json::Value) -> Result<GoogleDriveConnectorConfig, NexusError> {
    let parsed: GoogleDriveConnectorConfig =
        serde_json::from_value(cfg).map_err(|e| NexusError::Serialization(e.to_string()))?;
    parsed.validate()?;
    Ok(parsed)
}

fn validate_google_drive_config(cfg: &serde_json::Value) -> Result<(), NexusError> {
    parse_and_validate(cfg.clone()).map(|_| ())
}

nexus_core::submit_source_builder!(
    "google-drive",
    validate_google_drive_config,
    |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Source>, NexusError>> {
        Box::pin(async move {
            let parsed = parse_and_validate(cfg)?;
            let source = GoogleDriveSource::connect(&parsed).await?;
            Ok(Box::new(source) as Box<dyn Source>)
        })
    }
);

nexus_core::submit_sink_builder!(
    "google-drive",
    validate_google_drive_config,
    |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Sink>, NexusError>> {
        Box::pin(async move {
            let parsed = parse_and_validate(cfg)?;
            let sink = GoogleDriveSink::connect(&parsed).await?;
            Ok(Box::new(sink) as Box<dyn Sink>)
        })
    }
);
