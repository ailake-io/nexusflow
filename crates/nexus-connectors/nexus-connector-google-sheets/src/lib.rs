//! Google Sheets connector — Sheets API v4 (`values.get`/`values.append`).
//! `Bridged` (REST + JSON, no ADBC/ODBC driver exists for this API).
//! Source + sink.
//!
//! Registers the same two ways `nexus-connector-excel` does — see that
//! crate's `lib.rs` doc comment for the full rationale. Distinct from
//! `nexus-connector-excel`: this reads/writes a live Google Sheet over
//! HTTP, not a local/`s3://` `.xlsx` file.

mod config;
mod sink;
mod source;

pub use config::GoogleSheetsConnectorConfig;
pub use sink::GoogleSheetsSink;
pub use source::GoogleSheetsSource;

use futures::future::BoxFuture;
use nexus_core::{ConnectorCapability, NexusError, Sink, Source};

nexus_core::submit_connector!(
    "google-sheets",
    ConnectorCapability::Bridged,
    GoogleSheetsConnectorConfig
);

fn parse_and_validate(cfg: serde_json::Value) -> Result<GoogleSheetsConnectorConfig, NexusError> {
    let parsed: GoogleSheetsConnectorConfig =
        serde_json::from_value(cfg).map_err(|e| NexusError::Serialization(e.to_string()))?;
    parsed.validate()?;
    Ok(parsed)
}

fn validate_google_sheets_config(cfg: &serde_json::Value) -> Result<(), NexusError> {
    parse_and_validate(cfg.clone()).map(|_| ())
}

nexus_core::submit_source_builder!(
    "google-sheets",
    validate_google_sheets_config,
    |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Source>, NexusError>> {
        Box::pin(async move {
            let parsed = parse_and_validate(cfg)?;
            let source = GoogleSheetsSource::connect(&parsed).await?;
            Ok(Box::new(source) as Box<dyn Source>)
        })
    }
);

nexus_core::submit_sink_builder!(
    "google-sheets",
    validate_google_sheets_config,
    |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Sink>, NexusError>> {
        Box::pin(async move {
            let parsed = parse_and_validate(cfg)?;
            let sink = GoogleSheetsSink::connect(&parsed).await?;
            Ok(Box::new(sink) as Box<dyn Sink>)
        })
    }
);
