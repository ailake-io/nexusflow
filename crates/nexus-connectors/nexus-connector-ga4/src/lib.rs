//! GA4 (Google Analytics 4) connector — tenth crate, first of the
//! tier-5 marketing/ads/payments group. `Bridged` (REST + JSON, no
//! ADBC/ODBC driver exists for this — same shape as
//! `nexus-connector-salesforce`, this repo). Source only — the Data
//! API is read-only reporting, no write/ingest endpoint.
//!
//! Registers the same two ways `nexus-connector-excel` does — see that
//! crate's `lib.rs` doc comment for the full rationale.

mod auth;
mod config;
mod source;

pub use config::Ga4ConnectorConfig;
pub use source::Ga4Source;

use futures::future::BoxFuture;
use nexus_core::{ConnectorCapability, NexusError, Source};

nexus_core::submit_connector!("ga4", ConnectorCapability::Bridged, Ga4ConnectorConfig);

fn parse_and_validate(cfg: serde_json::Value) -> Result<Ga4ConnectorConfig, NexusError> {
    let parsed: Ga4ConnectorConfig =
        serde_json::from_value(cfg).map_err(|e| NexusError::Serialization(e.to_string()))?;
    parsed.validate()?;
    Ok(parsed)
}

fn validate_ga4_config(cfg: &serde_json::Value) -> Result<(), NexusError> {
    parse_and_validate(cfg.clone()).map(|_| ())
}

nexus_core::submit_source_builder!(
    "ga4",
    validate_ga4_config,
    |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Source>, NexusError>> {
        Box::pin(async move {
            let parsed = parse_and_validate(cfg)?;
            let source = Ga4Source::connect(&parsed).await?;
            Ok(Box::new(source) as Box<dyn Source>)
        })
    }
);
