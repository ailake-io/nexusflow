//! Google Ads connector — eleventh crate, second of the tier-5
//! marketing/ads/payments group. `Bridged` (REST + JSON, no ADBC/ODBC
//! driver exists for this API). Source only — the reporting API this
//! connector uses is read-only (no mutate/write endpoint implemented).
//!
//! Registers the same two ways `nexus-connector-excel` does — see that
//! crate's `lib.rs` doc comment for the full rationale.

mod auth;
mod config;
mod source;

pub use config::GoogleAdsConnectorConfig;
pub use source::GoogleAdsSource;

use futures::future::BoxFuture;
use nexus_core::{ConnectorCapability, NexusError, Source};

nexus_core::submit_connector!(
    "google-ads",
    ConnectorCapability::Bridged,
    GoogleAdsConnectorConfig
);

fn validate_google_ads_config(cfg: &serde_json::Value) -> Result<(), NexusError> {
    serde_json::from_value::<GoogleAdsConnectorConfig>(cfg.clone())
        .map(|_| ())
        .map_err(|e| NexusError::Serialization(e.to_string()))
}

nexus_core::submit_source_builder!(
    "google-ads",
    validate_google_ads_config,
    |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Source>, NexusError>> {
        Box::pin(async move {
            let parsed: GoogleAdsConnectorConfig = serde_json::from_value(cfg)
                .map_err(|e| NexusError::Serialization(e.to_string()))?;
            let source = GoogleAdsSource::connect(&parsed).await?;
            Ok(Box::new(source) as Box<dyn Source>)
        })
    }
);
