//! X/Twitter Ads connector — `Bridged` (REST + JSON, no ADBC/ODBC driver
//! exists for this API). Source only — the Ads Analytics API this
//! connector uses is read-only.
//!
//! Only connector in this repo using OAuth 1.0a (`src/oauth1.rs`) —
//! every other one is OAuth2 or a static API key/token. Registers the
//! same way `nexus-connector-excel` does — see that crate's `lib.rs` doc
//! comment for the full rationale.

mod bulk;
mod config;
mod oauth1;
mod source;

pub use config::{XAdsConnectorConfig, XAdsEntity, XAdsGranularity};
pub use source::XAdsSource;

use futures::future::BoxFuture;
use nexus_core::{ConnectorCapability, NexusError, Source};

nexus_core::submit_connector!("x-ads", ConnectorCapability::Bridged, XAdsConnectorConfig);

fn validate_x_ads_config(cfg: &serde_json::Value) -> Result<(), NexusError> {
    serde_json::from_value::<XAdsConnectorConfig>(cfg.clone())
        .map(|_| ())
        .map_err(|e| NexusError::Serialization(e.to_string()))
}

nexus_core::submit_source_builder!(
    "x-ads",
    validate_x_ads_config,
    |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Source>, NexusError>> {
        Box::pin(async move {
            let parsed: XAdsConnectorConfig = serde_json::from_value(cfg)
                .map_err(|e| NexusError::Serialization(e.to_string()))?;
            let source = XAdsSource::connect(&parsed).await?;
            Ok(Box::new(source) as Box<dyn Source>)
        })
    }
);
