//! Meta Ads (Facebook/Instagram Marketing API) connector — twelfth
//! crate, third of the tier-5 marketing/ads/payments group. `Bridged`
//! (REST + JSON, no ADBC/ODBC driver exists for this API). Source
//! only — the Insights API this connector uses is read-only.
//!
//! Registers the same two ways `nexus-connector-excel` does — see that
//! crate's `lib.rs` doc comment for the full rationale.

mod bulk;
mod config;
mod source;

pub use config::{MetaAdsConnectorConfig, MetaAdsLevel};
pub use source::MetaAdsSource;

use futures::future::BoxFuture;
use nexus_core::{ConnectorCapability, NexusError, Source};

nexus_core::submit_connector!(
    "meta-ads",
    ConnectorCapability::Bridged,
    MetaAdsConnectorConfig
);

fn validate_meta_ads_config(cfg: &serde_json::Value) -> Result<(), NexusError> {
    serde_json::from_value::<MetaAdsConnectorConfig>(cfg.clone())
        .map(|_| ())
        .map_err(|e| NexusError::Serialization(e.to_string()))
}

nexus_core::submit_source_builder!(
    "meta-ads",
    validate_meta_ads_config,
    |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Source>, NexusError>> {
        Box::pin(async move {
            let parsed: MetaAdsConnectorConfig = serde_json::from_value(cfg)
                .map_err(|e| NexusError::Serialization(e.to_string()))?;
            let source = MetaAdsSource::connect(&parsed).await?;
            Ok(Box::new(source) as Box<dyn Source>)
        })
    }
);
