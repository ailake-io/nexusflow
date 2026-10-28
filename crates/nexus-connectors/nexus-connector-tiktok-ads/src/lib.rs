//! TikTok Ads (TikTok for Business Marketing API) connector —
//! sixteenth crate. `Bridged` (REST + JSON, no ADBC/ODBC driver
//! exists for this API). Source only — the Reporting API this
//! connector uses is read-only.
//!
//! Registers the same two ways `nexus-connector-excel` does — see that
//! crate's `lib.rs` doc comment for the full rationale.

mod config;
mod source;

pub use config::TiktokAdsConnectorConfig;
pub use source::TiktokAdsSource;

use futures::future::BoxFuture;
use nexus_core::{ConnectorCapability, NexusError, Source};

nexus_core::submit_connector!(
    "tiktok-ads",
    ConnectorCapability::Bridged,
    TiktokAdsConnectorConfig
);

fn validate_tiktok_ads_config(cfg: &serde_json::Value) -> Result<(), NexusError> {
    serde_json::from_value::<TiktokAdsConnectorConfig>(cfg.clone())
        .map(|_| ())
        .map_err(|e| NexusError::Serialization(e.to_string()))
}

nexus_core::submit_source_builder!(
    "tiktok-ads",
    validate_tiktok_ads_config,
    |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Source>, NexusError>> {
        Box::pin(async move {
            let parsed: TiktokAdsConnectorConfig = serde_json::from_value(cfg)
                .map_err(|e| NexusError::Serialization(e.to_string()))?;
            let source = TiktokAdsSource::connect(&parsed).await?;
            Ok(Box::new(source) as Box<dyn Source>)
        })
    }
);
