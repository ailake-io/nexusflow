//! LinkedIn Ads (LinkedIn Marketing API) connector — fifteenth crate.
//! `Bridged` (REST + JSON, no ADBC/ODBC driver exists for this API).
//! Source only — the Ads Reporting API this connector uses is
//! read-only.
//!
//! **Real limitation, not implementation debt**: programmatic refresh
//! tokens for unattended access require Marketing Developer Platform
//! (MDP) partner approval (discretionary, weeks to months — see
//! `docs/PLANNED_CONNECTORS.md`). Without MDP status, `access_token`
//! (config) is obtained externally and expires after 60 days with no
//! automatic renewal — the operator re-authenticates manually and
//! updates the config. With MDP status, `refresh_token`/`client_id`/
//! `client_secret` enable automatic renewal.
//!
//! Registers the same two ways `nexus-connector-excel` does — see that
//! crate's `lib.rs` doc comment for the full rationale.

mod auth;
mod config;
mod source;

pub use config::LinkedinAdsConnectorConfig;
pub use source::LinkedinAdsSource;

use futures::future::BoxFuture;
use nexus_core::{ConnectorCapability, NexusError, Source};

nexus_core::submit_connector!(
    "linkedin-ads",
    ConnectorCapability::Bridged,
    LinkedinAdsConnectorConfig
);

fn validate_linkedin_ads_config(cfg: &serde_json::Value) -> Result<(), NexusError> {
    serde_json::from_value::<LinkedinAdsConnectorConfig>(cfg.clone())
        .map(|_| ())
        .map_err(|e| NexusError::Serialization(e.to_string()))
}

nexus_core::submit_source_builder!(
    "linkedin-ads",
    validate_linkedin_ads_config,
    |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Source>, NexusError>> {
        Box::pin(async move {
            let parsed: LinkedinAdsConnectorConfig = serde_json::from_value(cfg)
                .map_err(|e| NexusError::Serialization(e.to_string()))?;
            let source = LinkedinAdsSource::connect(&parsed).await?;
            Ok(Box::new(source) as Box<dyn Source>)
        })
    }
);
