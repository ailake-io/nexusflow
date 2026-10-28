//! Stripe connector — thirteenth crate, fourth of the tier-5
//! marketing/ads/payments group. `Bridged` (REST + JSON, no ADBC/ODBC
//! driver exists for this API). **Source only, permanently** — this
//! connector never gains a sink; executing a financial charge/refund/
//! transfer is out of scope by standing instruction, not a v1
//! simplification.
//!
//! Registers the same two ways `nexus-connector-excel` does — see that
//! crate's `lib.rs` doc comment for the full rationale.

mod config;
mod source;

pub use config::{StripeConnectorConfig, StripeResource};
pub use source::StripeSource;

use futures::future::BoxFuture;
use nexus_core::{ConnectorCapability, NexusError, Source};

nexus_core::submit_connector!(
    "stripe",
    ConnectorCapability::Bridged,
    StripeConnectorConfig
);

fn parse_and_validate(cfg: serde_json::Value) -> Result<StripeConnectorConfig, NexusError> {
    let parsed: StripeConnectorConfig =
        serde_json::from_value(cfg).map_err(|e| NexusError::Serialization(e.to_string()))?;
    parsed.validate()?;
    Ok(parsed)
}

fn validate_stripe_config(cfg: &serde_json::Value) -> Result<(), NexusError> {
    parse_and_validate(cfg.clone()).map(|_| ())
}

nexus_core::submit_source_builder!(
    "stripe",
    validate_stripe_config,
    |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Source>, NexusError>> {
        Box::pin(async move {
            let parsed = parse_and_validate(cfg)?;
            let source = StripeSource::connect(&parsed).await?;
            Ok(Box::new(source) as Box<dyn Source>)
        })
    }
);
