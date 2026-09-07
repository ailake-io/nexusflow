//! Registers the two LLMOps enterprise slugs
//! (LLMOPS_IMPLEMENTATION_PLAN.md Marco L8) with `ConnectorRegistry` —
//! nothing here is a real connector, both are license-check targets for
//! features that already live in this public binary
//! (`rag::generation_detail_handler`, `runner::run_passthrough_pipeline`'s
//! CDC+embedding combination).
//!
//! Deliberately registered from *this* crate, not a private enterprise
//! crate: `check_connector_license` only blocks when `ConnectorRegistry::find`
//! actually finds the slug — if it doesn't, the function returns `Ok(())`
//! (allowed), which is safe for a real connector (an unregistered connector
//! name is also rejected by `build_source`/`build_sink`'s own match arms,
//! a second gate that doesn't exist here). `/lineage/generation/{id}` and
//! the CDC+embedding combination are core code, always compiled — if the
//! slug only existed when a private crate was linked, the OSS binary would
//! silently *allow* both instead of denying them. Registering here means
//! every binary, OSS or enterprise, always finds the descriptor; only an
//! installed license that covers the slug (`LicenseClaims::covers`)
//! unlocks it.
use nexus_core::ConnectorCapability;

#[derive(schemars::JsonSchema)]
struct LlmLineageCapabilityConfig {}
nexus_core::submit_enterprise_connector!(
    "llm-lineage-tracking",
    ConnectorCapability::Capability,
    LlmLineageCapabilityConfig
);

#[derive(schemars::JsonSchema)]
struct ReactiveRagCapabilityConfig {}
nexus_core::submit_enterprise_connector!(
    "reactive-rag-cdc",
    ConnectorCapability::Capability,
    ReactiveRagCapabilityConfig
);

#[cfg(test)]
mod tests {
    use crate::connectors::check_connector_license;
    use crate::license::test_support::claims;
    use nexus_core::{ConnectorCapability, ConnectorRegistry};

    #[test]
    fn both_slugs_are_registered_as_capabilities_not_real_connectors() {
        for slug in ["llm-lineage-tracking", "reactive-rag-cdc"] {
            let descriptor = ConnectorRegistry::find(slug).expect("registered by this crate");
            assert_eq!(descriptor.capability, ConnectorCapability::Capability);
        }
    }

    #[test]
    fn llm_lineage_tracking_is_denied_without_a_covering_license() {
        assert!(check_connector_license("llm-lineage-tracking", None).is_err());
        let unrelated = claims(vec!["reactive-rag-cdc"]);
        assert!(check_connector_license("llm-lineage-tracking", Some(&unrelated)).is_err());
    }

    #[test]
    fn llm_lineage_tracking_is_allowed_with_a_covering_license() {
        let license = claims(vec!["llm-lineage-tracking"]);
        assert!(check_connector_license("llm-lineage-tracking", Some(&license)).is_ok());
    }

    #[test]
    fn reactive_rag_cdc_is_denied_without_a_covering_license() {
        assert!(check_connector_license("reactive-rag-cdc", None).is_err());
    }

    #[test]
    fn reactive_rag_cdc_is_allowed_with_a_covering_license() {
        let license = claims(vec!["reactive-rag-cdc"]);
        assert!(check_connector_license("reactive-rag-cdc", Some(&license)).is_ok());
    }
}
