//! Registers the LLMOps enterprise slugs
//! (LLMOPS_IMPLEMENTATION_PLAN.md Marco L8, plus the git-versioning
//! follow-up) with `ConnectorRegistry` — nothing here is a real connector,
//! each is a license-check target for a feature that already lives in
//! this public binary (`rag::generation_detail_handler`,
//! `runner::run_passthrough_pipeline`'s CDC+embedding combination,
//! `lib.rs`'s optional git-history-to-GitHub push mirror).
//!
//! Deliberately registered from *this* crate, not a private enterprise
//! crate: `check_connector_license` only blocks when `ConnectorRegistry::find`
//! actually finds the slug — if it doesn't, the function returns `Ok(())`
//! (allowed), which is safe for a real connector (an unregistered connector
//! name is also rejected by `build_source`/`build_sink`'s own match arms,
//! a second gate that doesn't exist here). `/lineage/generation/{id}`, the
//! CDC+embedding combination, and the GitHub push mirror are all core
//! code, always compiled (the mirror itself is additionally feature-gated
//! behind `version-history`, but the slug is registered unconditionally,
//! same as the other two) — if a slug only existed when a private crate
//! was linked, the OSS binary would silently *allow* the feature instead
//! of denying it. Registering here means every binary, OSS or enterprise,
//! always finds the descriptor; only an installed license that covers the
//! slug (`LicenseClaims::covers`) unlocks it.
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

#[derive(schemars::JsonSchema)]
struct GitHistoryGithubSyncCapabilityConfig {}
nexus_core::submit_enterprise_connector!(
    "git-history-github-sync",
    ConnectorCapability::Capability,
    GitHistoryGithubSyncCapabilityConfig
);

#[cfg(test)]
mod tests {
    use crate::connectors::check_connector_license;
    use crate::license::test_support::claims;
    use nexus_core::{ConnectorCapability, ConnectorRegistry};

    #[test]
    fn all_slugs_are_registered_as_capabilities_not_real_connectors() {
        for slug in [
            "llm-lineage-tracking",
            "reactive-rag-cdc",
            "git-history-github-sync",
        ] {
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

    #[test]
    fn git_history_github_sync_is_denied_without_a_covering_license() {
        assert!(check_connector_license("git-history-github-sync", None).is_err());
        let unrelated = claims(vec!["reactive-rag-cdc"]);
        assert!(check_connector_license("git-history-github-sync", Some(&unrelated)).is_err());
    }

    #[test]
    fn git_history_github_sync_is_allowed_with_a_covering_license() {
        let license = claims(vec!["git-history-github-sync"]);
        assert!(check_connector_license("git-history-github-sync", Some(&license)).is_ok());
    }
}
