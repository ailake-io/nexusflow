//! Force-links every connector crate that has no match arm in
//! `connectors.rs` (it registers itself purely through `inventory` —
//! `submit_connector!` + `submit_source_builder!`/`submit_sink_builder!`,
//! see `nexus-core/src/registry.rs`), plus `nexus-infra-terraform`.
//!
//! Load-bearing, not decorative: nothing else in this crate references a
//! symbol from these crates, and a dependency with zero symbols used is
//! dropped from the link entirely — silently dropping its `inventory`
//! registrations too, so the connector would compile fine yet never show
//! up in `GET /connectors` nor build at run time. Each line is
//! `#[cfg(feature = ...)]`-gated to match its optional dependency in
//! Cargo.toml (an `extern crate` for an absent optional dependency is a
//! compile error, not a no-op). A connector added here also needs its own
//! feature in Cargo.toml and a slot in the `connectors-all` bundles.

#[cfg(feature = "azure-ai-search")]
extern crate nexus_connector_azure_ai_search;
#[cfg(feature = "bigquery")]
extern crate nexus_connector_bigquery;
#[cfg(feature = "databricks")]
extern crate nexus_connector_databricks;
#[cfg(feature = "dropbox")]
extern crate nexus_connector_dropbox;
#[cfg(feature = "dynamics365")]
extern crate nexus_connector_dynamics365;
#[cfg(feature = "elasticsearch")]
extern crate nexus_connector_elasticsearch;
#[cfg(feature = "excel")]
extern crate nexus_connector_excel;
#[cfg(feature = "ga4")]
extern crate nexus_connector_ga4;
#[cfg(feature = "google-ads")]
extern crate nexus_connector_google_ads;
#[cfg(feature = "google-drive")]
extern crate nexus_connector_google_drive;
#[cfg(feature = "google-sheets")]
extern crate nexus_connector_google_sheets;
#[cfg(feature = "hana")]
extern crate nexus_connector_hana;
#[cfg(feature = "hubspot")]
extern crate nexus_connector_hubspot;
#[cfg(feature = "kinesis")]
extern crate nexus_connector_kinesis;
#[cfg(feature = "linkedin-ads")]
extern crate nexus_connector_linkedin_ads;
#[cfg(feature = "meta-ads")]
extern crate nexus_connector_meta_ads;
#[cfg(feature = "mssql")]
extern crate nexus_connector_mssql;
#[cfg(feature = "netsuite")]
extern crate nexus_connector_netsuite;
#[cfg(feature = "oracle")]
extern crate nexus_connector_oracle;
#[cfg(feature = "pdf-ocr")]
extern crate nexus_connector_pdf_ocr;
#[cfg(feature = "pulsar")]
extern crate nexus_connector_pulsar;
#[cfg(feature = "redshift")]
extern crate nexus_connector_redshift;
#[cfg(feature = "salesforce")]
extern crate nexus_connector_salesforce;
#[cfg(feature = "servicenow")]
extern crate nexus_connector_servicenow;
#[cfg(feature = "sharepoint")]
extern crate nexus_connector_sharepoint;
#[cfg(feature = "shopify")]
extern crate nexus_connector_shopify;
#[cfg(feature = "snowflake")]
extern crate nexus_connector_snowflake;
#[cfg(feature = "starburst")]
extern crate nexus_connector_starburst;
#[cfg(feature = "stripe")]
extern crate nexus_connector_stripe;
#[cfg(feature = "teradata")]
extern crate nexus_connector_teradata;
#[cfg(feature = "tiktok-ads")]
extern crate nexus_connector_tiktok_ads;
#[cfg(feature = "vertex-vector-search")]
extern crate nexus_connector_vertex_vector_search;
#[cfg(feature = "vertica")]
extern crate nexus_connector_vertica;
#[cfg(feature = "weaviate")]
extern crate nexus_connector_weaviate;
#[cfg(feature = "workday")]
extern crate nexus_connector_workday;
#[cfg(feature = "x-ads")]
extern crate nexus_connector_x_ads;
#[cfg(feature = "youtube-analytics")]
extern crate nexus_connector_youtube_analytics;
#[cfg(feature = "zendesk")]
extern crate nexus_connector_zendesk;
#[cfg(feature = "infra-terraform")]
extern crate nexus_infra_terraform;

#[cfg(test)]
mod tests {
    #[cfg(feature = "excel")]
    #[test]
    fn plugin_connector_is_in_the_catalog() {
        assert!(nexus_core::ConnectorRegistry::find("excel").is_some());
    }

    #[cfg(feature = "salesforce")]
    #[test]
    fn migrated_saas_connector_is_in_the_catalog() {
        assert!(nexus_core::ConnectorRegistry::find("salesforce").is_some());
    }

    #[cfg(feature = "infra-terraform")]
    #[test]
    fn infra_generator_is_linked() {
        assert!(nexus_core::InfraGenerator::get().is_some());
    }
}
