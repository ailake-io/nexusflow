//! Plain `google_composer_environment` (Cloud Composer, managed
//! Airflow) — GCP analog of `aws-mwaa-environment`.

use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

use super::ModuleImpl;

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    name: String,
    project: String,
    region: String,
    #[serde(default = "default_image_version")]
    image_version: String,
    /// Service account email Composer's GKE nodes run as — wire from a
    /// `gcp-service-account` node's `email` output.
    service_account: String,
    /// VPC network self-link — wire from a `gcp-vpc-network` node's
    /// `network_id` output.
    #[serde(default)]
    network: Option<String>,
}

fn default_image_version() -> String {
    "composer-2-airflow-2".to_string()
}

submit_infra_module!(
    "gcp-composer-environment",
    "Cloud Composer (Airflow)",
    "data",
    "gcp",
    Config,
    &["gke_cluster", "dag_gcs_prefix"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    let get = |k: &str, default: &str| args.get(k).cloned().unwrap_or_else(|| default.to_string());
    let name = get("name", &format!("\"{id}\""));
    let project = get("project", "\"\"");
    let region = get("region", "\"\"");
    let image_version = get("image_version", "\"composer-2-airflow-2\"");
    let service_account = get("service_account", "\"\"");

    let network_line = args
        .get("network")
        .map(|n| format!("\n\n      network_config {{\n        network = {n}\n      }}"))
        .unwrap_or_default();

    format!(
        "resource \"google_composer_environment\" \"{id}\" {{\n  name    = {name}\n  project = {project}\n  region  = {region}\n\n  config {{\n    software_config {{\n      image_version = {image_version}\n    }}\n\n    node_config {{\n      service_account = {service_account}\n    }}{network_line}\n  }}\n}}\n"
    )
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "gke_cluster" => Some(format!(
            "google_composer_environment.{id}.config[0].gke_cluster"
        )),
        "dag_gcs_prefix" => Some(format!(
            "google_composer_environment.{id}.config[0].dag_gcs_prefix"
        )),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
