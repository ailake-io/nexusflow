//! 2-resource custom emit: `google_container_cluster` (default node pool
//! removed, per Google's own recommended pattern) +
//! `google_container_node_pool` — GCP analog of `aws-eks-cluster`.

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
    #[serde(default = "default_location")]
    location: String,
    /// Network self-link — wire from a `gcp-vpc-network` node's
    /// `network_id` output.
    network: String,
    /// Subnetwork self-link — wire from a `gcp-vpc-network` node's
    /// `subnets_ids` output (pick one element).
    subnetwork: String,
    #[serde(default = "default_node_count")]
    node_count: u32,
    #[serde(default = "default_machine_type")]
    machine_type: String,
}

fn default_location() -> String {
    "us-central1-a".to_string()
}
fn default_node_count() -> u32 {
    2
}
fn default_machine_type() -> String {
    "e2-medium".to_string()
}

submit_infra_module!(
    "gcp-gke-cluster",
    "GKE Cluster",
    "compute",
    "gcp",
    Config,
    &["endpoint", "cluster_name", "ca_certificate"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    let get = |k: &str, default: &str| args.get(k).cloned().unwrap_or_else(|| default.to_string());
    let name = get("name", &format!("\"{id}\""));
    let project = get("project", "\"\"");
    let location = get("location", "\"us-central1-a\"");
    let network = get("network", "\"\"");
    let subnetwork = get("subnetwork", "\"\"");
    let node_count = get("node_count", "2");
    let machine_type = get("machine_type", "\"e2-medium\"");

    format!(
        "resource \"google_container_cluster\" \"{id}\" {{\n  name     = {name}\n  project  = {project}\n  location = {location}\n\n  network    = {network}\n  subnetwork = {subnetwork}\n\n  remove_default_node_pool = true\n  initial_node_count       = 1\n}}\n\nresource \"google_container_node_pool\" \"{id}_nodes\" {{\n  name     = \"{id}-nodes\"\n  project  = {project}\n  location = {location}\n  cluster  = google_container_cluster.{id}.name\n\n  node_count = {node_count}\n\n  node_config {{\n    machine_type = {machine_type}\n  }}\n}}\n"
    )
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "endpoint" => Some(format!("google_container_cluster.{id}.endpoint")),
        "cluster_name" => Some(format!("google_container_cluster.{id}.name")),
        "ca_certificate" => Some(format!(
            "google_container_cluster.{id}.master_auth[0].cluster_ca_certificate"
        )),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
