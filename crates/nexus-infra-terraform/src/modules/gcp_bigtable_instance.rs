//! Plain `google_bigtable_instance` — GCP analog of
//! `aws-dynamodb-table`, one cluster by default (development-tier
//! instances only support one; add more via config for production).

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
    cluster_id: String,
    #[serde(default = "default_zone")]
    zone: String,
    #[serde(default = "default_storage_type")]
    storage_type: String,
    #[serde(default = "default_num_nodes")]
    num_nodes: u32,
}

fn default_zone() -> String {
    "us-central1-b".to_string()
}
fn default_storage_type() -> String {
    "SSD".to_string()
}
fn default_num_nodes() -> u32 {
    1
}

submit_infra_module!(
    "gcp-bigtable-instance",
    "Bigtable Instance",
    "data",
    "gcp",
    Config,
    &["instance_name", "id"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    let get = |k: &str, default: &str| args.get(k).cloned().unwrap_or_else(|| default.to_string());
    let name = get("name", &format!("\"{id}\""));
    let project = get("project", "\"\"");
    let cluster_id = get("cluster_id", &format!("\"{id}-cluster\""));
    let zone = get("zone", "\"us-central1-b\"");
    let storage_type = get("storage_type", "\"SSD\"");
    let num_nodes = get("num_nodes", "1");

    format!(
        "resource \"google_bigtable_instance\" \"{id}\" {{\n  name    = {name}\n  project = {project}\n\n  cluster {{\n    cluster_id   = {cluster_id}\n    zone         = {zone}\n    num_nodes    = {num_nodes}\n    storage_type = {storage_type}\n  }}\n}}\n"
    )
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "instance_name" => Some(format!("google_bigtable_instance.{id}.name")),
        "id" => Some(format!("google_bigtable_instance.{id}.id")),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
