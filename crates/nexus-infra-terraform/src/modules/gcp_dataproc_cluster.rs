//! Plain `google_dataproc_cluster` — GCP analog of `aws-emr-cluster`.

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
    #[serde(default = "default_master_machine_type")]
    master_machine_type: String,
    #[serde(default = "default_worker_machine_type")]
    worker_machine_type: String,
    #[serde(default = "default_num_workers")]
    num_workers: u32,
}

fn default_master_machine_type() -> String {
    "n2-standard-4".to_string()
}
fn default_worker_machine_type() -> String {
    "n2-standard-4".to_string()
}
fn default_num_workers() -> u32 {
    2
}

submit_infra_module!(
    "gcp-dataproc-cluster",
    "Dataproc Cluster",
    "data",
    "gcp",
    Config,
    &["cluster_name"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    let get = |k: &str, default: &str| args.get(k).cloned().unwrap_or_else(|| default.to_string());
    let name = get("name", &format!("\"{id}\""));
    let project = get("project", "\"\"");
    let region = get("region", "\"\"");
    let master_machine_type = get("master_machine_type", "\"n2-standard-4\"");
    let worker_machine_type = get("worker_machine_type", "\"n2-standard-4\"");
    let num_workers = get("num_workers", "2");

    format!(
        "resource \"google_dataproc_cluster\" \"{id}\" {{\n  name    = {name}\n  project = {project}\n  region  = {region}\n\n  cluster_config {{\n    master_config {{\n      machine_type = {master_machine_type}\n    }}\n\n    worker_config {{\n      machine_type  = {worker_machine_type}\n      num_instances = {num_workers}\n    }}\n  }}\n}}\n"
    )
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "cluster_name" => Some(format!("google_dataproc_cluster.{id}.name")),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
