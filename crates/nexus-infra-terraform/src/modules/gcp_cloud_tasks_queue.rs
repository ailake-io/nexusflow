//! Plain `google_cloud_tasks_queue` — GCP analog of `aws-sqs-queue`/
//! `azure-service-bus-queue`.

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
    #[serde(default = "default_max_dispatches")]
    max_dispatches_per_second: f64,
    #[serde(default = "default_max_concurrent")]
    max_concurrent_dispatches: u32,
    #[serde(default = "default_max_attempts")]
    max_attempts: u32,
}

fn default_location() -> String {
    "us-central1".to_string()
}
fn default_max_dispatches() -> f64 {
    500.0
}
fn default_max_concurrent() -> u32 {
    1000
}
fn default_max_attempts() -> u32 {
    100
}

submit_infra_module!(
    "gcp-cloud-tasks-queue",
    "Cloud Tasks Queue",
    "data",
    "gcp",
    Config,
    &["id"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    let get = |k: &str, default: &str| args.get(k).cloned().unwrap_or_else(|| default.to_string());
    let name = get("name", &format!("\"{id}\""));
    let project = get("project", "\"\"");
    let location = get("location", "\"us-central1\"");
    let max_dispatches_per_second = get("max_dispatches_per_second", "500");
    let max_concurrent_dispatches = get("max_concurrent_dispatches", "1000");
    let max_attempts = get("max_attempts", "100");

    format!(
        "resource \"google_cloud_tasks_queue\" \"{id}\" {{\n  name     = {name}\n  project  = {project}\n  location = {location}\n\n  rate_limits {{\n    max_dispatches_per_second = {max_dispatches_per_second}\n    max_concurrent_dispatches = {max_concurrent_dispatches}\n  }}\n\n  retry_config {{\n    max_attempts = {max_attempts}\n  }}\n}}\n"
    )
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "id" => Some(format!("google_cloud_tasks_queue.{id}.id")),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
