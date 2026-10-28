//! Plain `google_cloud_scheduler_job`, HTTP target — GCP analog of
//! `aws-eventbridge-rule`'s scheduled-trigger role, commonly pointed at
//! a `gcp-cloud-run-service`/`gcp-cloud-functions` URL.

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
    #[serde(default = "default_region")]
    region: String,
    /// Cron expression, e.g. "0 6 * * *".
    schedule: String,
    #[serde(default = "default_time_zone")]
    time_zone: String,
    /// Endpoint invoked when the job fires — wire from a
    /// `gcp-cloud-run-service` node's `uri` output or a
    /// `gcp-cloud-functions` node's `url` output.
    uri: String,
    #[serde(default = "default_http_method")]
    http_method: String,
}

fn default_region() -> String {
    "us-central1".to_string()
}
fn default_time_zone() -> String {
    "Etc/UTC".to_string()
}
fn default_http_method() -> String {
    "POST".to_string()
}

submit_infra_module!(
    "gcp-cloud-scheduler-job",
    "Cloud Scheduler Job",
    "cicd",
    "gcp",
    Config,
    &["id"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    let get = |k: &str, default: &str| args.get(k).cloned().unwrap_or_else(|| default.to_string());
    let name = get("name", &format!("\"{id}\""));
    let project = get("project", "\"\"");
    let region = get("region", "\"us-central1\"");
    let schedule = get("schedule", "\"\"");
    let time_zone = get("time_zone", "\"Etc/UTC\"");
    let uri = get("uri", "\"\"");
    let http_method = get("http_method", "\"POST\"");

    format!(
        "resource \"google_cloud_scheduler_job\" \"{id}\" {{\n  name      = {name}\n  project   = {project}\n  region    = {region}\n  schedule  = {schedule}\n  time_zone = {time_zone}\n\n  http_target {{\n    uri         = {uri}\n    http_method = {http_method}\n  }}\n}}\n"
    )
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "id" => Some(format!("google_cloud_scheduler_job.{id}.id")),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
