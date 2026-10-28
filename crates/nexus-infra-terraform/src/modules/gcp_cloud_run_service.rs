//! Plain `google_cloud_run_v2_service` — GCP analog of
//! `aws-ecs-fargate-service`, much simpler since Cloud Run has no
//! separate cluster/task-definition split.

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
    /// Container image URI — wire from a `gcp-artifact-registry-repo`
    /// node and append your own `:tag`.
    image: String,
    #[serde(default = "default_container_port")]
    container_port: u32,
    /// Service account this revision runs as — wire from a
    /// `gcp-service-account` node's `email` output.
    #[serde(default)]
    service_account: Option<String>,
    #[serde(default = "default_ingress")]
    ingress: String,
}

fn default_location() -> String {
    "us-central1".to_string()
}
fn default_container_port() -> u32 {
    8080
}
fn default_ingress() -> String {
    "INGRESS_TRAFFIC_ALL".to_string()
}

submit_infra_module!(
    "gcp-cloud-run-service",
    "Cloud Run Service",
    "cicd",
    "gcp",
    Config,
    &["uri", "name"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    let get = |k: &str, default: &str| args.get(k).cloned().unwrap_or_else(|| default.to_string());
    let name = get("name", &format!("\"{id}\""));
    let project = get("project", "\"\"");
    let location = get("location", "\"us-central1\"");
    let image = get("image", "\"\"");
    let container_port = get("container_port", "8080");
    let ingress = get("ingress", "\"INGRESS_TRAFFIC_ALL\"");

    let service_account_line = args
        .get("service_account")
        .map(|sa| format!("\n  template {{\n    service_account = {sa}\n\n    containers {{\n      image = {image}\n\n      ports {{\n        container_port = {container_port}\n      }}\n    }}\n  }}"))
        .unwrap_or_else(|| format!("\n  template {{\n    containers {{\n      image = {image}\n\n      ports {{\n        container_port = {container_port}\n      }}\n    }}\n  }}"));

    format!(
        "resource \"google_cloud_run_v2_service\" \"{id}\" {{\n  name     = {name}\n  project  = {project}\n  location = {location}\n  ingress  = {ingress}\n{service_account_line}\n}}\n"
    )
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "uri" => Some(format!("google_cloud_run_v2_service.{id}.uri")),
        "name" => Some(format!("google_cloud_run_v2_service.{id}.name")),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
