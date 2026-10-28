//! Plain `google_workbench_instance` (Vertex AI Workbench) — GCP analog
//! of `aws-sagemaker-notebook`.

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
    #[serde(default = "default_machine_type")]
    machine_type: String,
    /// Service account email the instance runs as — wire from a
    /// `gcp-service-account` node's `email` output.
    service_account_email: String,
}

fn default_location() -> String {
    "us-central1-a".to_string()
}
fn default_machine_type() -> String {
    "e2-standard-4".to_string()
}

submit_infra_module!(
    "gcp-vertex-ai-workbench",
    "Vertex AI Workbench Instance",
    "ai",
    "gcp",
    Config,
    &["id", "state"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    let get = |k: &str, default: &str| args.get(k).cloned().unwrap_or_else(|| default.to_string());
    let name = get("name", &format!("\"{id}\""));
    let project = get("project", "\"\"");
    let location = get("location", "\"us-central1-a\"");
    let machine_type = get("machine_type", "\"e2-standard-4\"");
    let service_account_email = get("service_account_email", "\"\"");

    format!(
        "resource \"google_workbench_instance\" \"{id}\" {{\n  name     = {name}\n  project  = {project}\n  location = {location}\n\n  gce_setup {{\n    machine_type = {machine_type}\n\n    service_accounts {{\n      email = {service_account_email}\n    }}\n  }}\n}}\n"
    )
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "id" => Some(format!("google_workbench_instance.{id}.id")),
        "state" => Some(format!("google_workbench_instance.{id}.state")),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
