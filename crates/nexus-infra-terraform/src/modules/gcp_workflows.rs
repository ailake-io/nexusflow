//! Plain `google_workflows_workflow` — GCP analog of
//! `azure-logic-app`/`aws-stepfunctions-state-machine`.

use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

use super::{render_resource_block, ModuleImpl};

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    name: String,
    project: String,
    #[serde(default = "default_region")]
    region: String,
    /// Raw workflow definition (YAML or JSON).
    source_contents: String,
    /// Service account email the workflow runs as — wire from a
    /// `gcp-service-account` node's `email` output.
    #[serde(default)]
    service_account: Option<String>,
}

fn default_region() -> String {
    "us-central1".to_string()
}

submit_infra_module!(
    "gcp-workflows",
    "Workflows",
    "cicd",
    "gcp",
    Config,
    &["id", "revision_id"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    render_resource_block("google_workflows_workflow", id, args)
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "id" | "revision_id" => Some(format!("google_workflows_workflow.{id}.{output}")),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
