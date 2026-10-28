//! Plain `google_vertex_ai_endpoint` — GCP analog of
//! `aws-sagemaker-endpoint`. Deploying a model version onto the endpoint
//! (`google_vertex_ai_endpoint_deployed_model` doesn't actually exist as
//! a dedicated resource — it's a sub-block of a model or handled via the
//! `gcloud`/API `deployModel` call) is a known v1 gap: this module only
//! creates the routable endpoint, not a deployed model behind it.

use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

use super::{render_resource_block, ModuleImpl};

const RESOURCE_TYPE: &str = "google_vertex_ai_endpoint";

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    name: String,
    display_name: String,
    project: String,
    #[serde(default = "default_location")]
    location: String,
}

fn default_location() -> String {
    "us-central1".to_string()
}

submit_infra_module!(
    "gcp-vertex-ai-endpoint",
    "Vertex AI Endpoint",
    "ai",
    "gcp",
    Config,
    &["id", "name"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    render_resource_block(RESOURCE_TYPE, id, args)
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "id" | "name" => Some(format!("{RESOURCE_TYPE}.{id}.{output}")),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
