//! Plain `google_vertex_ai_dataset` — GCP analog of
//! `aws-sagemaker-model-package-group`'s "registry entry" role, here for
//! training data instead of trained models.

use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

use super::{render_resource_block, ModuleImpl};

const RESOURCE_TYPE: &str = "google_vertex_ai_dataset";

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    display_name: String,
    project: String,
    #[serde(default = "default_region")]
    region: String,
    /// Schema URI identifying the dataset type, e.g. the tabular schema
    /// `"gs://google-cloud-aiplatform/schema/dataset/metadata/tabular_1.0.0.yaml"`.
    metadata_schema_uri: String,
}

fn default_region() -> String {
    "us-central1".to_string()
}

submit_infra_module!(
    "gcp-vertex-ai-dataset",
    "Vertex AI Dataset",
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
