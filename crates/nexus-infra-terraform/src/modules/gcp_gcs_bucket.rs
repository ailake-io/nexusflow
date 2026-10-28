//! Plain `google_storage_bucket` — GCP analog of `aws-s3-data-lake`.

use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

use super::{render_resource_block, ModuleImpl};

const RESOURCE_TYPE: &str = "google_storage_bucket";

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    name: String,
    project: String,
    #[serde(default = "default_location")]
    location: String,
    #[serde(default = "default_storage_class")]
    storage_class: String,
    #[serde(default = "default_uniform_access")]
    uniform_bucket_level_access: bool,
    #[serde(default)]
    force_destroy: bool,
}

fn default_location() -> String {
    "US".to_string()
}
fn default_storage_class() -> String {
    "STANDARD".to_string()
}
fn default_uniform_access() -> bool {
    true
}

submit_infra_module!(
    "gcp-gcs-bucket",
    "Cloud Storage Bucket",
    "data",
    "gcp",
    Config,
    &["bucket_name", "bucket_url", "self_link"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    render_resource_block(RESOURCE_TYPE, id, args)
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "bucket_name" => Some(format!("{RESOURCE_TYPE}.{id}.name")),
        "bucket_url" => Some(format!("{RESOURCE_TYPE}.{id}.url")),
        "self_link" => Some(format!("{RESOURCE_TYPE}.{id}.self_link")),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
