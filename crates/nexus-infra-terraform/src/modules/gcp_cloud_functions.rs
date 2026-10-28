//! Plain `google_cloudfunctions2_function` (2nd gen) — GCP analog of
//! `aws-lambda-function`, source-archive packaging (an
//! already-uploaded zip in a GCS bucket, not a container image).

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
    #[serde(default = "default_runtime")]
    runtime: String,
    entry_point: String,
    /// Name (not ARN — GCS buckets have none) of the bucket holding the
    /// source zip — wire from a `gcp-gcs-bucket` node's `bucket_name`
    /// output.
    source_bucket: String,
    /// Object path of the source zip inside that bucket.
    source_object: String,
    #[serde(default)]
    service_account_email: Option<String>,
    #[serde(default = "default_max_instance_count")]
    max_instance_count: u32,
    #[serde(default = "default_available_memory")]
    available_memory: String,
}

fn default_location() -> String {
    "us-central1".to_string()
}
fn default_runtime() -> String {
    "python312".to_string()
}
fn default_max_instance_count() -> u32 {
    1
}
fn default_available_memory() -> String {
    "256M".to_string()
}

submit_infra_module!(
    "gcp-cloud-functions",
    "Cloud Functions (2nd gen)",
    "ai",
    "gcp",
    Config,
    &["id", "url"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    let get = |k: &str, default: &str| args.get(k).cloned().unwrap_or_else(|| default.to_string());
    let name = get("name", &format!("\"{id}\""));
    let project = get("project", "\"\"");
    let location = get("location", "\"us-central1\"");
    let runtime = get("runtime", "\"python312\"");
    let entry_point = get("entry_point", "\"\"");
    let source_bucket = get("source_bucket", "\"\"");
    let source_object = get("source_object", "\"\"");
    let max_instance_count = get("max_instance_count", "1");
    let available_memory = get("available_memory", "\"256M\"");

    let service_account_line = args
        .get("service_account_email")
        .map(|sa| format!("\n    service_account_email = {sa}"))
        .unwrap_or_default();

    format!(
        "resource \"google_cloudfunctions2_function\" \"{id}\" {{\n  name     = {name}\n  project  = {project}\n  location = {location}\n\n  build_config {{\n    runtime     = {runtime}\n    entry_point = {entry_point}\n\n    source {{\n      storage_source {{\n        bucket = {source_bucket}\n        object = {source_object}\n      }}\n    }}\n  }}\n\n  service_config {{\n    max_instance_count = {max_instance_count}\n    available_memory   = {available_memory}{service_account_line}\n  }}\n}}\n"
    )
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "id" => Some(format!("google_cloudfunctions2_function.{id}.id")),
        "url" => Some(format!("google_cloudfunctions2_function.{id}.url")),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
