//! Plain `google_dataflow_job`, classic (non-Flex) template-based —
//! GCP's managed Apache Beam runner, a real data-engineering gap this
//! catalog had (analog of `aws-glue-job`/`aws-emr-cluster`'s streaming
//! ETL role). Flex templates would need the separate
//! `google_dataflow_flex_template_job` resource — not modeled here.

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
    /// gs:// path to the Dataflow template.
    template_gcs_path: String,
    /// gs:// path Dataflow uses for staging temp files.
    temp_gcs_location: String,
    /// Template-specific runtime parameters.
    #[serde(default)]
    parameters: BTreeMap<String, String>,
    #[serde(default = "default_max_workers")]
    max_workers: u32,
}

fn default_region() -> String {
    "us-central1".to_string()
}
fn default_max_workers() -> u32 {
    1
}

submit_infra_module!(
    "gcp-dataflow-job",
    "Dataflow Job",
    "data",
    "gcp",
    Config,
    &["id", "state"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    let get = |k: &str, default: &str| args.get(k).cloned().unwrap_or_else(|| default.to_string());
    let name = get("name", &format!("\"{id}\""));
    let project = get("project", "\"\"");
    let region = get("region", "\"us-central1\"");
    let template_gcs_path = get("template_gcs_path", "\"\"");
    let temp_gcs_location = get("temp_gcs_location", "\"\"");
    let parameters = get("parameters", "{}");
    let max_workers = get("max_workers", "1");

    format!(
        "resource \"google_dataflow_job\" \"{id}\" {{\n  name               = {name}\n  project            = {project}\n  region             = {region}\n  template_gcs_path  = {template_gcs_path}\n  temp_gcs_location  = {temp_gcs_location}\n  max_workers        = {max_workers}\n  parameters         = {parameters}\n\n  on_delete = \"cancel\"\n}}\n"
    )
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "id" => Some(format!("google_dataflow_job.{id}.id")),
        "state" => Some(format!("google_dataflow_job.{id}.state")),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
