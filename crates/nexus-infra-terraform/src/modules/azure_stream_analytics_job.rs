//! Plain `azurerm_stream_analytics_job` — Azure analog of
//! `gcp-dataflow-job`/Kinesis Data Analytics' streaming-SQL role. Inputs
//! and outputs (e.g. an `azure-event-hub` source, an
//! `azure-storage-account` sink) are separate `azurerm_stream_analytics_*`
//! resources wired by hand — not modeled as catalog modules yet.

use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

use super::{render_resource_block, ModuleImpl};

const RESOURCE_TYPE: &str = "azurerm_stream_analytics_job";

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    name: String,
    resource_group_name: String,
    location: String,
    #[serde(default = "default_streaming_units")]
    streaming_units: u32,
    #[serde(default = "default_compatibility_level")]
    compatibility_level: String,
    /// Stream Analytics query language (SAQL) transformation.
    transformation_query: String,
}

fn default_streaming_units() -> u32 {
    1
}
fn default_compatibility_level() -> String {
    "1.2".to_string()
}

submit_infra_module!(
    "azure-stream-analytics-job",
    "Stream Analytics Job",
    "data",
    "azure",
    Config,
    &["id", "job_id"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    render_resource_block(RESOURCE_TYPE, id, args)
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "id" | "job_id" => Some(format!("{RESOURCE_TYPE}.{id}.{output}")),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
