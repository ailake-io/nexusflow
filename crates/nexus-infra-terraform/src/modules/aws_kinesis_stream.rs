//! Plain `aws_kinesis_stream` — a single well-defined resource, no
//! community module worth pinning to.

use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

use super::{render_resource_block, ModuleImpl};

const RESOURCE_TYPE: &str = "aws_kinesis_stream";

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    name: String,
    #[serde(default = "default_shard_count")]
    shard_count: u32,
    /// Hours of data retained in the stream.
    #[serde(default = "default_retention_period")]
    retention_period: u32,
    #[serde(default = "default_stream_mode")]
    stream_mode: String,
}

fn default_shard_count() -> u32 {
    1
}
fn default_retention_period() -> u32 {
    24
}
fn default_stream_mode() -> String {
    "PROVISIONED".to_string()
}

submit_infra_module!(
    "aws-kinesis-stream",
    "Kinesis Data Stream",
    "data",
    "aws",
    Config,
    &["arn", "name"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    let get = |k: &str, default: &str| args.get(k).cloned().unwrap_or_else(|| default.to_string());
    let stream_mode = get("stream_mode", "\"PROVISIONED\"");

    let mut args = args.clone();
    args.remove("stream_mode");
    let mut block = render_resource_block(RESOURCE_TYPE, id, &args);
    block.truncate(block.len() - "}\n".len());
    block.push_str(&format!(
        "  stream_mode_details {{\n    stream_mode = {stream_mode}\n  }}\n}}\n"
    ));
    block
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "arn" | "name" => Some(format!("{RESOURCE_TYPE}.{id}.{output}")),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
