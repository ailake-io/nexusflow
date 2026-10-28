//! Plain `aws_sqs_queue` — decouples pipeline stages (e.g. a Lambda
//! consumer polling a queue fed by an upstream job).

use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

use super::{render_resource_block, ModuleImpl};

const RESOURCE_TYPE: &str = "aws_sqs_queue";

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    name: String,
    #[serde(default = "default_visibility_timeout")]
    visibility_timeout_seconds: u32,
    #[serde(default = "default_message_retention")]
    message_retention_seconds: u32,
    /// FIFO queue names must end in ".fifo" — set this true and rename
    /// accordingly if ordering matters.
    #[serde(default)]
    fifo_queue: bool,
}

fn default_visibility_timeout() -> u32 {
    30
}
fn default_message_retention() -> u32 {
    345600
}

submit_infra_module!(
    "aws-sqs-queue",
    "SQS Queue",
    "data",
    "aws",
    Config,
    &["arn", "url"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    render_resource_block(RESOURCE_TYPE, id, args)
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "arn" => Some(format!("{RESOURCE_TYPE}.{id}.arn")),
        "url" => Some(format!("{RESOURCE_TYPE}.{id}.url")),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
