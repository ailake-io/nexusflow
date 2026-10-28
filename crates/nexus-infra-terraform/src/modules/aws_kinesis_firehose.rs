//! Plain `aws_kinesis_firehose_delivery_stream`, S3 destination
//! (`extended_s3_configuration`) with an optional Kinesis Data Stream
//! source (`kinesis_source_configuration`) — the common
//! `aws-kinesis-stream` → Firehose → S3 landing pattern. Omit
//! `source_stream_arn` for a direct-put stream instead.

use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

use super::ModuleImpl;

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    name: String,
    /// ARN of the destination S3 bucket — wire from an
    /// `aws-s3-data-lake` node's `bucket_arn` output.
    bucket_arn: String,
    /// ARN of the role Firehose assumes to read the source stream (if
    /// any) and write to S3 — wire from an `aws-iam-role` node's
    /// `iam_role_arn` output.
    role_arn: String,
    #[serde(default = "default_prefix")]
    prefix: String,
    #[serde(default = "default_buffer_size")]
    buffer_size: u32,
    #[serde(default = "default_buffer_interval")]
    buffer_interval: u32,
    /// ARN of an upstream Kinesis Data Stream — wire from an
    /// `aws-kinesis-stream` node's `arn` output. Omit for direct-put.
    #[serde(default)]
    source_stream_arn: Option<String>,
}

fn default_prefix() -> String {
    "data/".to_string()
}
fn default_buffer_size() -> u32 {
    64
}
fn default_buffer_interval() -> u32 {
    300
}

submit_infra_module!(
    "aws-kinesis-firehose",
    "Kinesis Data Firehose (to S3)",
    "data",
    "aws",
    Config,
    &["arn"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    let get = |k: &str, default: &str| args.get(k).cloned().unwrap_or_else(|| default.to_string());
    let name = get("name", &format!("\"{id}\""));
    let bucket_arn = get("bucket_arn", "\"\"");
    let role_arn = get("role_arn", "\"\"");
    let prefix = get("prefix", "\"data/\"");
    let buffer_size = get("buffer_size", "64");
    let buffer_interval = get("buffer_interval", "300");

    let destination = if args.contains_key("source_stream_arn") {
        "\"kinesis-firehose\"".to_string()
    } else {
        "\"direct-put\"".to_string()
    };

    let source_block = args
        .get("source_stream_arn")
        .map(|arn| {
            format!(
                "\n\n  kinesis_source_configuration {{\n    kinesis_stream_arn = {arn}\n    role_arn           = {role_arn}\n  }}"
            )
        })
        .unwrap_or_default();

    format!(
        "resource \"aws_kinesis_firehose_delivery_stream\" \"{id}\" {{\n  name        = {name}\n  destination = {destination}{source_block}\n\n  extended_s3_configuration {{\n    role_arn           = {role_arn}\n    bucket_arn         = {bucket_arn}\n    prefix             = {prefix}\n    buffering_size     = {buffer_size}\n    buffering_interval = {buffer_interval}\n  }}\n}}\n"
    )
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "arn" => Some(format!("aws_kinesis_firehose_delivery_stream.{id}.arn")),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
