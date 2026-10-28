//! No single community module is the obvious standard for "just a Glue
//! Catalog database" — emits the plain AWS provider resource directly
//! instead of pinning to someone's wrapper. Field names match
//! `aws_glue_catalog_database`'s real arguments exactly.

use super::{render_resource_block, ModuleImpl};
use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

const RESOURCE_TYPE: &str = "aws_glue_catalog_database";

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    /// Database name (lowercase, matches your S3 data lake's naming).
    name: String,
    /// Optional human-readable description.
    #[serde(default)]
    description: Option<String>,
    /// S3 location this catalog points at, e.g. "s3://my-bucket/raw/" —
    /// wire this to `aws-s3-data-lake`'s `s3_bucket_id` output plus your
    /// own prefix.
    #[serde(default)]
    location_uri: Option<String>,
}

submit_infra_module!(
    "aws-glue-catalog",
    "Glue Catalog Database",
    "data",
    "aws",
    Config,
    &["name", "arn"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    render_resource_block(RESOURCE_TYPE, id, args)
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "name" | "arn" | "id" => Some(format!("{RESOURCE_TYPE}.{id}.{output}")),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
