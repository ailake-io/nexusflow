//! Plain `aws_athena_workgroup` resource — no community module worth
//! pinning for this one either. `output_location` is flattened into a top-
//! level config field (the real resource nests it two levels deep,
//! `configuration.result_configuration.output_location`) so a user wiring
//! this to an S3 bucket's output doesn't need to know that nesting.

use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

use super::ModuleImpl;

const RESOURCE_TYPE: &str = "aws_athena_workgroup";

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    /// Workgroup name.
    name: String,
    /// S3 path query results are written to, e.g.
    /// "s3://my-bucket/athena-results/" — typically wired from an
    /// `aws-s3-data-lake` node's `s3_bucket_id` output.
    output_location: String,
}

submit_infra_module!(
    "aws-athena-workgroup",
    "Athena Workgroup",
    "data",
    "aws",
    Config,
    &["id"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    let name = args
        .get("name")
        .cloned()
        .unwrap_or_else(|| format!("\"{id}\""));
    let output_location = args
        .get("output_location")
        .cloned()
        .unwrap_or_else(|| "\"\"".to_string());
    format!(
        "resource \"{RESOURCE_TYPE}\" \"{id}\" {{\n  name = {name}\n\n  configuration {{\n    result_configuration {{\n      output_location = {output_location}\n    }}\n  }}\n}}\n"
    )
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "id" | "arn" => Some(format!("{RESOURCE_TYPE}.{id}.{output}")),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
