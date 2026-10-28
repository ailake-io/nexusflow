//! Wraps `terraform-aws-modules/dynamodb-table/aws`.

use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

use super::{render_module_block, ModuleImpl};

const SOURCE: &str = "terraform-aws-modules/dynamodb-table/aws";
const VERSION: &str = "~> 4.1";

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Attribute {
    name: String,
    /// "S" (string), "N" (number), or "B" (binary).
    #[serde(rename = "type")]
    type_: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    name: String,
    hash_key: String,
    #[serde(default)]
    range_key: Option<String>,
    /// Every attribute referenced by `hash_key`, `range_key`, or a GSI/LSI
    /// key — DynamoDB only wants key attributes declared here, not every
    /// item field.
    attributes: Vec<Attribute>,
    #[serde(default = "default_billing_mode")]
    billing_mode: String,
}

fn default_billing_mode() -> String {
    "PAY_PER_REQUEST".to_string()
}

submit_infra_module!(
    "aws-dynamodb-table",
    "DynamoDB Table",
    "data",
    "aws",
    Config,
    &["dynamodb_table_arn", "dynamodb_table_id"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    render_module_block(id, SOURCE, VERSION, args)
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "dynamodb_table_arn" | "dynamodb_table_id" => Some(format!("module.{id}.{output}")),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
