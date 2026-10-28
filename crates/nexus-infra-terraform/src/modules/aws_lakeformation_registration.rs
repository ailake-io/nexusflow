//! 2-resource custom emit: registers an S3 location with Lake Formation
//! (`aws_lakeformation_resource`) then grants `DATA_LOCATION_ACCESS` on
//! it to a principal (`aws_lakeformation_permissions`) — the minimum
//! bootstrap pattern for handing a data lake bucket to Lake Formation
//! governance. Grant additional permissions (table/database-level) with
//! further `aws_lakeformation_permissions` resources outside this
//! module if needed — not modeled here.

use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

use super::ModuleImpl;

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    /// ARN of the S3 location to register — wire from an
    /// `aws-s3-data-lake` node's `bucket_arn` output.
    resource_arn: String,
    /// IAM principal (user or role ARN) granted `DATA_LOCATION_ACCESS`.
    principal_arn: String,
}

submit_infra_module!(
    "aws-lakeformation-registration",
    "Lake Formation Registration",
    "data",
    "aws",
    Config,
    &["resource_arn"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    let get = |k: &str, default: &str| args.get(k).cloned().unwrap_or_else(|| default.to_string());
    let resource_arn = get("resource_arn", "\"\"");
    let principal_arn = get("principal_arn", "\"\"");

    let resource_id = format!("{id}_resource");

    format!(
        "resource \"aws_lakeformation_resource\" \"{resource_id}\" {{\n  arn = {resource_arn}\n}}\n\nresource \"aws_lakeformation_permissions\" \"{id}\" {{\n  principal   = {principal_arn}\n  permissions = [\"DATA_LOCATION_ACCESS\"]\n\n  resource {{\n    data_location {{\n      arn = {resource_arn}\n    }}\n  }}\n\n  depends_on = [aws_lakeformation_resource.{resource_id}]\n}}\n"
    )
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "resource_arn" => Some(format!("aws_lakeformation_resource.{id}_resource.arn")),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
