//! Plain `aws_quicksight_data_source`, Athena-backed (pairs naturally
//! with `aws-athena-workgroup` in a data-lake chain). Other QuickSight
//! source types (Redshift, RDS, S3) aren't modeled here — add a second
//! module if one's needed.

use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

use super::ModuleImpl;

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    data_source_id: String,
    name: String,
    /// 12-digit account id QuickSight is subscribed under.
    aws_account_id: String,
    /// Athena workgroup to query through — wire from an
    /// `aws-athena-workgroup` node's `name` config value (no graph output
    /// for it, so pass the same literal on both).
    #[serde(default = "default_work_group")]
    work_group: String,
}

fn default_work_group() -> String {
    "primary".to_string()
}

submit_infra_module!(
    "aws-quicksight-data-source",
    "QuickSight Data Source (Athena)",
    "data",
    "aws",
    Config,
    &["arn"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    let get = |k: &str, default: &str| args.get(k).cloned().unwrap_or_else(|| default.to_string());
    let data_source_id = get("data_source_id", &format!("\"{id}\""));
    let name = get("name", &format!("\"{id}\""));
    let aws_account_id = get("aws_account_id", "\"\"");
    let work_group = get("work_group", "\"primary\"");

    format!(
        "resource \"aws_quicksight_data_source\" \"{id}\" {{\n  data_source_id = {data_source_id}\n  name           = {name}\n  aws_account_id = {aws_account_id}\n  type           = \"ATHENA\"\n\n  parameters {{\n    athena {{\n      work_group = {work_group}\n    }}\n  }}\n\n  ssl_properties {{\n    disable_ssl = false\n  }}\n}}\n"
    )
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "arn" => Some(format!("aws_quicksight_data_source.{id}.arn")),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
