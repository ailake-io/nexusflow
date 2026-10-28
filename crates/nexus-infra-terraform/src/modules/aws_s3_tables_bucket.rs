//! 2-resource custom emit: `aws_s3tables_table_bucket` +
//! `aws_s3tables_namespace` — Amazon S3 Tables (managed Apache Iceberg
//! tables backed by S3), a distinct resource family from a regular
//! `aws-s3-data-lake` bucket. A bucket with no namespace can't hold
//! tables, so a default namespace is always created alongside it.

use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

use super::ModuleImpl;

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    /// Table bucket name.
    name: String,
    #[serde(default = "default_namespace")]
    namespace: String,
}

fn default_namespace() -> String {
    "default".to_string()
}

submit_infra_module!(
    "aws-s3-tables-bucket",
    "S3 Tables Bucket (Iceberg)",
    "data",
    "aws",
    Config,
    &["table_bucket_arn"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    let get = |k: &str, default: &str| args.get(k).cloned().unwrap_or_else(|| default.to_string());
    let name = get("name", &format!("\"{id}\""));
    let namespace = get("namespace", "\"default\"");

    let bucket_id = format!("{id}_bucket");

    format!(
        "resource \"aws_s3tables_table_bucket\" \"{bucket_id}\" {{\n  name = {name}\n}}\n\nresource \"aws_s3tables_namespace\" \"{id}\" {{\n  table_bucket_arn = aws_s3tables_table_bucket.{bucket_id}.arn\n  namespace        = {namespace}\n}}\n"
    )
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "table_bucket_arn" => Some(format!("aws_s3tables_table_bucket.{id}_bucket.arn")),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
