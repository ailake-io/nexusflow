//! Wraps `terraform-aws-modules/s3-bucket/aws` — encryption-by-default,
//! public-access-block-by-default, the security-hardened community
//! standard rather than a bare `aws_s3_bucket` resource (which defaults to
//! *none* of that and needs 4-5 extra resources hand-wired correctly to
//! match what this module already gets right).

use super::{render_module_block, ModuleImpl};
use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

const SOURCE: &str = "terraform-aws-modules/s3-bucket/aws";
const VERSION: &str = "~> 4.1";

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    /// Globally-unique bucket name.
    bucket: String,
    /// Enable versioning — recommended for a data lake's raw/curated
    /// zones so an overwritten object is always recoverable.
    #[serde(default = "default_true")]
    versioning: bool,
    /// Allow `terraform destroy` to delete this bucket even if it still
    /// has objects in it. Off by default — a data lake bucket almost
    /// never wants an accidental `destroy` to actually succeed.
    #[serde(default)]
    force_destroy: bool,
}

fn default_true() -> bool {
    true
}

submit_infra_module!(
    "aws-s3-data-lake",
    "S3 Data Lake",
    "data",
    "aws",
    Config,
    &["s3_bucket_id", "s3_bucket_arn"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    // The wrapped module's `versioning` variable is itself an object
    // (`{ enabled = bool }`), not a bare bool — translate the simpler
    // bool this crate's config exposes into that shape here, once, rather
    // than making every Infra canvas user learn the nested form.
    let mut args = args.clone();
    if let Some(v) = args.remove("versioning") {
        args.insert(
            "versioning".to_string(),
            format!("{{\n    enabled = {v}\n  }}"),
        );
    }
    render_module_block(id, SOURCE, VERSION, &args)
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "s3_bucket_id" | "s3_bucket_arn" => Some(format!("module.{id}.{output}")),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
