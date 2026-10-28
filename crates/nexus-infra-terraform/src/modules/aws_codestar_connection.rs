//! Plain `aws_codestarconnections_connection` — the source stage of a
//! `aws-codepipeline` chain. **Real limitation, documented rather than
//! hidden**: Terraform creates the connection resource in `PENDING`
//! status only — completing the OAuth handshake to the actual GitHub/
//! Bitbucket account is a manual one-time step in the AWS Console
//! (Developer Tools → Settings → Connections) after `apply`. No AWS API
//! exists to finish this from Terraform or the CLI.

use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

use super::{render_resource_block, ModuleImpl};

const RESOURCE_TYPE: &str = "aws_codestarconnections_connection";

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    /// Connection name, shown in the AWS Console pending-approval list.
    name: String,
    /// Source provider — "GitHub", "Bitbucket", or "GitHubEnterpriseServer".
    #[serde(default = "default_provider_type")]
    provider_type: String,
}

fn default_provider_type() -> String {
    "GitHub".to_string()
}

submit_infra_module!(
    "aws-codestar-connection",
    "CodeStar Connection",
    "cicd",
    "aws",
    Config,
    &["arn"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    render_resource_block(RESOURCE_TYPE, id, args)
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "arn" => Some(format!("{RESOURCE_TYPE}.{id}.arn")),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
