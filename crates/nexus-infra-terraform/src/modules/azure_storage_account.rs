//! Plain `azurerm_storage_account`, Data Lake Gen2 enabled by default
//! (`is_hns_enabled = true`) — Azure analog of `aws-s3-data-lake`/
//! `gcp-gcs-bucket`.

use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

use super::{render_resource_block, ModuleImpl};

const RESOURCE_TYPE: &str = "azurerm_storage_account";

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    name: String,
    resource_group_name: String,
    location: String,
    #[serde(default = "default_account_tier")]
    account_tier: String,
    #[serde(default = "default_replication_type")]
    account_replication_type: String,
    #[serde(default = "default_hns")]
    is_hns_enabled: bool,
}

fn default_account_tier() -> String {
    "Standard".to_string()
}
fn default_replication_type() -> String {
    "LRS".to_string()
}
fn default_hns() -> bool {
    true
}

submit_infra_module!(
    "azure-storage-account",
    "Storage Account (Data Lake Gen2)",
    "data",
    "azure",
    Config,
    &[
        "id",
        "name",
        "primary_blob_endpoint",
        "primary_dfs_endpoint"
    ]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    render_resource_block(RESOURCE_TYPE, id, args)
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "id" | "name" | "primary_blob_endpoint" | "primary_dfs_endpoint" => {
            Some(format!("{RESOURCE_TYPE}.{id}.{output}"))
        }
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
