//! Plain `azurerm_search_service` — commonly paired with Azure ML/OpenAI
//! for RAG (vector + hybrid search), the AI-adjacent search layer this
//! catalog doesn't have an AWS/GCP equivalent module for yet.

use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

use super::{render_resource_block, ModuleImpl};

const RESOURCE_TYPE: &str = "azurerm_search_service";

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    name: String,
    resource_group_name: String,
    location: String,
    #[serde(default = "default_sku")]
    sku: String,
    #[serde(default = "default_replica_count")]
    replica_count: u32,
    #[serde(default = "default_partition_count")]
    partition_count: u32,
}

fn default_sku() -> String {
    "standard".to_string()
}
fn default_replica_count() -> u32 {
    1
}
fn default_partition_count() -> u32 {
    1
}

submit_infra_module!(
    "azure-cognitive-search",
    "Cognitive Search",
    "ai",
    "azure",
    Config,
    &["id"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    render_resource_block(RESOURCE_TYPE, id, args)
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "id" => Some(format!("{RESOURCE_TYPE}.{id}.id")),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
