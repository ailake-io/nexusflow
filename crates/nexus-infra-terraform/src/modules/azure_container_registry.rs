//! Plain `azurerm_container_registry` — Azure analog of `aws-ecr-repo`/
//! `gcp-artifact-registry-repo`.

use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

use super::{render_resource_block, ModuleImpl};

const RESOURCE_TYPE: &str = "azurerm_container_registry";

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    name: String,
    resource_group_name: String,
    location: String,
    #[serde(default = "default_sku")]
    sku: String,
    #[serde(default)]
    admin_enabled: bool,
}

fn default_sku() -> String {
    "Standard".to_string()
}

submit_infra_module!(
    "azure-container-registry",
    "Container Registry",
    "cicd",
    "azure",
    Config,
    &["id", "login_server"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    render_resource_block(RESOURCE_TYPE, id, args)
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "id" | "login_server" => Some(format!("{RESOURCE_TYPE}.{id}.{output}")),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
