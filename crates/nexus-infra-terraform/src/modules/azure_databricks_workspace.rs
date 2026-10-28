//! Plain `azurerm_databricks_workspace` — Azure analog of
//! `aws-emr-cluster`/`gcp-dataproc-cluster`.

use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

use super::{render_resource_block, ModuleImpl};

const RESOURCE_TYPE: &str = "azurerm_databricks_workspace";

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    name: String,
    resource_group_name: String,
    location: String,
    #[serde(default = "default_sku")]
    sku: String,
}

fn default_sku() -> String {
    "standard".to_string()
}

submit_infra_module!(
    "azure-databricks-workspace",
    "Databricks Workspace",
    "data",
    "azure",
    Config,
    &["id", "workspace_url"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    render_resource_block(RESOURCE_TYPE, id, args)
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "id" | "workspace_url" => Some(format!("{RESOURCE_TYPE}.{id}.{output}")),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
