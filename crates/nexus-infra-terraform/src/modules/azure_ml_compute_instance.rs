//! Plain `azurerm_machine_learning_compute_instance` — Azure analog of
//! `aws-sagemaker-notebook`/`gcp-vertex-ai-workbench`.

use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

use super::{render_resource_block, ModuleImpl};

const RESOURCE_TYPE: &str = "azurerm_machine_learning_compute_instance";

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    name: String,
    /// ARM id of the parent workspace — wire from an `azure-ml-workspace`
    /// node's `id` output.
    machine_learning_workspace_id: String,
    #[serde(default = "default_vm_size")]
    vm_size: String,
}

fn default_vm_size() -> String {
    "Standard_DS3_v2".to_string()
}

submit_infra_module!(
    "azure-ml-compute-instance",
    "Azure ML Compute Instance",
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
