//! Plain `azurerm_logic_app_workflow` — Azure analog of
//! `aws-stepfunctions-state-machine`/`aws-eventbridge-rule`'s
//! orchestration role. Triggers/actions are configured inside the
//! Logic App Designer (or via separate `azurerm_logic_app_trigger_*`/
//! `azurerm_logic_app_action_*` resources) — this module only
//! provisions the empty workflow container.

use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

use super::{render_resource_block, ModuleImpl};

const RESOURCE_TYPE: &str = "azurerm_logic_app_workflow";

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    name: String,
    resource_group_name: String,
    location: String,
}

submit_infra_module!(
    "azure-logic-app",
    "Logic App Workflow",
    "cicd",
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
