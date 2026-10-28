//! Plain `azurerm_eventgrid_topic` — discrete-event pub/sub, complements
//! `azure-event-hub`'s streaming role. Azure analog of
//! `gcp-cloud-scheduler-job`/`aws-eventbridge-rule`'s event-pattern side
//! (not the scheduled side, which Logic Apps' recurrence trigger covers).

use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

use super::{render_resource_block, ModuleImpl};

const RESOURCE_TYPE: &str = "azurerm_eventgrid_topic";

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    name: String,
    resource_group_name: String,
    location: String,
}

submit_infra_module!(
    "azure-event-grid-topic",
    "Event Grid Topic",
    "cicd",
    "azure",
    Config,
    &["id", "endpoint"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    render_resource_block(RESOURCE_TYPE, id, args)
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "id" | "endpoint" => Some(format!("{RESOURCE_TYPE}.{id}.{output}")),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
