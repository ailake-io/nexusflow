//! 2-resource custom emit: `azurerm_eventhub_namespace` +
//! `azurerm_eventhub` — Azure analog of `aws-kinesis-stream`/
//! `gcp-pubsub-topic`.

use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

use super::ModuleImpl;

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    namespace_name: String,
    eventhub_name: String,
    resource_group_name: String,
    location: String,
    #[serde(default = "default_sku")]
    sku: String,
    #[serde(default = "default_capacity")]
    capacity: u32,
    #[serde(default = "default_partition_count")]
    partition_count: u32,
    #[serde(default = "default_message_retention")]
    message_retention: u32,
}

fn default_sku() -> String {
    "Standard".to_string()
}
fn default_capacity() -> u32 {
    1
}
fn default_partition_count() -> u32 {
    2
}
fn default_message_retention() -> u32 {
    1
}

submit_infra_module!(
    "azure-event-hub",
    "Event Hub",
    "data",
    "azure",
    Config,
    &["namespace_id", "eventhub_id"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    let get = |k: &str, default: &str| args.get(k).cloned().unwrap_or_else(|| default.to_string());
    let namespace_name = get("namespace_name", &format!("\"{id}-ns\""));
    let eventhub_name = get("eventhub_name", &format!("\"{id}\""));
    let resource_group_name = get("resource_group_name", "\"\"");
    let location = get("location", "\"\"");
    let sku = get("sku", "\"Standard\"");
    let capacity = get("capacity", "1");
    let partition_count = get("partition_count", "2");
    let message_retention = get("message_retention", "1");

    let namespace_id = format!("{id}_namespace");

    format!(
        "resource \"azurerm_eventhub_namespace\" \"{namespace_id}\" {{\n  name                = {namespace_name}\n  resource_group_name = {resource_group_name}\n  location            = {location}\n  sku                 = {sku}\n  capacity            = {capacity}\n}}\n\nresource \"azurerm_eventhub\" \"{id}\" {{\n  name                = {eventhub_name}\n  namespace_id        = azurerm_eventhub_namespace.{namespace_id}.id\n  partition_count     = {partition_count}\n  message_retention   = {message_retention}\n}}\n"
    )
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "namespace_id" => Some(format!("azurerm_eventhub_namespace.{id}_namespace.id")),
        "eventhub_id" => Some(format!("azurerm_eventhub.{id}.id")),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
