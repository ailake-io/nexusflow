//! 2-resource custom emit: `azurerm_servicebus_namespace` +
//! `azurerm_servicebus_queue` — Azure analog of `aws-sqs-queue`.

use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

use super::ModuleImpl;

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    namespace_name: String,
    queue_name: String,
    resource_group_name: String,
    location: String,
    #[serde(default = "default_sku")]
    sku: String,
}

fn default_sku() -> String {
    "Standard".to_string()
}

submit_infra_module!(
    "azure-service-bus-queue",
    "Service Bus Queue",
    "data",
    "azure",
    Config,
    &["namespace_id", "queue_id"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    let get = |k: &str, default: &str| args.get(k).cloned().unwrap_or_else(|| default.to_string());
    let namespace_name = get("namespace_name", &format!("\"{id}-ns\""));
    let queue_name = get("queue_name", &format!("\"{id}\""));
    let resource_group_name = get("resource_group_name", "\"\"");
    let location = get("location", "\"\"");
    let sku = get("sku", "\"Standard\"");

    let namespace_id = format!("{id}_namespace");

    format!(
        "resource \"azurerm_servicebus_namespace\" \"{namespace_id}\" {{\n  name                = {namespace_name}\n  resource_group_name = {resource_group_name}\n  location            = {location}\n  sku                 = {sku}\n}}\n\nresource \"azurerm_servicebus_queue\" \"{id}\" {{\n  name         = {queue_name}\n  namespace_id = azurerm_servicebus_namespace.{namespace_id}.id\n}}\n"
    )
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "namespace_id" => Some(format!("azurerm_servicebus_namespace.{id}_namespace.id")),
        "queue_id" => Some(format!("azurerm_servicebus_queue.{id}.id")),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
