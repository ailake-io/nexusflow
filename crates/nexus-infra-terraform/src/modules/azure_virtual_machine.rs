//! 2-resource custom emit: `azurerm_network_interface` +
//! `azurerm_linux_virtual_machine` — Azure analog of `aws-ec2-instance`/
//! `gcp-compute-instance`. SSH-key auth only (`admin_ssh_key`), no
//! plaintext password field, matching this catalog's other Azure
//! modules. Private IP only in v1 — attach a public IP resource by hand
//! if the VM needs one.

use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

use super::ModuleImpl;

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    name: String,
    resource_group_name: String,
    location: String,
    #[serde(default = "default_size")]
    size: String,
    #[serde(default = "default_admin_username")]
    admin_username: String,
    /// SSH public key contents for `admin_username`.
    ssh_public_key: String,
    /// Subnet id the VM's NIC attaches to — wire from an `azure-vnet`
    /// node's `vnet_subnets` output (pick one element).
    subnet_id: String,
    #[serde(default = "default_publisher")]
    image_publisher: String,
    #[serde(default = "default_offer")]
    image_offer: String,
    #[serde(default = "default_sku")]
    image_sku: String,
    #[serde(default = "default_version")]
    image_version: String,
}

fn default_size() -> String {
    "Standard_B2s".to_string()
}
fn default_admin_username() -> String {
    "azureuser".to_string()
}
fn default_publisher() -> String {
    "Canonical".to_string()
}
fn default_offer() -> String {
    "0001-com-ubuntu-server-jammy".to_string()
}
fn default_sku() -> String {
    "22_04-lts".to_string()
}
fn default_version() -> String {
    "latest".to_string()
}

submit_infra_module!(
    "azure-virtual-machine",
    "Virtual Machine",
    "compute",
    "azure",
    Config,
    &["id", "private_ip_address"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    let get = |k: &str, default: &str| args.get(k).cloned().unwrap_or_else(|| default.to_string());
    let name = get("name", &format!("\"{id}\""));
    let resource_group_name = get("resource_group_name", "\"\"");
    let location = get("location", "\"\"");
    let size = get("size", "\"Standard_B2s\"");
    let admin_username = get("admin_username", "\"azureuser\"");
    let ssh_public_key = get("ssh_public_key", "\"\"");
    let subnet_id = get("subnet_id", "\"\"");
    let image_publisher = get("image_publisher", "\"Canonical\"");
    let image_offer = get("image_offer", "\"0001-com-ubuntu-server-jammy\"");
    let image_sku = get("image_sku", "\"22_04-lts\"");
    let image_version = get("image_version", "\"latest\"");

    let nic_id = format!("{id}_nic");

    format!(
        "resource \"azurerm_network_interface\" \"{nic_id}\" {{\n  name                = \"{id}-nic\"\n  resource_group_name = {resource_group_name}\n  location            = {location}\n\n  ip_configuration {{\n    name                          = \"internal\"\n    subnet_id                     = {subnet_id}\n    private_ip_address_allocation = \"Dynamic\"\n  }}\n}}\n\nresource \"azurerm_linux_virtual_machine\" \"{id}\" {{\n  name                = {name}\n  resource_group_name = {resource_group_name}\n  location            = {location}\n  size                = {size}\n  admin_username      = {admin_username}\n  network_interface_ids = [\n    azurerm_network_interface.{nic_id}.id,\n  ]\n\n  admin_ssh_key {{\n    username   = {admin_username}\n    public_key = {ssh_public_key}\n  }}\n\n  os_disk {{\n    caching              = \"ReadWrite\"\n    storage_account_type = \"Standard_LRS\"\n  }}\n\n  source_image_reference {{\n    publisher = {image_publisher}\n    offer     = {image_offer}\n    sku       = {image_sku}\n    version   = {image_version}\n  }}\n}}\n"
    )
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "id" => Some(format!("azurerm_linux_virtual_machine.{id}.id")),
        "private_ip_address" => Some(format!(
            "azurerm_network_interface.{id}_nic.private_ip_address"
        )),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
