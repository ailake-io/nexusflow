//! Plain `google_pubsub_topic` + `google_pubsub_subscription` — GCP
//! analog of `aws-sqs-queue`/`aws-kinesis-stream`'s decoupling role. A
//! topic with zero subscriptions can't be consumed, so one pull
//! subscription is always created alongside it.

use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

use super::ModuleImpl;

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    name: String,
    project: String,
    #[serde(default = "default_subscription_name")]
    subscription_name: String,
    #[serde(default = "default_ack_deadline")]
    ack_deadline_seconds: u32,
    #[serde(default = "default_message_retention")]
    message_retention_duration: String,
}

fn default_subscription_name() -> String {
    "default-sub".to_string()
}
fn default_ack_deadline() -> u32 {
    20
}
fn default_message_retention() -> String {
    "604800s".to_string()
}

submit_infra_module!(
    "gcp-pubsub-topic",
    "Pub/Sub Topic",
    "data",
    "gcp",
    Config,
    &["topic_id", "subscription_id"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    let get = |k: &str, default: &str| args.get(k).cloned().unwrap_or_else(|| default.to_string());
    let name = get("name", &format!("\"{id}\""));
    let project = get("project", "\"\"");
    let subscription_name = get("subscription_name", "\"default-sub\"");
    let ack_deadline_seconds = get("ack_deadline_seconds", "20");
    let message_retention_duration = get("message_retention_duration", "\"604800s\"");

    let topic_id = format!("{id}_topic");

    format!(
        "resource \"google_pubsub_topic\" \"{topic_id}\" {{\n  name    = {name}\n  project = {project}\n\n  message_retention_duration = {message_retention_duration}\n}}\n\nresource \"google_pubsub_subscription\" \"{id}\" {{\n  name    = {subscription_name}\n  topic   = google_pubsub_topic.{topic_id}.id\n  project = {project}\n\n  ack_deadline_seconds = {ack_deadline_seconds}\n}}\n"
    )
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "topic_id" => Some(format!("google_pubsub_topic.{id}_topic.id")),
        "subscription_id" => Some(format!("google_pubsub_subscription.{id}.id")),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
