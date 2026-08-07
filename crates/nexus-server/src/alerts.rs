use serde_json::{json, Value};

/// Which webhook URL to notify per channel — `None` on any field means that
/// channel is simply off (not configured), never a startup failure — unlike
/// the JWT secret/encryption key, a missing alert channel doesn't
/// compromise anything, it just means nobody gets pinged on it.
/// IMPLEMENTATION_PLAN.md Marco 7 task #10 priority order: Slack → Teams →
/// PagerDuty → Email → Webhook.
#[derive(Clone, Default)]
pub struct AlertConfig {
    pub slack_webhook_url: Option<String>,
    pub teams_webhook_url: Option<String>,
}

/// Fires async alerts on pipeline failure, fanning out to every configured
/// channel independently — one channel's failure/absence never affects
/// another's.
#[derive(Clone)]
pub struct AlertNotifier {
    config: AlertConfig,
    client: reqwest::Client,
}

impl AlertNotifier {
    pub fn new(config: AlertConfig) -> Self {
        Self {
            config,
            client: reqwest::Client::new(),
        }
    }

    /// Spawns one task per configured channel and returns immediately — the
    /// pipeline run handler must never wait on a third-party HTTP round trip
    /// (`tokio::spawn`, ARCHITECTURE.md §9 "Alertas Assíncronos"). Send
    /// failures are logged, never propagated: an alert failing to send must
    /// never fail the run it's reporting on.
    pub fn notify_pipeline_failed(&self, pipeline_id: &str, run_id: i64, error: &str) {
        if let Some(url) = self.config.slack_webhook_url.clone() {
            let client = self.client.clone();
            let payload = slack_failure_payload(pipeline_id, run_id, error);
            spawn_webhook_post(client, url, payload, "Slack");
        }
        if let Some(url) = self.config.teams_webhook_url.clone() {
            let client = self.client.clone();
            let payload = teams_failure_payload(pipeline_id, run_id, error);
            spawn_webhook_post(client, url, payload, "Teams");
        }
    }
}

/// Shared fire-and-forget POST used by every webhook-shaped channel (Slack,
/// Teams, and — later — the generic Webhook channel): same logging
/// contract, same "never propagate a send failure" rule, just a different
/// JSON body shape per channel.
fn spawn_webhook_post(client: reqwest::Client, url: String, payload: Value, channel: &'static str) {
    tokio::spawn(async move {
        match client.post(&url).json(&payload).send().await {
            Ok(response) if !response.status().is_success() => {
                tracing::warn!(
                    channel,
                    status = %response.status(),
                    "alert webhook returned a non-success status"
                );
            }
            Err(e) => tracing::warn!(channel, error = %e, "failed to send alert"),
            Ok(_) => {}
        }
    });
}

fn slack_failure_payload(pipeline_id: &str, run_id: i64, error: &str) -> Value {
    json!({
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": format!(
                        ":rotating_light: *Pipeline failed*\n*Pipeline:* `{pipeline_id}`\n*Run:* `{run_id}`\n*Error:* {error}"
                    )
                }
            }
        ]
    })
}

/// Adaptive Card wrapped for Teams' current Incoming Webhook contract
/// (Workflows-based, replacing the legacy "O365 connector card"/MessageCard
/// format Microsoft is deprecating) — see
/// https://learn.microsoft.com/microsoftteams/platform/webhooks-and-connectors/how-to/add-incoming-webhook.
fn teams_failure_payload(pipeline_id: &str, run_id: i64, error: &str) -> Value {
    json!({
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "type": "AdaptiveCard",
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "version": "1.4",
                    "body": [
                        {
                            "type": "TextBlock",
                            "size": "Medium",
                            "weight": "Bolder",
                            "text": "🚨 Pipeline failed"
                        },
                        {
                            "type": "FactSet",
                            "facts": [
                                {"title": "Pipeline", "value": pipeline_id},
                                {"title": "Run", "value": run_id.to_string()},
                                {"title": "Error", "value": error}
                            ]
                        }
                    ]
                }
            }
        ]
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::routing::post;
    use axum::{Json, Router};
    use std::net::SocketAddr;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    #[test]
    fn slack_payload_includes_pipeline_run_and_error() {
        let payload = slack_failure_payload("p1", 7, "connector unreachable");
        let text = payload["blocks"][0]["text"]["text"].as_str().unwrap();
        assert!(text.contains("p1"));
        assert!(text.contains('7'));
        assert!(text.contains("connector unreachable"));
    }

    #[test]
    fn teams_payload_includes_pipeline_run_and_error() {
        let payload = teams_failure_payload("p1", 7, "connector unreachable");
        let facts = payload["attachments"][0]["content"]["body"][1]["facts"]
            .as_array()
            .unwrap();
        let joined: String = facts
            .iter()
            .map(|f| f["value"].as_str().unwrap())
            .collect::<Vec<_>>()
            .join(" ");
        assert!(joined.contains("p1"));
        assert!(joined.contains('7'));
        assert!(joined.contains("connector unreachable"));
    }

    #[tokio::test]
    async fn notify_without_configured_webhook_is_a_no_op() {
        // No channel configured — must not panic, spawn nothing that
        // observably does anything. Absence of a crash is the assertion.
        let notifier = AlertNotifier::new(AlertConfig::default());
        notifier.notify_pipeline_failed("p1", 1, "boom");
    }

    /// Starts a throwaway HTTP server capturing the JSON body of the first
    /// request it receives — shared by every webhook-shaped channel's test.
    async fn capture_webhook() -> (SocketAddr, Arc<Mutex<Option<Value>>>) {
        let received: Arc<Mutex<Option<Value>>> = Arc::new(Mutex::new(None));
        let received_clone = received.clone();

        let app = Router::new().route(
            "/webhook",
            post(move |Json(body): Json<Value>| {
                let received = received_clone.clone();
                async move {
                    *received.lock().unwrap() = Some(body);
                    "ok"
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (addr, received)
    }

    /// Fire-and-forget by design — poll briefly instead of assuming the
    /// background task has already run.
    async fn wait_for_capture(received: &Arc<Mutex<Option<Value>>>) -> Value {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(body) = received.lock().unwrap().clone() {
                return body;
            }
            if tokio::time::Instant::now() > deadline {
                panic!("webhook was never called");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn notify_posts_slack_block_kit_payload_to_configured_webhook() {
        let (addr, received) = capture_webhook().await;

        let notifier = AlertNotifier::new(AlertConfig {
            slack_webhook_url: Some(format!("http://{addr}/webhook")),
            ..Default::default()
        });
        notifier.notify_pipeline_failed("p1", 42, "unsupported connector");

        let body = wait_for_capture(&received).await;
        let text = body["blocks"][0]["text"]["text"].as_str().unwrap();
        assert!(text.contains("p1"));
        assert!(text.contains("42"));
        assert!(text.contains("unsupported connector"));
    }

    #[tokio::test]
    async fn notify_posts_teams_adaptive_card_to_configured_webhook() {
        let (addr, received) = capture_webhook().await;

        let notifier = AlertNotifier::new(AlertConfig {
            teams_webhook_url: Some(format!("http://{addr}/webhook")),
            ..Default::default()
        });
        notifier.notify_pipeline_failed("p2", 43, "timeout");

        let body = wait_for_capture(&received).await;
        assert_eq!(body["type"], "message");
        let facts = body["attachments"][0]["content"]["body"][1]["facts"]
            .as_array()
            .unwrap();
        let joined: String = facts
            .iter()
            .map(|f| f["value"].as_str().unwrap())
            .collect::<Vec<_>>()
            .join(" ");
        assert!(joined.contains("p2"));
        assert!(joined.contains("43"));
        assert!(joined.contains("timeout"));
    }

    #[tokio::test]
    async fn notify_fans_out_to_both_slack_and_teams_independently() {
        let (slack_addr, slack_received) = capture_webhook().await;
        let (teams_addr, teams_received) = capture_webhook().await;

        let notifier = AlertNotifier::new(AlertConfig {
            slack_webhook_url: Some(format!("http://{slack_addr}/webhook")),
            teams_webhook_url: Some(format!("http://{teams_addr}/webhook")),
        });
        notifier.notify_pipeline_failed("p3", 44, "both channels");

        let slack_body = wait_for_capture(&slack_received).await;
        let teams_body = wait_for_capture(&teams_received).await;
        assert!(slack_body["blocks"].is_array());
        assert_eq!(teams_body["type"], "message");
    }
}
