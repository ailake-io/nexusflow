use serde_json::{json, Value};

/// Fires async alerts on pipeline failure (IMPLEMENTATION_PLAN.md Marco 7
/// task #10: "Slack primeiro" — Teams/PagerDuty/Email/Webhook are
/// documented next-priority, not built here). `None` webhook URL means
/// alerting is simply off (not configured), never a startup failure —
/// unlike the JWT secret/encryption key, a missing alert channel doesn't
/// compromise anything, it just means nobody gets pinged.
#[derive(Clone)]
pub struct AlertNotifier {
    slack_webhook_url: Option<String>,
    client: reqwest::Client,
}

impl AlertNotifier {
    pub fn new(slack_webhook_url: Option<String>) -> Self {
        Self {
            slack_webhook_url,
            client: reqwest::Client::new(),
        }
    }

    /// Spawns its own task and returns immediately — the pipeline run
    /// handler must never wait on a third-party HTTP round trip (`tokio::spawn`,
    /// ARCHITECTURE.md §9 "Alertas Assíncronos"). Send failures are logged,
    /// never propagated: an alert failing to send must never fail the run
    /// it's reporting on.
    pub fn notify_pipeline_failed(&self, pipeline_id: &str, run_id: i64, error: &str) {
        let Some(url) = self.slack_webhook_url.clone() else {
            return;
        };
        let client = self.client.clone();
        let payload = slack_failure_payload(pipeline_id, run_id, error);

        tokio::spawn(async move {
            match client.post(&url).json(&payload).send().await {
                Ok(response) if !response.status().is_success() => {
                    tracing::warn!(
                        status = %response.status(),
                        "Slack alert webhook returned a non-success status"
                    );
                }
                Err(e) => tracing::warn!(error = %e, "failed to send Slack alert"),
                Ok(_) => {}
            }
        });
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::routing::post;
    use axum::{Json, Router};
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

    #[tokio::test]
    async fn notify_without_configured_webhook_is_a_no_op() {
        // No webhook configured — must not panic, spawn nothing that
        // observably does anything. Absence of a crash is the assertion.
        let notifier = AlertNotifier::new(None);
        notifier.notify_pipeline_failed("p1", 1, "boom");
    }

    #[tokio::test]
    async fn notify_posts_slack_block_kit_payload_to_configured_webhook() {
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

        let notifier = AlertNotifier::new(Some(format!("http://{addr}/webhook")));
        notifier.notify_pipeline_failed("p1", 42, "unsupported connector");

        // Fire-and-forget by design — poll briefly instead of assuming the
        // background task has already run.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if received.lock().unwrap().is_some() {
                break;
            }
            if tokio::time::Instant::now() > deadline {
                panic!("webhook was never called");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let body = received.lock().unwrap().take().unwrap();
        let text = body["blocks"][0]["text"]["text"].as_str().unwrap();
        assert!(text.contains("p1"));
        assert!(text.contains("42"));
        assert!(text.contains("unsupported connector"));
    }
}
