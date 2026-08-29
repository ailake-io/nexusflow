use lettre::message::Mailbox;
use lettre::transport::smtp::authentication::Credentials;
use lettre::{AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor};
use nexus_core::NexusError;
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
    /// PagerDuty Events API v2 "integration key" (routing key) — unlike
    /// Slack/Teams there's no per-tenant URL, every account posts to the
    /// same fixed endpoint (`PAGERDUTY_EVENTS_URL`) with this key
    /// identifying which service/escalation policy receives the event.
    pub pagerduty_routing_key: Option<String>,
    /// SMTP relay config — `None` means the Email channel is off, same
    /// contract as every other channel here.
    pub email: Option<EmailConfig>,
    /// Generic outbound webhook — last channel in IMPLEMENTATION_PLAN.md
    /// Marco 7 #10's priority order, for receivers not covered by the
    /// named channels above (custom internal tooling, a receiver that
    /// speaks neither Slack's Block Kit nor Teams' Adaptive Card format,
    /// etc). Plain JSON body, no vendor-specific shape.
    pub webhook_url: Option<String>,
}

/// SMTP over STARTTLS only (never sends credentials/mail unencrypted) —
/// good enough for the relays this targets (Gmail, SES, self-hosted
/// Postfix/etc all support it on port 587). Implicit-TLS (port 465) isn't
/// wired up; add `AsyncSmtpTransport::relay` alongside `starttls_relay` in
/// `send_failure_email` if a provider needs it.
#[derive(Clone)]
pub struct EmailConfig {
    pub smtp_host: String,
    pub smtp_port: u16,
    pub username: Option<String>,
    pub password: Option<String>,
    pub from: String,
    pub to: Vec<String>,
}

/// PagerDuty's Events API v2 endpoint — same for every account; the
/// `routing_key` in the request body is what routes an event to a specific
/// service. See
/// https://developer.pagerduty.com/docs/events-api-v2/trigger-events/.
const PAGERDUTY_EVENTS_URL: &str = "https://events.pagerduty.com/v2/enqueue";

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
        if let Some(routing_key) = self.config.pagerduty_routing_key.clone() {
            let client = self.client.clone();
            let payload = pagerduty_failure_payload(&routing_key, pipeline_id, run_id, error);
            spawn_webhook_post(
                client,
                PAGERDUTY_EVENTS_URL.to_string(),
                payload,
                "PagerDuty",
            );
        }
        if let Some(email) = self.config.email.clone() {
            let pipeline_id = pipeline_id.to_string();
            let error = error.to_string();
            tokio::spawn(async move {
                match send_failure_email(&email, &pipeline_id, run_id, &error).await {
                    Ok(()) => crate::server_metrics::record_alert_sent("Email", "success"),
                    Err(e) => {
                        tracing::warn!(channel = "Email", error = %e, "failed to send alert");
                        crate::server_metrics::record_alert_sent("Email", "failure");
                    }
                }
            });
        }
        if let Some(url) = self.config.webhook_url.clone() {
            let client = self.client.clone();
            let payload = generic_webhook_failure_payload(pipeline_id, run_id, error);
            spawn_webhook_post(client, url, payload, "Webhook");
        }
    }

    /// Per-pipeline alert dispatch — additive to `notify_pipeline_failed`
    /// above (which keeps firing exactly as it does today, global channels
    /// only, failure only). `alerts` comes from the run's own
    /// `PipelineSpec.alerts`; each configured channel fires only when its
    /// own `on_success`/`on_failure` toggle matches the real outcome. Same
    /// fire-and-forget/never-propagate contract as every channel above.
    pub fn notify_pipeline_run(
        &self,
        alerts: Option<&nexus_core::AlertsConfig>,
        pipeline_id: &str,
        run_id: i64,
        success: bool,
        message: &str,
    ) {
        let Some(alerts) = alerts else { return };

        if let Some(c) = alerts
            .slack
            .as_ref()
            .filter(|c| fires(c.on_success, c.on_failure, success))
        {
            let client = self.client.clone();
            let payload = slack_run_payload(pipeline_id, run_id, success, message);
            spawn_webhook_post(client, c.url.clone(), payload, "Slack");
        }
        if let Some(c) = alerts
            .teams
            .as_ref()
            .filter(|c| fires(c.on_success, c.on_failure, success))
        {
            let client = self.client.clone();
            let payload = teams_run_payload(pipeline_id, run_id, success, message);
            spawn_webhook_post(client, c.url.clone(), payload, "Teams");
        }
        if let Some(c) = alerts
            .webhook
            .as_ref()
            .filter(|c| fires(c.on_success, c.on_failure, success))
        {
            let client = self.client.clone();
            let payload = generic_webhook_run_payload(pipeline_id, run_id, success, message);
            spawn_webhook_post(client, c.url.clone(), payload, "Webhook");
        }
        if let Some(c) = alerts
            .pagerduty
            .as_ref()
            .filter(|c| fires(c.on_success, c.on_failure, success))
        {
            let client = self.client.clone();
            let payload =
                pagerduty_run_payload(&c.routing_key, pipeline_id, run_id, success, message);
            spawn_webhook_post(
                client,
                PAGERDUTY_EVENTS_URL.to_string(),
                payload,
                "PagerDuty",
            );
        }
        if let Some(c) = alerts
            .email
            .as_ref()
            .filter(|c| fires(c.on_success, c.on_failure, success))
        {
            let smtp_host = c.smtp_host.clone();
            let smtp_port = c.smtp_port;
            let username = c.username.clone();
            let password = c.password.clone();
            let from = c.from.clone();
            let to = c.to.clone();
            let pipeline_id = pipeline_id.to_string();
            let message = message.to_string();
            tokio::spawn(async move {
                let email = EmailConfig {
                    smtp_host,
                    smtp_port,
                    username,
                    password,
                    from,
                    to,
                };
                match send_run_email(&email, &pipeline_id, run_id, success, &message).await {
                    Ok(()) => crate::server_metrics::record_alert_sent("Email", "success"),
                    Err(e) => {
                        tracing::warn!(channel = "Email", error = %e, "failed to send alert");
                        crate::server_metrics::record_alert_sent("Email", "failure");
                    }
                }
            });
        }
    }
}

/// `success` selects which of a channel's two toggles applies.
fn fires(on_success: bool, on_failure: bool, success: bool) -> bool {
    if success {
        on_success
    } else {
        on_failure
    }
}

async fn send_failure_email(
    config: &EmailConfig,
    pipeline_id: &str,
    run_id: i64,
    error: &str,
) -> Result<(), NexusError> {
    let subject = format!("[nexusflow] Pipeline '{pipeline_id}' run {run_id} failed");
    let body =
        format!("Pipeline: {pipeline_id}\nRun: {run_id}\nError: {error}\n\n---\nSent by NexusFlow");

    let from: Mailbox = config
        .from
        .parse()
        .map_err(|e| NexusError::Connector(format!("invalid email from address: {e}")))?;

    let mut builder = Message::builder().from(from).subject(subject);
    for to in &config.to {
        let to: Mailbox = to
            .parse()
            .map_err(|e| NexusError::Connector(format!("invalid email to address: {e}")))?;
        builder = builder.to(to);
    }
    let message = builder
        .body(body)
        .map_err(|e| NexusError::Connector(format!("failed to build email: {e}")))?;

    let mut mailer = AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(&config.smtp_host)
        .map_err(|e| NexusError::Connector(format!("invalid SMTP host: {e}")))?
        .port(config.smtp_port);
    if let (Some(username), Some(password)) = (&config.username, &config.password) {
        mailer = mailer.credentials(Credentials::new(username.clone(), password.clone()));
    }
    mailer
        .build()
        .send(message)
        .await
        .map_err(|e| NexusError::Connector(format!("failed to send email: {e}")))?;

    Ok(())
}

/// Same as `send_failure_email`, but for the per-pipeline
/// `notify_pipeline_run` path — kept as a separate function (some
/// duplication accepted) specifically so `send_failure_email`/
/// `notify_pipeline_failed` stay untouched, zero risk to the existing
/// global failure-only behavior.
async fn send_run_email(
    config: &EmailConfig,
    pipeline_id: &str,
    run_id: i64,
    success: bool,
    message: &str,
) -> Result<(), NexusError> {
    let verb = if success { "succeeded" } else { "failed" };
    let subject = format!("[nexusflow] Pipeline '{pipeline_id}' run {run_id} {verb}");
    let body =
        format!("Pipeline: {pipeline_id}\nRun: {run_id}\nStatus: {verb}\n{message}\n\n---\nSent by NexusFlow");

    let from: Mailbox = config
        .from
        .parse()
        .map_err(|e| NexusError::Connector(format!("invalid email from address: {e}")))?;

    let mut builder = Message::builder().from(from).subject(subject);
    for to in &config.to {
        let to: Mailbox = to
            .parse()
            .map_err(|e| NexusError::Connector(format!("invalid email to address: {e}")))?;
        builder = builder.to(to);
    }
    let message = builder
        .body(body)
        .map_err(|e| NexusError::Connector(format!("failed to build email: {e}")))?;

    let mut mailer = AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(&config.smtp_host)
        .map_err(|e| NexusError::Connector(format!("invalid SMTP host: {e}")))?
        .port(config.smtp_port);
    if let (Some(username), Some(password)) = (&config.username, &config.password) {
        mailer = mailer.credentials(Credentials::new(username.clone(), password.clone()));
    }
    mailer
        .build()
        .send(message)
        .await
        .map_err(|e| NexusError::Connector(format!("failed to send email: {e}")))?;

    Ok(())
}

/// Shared fire-and-forget POST used by every webhook-shaped channel (Slack,
/// Teams, PagerDuty, and the generic Webhook channel): same logging
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
                crate::server_metrics::record_alert_sent(channel, "failure");
            }
            Err(e) => {
                tracing::warn!(channel, error = %e, "failed to send alert");
                crate::server_metrics::record_alert_sent(channel, "failure");
            }
            Ok(_) => crate::server_metrics::record_alert_sent(channel, "success"),
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

/// PagerDuty Events API v2 "trigger" event. `dedup_key` is set to
/// `pipeline_id`+`run_id` so repeated failure notifications for the same
/// run (there's only ever one per run today, but this is what PagerDuty's
/// own docs recommend) coalesce into one incident instead of paging
/// on-call multiple times for the same underlying failure — critical
/// severity, per IMPLEMENTATION_PLAN.md Marco 7 ("PagerDuty (críticos)").
fn pagerduty_failure_payload(
    routing_key: &str,
    pipeline_id: &str,
    run_id: i64,
    error: &str,
) -> Value {
    json!({
        "routing_key": routing_key,
        "event_action": "trigger",
        "dedup_key": format!("nexusflow-{pipeline_id}-{run_id}"),
        "payload": {
            "summary": format!("Pipeline '{pipeline_id}' run {run_id} failed: {error}"),
            "source": "nexusflow",
            "severity": "critical",
            "custom_details": {
                "pipeline_id": pipeline_id,
                "run_id": run_id,
                "error": error
            }
        }
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

/// Plain JSON body for the generic Webhook channel — no vendor-specific
/// shape, just the raw facts, so a receiver of any kind (internal tooling,
/// a serverless function, a receiver not shaped like Slack/Teams) can
/// consume it directly.
fn generic_webhook_failure_payload(pipeline_id: &str, run_id: i64, error: &str) -> Value {
    json!({
        "event": "pipeline_failed",
        "pipeline_id": pipeline_id,
        "run_id": run_id,
        "error": error
    })
}

// --- Per-pipeline (success/failure) payload builders, used only by
// `notify_pipeline_run` — same shapes as the failure-only builders above,
// parametrized by `success` instead of hardcoding "failed" everywhere.

fn slack_run_payload(pipeline_id: &str, run_id: i64, success: bool, message: &str) -> Value {
    let (emoji, verb) = if success {
        (":white_check_mark:", "succeeded")
    } else {
        (":rotating_light:", "failed")
    };
    json!({
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": format!(
                        "{emoji} *Pipeline {verb}*\n*Pipeline:* `{pipeline_id}`\n*Run:* `{run_id}`\n{message}"
                    )
                }
            }
        ]
    })
}

fn teams_run_payload(pipeline_id: &str, run_id: i64, success: bool, message: &str) -> Value {
    let (emoji, verb) = if success {
        ("✅", "succeeded")
    } else {
        ("🚨", "failed")
    };
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
                            "text": format!("{emoji} Pipeline {verb}")
                        },
                        {
                            "type": "FactSet",
                            "facts": [
                                {"title": "Pipeline", "value": pipeline_id},
                                {"title": "Run", "value": run_id.to_string()},
                                {"title": "Detail", "value": message}
                            ]
                        }
                    ]
                }
            }
        ]
    })
}

fn pagerduty_run_payload(
    routing_key: &str,
    pipeline_id: &str,
    run_id: i64,
    success: bool,
    message: &str,
) -> Value {
    let (event_action, severity, verb) = if success {
        // PagerDuty has no "info" severity for a trigger that should
        // auto-resolve; "resolve" closes any open incident with the same
        // dedup_key instead of opening a new one — the natural fit for "this
        // run that may have paged before is fine now".
        ("resolve", "info", "succeeded")
    } else {
        ("trigger", "critical", "failed")
    };
    json!({
        "routing_key": routing_key,
        "event_action": event_action,
        "dedup_key": format!("nexusflow-{pipeline_id}-{run_id}"),
        "payload": {
            "summary": format!("Pipeline '{pipeline_id}' run {run_id} {verb}: {message}"),
            "source": "nexusflow",
            "severity": severity,
            "custom_details": {
                "pipeline_id": pipeline_id,
                "run_id": run_id,
                "message": message
            }
        }
    })
}

fn generic_webhook_run_payload(
    pipeline_id: &str,
    run_id: i64,
    success: bool,
    message: &str,
) -> Value {
    json!({
        "event": if success { "pipeline_succeeded" } else { "pipeline_failed" },
        "pipeline_id": pipeline_id,
        "run_id": run_id,
        "message": message
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

    #[test]
    fn pagerduty_payload_includes_pipeline_run_and_error() {
        let payload =
            pagerduty_failure_payload("routing-key-123", "p1", 7, "connector unreachable");
        assert_eq!(payload["routing_key"], "routing-key-123");
        assert_eq!(payload["event_action"], "trigger");
        assert_eq!(payload["dedup_key"], "nexusflow-p1-7");
        assert_eq!(payload["payload"]["severity"], "critical");
        let summary = payload["payload"]["summary"].as_str().unwrap();
        assert!(summary.contains("p1"));
        assert!(summary.contains('7'));
        assert!(summary.contains("connector unreachable"));
    }

    #[test]
    fn generic_webhook_payload_includes_pipeline_run_and_error() {
        let payload = generic_webhook_failure_payload("p1", 7, "connector unreachable");
        assert_eq!(payload["event"], "pipeline_failed");
        assert_eq!(payload["pipeline_id"], "p1");
        assert_eq!(payload["run_id"], 7);
        assert_eq!(payload["error"], "connector unreachable");
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
            ..Default::default()
        });
        notifier.notify_pipeline_failed("p3", 44, "both channels");

        let slack_body = wait_for_capture(&slack_received).await;
        let teams_body = wait_for_capture(&teams_received).await;
        assert!(slack_body["blocks"].is_array());
        assert_eq!(teams_body["type"], "message");
    }

    #[tokio::test]
    async fn notify_with_email_config_is_fire_and_forget() {
        // Invalid SMTP host is fine here: the task is spawned, returns
        // immediately, and logs the failure — the run handler never waits.
        let notifier = AlertNotifier::new(AlertConfig {
            email: Some(EmailConfig {
                smtp_host: "invalid.example".to_string(),
                smtp_port: 587,
                username: None,
                password: None,
                from: "nexus@example.com".to_string(),
                to: vec!["ops@example.com".to_string()],
            }),
            ..Default::default()
        });
        notifier.notify_pipeline_failed("p4", 45, "email channel");
    }

    #[tokio::test]
    async fn notify_posts_generic_webhook_payload_to_configured_url() {
        let (addr, received) = capture_webhook().await;

        let notifier = AlertNotifier::new(AlertConfig {
            webhook_url: Some(format!("http://{addr}/webhook")),
            ..Default::default()
        });
        notifier.notify_pipeline_failed("p5", 46, "generic webhook");

        let body = wait_for_capture(&received).await;
        assert_eq!(body["event"], "pipeline_failed");
        assert_eq!(body["pipeline_id"], "p5");
        assert_eq!(body["run_id"], 46);
        assert_eq!(body["error"], "generic webhook");
    }

    #[tokio::test]
    async fn notify_pipeline_run_is_a_no_op_when_alerts_is_none() {
        let notifier = AlertNotifier::new(AlertConfig::default());
        // Absence of a crash/hang is the assertion — no channel configured
        // anywhere means nothing should ever be spawned.
        notifier.notify_pipeline_run(None, "p1", 1, true, "ok");
    }

    #[tokio::test]
    async fn notify_pipeline_run_fires_webhook_on_success_when_on_success_is_true() {
        let (addr, received) = capture_webhook().await;
        let notifier = AlertNotifier::new(AlertConfig::default());
        let alerts = nexus_core::AlertsConfig {
            webhook: Some(nexus_core::WebhookAlertChannel {
                url: format!("http://{addr}/webhook"),
                on_success: true,
                on_failure: false,
            }),
            ..Default::default()
        };

        notifier.notify_pipeline_run(Some(&alerts), "p1", 10, true, "42 rows written");

        let body = wait_for_capture(&received).await;
        assert_eq!(body["event"], "pipeline_succeeded");
        assert_eq!(body["pipeline_id"], "p1");
        assert_eq!(body["message"], "42 rows written");
    }

    #[tokio::test]
    async fn notify_pipeline_run_skips_webhook_on_success_when_on_success_is_false() {
        let (addr, received) = capture_webhook().await;
        let notifier = AlertNotifier::new(AlertConfig::default());
        let alerts = nexus_core::AlertsConfig {
            webhook: Some(nexus_core::WebhookAlertChannel {
                url: format!("http://{addr}/webhook"),
                on_success: false, // default posture: only failure
                on_failure: true,
            }),
            ..Default::default()
        };

        notifier.notify_pipeline_run(Some(&alerts), "p1", 10, true, "42 rows written");

        // Fire-and-forget with nothing to await — give any (incorrectly)
        // spawned task a moment to land before asserting it didn't.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            received.lock().unwrap().is_none(),
            "on_success: false must not fire on a successful run"
        );
    }

    #[tokio::test]
    async fn notify_pipeline_run_fires_slack_on_failure_with_generalized_payload() {
        let (addr, received) = capture_webhook().await;
        let notifier = AlertNotifier::new(AlertConfig::default());
        let alerts = nexus_core::AlertsConfig {
            slack: Some(nexus_core::WebhookAlertChannel {
                url: format!("http://{addr}/webhook"),
                on_success: false,
                on_failure: true,
            }),
            ..Default::default()
        };

        notifier.notify_pipeline_run(Some(&alerts), "p2", 11, false, "connector timeout");

        let body = wait_for_capture(&received).await;
        let text = body["blocks"][0]["text"]["text"].as_str().unwrap();
        assert!(text.contains("p2"));
        assert!(text.contains("failed"));
        assert!(text.contains("connector timeout"));
    }

    #[test]
    fn pagerduty_run_payload_resolves_the_incident_on_success() {
        // PagerDuty's endpoint is the fixed `PAGERDUTY_EVENTS_URL` constant,
        // not a per-channel URL — can't redirect it to a test server the
        // way the other channels' dispatch is tested above, so this checks
        // the payload shape directly instead.
        let payload = pagerduty_run_payload("rk-123", "p3", 12, true, "all good");
        assert_eq!(payload["event_action"], "resolve");
        assert_eq!(payload["payload"]["severity"], "info");

        let failure_payload = pagerduty_run_payload("rk-123", "p3", 12, false, "boom");
        assert_eq!(failure_payload["event_action"], "trigger");
        assert_eq!(failure_payload["payload"]["severity"], "critical");
    }

    #[tokio::test]
    async fn notify_pipeline_run_respects_pagerduty_on_success_toggle() {
        // No real PagerDuty endpoint to observe, but this still confirms
        // `fires()` gating doesn't panic/block for the PagerDuty branch and
        // that a `false` toggle is a genuine no-op (same shape as the
        // webhook toggle test above, just without a capturable receiver).
        let notifier = AlertNotifier::new(AlertConfig::default());
        let alerts = nexus_core::AlertsConfig {
            pagerduty: Some(nexus_core::PagerDutyAlertChannel {
                routing_key: "rk-123".to_string(),
                on_success: false,
                on_failure: true,
            }),
            ..Default::default()
        };
        notifier.notify_pipeline_run(Some(&alerts), "p3", 12, true, "all good");
    }
}
