use crate::run_log_store::RunLogStore;
use chrono::{DateTime, Utc};
use nexus_core::{ProgressEvent, ProgressSender};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;

/// Severity of one execution log line — kept to 3 levels (no `Debug`/`Trace`)
/// since these are user-facing narration of a pipeline run, not developer
/// diagnostics (those stay in the process-global `tracing` stream, see
/// `telemetry.rs`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogLevel {
    Info,
    Warn,
    Error,
}

impl LogLevel {
    pub fn as_str(&self) -> &'static str {
        match self {
            LogLevel::Info => "info",
            LogLevel::Warn => "warn",
            LogLevel::Error => "error",
        }
    }

    pub fn from_str(s: &str) -> anyhow::Result<Self> {
        match s {
            "info" => Ok(LogLevel::Info),
            "warn" => Ok(LogLevel::Warn),
            "error" => Ok(LogLevel::Error),
            other => anyhow::bail!("unknown log level {other:?}"),
        }
    }
}

/// One execution log line for a run — narration of what the pipeline is
/// doing (connecting, partition counts, dbt steps, final outcome), not a
/// substitute for the numeric `ProgressEvent` stream (rows/bytes per
/// partition) that already exists. `"type": "log"` in the wire format
/// (`#[serde(tag = "type"...)]` isn't used here to keep `ProgressEvent`'s
/// existing untagged wire shape byte-for-byte unchanged — see
/// `forward_progress` in lib.rs, which adds the tag manually only on this
/// variant) distinguishes it client-side from a `ProgressEvent` frame or the
/// untagged `{"hardware_stats": {...}}` frame already sent on the same
/// socket.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunLogEvent {
    pub ts: DateTime<Utc>,
    pub level: LogLevel,
    pub message: String,
}

pub type LogSender = broadcast::Sender<RunLogEvent>;

/// Emits one execution log line for a run: broadcasts it to any live
/// WebSocket subscriber (best-effort, `send` failing just means nobody's
/// listening right now) *and* persists it to `RunLogStore` so `GET
/// .../logs` can replay it later — the whole reason "ver logs" also has to
/// work for a scheduled run nobody was watching live. A store write failure
/// is logged and swallowed: a pipeline run must never fail because writing
/// its own narration failed.
#[derive(Clone)]
pub struct RunLogger {
    run_id: i64,
    tx: LogSender,
    store: RunLogStore,
}

impl RunLogger {
    pub fn new(run_id: i64, tx: LogSender, store: RunLogStore) -> Self {
        Self { run_id, tx, store }
    }

    pub async fn log(&self, level: LogLevel, message: impl Into<String>) {
        let event = RunLogEvent {
            ts: Utc::now(),
            level,
            message: message.into(),
        };
        let _ = self.tx.send(event.clone());
        if let Err(e) = self.store.insert(self.run_id, &event).await {
            tracing::warn!(error = %e, run_id = self.run_id, "failed to persist run log line");
        }
    }

    pub async fn info(&self, message: impl Into<String>) {
        self.log(LogLevel::Info, message).await;
    }

    pub async fn error(&self, message: impl Into<String>) {
        self.log(LogLevel::Error, message).await;
    }
}

/// Maps a running pipeline execution (`run_id`, from `PipelineStore::start_run`)
/// to the broadcast channels its progress and log events go out on — every
/// WebSocket client subscribed to that run gets every event (ARCHITECTURE.md §8 /
/// IMPLEMENTATION_PLAN.md Marco 7 task #9). Entries are removed once the run
/// finishes: a client connecting after that gets a 404 for the *live* socket,
/// not stale/no events — a known MVP limitation for the numeric progress
/// stream specifically. Logs don't have this gap: they're persisted via
/// `RunLogStore` independently of whether this hub still has a live channel
/// for the run (see `GET /pipelines/{id}/runs/{run_id}/logs`).
#[derive(Clone, Default)]
pub struct ProgressHub {
    channels: Arc<Mutex<HashMap<i64, (ProgressSender, LogSender)>>>,
}

const CHANNEL_CAPACITY: usize = 256;

impl ProgressHub {
    pub fn start(&self, run_id: i64) -> (ProgressSender, LogSender) {
        let (ptx, _) = broadcast::channel(CHANNEL_CAPACITY);
        let (ltx, _) = broadcast::channel(CHANNEL_CAPACITY);
        self.channels
            .lock()
            .unwrap()
            .insert(run_id, (ptx.clone(), ltx.clone()));
        (ptx, ltx)
    }

    pub fn subscribe(
        &self,
        run_id: i64,
    ) -> Option<(
        broadcast::Receiver<ProgressEvent>,
        broadcast::Receiver<RunLogEvent>,
    )> {
        self.channels
            .lock()
            .unwrap()
            .get(&run_id)
            .map(|(ptx, ltx)| (ptx.subscribe(), ltx.subscribe()))
    }

    pub fn finish(&self, run_id: i64) {
        self.channels.lock().unwrap().remove(&run_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscribe_before_start_returns_none() {
        let hub = ProgressHub::default();
        assert!(hub.subscribe(1).is_none());
    }

    #[tokio::test]
    async fn subscriber_receives_events_sent_after_start() {
        let hub = ProgressHub::default();
        let (tx, _log_tx) = hub.start(1);
        let (mut rx, _log_rx) = hub.subscribe(1).unwrap();

        tx.send(ProgressEvent {
            partition_id: "p0".to_string(),
            batches_written: 1,
            rows_written: 10,
            bytes_written: 100,
            done: false,
        })
        .unwrap();

        let event = rx.recv().await.unwrap();
        assert_eq!(event.rows_written, 10);
    }

    #[tokio::test]
    async fn log_subscriber_receives_log_events_sent_after_start() {
        let hub = ProgressHub::default();
        let (_tx, log_tx) = hub.start(1);
        let (_rx, mut log_rx) = hub.subscribe(1).unwrap();

        log_tx
            .send(RunLogEvent {
                ts: Utc::now(),
                level: LogLevel::Info,
                message: "hello".to_string(),
            })
            .unwrap();

        let event = log_rx.recv().await.unwrap();
        assert_eq!(event.message, "hello");
    }

    #[test]
    fn subscribe_after_finish_returns_none() {
        let hub = ProgressHub::default();
        hub.start(1);
        hub.finish(1);
        assert!(hub.subscribe(1).is_none());
    }

    #[tokio::test]
    async fn multiple_subscribers_each_get_every_event() {
        let hub = ProgressHub::default();
        let (tx, _log_tx) = hub.start(1);
        let (mut rx_a, _) = hub.subscribe(1).unwrap();
        let (mut rx_b, _) = hub.subscribe(1).unwrap();

        tx.send(ProgressEvent {
            partition_id: "p0".to_string(),
            batches_written: 1,
            rows_written: 5,
            bytes_written: 50,
            done: false,
        })
        .unwrap();

        assert_eq!(rx_a.recv().await.unwrap().rows_written, 5);
        assert_eq!(rx_b.recv().await.unwrap().rows_written, 5);
    }
}
