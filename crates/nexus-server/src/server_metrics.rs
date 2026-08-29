//! Prometheus metrics recorded server-side (as opposed to
//! `nexus-core::pipeline`'s `nexus_pipeline_rows_written_total`/
//! `bytes_written_total`, which are engine-level and crate-independent).
//! Same registration mechanism (`opentelemetry::global::meter`, backed by
//! `telemetry::PROMETHEUS_REGISTRY`), just centralized here instead of one
//! `LazyLock` block per file — every instrument below is real, already-computed
//! data that already exists somewhere in this crate (a DB write, a log line,
//! a rejected request); this module only adds the OTel recording call next
//! to where that data is already produced.

use crate::dbt_test_result_store::DbtTestOutcome;
use crate::resource_stats::ResourceSample;
use opentelemetry::metrics::{Counter, Gauge, Histogram, Meter};
use opentelemetry::KeyValue;
use std::sync::LazyLock;
use std::time::Duration;

fn meter() -> Meter {
    opentelemetry::global::meter("nexus-server")
}

static CPU_PERCENT: LazyLock<Gauge<f64>> = LazyLock::new(|| {
    meter()
        .f64_gauge("nexus_resource_cpu_percent")
        .with_description("Host CPU usage percent, sampled every 60s")
        .build()
});
static MEMORY_USED_BYTES: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    meter()
        .u64_gauge("nexus_resource_memory_used_bytes")
        .with_description("Host memory used, bytes")
        .build()
});
static MEMORY_TOTAL_BYTES: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    meter()
        .u64_gauge("nexus_resource_memory_total_bytes")
        .with_description("Host memory total, bytes")
        .build()
});
static DISK_USED_BYTES: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    meter()
        .u64_gauge("nexus_resource_disk_used_bytes")
        .with_description("Disk used, bytes, for the data directory's filesystem")
        .build()
});
static DISK_TOTAL_BYTES: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    meter()
        .u64_gauge("nexus_resource_disk_total_bytes")
        .with_description("Disk total, bytes, for the data directory's filesystem")
        .build()
});

/// Called from `resource_stats::spawn`'s sampling loop, right next to where
/// the same `ResourceSample` is persisted for `GET /system/resource-stats` —
/// one source of truth, not two independently-sampled readings.
pub fn record_resource_sample(sample: &ResourceSample) {
    CPU_PERCENT.record(sample.cpu_percent as f64, &[]);
    MEMORY_USED_BYTES.record(sample.memory_used_bytes, &[]);
    MEMORY_TOTAL_BYTES.record(sample.memory_total_bytes, &[]);
    if let Some(used) = sample.disk_used_bytes {
        DISK_USED_BYTES.record(used, &[]);
    }
    if let Some(total) = sample.disk_total_bytes {
        DISK_TOTAL_BYTES.record(total, &[]);
    }
}

static RUNS_TOTAL: LazyLock<Counter<u64>> = LazyLock::new(|| {
    meter()
        .u64_counter("nexus_pipeline_runs_total")
        .with_description("Pipeline runs, by terminal status (success/failed)")
        .build()
});
static RUN_DURATION_SECONDS: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    meter()
        .f64_histogram("nexus_pipeline_run_duration_seconds")
        .with_description("Pipeline run wall-clock duration, start to terminal state")
        .build()
});

/// Called once per run from `execute_pipeline_run`, right after
/// `finish_run_success`/`finish_run_failure` persists the same terminal
/// state to `pipeline_runs`.
pub fn record_run_outcome(pipeline_id: &str, status: &'static str, duration: Duration) {
    let attrs = [
        KeyValue::new("pipeline_id", pipeline_id.to_string()),
        KeyValue::new("status", status),
    ];
    RUNS_TOTAL.add(1, &attrs);
    RUN_DURATION_SECONDS.record(duration.as_secs_f64(), &attrs);
}

#[allow(dead_code)] // only reached via `record_dbt_test_results`, see its own note
static DBT_TEST_RESULTS_TOTAL: LazyLock<Counter<u64>> = LazyLock::new(|| {
    meter()
        .u64_counter("nexus_dbt_test_results_total")
        .with_description("dbt test results, by outcome (pass/fail/warn)")
        .build()
});

/// Called from `execute_pipeline_run`, right next to
/// `DbtTestResultStore::record_all` — same slice, so counts can never drift
/// from the persisted per-test history. Only called from lib.rs's
/// `#[cfg(feature = "dbt")]` block — see `DbtTestResultStore::record_all`'s
/// identical note.
#[allow(dead_code)]
pub fn record_dbt_test_results(pipeline_id: &str, results: &[DbtTestOutcome]) {
    for r in results {
        let attrs = [
            KeyValue::new("pipeline_id", pipeline_id.to_string()),
            KeyValue::new("status", r.status.clone()),
        ];
        DBT_TEST_RESULTS_TOTAL.add(1, &attrs);
    }
}

static CHECKPOINT_LAST_COMMITTED_TIMESTAMP: LazyLock<Gauge<f64>> = LazyLock::new(|| {
    meter()
        .f64_gauge("nexus_checkpoint_last_committed_timestamp_seconds")
        .with_description(
            "Unix timestamp of the last successful checkpoint commit — \
             `time() - this` in PromQL is checkpoint staleness",
        )
        .build()
});

/// Called from `CheckpointStore::commit`, right after the upsert succeeds.
pub fn record_checkpoint_commit(pipeline_id: &str, partition_id: &str) {
    let attrs = [
        KeyValue::new("pipeline_id", pipeline_id.to_string()),
        KeyValue::new("partition_id", partition_id.to_string()),
    ];
    CHECKPOINT_LAST_COMMITTED_TIMESTAMP.record(chrono::Utc::now().timestamp() as f64, &attrs);
}

static LOGIN_ATTEMPTS_TOTAL: LazyLock<Counter<u64>> = LazyLock::new(|| {
    meter()
        .u64_counter("nexus_login_attempts_total")
        .with_description("Login attempts, by outcome (success/invalid_credentials/error)")
        .build()
});

/// Called from `login_handler`, right next to the same outcome's `tracing::info!`.
pub fn record_login_attempt(outcome: &'static str) {
    LOGIN_ATTEMPTS_TOTAL.add(1, &[KeyValue::new("outcome", outcome)]);
}

static RATE_LIMIT_REJECTIONS_TOTAL: LazyLock<Counter<u64>> = LazyLock::new(|| {
    meter()
        .u64_counter("nexus_rate_limit_rejections_total")
        .with_description("Requests rejected by the per-IP login rate limiter")
        .build()
});

/// Called from `rate_limit::login_rate_limit` when a request is rejected.
pub fn record_rate_limit_rejection() {
    RATE_LIMIT_REJECTIONS_TOTAL.add(1, &[]);
}

static ALERTS_SENT_TOTAL: LazyLock<Counter<u64>> = LazyLock::new(|| {
    meter()
        .u64_counter("nexus_alerts_sent_total")
        .with_description("Alert notifications sent, by channel and outcome (success/failure)")
        .build()
});

/// Called from `alerts::spawn_webhook_post` and `send_failure_email`'s call
/// site, right where each channel's send already logs its own outcome.
pub fn record_alert_sent(channel: &'static str, outcome: &'static str) {
    ALERTS_SENT_TOTAL.add(
        1,
        &[
            KeyValue::new("channel", channel),
            KeyValue::new("outcome", outcome),
        ],
    );
}
