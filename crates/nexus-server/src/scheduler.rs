use crate::AppState;
use chrono::{DateTime, NaiveDateTime, Utc};
use std::time::Duration;

/// cron granularity is 1 minute — polling every 30s keeps drift under the
/// tick interval without busy-looping.
const TICK_INTERVAL: Duration = Duration::from_secs(30);

/// Cron-based automatic pipeline triggering. A pipeline opts in by setting
/// `PipelineSpec.schedule` (validated as a real cron expression at create/
/// update time — see `nexus_core::schedule`). This background task polls
/// every persisted pipeline that has one and fires `start_pipeline_run`
/// for whichever are due, using the exact same run/record/dbt/alert path a
/// manual `POST /pipelines/{id}/run` goes through.
///
/// Not spawned by `build_app` (see its doc comment) — only `run()`'s real
/// boot path starts this, so tests never get a surprise background task
/// racing their own assertions.
pub fn spawn(state: AppState) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(TICK_INTERVAL);
        loop {
            interval.tick().await;
            if let Err(e) = tick(&state).await {
                tracing::warn!(error = %e, "scheduler tick failed");
            }
        }
    });
}

async fn tick(state: &AppState) -> anyhow::Result<()> {
    let summaries = state.pipelines.list_summaries(&state.secrets).await?;
    for summary in summaries {
        let Some(expr) = &summary.schedule else {
            continue;
        };

        let schedule = match nexus_core::parse_cron_expression(expr) {
            Ok(s) => s,
            Err(e) => {
                // Already validated at create/update time — this only
                // fires if a spec got persisted before that validation
                // existed, or was edited directly in the DB.
                tracing::warn!(
                    pipeline_id = %summary.pipeline_id,
                    error = %e,
                    "pipeline has an invalid schedule, skipping"
                );
                continue;
            }
        };

        let runs = state.pipelines.list_runs(&summary.pipeline_id).await?;
        // `list_runs` is ordered most-recent-first (see pipeline_store.rs).
        if let Some(last) = runs.first() {
            if last.finished_at.is_none() {
                continue; // still running — never overlap a scheduled run with itself
            }
        }

        let anchor = match runs.first() {
            Some(last) => parse_sqlite_datetime(&last.started_at),
            None => parse_sqlite_datetime(&summary.created_at),
        };
        let Some(anchor) = anchor else {
            tracing::warn!(pipeline_id = %summary.pipeline_id, "could not parse anchor timestamp, skipping");
            continue;
        };

        let Some(next_fire) = schedule.after(&anchor).next() else {
            continue; // exhausted schedule (e.g. a cron expression that never matches again)
        };
        if next_fire > Utc::now() {
            continue; // not due yet
        }

        let state = state.clone();
        let pipeline_id = summary.pipeline_id.clone();
        tokio::spawn(async move {
            let spec = match state.pipelines.get_spec(&pipeline_id, &state.secrets).await {
                Ok(spec) => spec,
                Err(e) => {
                    tracing::warn!(pipeline_id = %pipeline_id, error = %e, "scheduler: failed to load pipeline spec");
                    return;
                }
            };
            // Spawns the run's supervisor task itself, so this returns as
            // soon as the run row exists — the tick is never blocked by a
            // running pipeline.
            if let Err(e) = crate::start_pipeline_run(&state, &spec).await {
                tracing::warn!(pipeline_id = %pipeline_id, error = %e, "scheduler: failed to start pipeline run");
            }
        });
    }
    Ok(())
}

/// `pipeline_runs.started_at`/`pipelines.created_at` are SQLite
/// `datetime('now')` strings (`"YYYY-MM-DD HH:MM:SS"`, UTC, no offset).
fn parse_sqlite_datetime(s: &str) -> Option<DateTime<Utc>> {
    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
        .ok()
        .map(|naive| naive.and_utc())
}
