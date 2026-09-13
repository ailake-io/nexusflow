use crate::AppState;
use std::time::Duration;

/// How often a replica polls the queue for pending jobs.
const POLL_INTERVAL: Duration = Duration::from_secs(2);

/// How long a `claimed` job can sit unfinished before another replica
/// requeues it — see `WorkQueueStore::requeue_stale`'s doc comment for
/// exactly what this protects against (a narrow claim-to-dispatch window,
/// not a run's full duration).
const STALE_AFTER: chrono::Duration = chrono::Duration::minutes(5);

/// Cross-pipeline execution distribution (Fase 29) — when
/// `AppState.work_queue` is configured (`NEXUS_QUEUE_MODE=true`, Postgres
/// backend only), every replica running this loop competes to claim
/// pending jobs `start_pipeline_run` (lib.rs) enqueues instead of running
/// inline. This is deliberately *not* a separate worker-only process/mode
/// — every replica that opts in both serves HTTP *and* claims jobs, same
/// "no separate deployment topology required" posture the scheduler's
/// leader election already has. A dedicated worker-only replica (no HTTP
/// serving at all) is a natural follow-up once/if load ever calls for it,
/// not implemented here.
///
/// Not spawned by `build_app` (same reasoning as `scheduler::spawn`/
/// `resource_stats::spawn` — see their doc comments) — only `run()`'s real
/// boot path starts this.
pub fn spawn(state: AppState) {
    let Some(queue) = state.work_queue.clone() else {
        return; // NEXUS_QUEUE_MODE not set — this replica dispatches inline, same as before this feature existed.
    };
    if !queue.is_postgres() {
        tracing::warn!(
            "NEXUS_QUEUE_MODE is set but the pipelines metadata backend is SQLite — queue-based \
             execution requires Postgres (same restriction scheduler.rs's leader election has); \
             this replica will never claim a queued run. Runs are still being enqueued and will \
             sit pending until a Postgres-backed replica claims them."
        );
        return;
    }
    let worker_id = format!(
        "{}-{}",
        std::env::var("HOSTNAME").unwrap_or_else(|_| "nexus-server".to_string()),
        std::process::id()
    );

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(POLL_INTERVAL);
        loop {
            interval.tick().await;
            if let Err(e) = queue.requeue_stale(STALE_AFTER).await {
                tracing::warn!(error = %e, "failed to requeue stale work-queue entries");
            }
            // Drains everything currently pending before waiting for the
            // next tick, rather than claiming one job per tick — a burst
            // of triggers (e.g. several dependency-chained pipelines
            // firing at once) shouldn't sit throttled by `POLL_INTERVAL`
            // when this replica has capacity to just take them all now.
            loop {
                match queue.claim_next(&worker_id).await {
                    Ok(Some(job)) => {
                        let spec = match state.pipelines.get_spec(&job.pipeline_id, &state.secrets).await {
                            Ok(spec) => spec,
                            Err(e) => {
                                tracing::warn!(
                                    pipeline_id = %job.pipeline_id,
                                    run_id = job.run_id,
                                    error = %e,
                                    "queued run's pipeline spec could not be loaded, dropping from queue"
                                );
                                let _ = queue.complete(job.id).await;
                                continue;
                            }
                        };
                        crate::dispatch_execute_pipeline_run(&state, spec, job.run_id).await;
                        if let Err(e) = queue.complete(job.id).await {
                            tracing::warn!(error = %e, "failed to remove completed job from the work queue");
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        tracing::warn!(error = %e, "failed to claim from the work queue");
                        break;
                    }
                }
            }
        }
    });
}
