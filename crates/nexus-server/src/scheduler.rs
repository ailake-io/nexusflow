use crate::AppState;
use chrono::{DateTime, NaiveDateTime, Utc};
use sqlx::pool::PoolConnection;
use sqlx::{PgPool, Postgres};
use std::time::Duration;

/// cron granularity is 1 minute — polling every 30s keeps drift under the
/// tick interval without busy-looping.
const TICK_INTERVAL: Duration = Duration::from_secs(30);

/// Fixed, arbitrary key for `pg_try_advisory_lock` (see `is_leader` below) —
/// this is the only advisory lock this schema uses, so any constant i64
/// works; it just has to stay the same across every replica/restart.
const SCHEDULER_LOCK_KEY: i64 = 8_193_042_178_501_223;

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
///
/// Leader election (Postgres backend only): with >1 replica sharing the
/// same Postgres metadata store (see `db::MetadataPool`), every replica's
/// scheduler would otherwise poll the same due pipelines and fire them all
/// — `is_leader` gates each tick on holding a Postgres advisory lock, so
/// only one replica actually dispatches. On SQLite there's no multi-replica
/// scenario to guard against (SQLite can't safely be shared across
/// replicas anyway), so this is skipped entirely and every tick runs, same
/// as before this existed.
pub fn spawn(state: AppState) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(TICK_INTERVAL);
        // Held across ticks (not re-acquired from the pool each time):
        // `pg_try_advisory_lock` is scoped to the session/connection that
        // took it, so the lock only persists as long as this connection
        // does. Losing the connection (this replica crashing, a network
        // blip) releases the lock automatically, letting another replica
        // pick it up on its own next tick — that self-healing behavior is
        // exactly why this must be one dedicated connection, not a fresh
        // one borrowed from the shared pool per call.
        let mut leader_conn: Option<PoolConnection<Postgres>> = None;
        loop {
            interval.tick().await;
            if let Some(pg_pool) = state.pipelines.pool().as_postgres() {
                if !is_leader(pg_pool, &mut leader_conn).await {
                    continue;
                }
            }
            if let Err(e) = tick(&state).await {
                tracing::warn!(error = %e, "scheduler tick failed");
            }
        }
    });
}

/// Returns whether this replica currently holds the scheduler's advisory
/// lock, acquiring (and remembering) a dedicated connection the first time
/// it's called, and transparently reconnecting if that connection ever
/// drops. `pg_try_advisory_lock` is non-blocking and idempotent per session
/// — calling it again while already holding the lock just re-confirms `true`
/// immediately, no risk of self-deadlock across ticks.
async fn is_leader(pg_pool: &PgPool, leader_conn: &mut Option<PoolConnection<Postgres>>) -> bool {
    if leader_conn.is_none() {
        match pg_pool.acquire().await {
            Ok(conn) => *leader_conn = Some(conn),
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "scheduler: failed to acquire a dedicated connection for leader election"
                );
                return false;
            }
        }
    }
    let conn = leader_conn.as_mut().expect("just ensured Some above");
    match sqlx::query_scalar::<_, bool>("SELECT pg_try_advisory_lock($1)")
        .bind(SCHEDULER_LOCK_KEY)
        .fetch_one(&mut **conn)
        .await
    {
        Ok(acquired) => acquired,
        Err(e) => {
            tracing::warn!(
                error = %e,
                "scheduler: lost the leader-election connection, will reconnect next tick"
            );
            *leader_conn = None;
            false
        }
    }
}

const SCHEDULER_PAGE_LIMIT: i64 = 10_000;

async fn tick(state: &AppState) -> anyhow::Result<()> {
    let summaries = state
        .pipelines
        .list_summaries(&state.secrets, SCHEDULER_PAGE_LIMIT, 0)
        .await?;
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

        let runs = state
            .pipelines
            .list_runs(&summary.pipeline_id, SCHEDULER_PAGE_LIMIT, 0)
            .await?;
        // `list_runs` is ordered most-recent-first (see pipeline_store.rs).
        if let Some(last) = runs.first() {
            if last.finished_at.is_none() {
                continue; // still running — never overlap a scheduled run with itself
            }
        }

        let anchor = match runs.first() {
            Some(last) => parse_stored_datetime(&last.started_at),
            None => parse_stored_datetime(&summary.created_at),
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

/// `pipeline_runs.started_at`/`pipelines.created_at` are always
/// `"YYYY-MM-DD HH:MM:SS"` (UTC, no offset) regardless of backend — SQLite's
/// native `datetime('now')` on that branch, and an explicit `to_char(now()
/// AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')` on the Postgres branch
/// (`pipeline_store.rs`) specifically so this parser doesn't need to care
/// which backend produced the string.
fn parse_stored_datetime(s: &str) -> Option<DateTime<Utc>> {
    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
        .ok()
        .map(|naive| naive.and_utc())
}

#[cfg(test)]
mod tests {
    use super::*;
    use testcontainers_modules::postgres;
    use testcontainers_modules::testcontainers::runners::AsyncRunner;

    #[test]
    fn parses_the_shared_stored_datetime_format() {
        let parsed = parse_stored_datetime("2026-01-15 12:30:00").unwrap();
        assert_eq!(parsed.to_rfc3339(), "2026-01-15T12:30:00+00:00");
    }

    #[test]
    fn rejects_malformed_datetime() {
        assert!(parse_stored_datetime("not a date").is_none());
    }

    /// Only one of two "replicas" (two `PgPool`s pointed at the same
    /// Postgres, mirroring 2 nexus-server processes sharing one metadata
    /// store) can hold the scheduler's advisory lock at a time — proves the
    /// leader-election mechanism itself, independent of the scheduler's own
    /// tick/dispatch logic above.
    #[tokio::test]
    async fn only_one_replica_holds_the_lock_at_a_time() {
        let container = postgres::Postgres::default().start().await.unwrap();
        let host = container.get_host().await.unwrap();
        let port = container.get_host_port_ipv4(5432).await.unwrap();
        let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");

        let replica_a = PgPool::connect(&url).await.unwrap();
        let replica_b = PgPool::connect(&url).await.unwrap();
        let mut conn_a: Option<PoolConnection<Postgres>> = None;
        let mut conn_b: Option<PoolConnection<Postgres>> = None;

        assert!(
            is_leader(&replica_a, &mut conn_a).await,
            "replica A takes the lock first"
        );
        assert!(
            !is_leader(&replica_b, &mut conn_b).await,
            "replica B must not also hold it"
        );

        // Calling again while already holding it must keep returning true
        // (idempotent per session), not deadlock or flip.
        assert!(is_leader(&replica_a, &mut conn_a).await);

        // Replica A "crashes" — merely dropping the `PoolConnection` isn't
        // enough to prove this (sqlx recycles it back into replica_a's own
        // pool, keeping the underlying Postgres session — and its advisory
        // lock — alive); `.close()` actually terminates that session, the
        // same effect a real process crash / TCP-level disconnect has.
        conn_a.take().unwrap().close().await.unwrap();
        assert!(
            is_leader(&replica_b, &mut conn_b).await,
            "replica B takes over after A's connection actually closes"
        );
    }
}
