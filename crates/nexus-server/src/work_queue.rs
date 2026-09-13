use crate::db::{rewrite_placeholders, MetadataPool};

/// One claimed job — the fields `worker.rs` needs to actually run it.
#[derive(Debug, Clone, PartialEq)]
pub struct QueuedRun {
    pub id: i64,
    pub pipeline_id: String,
    pub run_id: i64,
}

/// Postgres-backed work queue (Fase 29) — when `AppState.work_queue` is
/// `Some`, `start_pipeline_run` (lib.rs) enqueues a row here instead of
/// dispatching execution inline on whichever replica happened to receive
/// the triggering request (a manual run, a cron tick, or a dependency
/// trigger — all three go through `start_pipeline_run`, so all three
/// benefit uniformly). Any replica running `worker::spawn`'s poll loop can
/// then claim and execute it — this is the piece ARCHITECTURE.md §6's
/// "single-node" note identifies as actually missing: multiple replicas
/// sharing one Postgres metadata store already coordinate *scheduling*
/// decisions (`scheduler.rs`'s leader election), but until this, every
/// *execution* still ran wherever it was triggered, with no way to
/// rebalance load across replicas.
///
/// Postgres-only, same restriction `scheduler.rs`'s leader election has and
/// for the same reason: `claim_next` needs `SELECT ... FOR UPDATE SKIP
/// LOCKED` so N replicas can each claim a *different* pending job
/// concurrently without double-processing one — SQLite has no equivalent
/// and (being inherently single-instance) has no multi-replica scenario to
/// guard against anyway. `enqueue`/`complete` work on either backend (they
/// don't need SKIP LOCKED), but in practice `AppState.work_queue` is only
/// ever `Some` when the backend is Postgres — see `build_state`.
#[derive(Clone)]
pub struct WorkQueueStore {
    pool: MetadataPool,
}

impl WorkQueueStore {
    fn q(&self, sql: &'static str) -> std::borrow::Cow<'static, str> {
        rewrite_placeholders(sql, self.pool.is_postgres())
    }

    pub fn is_postgres(&self) -> bool {
        self.pool.is_postgres()
    }

    pub async fn connect(database_url: &str) -> anyhow::Result<Self> {
        let pool = MetadataPool::connect(database_url).await?;
        let create_sqlite = r#"
            CREATE TABLE IF NOT EXISTS pipeline_run_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pipeline_id TEXT NOT NULL,
                run_id BIGINT NOT NULL,
                status TEXT NOT NULL,
                claimed_by TEXT,
                claimed_at TEXT,
                created_at TEXT NOT NULL
            )
        "#;
        let create_postgres = r#"
            CREATE TABLE IF NOT EXISTS pipeline_run_queue (
                id BIGSERIAL PRIMARY KEY,
                pipeline_id TEXT NOT NULL,
                run_id BIGINT NOT NULL,
                status TEXT NOT NULL,
                claimed_by TEXT,
                claimed_at TEXT,
                created_at TEXT NOT NULL
            )
        "#;
        match &pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query(create_sqlite).execute(p).await?;
            }
            MetadataPool::Postgres(p) => {
                sqlx::query(create_postgres).execute(p).await?;
                sqlx::query(
                    "CREATE INDEX IF NOT EXISTS idx_pipeline_run_queue_status \
                     ON pipeline_run_queue(status)",
                )
                .execute(p)
                .await?;
            }
        }
        Ok(Self { pool })
    }

    pub async fn enqueue(&self, pipeline_id: &str, run_id: i64) -> anyhow::Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        let sql = self.q(
            "INSERT INTO pipeline_run_queue (pipeline_id, run_id, status, created_at) \
             VALUES (?, ?, 'pending', ?)",
        );
        match &self.pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query(sqlx::AssertSqlSafe(sql))
                    .bind(pipeline_id)
                    .bind(run_id)
                    .bind(&now)
                    .execute(p)
                    .await?;
            }
            MetadataPool::Postgres(p) => {
                sqlx::query(sqlx::AssertSqlSafe(sql))
                    .bind(pipeline_id)
                    .bind(run_id)
                    .bind(&now)
                    .execute(p)
                    .await?;
            }
        }
        Ok(())
    }

    /// Atomically claims and marks one pending job `claimed`, or `None` if
    /// the queue is empty right now. `Ok(None)` on SQLite unconditionally
    /// (see this struct's doc comment) — never called in practice there
    /// since `worker::spawn` refuses to start its poll loop on a SQLite
    /// backend, but staying safe-by-construction here means a future
    /// caller can't accidentally introduce double-processing by skipping
    /// that check.
    pub async fn claim_next(&self, worker_id: &str) -> anyhow::Result<Option<QueuedRun>> {
        let MetadataPool::Postgres(p) = &self.pool else {
            return Ok(None);
        };
        let now = chrono::Utc::now().to_rfc3339();
        let row: Option<(i64, String, i64)> = sqlx::query_as(
            "UPDATE pipeline_run_queue SET status = 'claimed', claimed_by = $1, claimed_at = $2 \
             WHERE id = ( \
                 SELECT id FROM pipeline_run_queue \
                 WHERE status = 'pending' \
                 ORDER BY id \
                 FOR UPDATE SKIP LOCKED \
                 LIMIT 1 \
             ) \
             RETURNING id, pipeline_id, run_id",
        )
        .bind(worker_id)
        .bind(&now)
        .fetch_optional(p)
        .await?;
        Ok(row.map(|(id, pipeline_id, run_id)| QueuedRun {
            id,
            pipeline_id,
            run_id,
        }))
    }

    /// Removes a job once it's been handed off to `execute_pipeline_run`
    /// (successfully or not — the run's own outcome is durably recorded in
    /// `pipeline_runs` by then regardless; this table only tracks queue
    /// position, not run outcome). Called right after dispatch, not after
    /// the run actually finishes — see `worker.rs`'s doc comment for why
    /// that's an intentional, narrower reliability envelope (protects
    /// against the worker crashing between claim and dispatch, not against
    /// the dispatched run itself crashing, which `PipelineStore::
    /// fail_interrupted_runs`'s existing boot-time reaper already covers).
    pub async fn complete(&self, id: i64) -> anyhow::Result<()> {
        let sql = self.q("DELETE FROM pipeline_run_queue WHERE id = ?");
        match &self.pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query(sqlx::AssertSqlSafe(sql)).bind(id).execute(p).await?;
            }
            MetadataPool::Postgres(p) => {
                sqlx::query(sqlx::AssertSqlSafe(sql)).bind(id).execute(p).await?;
            }
        }
        Ok(())
    }

    /// Resets any `claimed` row older than `stale_after` back to `pending`
    /// — recovers a job whose worker crashed between claiming it and
    /// calling `complete` (a narrow window: claim-to-dispatch, not the
    /// full run duration — see `complete`'s doc comment). Timestamps are
    /// compared in Rust after parsing (not in SQL), since `claimed_at` is
    /// stored as an RFC3339 string like every other store in this crate,
    /// not a native timestamp column — avoids relying on RFC3339's
    /// fractional-second width being lexicographically comparable across
    /// rows, which it isn't guaranteed to be.
    pub async fn requeue_stale(&self, stale_after: chrono::Duration) -> anyhow::Result<u64> {
        let sql = self.q("SELECT id, claimed_at FROM pipeline_run_queue WHERE status = 'claimed'");
        let rows: Vec<(i64, Option<String>)> = match &self.pool {
            MetadataPool::Sqlite(p) => sqlx::query_as(sqlx::AssertSqlSafe(sql)).fetch_all(p).await?,
            MetadataPool::Postgres(p) => {
                sqlx::query_as(sqlx::AssertSqlSafe(sql)).fetch_all(p).await?
            }
        };
        let cutoff = chrono::Utc::now() - stale_after;
        let mut requeued = 0u64;
        let reset_sql = self.q(
            "UPDATE pipeline_run_queue SET status = 'pending', claimed_by = NULL, claimed_at = NULL \
             WHERE id = ?",
        );
        for (id, claimed_at) in rows {
            let is_stale = claimed_at
                .as_deref()
                .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                .is_none_or(|t| t < cutoff);
            if !is_stale {
                continue;
            }
            match &self.pool {
                MetadataPool::Sqlite(p) => {
                    sqlx::query(sqlx::AssertSqlSafe(reset_sql.clone()))
                        .bind(id)
                        .execute(p)
                        .await?;
                }
                MetadataPool::Postgres(p) => {
                    sqlx::query(sqlx::AssertSqlSafe(reset_sql.clone()))
                        .bind(id)
                        .execute(p)
                        .await?;
                }
            }
            requeued += 1;
        }
        Ok(requeued)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn enqueue_and_complete_round_trip_on_sqlite() {
        let store = WorkQueueStore::connect("sqlite::memory:").await.unwrap();
        store.enqueue("pipe-1", 42).await.unwrap();
        // claim_next is a documented no-op on SQLite (Postgres-only feature).
        assert_eq!(store.claim_next("w1").await.unwrap(), None);
    }

    #[tokio::test]
    async fn postgres_claim_next_returns_a_pending_job_exactly_once() {
        use testcontainers_modules::postgres;
        use testcontainers_modules::testcontainers::runners::AsyncRunner;

        let container = postgres::Postgres::default().start().await.unwrap();
        let host = container.get_host().await.unwrap();
        let port = container.get_host_port_ipv4(5432).await.unwrap();
        let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");

        let store = WorkQueueStore::connect(&url).await.unwrap();
        assert!(store.is_postgres());
        store.enqueue("pipe-1", 1).await.unwrap();

        let claimed = store.claim_next("worker-a").await.unwrap().unwrap();
        assert_eq!(claimed.pipeline_id, "pipe-1");
        assert_eq!(claimed.run_id, 1);

        // Nothing left to claim — the queue had exactly one pending job.
        assert_eq!(store.claim_next("worker-b").await.unwrap(), None);
    }

    #[tokio::test]
    async fn postgres_two_workers_never_claim_the_same_job() {
        use testcontainers_modules::postgres;
        use testcontainers_modules::testcontainers::runners::AsyncRunner;

        let container = postgres::Postgres::default().start().await.unwrap();
        let host = container.get_host().await.unwrap();
        let port = container.get_host_port_ipv4(5432).await.unwrap();
        let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");

        let store = WorkQueueStore::connect(&url).await.unwrap();
        for i in 0..5 {
            store.enqueue("pipe-1", i).await.unwrap();
        }

        // Sequential claims (this test proves correctness of the SQL, not
        // concurrency itself — `SELECT ... FOR UPDATE SKIP LOCKED`'s
        // concurrent-safety is Postgres's own guarantee, not something a
        // single-process test can meaningfully re-prove) must never repeat
        // a run_id and must eventually exhaust the queue.
        let mut seen = std::collections::HashSet::new();
        for _ in 0..5 {
            let job = store.claim_next("worker").await.unwrap().unwrap();
            assert!(seen.insert(job.run_id), "run_id claimed twice: {}", job.run_id);
        }
        assert_eq!(store.claim_next("worker").await.unwrap(), None);
    }

    #[tokio::test]
    async fn postgres_complete_removes_the_row() {
        use testcontainers_modules::postgres;
        use testcontainers_modules::testcontainers::runners::AsyncRunner;

        let container = postgres::Postgres::default().start().await.unwrap();
        let host = container.get_host().await.unwrap();
        let port = container.get_host_port_ipv4(5432).await.unwrap();
        let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");

        let store = WorkQueueStore::connect(&url).await.unwrap();
        store.enqueue("pipe-1", 1).await.unwrap();
        let job = store.claim_next("worker").await.unwrap().unwrap();
        store.complete(job.id).await.unwrap();

        // Requeuing immediately after completion must not resurrect it —
        // the row is gone, not just reset to pending.
        assert_eq!(store.requeue_stale(chrono::Duration::seconds(0)).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn postgres_requeue_stale_resets_an_old_claim_but_not_a_fresh_one() {
        use testcontainers_modules::postgres;
        use testcontainers_modules::testcontainers::runners::AsyncRunner;

        let container = postgres::Postgres::default().start().await.unwrap();
        let host = container.get_host().await.unwrap();
        let port = container.get_host_port_ipv4(5432).await.unwrap();
        let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");

        let store = WorkQueueStore::connect(&url).await.unwrap();
        store.enqueue("pipe-1", 1).await.unwrap();
        let job = store.claim_next("worker-a").await.unwrap().unwrap();

        // A claim from just now is not stale under any real threshold.
        let requeued = store.requeue_stale(chrono::Duration::minutes(10)).await.unwrap();
        assert_eq!(requeued, 0);
        assert_eq!(store.claim_next("worker-b").await.unwrap(), None);

        // Under a zero threshold, that same claim is immediately stale.
        let requeued = store.requeue_stale(chrono::Duration::seconds(0)).await.unwrap();
        assert_eq!(requeued, 1);
        let reclaimed = store.claim_next("worker-b").await.unwrap().unwrap();
        assert_eq!(reclaimed.id, job.id);
    }
}
