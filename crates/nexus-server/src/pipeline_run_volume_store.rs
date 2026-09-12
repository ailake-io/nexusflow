use crate::db::{rewrite_placeholders, MetadataPool};

/// One run's total output row count, for the volume-anomaly baseline
/// (Fase 27) and the Quality tab's volume trend chart.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct VolumeSample {
    pub run_id: i64,
    pub recorded_at: String,
    pub rows_written: i64,
}

/// Append-only history of total rows written per successful run — same
/// "one row per event, never overwritten" posture as
/// `quality_check_store.rs`, but orientated to a single event per run
/// (this run's success) rather than one row per check per run. Unlike
/// `resource_stats.rs`'s `tokio::spawn`+`interval` background sampler
/// (which samples the *host* on a fixed clock, independent of any
/// pipeline run), this is populated by a direct call from
/// `execute_pipeline_run`'s success hook — volume is inherently per-run,
/// not per-wall-clock-tick.
#[derive(Clone)]
pub struct PipelineRunVolumeStore {
    pool: MetadataPool,
}

impl PipelineRunVolumeStore {
    fn q(&self, sql: &'static str) -> std::borrow::Cow<'static, str> {
        rewrite_placeholders(sql, self.pool.is_postgres())
    }

    pub async fn connect(database_url: &str) -> anyhow::Result<Self> {
        let pool = MetadataPool::connect(database_url).await?;
        let create = r#"
            CREATE TABLE IF NOT EXISTS pipeline_run_volume (
                pipeline_id TEXT NOT NULL,
                run_id BIGINT NOT NULL,
                recorded_at TEXT NOT NULL,
                rows_written BIGINT NOT NULL,
                PRIMARY KEY (pipeline_id, run_id)
            )
        "#;
        match &pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query(create).execute(p).await?;
                sqlx::query(
                    "CREATE INDEX IF NOT EXISTS idx_pipeline_run_volume_pipeline_id \
                     ON pipeline_run_volume(pipeline_id)",
                )
                .execute(p)
                .await?;
            }
            MetadataPool::Postgres(p) => {
                sqlx::query(create).execute(p).await?;
                sqlx::query(
                    "CREATE INDEX IF NOT EXISTS idx_pipeline_run_volume_pipeline_id \
                     ON pipeline_run_volume(pipeline_id)",
                )
                .execute(p)
                .await?;
            }
        }
        Ok(Self { pool })
    }

    /// `ON CONFLICT DO NOTHING` on the natural `(pipeline_id, run_id)` key —
    /// same dedup idiom `resource_stats.rs` uses for its timestamp PK. A
    /// run only ever succeeds once, but this makes a caller-side retry (or
    /// a future second call site) safe by construction rather than by
    /// discipline.
    pub async fn record(
        &self,
        pipeline_id: &str,
        run_id: i64,
        rows_written: i64,
    ) -> anyhow::Result<()> {
        let recorded_at = chrono::Utc::now().to_rfc3339();
        let sql = self.q(
            "INSERT INTO pipeline_run_volume (pipeline_id, run_id, recorded_at, rows_written) \
             VALUES (?, ?, ?, ?) ON CONFLICT (pipeline_id, run_id) DO NOTHING",
        );
        match &self.pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query(sqlx::AssertSqlSafe(sql))
                    .bind(pipeline_id)
                    .bind(run_id)
                    .bind(&recorded_at)
                    .bind(rows_written)
                    .execute(p)
                    .await?;
            }
            MetadataPool::Postgres(p) => {
                sqlx::query(sqlx::AssertSqlSafe(sql))
                    .bind(pipeline_id)
                    .bind(run_id)
                    .bind(&recorded_at)
                    .bind(rows_written)
                    .execute(p)
                    .await?;
            }
        }
        Ok(())
    }

    /// The most recent `limit` samples, oldest-first — same "DB returns
    /// newest-first, caller doesn't need to re-sort for a chronological
    /// chart" contract `list_runs`/`QualityPanel.tsx` already follow (there
    /// the frontend reverses; here it's cheap enough to do once in Rust).
    pub async fn recent(&self, pipeline_id: &str, limit: i64) -> anyhow::Result<Vec<VolumeSample>> {
        let sql = self.q(
            "SELECT run_id, recorded_at, rows_written FROM pipeline_run_volume \
             WHERE pipeline_id = ? ORDER BY run_id DESC LIMIT ?",
        );
        let rows: Vec<(i64, String, i64)> = match &self.pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query_as(sqlx::AssertSqlSafe(sql))
                    .bind(pipeline_id)
                    .bind(limit)
                    .fetch_all(p)
                    .await?
            }
            MetadataPool::Postgres(p) => {
                sqlx::query_as(sqlx::AssertSqlSafe(sql))
                    .bind(pipeline_id)
                    .bind(limit)
                    .fetch_all(p)
                    .await?
            }
        };
        Ok(rows
            .into_iter()
            .rev()
            .map(|(run_id, recorded_at, rows_written)| VolumeSample {
                run_id,
                recorded_at,
                rows_written,
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn record_and_recent_round_trip_oldest_first() {
        let store = PipelineRunVolumeStore::connect("sqlite::memory:").await.unwrap();
        store.record("pipe-1", 1, 100).await.unwrap();
        store.record("pipe-1", 2, 150).await.unwrap();
        store.record("pipe-1", 3, 90).await.unwrap();

        let samples = store.recent("pipe-1", 10).await.unwrap();
        assert_eq!(
            samples.iter().map(|s| s.run_id).collect::<Vec<_>>(),
            vec![1, 2, 3],
            "must be oldest-first regardless of DB fetch order"
        );
        assert_eq!(samples[1].rows_written, 150);
    }

    #[tokio::test]
    async fn recent_respects_limit_keeping_the_newest() {
        let store = PipelineRunVolumeStore::connect("sqlite::memory:").await.unwrap();
        for run_id in 1..=5 {
            store.record("pipe-1", run_id, run_id * 10).await.unwrap();
        }
        let samples = store.recent("pipe-1", 3).await.unwrap();
        assert_eq!(
            samples.iter().map(|s| s.run_id).collect::<Vec<_>>(),
            vec![3, 4, 5]
        );
    }

    #[tokio::test]
    async fn recording_the_same_run_twice_does_not_duplicate() {
        let store = PipelineRunVolumeStore::connect("sqlite::memory:").await.unwrap();
        store.record("pipe-1", 1, 100).await.unwrap();
        store.record("pipe-1", 1, 999).await.unwrap();
        let samples = store.recent("pipe-1", 10).await.unwrap();
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].rows_written, 100, "first write wins, not overwritten");
    }

    #[tokio::test]
    async fn recent_is_scoped_per_pipeline() {
        let store = PipelineRunVolumeStore::connect("sqlite::memory:").await.unwrap();
        store.record("pipe-1", 1, 100).await.unwrap();
        store.record("pipe-2", 1, 500).await.unwrap();
        let samples = store.recent("pipe-1", 10).await.unwrap();
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].rows_written, 100);
    }

    #[tokio::test]
    async fn postgres_backend_records_and_lists() {
        use testcontainers_modules::postgres;
        use testcontainers_modules::testcontainers::runners::AsyncRunner;

        let container = postgres::Postgres::default().start().await.unwrap();
        let host = container.get_host().await.unwrap();
        let port = container.get_host_port_ipv4(5432).await.unwrap();
        let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");

        let store = PipelineRunVolumeStore::connect(&url).await.unwrap();
        assert!(matches!(store.pool, MetadataPool::Postgres(_)));

        store.record("pipe-1", 1, 42).await.unwrap();
        let samples = store.recent("pipe-1", 10).await.unwrap();
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].rows_written, 42);
    }
}
