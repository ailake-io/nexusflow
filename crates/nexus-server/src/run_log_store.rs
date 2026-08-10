use crate::db::{rewrite_placeholders, MetadataPool};
use crate::progress::RunLogEvent;

/// Persists every `RunLogEvent` a run emits (`ProgressHub`/`RunLogger` handle
/// the live broadcast side) so logs survive past the run's WebSocket
/// lifetime — the whole point being that a scheduled run nobody was watching
/// live can still be inspected afterwards via `GET
/// /pipelines/{id}/runs/{run_id}/logs`. Same dual-dialect pattern as
/// `checkpoint_store.rs` (`db::MetadataPool`). No retention/cleanup yet, same
/// as `pipeline_runs`/`checkpoints` — not a regression, just not built yet.
#[derive(Clone)]
pub struct RunLogStore {
    pool: MetadataPool,
}

impl RunLogStore {
    fn q(&self, sql: &'static str) -> std::borrow::Cow<'static, str> {
        rewrite_placeholders(sql, self.pool.is_postgres())
    }

    pub async fn connect(database_url: &str) -> anyhow::Result<Self> {
        let pool = MetadataPool::connect(database_url).await?;

        match &pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query(
                    r#"
                    CREATE TABLE IF NOT EXISTS pipeline_run_logs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        run_id INTEGER NOT NULL,
                        ts TEXT NOT NULL,
                        level TEXT NOT NULL,
                        message TEXT NOT NULL
                    )
                    "#,
                )
                .execute(p)
                .await?;
                sqlx::query(
                    "CREATE INDEX IF NOT EXISTS idx_pipeline_run_logs_run_id ON pipeline_run_logs(run_id)",
                )
                .execute(p)
                .await?;
            }
            MetadataPool::Postgres(p) => {
                sqlx::query(
                    r#"
                    CREATE TABLE IF NOT EXISTS pipeline_run_logs (
                        id BIGSERIAL PRIMARY KEY,
                        run_id BIGINT NOT NULL,
                        ts TEXT NOT NULL,
                        level TEXT NOT NULL,
                        message TEXT NOT NULL
                    )
                    "#,
                )
                .execute(p)
                .await?;
                sqlx::query(
                    "CREATE INDEX IF NOT EXISTS idx_pipeline_run_logs_run_id ON pipeline_run_logs(run_id)",
                )
                .execute(p)
                .await?;
            }
        }

        Ok(Self { pool })
    }

    /// Best-effort from the caller's point of view (`RunLogger` logs a
    /// warning and moves on rather than failing the pipeline over a log
    /// line) — this method itself still surfaces the real error so the
    /// caller can decide that.
    pub async fn insert(&self, run_id: i64, event: &RunLogEvent) -> anyhow::Result<()> {
        let sql = self
            .q("INSERT INTO pipeline_run_logs (run_id, ts, level, message) VALUES (?, ?, ?, ?)");
        match &self.pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query(sqlx::AssertSqlSafe(sql.into_owned()))
                    .bind(run_id)
                    .bind(event.ts.to_rfc3339())
                    .bind(event.level.as_str())
                    .bind(&event.message)
                    .execute(p)
                    .await?;
            }
            MetadataPool::Postgres(p) => {
                sqlx::query(sqlx::AssertSqlSafe(sql.into_owned()))
                    .bind(run_id)
                    .bind(event.ts.to_rfc3339())
                    .bind(event.level.as_str())
                    .bind(&event.message)
                    .execute(p)
                    .await?;
            }
        }
        Ok(())
    }

    /// Insertion order (`id` is autoincrement on both dialects) — chronological,
    /// same guarantee `pipeline_run_logs.ts` alone wouldn't give under clock
    /// coarseness on very fast successive log lines.
    pub async fn list(&self, run_id: i64) -> anyhow::Result<Vec<RunLogEvent>> {
        let sql = self
            .q("SELECT ts, level, message FROM pipeline_run_logs WHERE run_id = ? ORDER BY id ASC");
        let rows: Vec<(String, String, String)> = match &self.pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query_as(sqlx::AssertSqlSafe(sql.into_owned()))
                    .bind(run_id)
                    .fetch_all(p)
                    .await?
            }
            MetadataPool::Postgres(p) => {
                sqlx::query_as(sqlx::AssertSqlSafe(sql.into_owned()))
                    .bind(run_id)
                    .fetch_all(p)
                    .await?
            }
        };
        rows.into_iter()
            .map(|(ts, level, message)| {
                Ok(RunLogEvent {
                    ts: chrono::DateTime::parse_from_rfc3339(&ts)?.into(),
                    level: crate::progress::LogLevel::from_str(&level)?,
                    message,
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::progress::LogLevel;

    fn info(message: &str) -> RunLogEvent {
        RunLogEvent {
            ts: chrono::Utc::now(),
            level: LogLevel::Info,
            message: message.to_string(),
        }
    }

    #[tokio::test]
    async fn list_returns_logs_in_insertion_order() {
        let store = RunLogStore::connect("sqlite::memory:").await.unwrap();
        store.insert(1, &info("first")).await.unwrap();
        store.insert(1, &info("second")).await.unwrap();

        let logs = store.list(1).await.unwrap();
        assert_eq!(logs.len(), 2);
        assert_eq!(logs[0].message, "first");
        assert_eq!(logs[1].message, "second");
    }

    #[tokio::test]
    async fn list_scoped_per_run_id() {
        let store = RunLogStore::connect("sqlite::memory:").await.unwrap();
        store.insert(1, &info("run 1 log")).await.unwrap();
        store.insert(2, &info("run 2 log")).await.unwrap();

        assert_eq!(store.list(1).await.unwrap().len(), 1);
        assert_eq!(store.list(2).await.unwrap().len(), 1);
        assert!(store.list(3).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn level_round_trips_through_storage() {
        let store = RunLogStore::connect("sqlite::memory:").await.unwrap();
        store
            .insert(
                1,
                &RunLogEvent {
                    ts: chrono::Utc::now(),
                    level: LogLevel::Error,
                    message: "boom".to_string(),
                },
            )
            .await
            .unwrap();

        let logs = store.list(1).await.unwrap();
        assert_eq!(logs[0].level, LogLevel::Error);
    }

    /// Proves the Postgres branch behaves identically to SQLite above.
    #[tokio::test]
    async fn postgres_backend_insert_and_list() {
        use testcontainers_modules::postgres;
        use testcontainers_modules::testcontainers::runners::AsyncRunner;

        let container = postgres::Postgres::default().start().await.unwrap();
        let host = container.get_host().await.unwrap();
        let port = container.get_host_port_ipv4(5432).await.unwrap();
        let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");

        let store = RunLogStore::connect(&url).await.unwrap();
        assert!(matches!(store.pool, MetadataPool::Postgres(_)));

        store.insert(1, &info("hello from postgres")).await.unwrap();
        let logs = store.list(1).await.unwrap();
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].message, "hello from postgres");
    }
}
