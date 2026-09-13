use crate::db::{rewrite_placeholders, MetadataPool};
use chrono::DateTime;
use nexus_core::CheckpointCursor;
use std::collections::HashSet;

/// Checkpoint is persisted per `(pipeline_id, partition_id)`, not per
/// pipeline — see ARCHITECTURE.md §5. This is Marco 1's actual persistence
/// layer; `Sink::commit_checkpoint` itself is a no-op for connectors, since
/// only nexus-server knows about run history across retries. Backed by
/// SQLite (default) or Postgres (multi-replica deployments, see
/// `db::MetadataPool`).
#[derive(Clone)]
pub struct CheckpointStore {
    pool: MetadataPool,
}

impl CheckpointStore {
    /// Every query below is written with `?` placeholders as a `&'static`
    /// literal; `q()` rewrites them to `$1, $2, ...` when running against
    /// Postgres (see `db::rewrite_placeholders`). `commit`'s query embeds
    /// SQLite's `datetime('now')` inline and references the `offset` column
    /// (a reserved word in Postgres, needing `"offset"`) — that one doesn't
    /// go through `q()`, see its two-arm `match` instead.
    fn q(&self, sql: &'static str) -> std::borrow::Cow<'static, str> {
        rewrite_placeholders(sql, self.pool.is_postgres())
    }

    pub async fn connect(database_url: &str) -> anyhow::Result<Self> {
        let pool = MetadataPool::connect(database_url).await?;

        match &pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query(
                    r#"
                    CREATE TABLE IF NOT EXISTS checkpoints (
                        pipeline_id TEXT NOT NULL,
                        partition_id TEXT NOT NULL,
                        last_updated_at TEXT,
                        offset INTEGER,
                        updated_at TEXT NOT NULL DEFAULT (datetime('now')),
                        PRIMARY KEY (pipeline_id, partition_id)
                    )
                    "#,
                )
                .execute(p)
                .await?;
                // First schema migration in this codebase — `CREATE TABLE
                // IF NOT EXISTS` above is a no-op against a `checkpoints`
                // table that already existed before `resume_state` was
                // added, so a real `ALTER TABLE` is needed too. No
                // `IF NOT EXISTS` support for `ADD COLUMN` on older SQLite,
                // so this just runs it and ignores the error — the only
                // realistic failure mode right after `CREATE TABLE`
                // succeeded (proving connectivity) is "column already
                // exists" on a second/later boot.
                let _ = sqlx::query("ALTER TABLE checkpoints ADD COLUMN resume_state TEXT")
                    .execute(p)
                    .await;
            }
            MetadataPool::Postgres(p) => {
                // "offset" is a reserved word in Postgres — must be quoted.
                sqlx::query(
                    r#"
                    CREATE TABLE IF NOT EXISTS checkpoints (
                        pipeline_id TEXT NOT NULL,
                        partition_id TEXT NOT NULL,
                        last_updated_at TEXT,
                        "offset" BIGINT,
                        updated_at TEXT NOT NULL DEFAULT (to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')),
                        PRIMARY KEY (pipeline_id, partition_id)
                    )
                    "#,
                )
                .execute(p)
                .await?;
                // Postgres does support `ADD COLUMN IF NOT EXISTS` (unlike
                // SQLite) — real idempotent migration, no error-swallowing
                // needed here.
                sqlx::query("ALTER TABLE checkpoints ADD COLUMN IF NOT EXISTS resume_state TEXT")
                    .execute(p)
                    .await?;
            }
        }

        Ok(Self { pool })
    }

    /// Partitions already committed for this pipeline — the resume set to
    /// skip on the next run.
    pub async fn done_partitions(&self, pipeline_id: &str) -> anyhow::Result<HashSet<String>> {
        let sql = self.q("SELECT partition_id FROM checkpoints WHERE pipeline_id = ?");
        let rows: Vec<(String,)> = match &self.pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query_as(sqlx::AssertSqlSafe(sql))
                    .bind(pipeline_id)
                    .fetch_all(p)
                    .await?
            }
            MetadataPool::Postgres(p) => {
                sqlx::query_as(sqlx::AssertSqlSafe(sql))
                    .bind(pipeline_id)
                    .fetch_all(p)
                    .await?
            }
        };
        Ok(rows.into_iter().map(|(p,)| p).collect())
    }

    pub async fn commit(&self, pipeline_id: &str, cursor: &CheckpointCursor) -> anyhow::Result<()> {
        match &self.pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query(
                    r#"
                    INSERT INTO checkpoints (pipeline_id, partition_id, last_updated_at, offset, resume_state, updated_at)
                    VALUES (?, ?, ?, ?, ?, datetime('now'))
                    ON CONFLICT(pipeline_id, partition_id) DO UPDATE SET
                        last_updated_at = excluded.last_updated_at,
                        offset = excluded.offset,
                        resume_state = excluded.resume_state,
                        updated_at = excluded.updated_at
                    "#,
                )
                .bind(pipeline_id)
                .bind(&cursor.partition_id)
                .bind(cursor.last_updated_at.map(|t| t.to_rfc3339()))
                .bind(cursor.offset)
                .bind(&cursor.resume_state)
                .execute(p)
                .await?;
            }
            MetadataPool::Postgres(p) => {
                sqlx::query(
                    r#"
                    INSERT INTO checkpoints (pipeline_id, partition_id, last_updated_at, "offset", resume_state, updated_at)
                    VALUES ($1, $2, $3, $4, $5, to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS'))
                    ON CONFLICT(pipeline_id, partition_id) DO UPDATE SET
                        last_updated_at = excluded.last_updated_at,
                        "offset" = excluded."offset",
                        resume_state = excluded.resume_state,
                        updated_at = excluded.updated_at
                    "#,
                )
                .bind(pipeline_id)
                .bind(&cursor.partition_id)
                .bind(cursor.last_updated_at.map(|t| t.to_rfc3339()))
                .bind(cursor.offset)
                .bind(&cursor.resume_state)
                .execute(p)
                .await?;
            }
        }

        crate::server_metrics::record_checkpoint_commit(pipeline_id, &cursor.partition_id);
        Ok(())
    }

    /// Reads back the last committed cursor for `(pipeline_id,
    /// partition_id)`, if any — didn't exist before this: `done_partitions`
    /// only ever answered "was this partition ever finished" (a boolean),
    /// never "what position did it reach." Callers use this to resume a
    /// CDC source from where it left off (feeding `resume_state` back into
    /// that connector's config) instead of restarting from scratch.
    pub async fn get(
        &self,
        pipeline_id: &str,
        partition_id: &str,
    ) -> anyhow::Result<Option<CheckpointCursor>> {
        // `"offset"` quoted even in this single shared-dialect string (not
        // routed through the two-arm match `commit`, just above, uses for
        // its own `offset` references) — SQLite accepts a double-quoted
        // identifier same as an unquoted one, but Postgres *requires* the
        // quotes for this reserved word. Unquoted here silently broke every
        // CDC resume (this is the only place `resume_state` is read back)
        // against a Postgres metadata backend with "syntax error at or
        // near offset" — found testing postgres-cdc end to end against
        // this repo's own `nexus-test` compose, which points metadata at
        // Postgres.
        let sql = self.q(
            "SELECT last_updated_at, \"offset\", resume_state FROM checkpoints \
             WHERE pipeline_id = ? AND partition_id = ?",
        );
        let row: Option<(Option<String>, Option<i64>, Option<String>)> = match &self.pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query_as(sqlx::AssertSqlSafe(sql))
                    .bind(pipeline_id)
                    .bind(partition_id)
                    .fetch_optional(p)
                    .await?
            }
            MetadataPool::Postgres(p) => {
                sqlx::query_as(sqlx::AssertSqlSafe(sql))
                    .bind(pipeline_id)
                    .bind(partition_id)
                    .fetch_optional(p)
                    .await?
            }
        };

        Ok(
            row.map(|(last_updated_at, offset, resume_state)| CheckpointCursor {
                partition_id: partition_id.to_string(),
                last_updated_at: last_updated_at
                    .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
                    .map(|dt| dt.with_timezone(&chrono::Utc)),
                offset,
                opcode: None,
                resume_state,
            }),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn get_returns_none_before_any_commit() {
        let store = CheckpointStore::connect("sqlite::memory:").await.unwrap();
        assert!(store.get("pipe-1", "p0").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn get_round_trips_resume_state() {
        let store = CheckpointStore::connect("sqlite::memory:").await.unwrap();

        let cursor = CheckpointCursor {
            resume_state: Some("mysql-bin.000003:15926".to_string()),
            ..CheckpointCursor::new("p0")
        };
        store.commit("pipe-1", &cursor).await.unwrap();

        let fetched = store.get("pipe-1", "p0").await.unwrap().unwrap();
        assert_eq!(
            fetched.resume_state.as_deref(),
            Some("mysql-bin.000003:15926")
        );

        // Overwriting with a fresher position must replace, not append.
        let cursor2 = CheckpointCursor {
            resume_state: Some("mysql-bin.000003:30412".to_string()),
            ..CheckpointCursor::new("p0")
        };
        store.commit("pipe-1", &cursor2).await.unwrap();
        let fetched2 = store.get("pipe-1", "p0").await.unwrap().unwrap();
        assert_eq!(
            fetched2.resume_state.as_deref(),
            Some("mysql-bin.000003:30412")
        );
    }

    #[tokio::test]
    async fn get_is_scoped_per_pipeline_and_partition() {
        let store = CheckpointStore::connect("sqlite::memory:").await.unwrap();

        store
            .commit(
                "pipe-1",
                &CheckpointCursor {
                    resume_state: Some("token-a".to_string()),
                    ..CheckpointCursor::new("p0")
                },
            )
            .await
            .unwrap();

        assert!(store.get("pipe-2", "p0").await.unwrap().is_none());
        assert!(store.get("pipe-1", "p1").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn partition_is_done_only_after_commit() {
        let store = CheckpointStore::connect("sqlite::memory:").await.unwrap();

        assert!(!store
            .done_partitions("pipe-1")
            .await
            .unwrap()
            .contains("p0"));

        store
            .commit("pipe-1", &CheckpointCursor::new("p0"))
            .await
            .unwrap();

        let done = store.done_partitions("pipe-1").await.unwrap();
        assert!(done.contains("p0"));
        assert_eq!(done.len(), 1);
    }

    #[tokio::test]
    async fn commit_is_idempotent_upsert_not_duplicate_rows() {
        let store = CheckpointStore::connect("sqlite::memory:").await.unwrap();

        store
            .commit("pipe-1", &CheckpointCursor::new("p0"))
            .await
            .unwrap();
        store
            .commit("pipe-1", &CheckpointCursor::new("p0"))
            .await
            .unwrap();

        assert_eq!(store.done_partitions("pipe-1").await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn checkpoints_are_scoped_per_pipeline() {
        let store = CheckpointStore::connect("sqlite::memory:").await.unwrap();

        store
            .commit("pipe-1", &CheckpointCursor::new("p0"))
            .await
            .unwrap();

        assert!(store.done_partitions("pipe-2").await.unwrap().is_empty());
    }

    /// Proves the Postgres branch — most notably the quoted `"offset"`
    /// identifier (a reserved word in Postgres, unlike SQLite) in both the
    /// DDL and the upsert — behaves identically to the SQLite path above.
    #[tokio::test]
    async fn postgres_backend_upsert_and_done_partitions() {
        use testcontainers_modules::postgres;
        use testcontainers_modules::testcontainers::runners::AsyncRunner;

        let container = postgres::Postgres::default().start().await.unwrap();
        let host = container.get_host().await.unwrap();
        let port = container.get_host_port_ipv4(5432).await.unwrap();
        let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");

        let store = CheckpointStore::connect(&url).await.unwrap();
        assert!(matches!(store.pool, MetadataPool::Postgres(_)));

        assert!(!store
            .done_partitions("pipe-1")
            .await
            .unwrap()
            .contains("p0"));

        let cursor = CheckpointCursor {
            offset: Some(42),
            ..CheckpointCursor::new("p0")
        };
        store.commit("pipe-1", &cursor).await.unwrap();
        // Idempotent upsert — committing the same partition again must not
        // duplicate the row.
        store.commit("pipe-1", &cursor).await.unwrap();

        let done = store.done_partitions("pipe-1").await.unwrap();
        assert_eq!(done.len(), 1);
        assert!(done.contains("p0"));
        assert!(store.done_partitions("pipe-2").await.unwrap().is_empty());
    }
}
