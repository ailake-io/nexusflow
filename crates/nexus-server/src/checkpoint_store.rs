use crate::db::{rewrite_placeholders, MetadataPool};
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
                    INSERT INTO checkpoints (pipeline_id, partition_id, last_updated_at, offset, updated_at)
                    VALUES (?, ?, ?, ?, datetime('now'))
                    ON CONFLICT(pipeline_id, partition_id) DO UPDATE SET
                        last_updated_at = excluded.last_updated_at,
                        offset = excluded.offset,
                        updated_at = excluded.updated_at
                    "#,
                )
                .bind(pipeline_id)
                .bind(&cursor.partition_id)
                .bind(cursor.last_updated_at.map(|t| t.to_rfc3339()))
                .bind(cursor.offset)
                .execute(p)
                .await?;
            }
            MetadataPool::Postgres(p) => {
                sqlx::query(
                    r#"
                    INSERT INTO checkpoints (pipeline_id, partition_id, last_updated_at, "offset", updated_at)
                    VALUES ($1, $2, $3, $4, to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS'))
                    ON CONFLICT(pipeline_id, partition_id) DO UPDATE SET
                        last_updated_at = excluded.last_updated_at,
                        "offset" = excluded."offset",
                        updated_at = excluded.updated_at
                    "#,
                )
                .bind(pipeline_id)
                .bind(&cursor.partition_id)
                .bind(cursor.last_updated_at.map(|t| t.to_rfc3339()))
                .bind(cursor.offset)
                .execute(p)
                .await?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
