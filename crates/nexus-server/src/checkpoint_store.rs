use nexus_core::CheckpointCursor;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool};
use std::collections::HashSet;
use std::str::FromStr;

/// Checkpoint is persisted per `(pipeline_id, partition_id)`, not per
/// pipeline — see ARCHITECTURE.md §5. This is Marco 1's actual persistence
/// layer; `Sink::commit_checkpoint` itself is a no-op for connectors, since
/// only nexus-server knows about run history across retries.
#[derive(Clone)]
pub struct CheckpointStore {
    pool: SqlitePool,
}

impl CheckpointStore {
    pub async fn connect(database_url: &str) -> anyhow::Result<Self> {
        let options = SqliteConnectOptions::from_str(database_url)?.create_if_missing(true);
        let pool = SqlitePool::connect_with(options).await?;

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
        .execute(&pool)
        .await?;

        Ok(Self { pool })
    }

    /// Partitions already committed for this pipeline — the resume set to
    /// skip on the next run.
    pub async fn done_partitions(&self, pipeline_id: &str) -> anyhow::Result<HashSet<String>> {
        let rows: Vec<(String,)> =
            sqlx::query_as("SELECT partition_id FROM checkpoints WHERE pipeline_id = ?")
                .bind(pipeline_id)
                .fetch_all(&self.pool)
                .await?;
        Ok(rows.into_iter().map(|(p,)| p).collect())
    }

    pub async fn commit(&self, pipeline_id: &str, cursor: &CheckpointCursor) -> anyhow::Result<()> {
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
        .execute(&self.pool)
        .await?;

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
}
