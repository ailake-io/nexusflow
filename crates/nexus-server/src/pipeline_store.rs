use crate::crypto::SecretCipher;
use nexus_core::PipelineSpec;
use serde::Serialize;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool};
use std::str::FromStr;

#[derive(Debug, thiserror::Error)]
pub enum PipelineStoreError {
    #[error("pipeline {0:?} already exists")]
    AlreadyExists(String),
    #[error("pipeline {0:?} not found")]
    NotFound(String),
    #[error("stored pipeline is corrupt: {0}")]
    Corrupt(String),
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
}

#[derive(Serialize)]
pub struct NodeSummary {
    pub connector: String,
    pub name: Option<String>,
}

/// A pipeline as exposed over REST — connector *names* only, never the
/// config blob a node carries (that's where connector secrets live). See
/// CLAUDE.md §5 / IMPLEMENTATION_PLAN.md Marco 8 task #17: the API itself
/// never hands back a secret once persisted, not just the frontend.
#[derive(Serialize)]
pub struct PipelineSummary {
    pub pipeline_id: String,
    pub sources: Vec<NodeSummary>,
    pub sinks: Vec<NodeSummary>,
    pub has_transform: bool,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Serialize)]
pub struct RunRecord {
    pub id: i64,
    pub pipeline_id: String,
    pub started_at: String,
    pub finished_at: Option<String>,
    pub status: String,
    pub error: Option<String>,
    pub stats: Option<serde_json::Value>,
}

/// Persists `PipelineSpec`s (encrypted at rest, see `crypto.rs`) and the
/// run history recorded each time one executes — ARCHITECTURE.md §7 /
/// IMPLEMENTATION_PLAN.md Marco 7 task #8.
#[derive(Clone)]
pub struct PipelineStore {
    pool: SqlitePool,
}

impl PipelineStore {
    pub async fn connect(database_url: &str) -> anyhow::Result<Self> {
        let options = SqliteConnectOptions::from_str(database_url)?.create_if_missing(true);
        let pool = SqlitePool::connect_with(options).await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS pipelines (
                id TEXT PRIMARY KEY,
                spec_ciphertext TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
            "#,
        )
        .execute(&pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pipeline_id TEXT NOT NULL,
                started_at TEXT NOT NULL DEFAULT (datetime('now')),
                finished_at TEXT,
                status TEXT NOT NULL,
                error TEXT,
                stats_json TEXT
            )
            "#,
        )
        .execute(&pool)
        .await?;

        Ok(Self { pool })
    }

    pub async fn create(
        &self,
        spec: &PipelineSpec,
        cipher: &SecretCipher,
    ) -> Result<(), PipelineStoreError> {
        let existing: Option<(String,)> = sqlx::query_as("SELECT id FROM pipelines WHERE id = ?")
            .bind(&spec.pipeline_id)
            .fetch_optional(&self.pool)
            .await?;
        if existing.is_some() {
            return Err(PipelineStoreError::AlreadyExists(spec.pipeline_id.clone()));
        }

        let ciphertext = encode_spec(spec, cipher);
        sqlx::query("INSERT INTO pipelines (id, spec_ciphertext) VALUES (?, ?)")
            .bind(&spec.pipeline_id)
            .bind(&ciphertext)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn update(
        &self,
        id: &str,
        spec: &PipelineSpec,
        cipher: &SecretCipher,
    ) -> Result<(), PipelineStoreError> {
        let ciphertext = encode_spec(spec, cipher);
        let result = sqlx::query(
            "UPDATE pipelines SET spec_ciphertext = ?, updated_at = datetime('now') WHERE id = ?",
        )
        .bind(&ciphertext)
        .bind(id)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            return Err(PipelineStoreError::NotFound(id.to_string()));
        }
        Ok(())
    }

    pub async fn delete(&self, id: &str) -> Result<(), PipelineStoreError> {
        let result = sqlx::query("DELETE FROM pipelines WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await?;
        if result.rows_affected() == 0 {
            return Err(PipelineStoreError::NotFound(id.to_string()));
        }
        Ok(())
    }

    pub async fn get_summary(
        &self,
        id: &str,
        cipher: &SecretCipher,
    ) -> Result<PipelineSummary, PipelineStoreError> {
        let row: Option<(String, String, String)> = sqlx::query_as(
            "SELECT spec_ciphertext, created_at, updated_at FROM pipelines WHERE id = ?",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        let (ciphertext, created_at, updated_at) =
            row.ok_or_else(|| PipelineStoreError::NotFound(id.to_string()))?;
        let spec = decode_spec(&ciphertext, cipher)?;
        Ok(summarize(spec, created_at, updated_at))
    }

    pub async fn list_summaries(
        &self,
        cipher: &SecretCipher,
    ) -> Result<Vec<PipelineSummary>, PipelineStoreError> {
        let rows: Vec<(String, String, String)> = sqlx::query_as(
            "SELECT spec_ciphertext, created_at, updated_at FROM pipelines ORDER BY created_at",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|(ciphertext, created_at, updated_at)| {
                let spec = decode_spec(&ciphertext, cipher)?;
                Ok(summarize(spec, created_at, updated_at))
            })
            .collect()
    }

    /// Called right before a pipeline starts executing — returns the new
    /// run's id, to be closed out via `finish_run_success`/`finish_run_failure`.
    pub async fn start_run(&self, pipeline_id: &str) -> Result<i64, PipelineStoreError> {
        let result =
            sqlx::query("INSERT INTO pipeline_runs (pipeline_id, status) VALUES (?, 'running')")
                .bind(pipeline_id)
                .execute(&self.pool)
                .await?;
        Ok(result.last_insert_rowid())
    }

    pub async fn finish_run_success(
        &self,
        run_id: i64,
        stats: &[nexus_core::PartitionStats],
    ) -> Result<(), PipelineStoreError> {
        let stats_json =
            serde_json::to_string(stats).map_err(|e| PipelineStoreError::Corrupt(e.to_string()))?;
        sqlx::query(
            "UPDATE pipeline_runs SET finished_at = datetime('now'), status = 'success', \
             stats_json = ? WHERE id = ?",
        )
        .bind(&stats_json)
        .bind(run_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn finish_run_failure(
        &self,
        run_id: i64,
        error: &str,
    ) -> Result<(), PipelineStoreError> {
        sqlx::query(
            "UPDATE pipeline_runs SET finished_at = datetime('now'), status = 'failed', \
             error = ? WHERE id = ?",
        )
        .bind(error)
        .bind(run_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn list_runs(&self, pipeline_id: &str) -> Result<Vec<RunRecord>, PipelineStoreError> {
        type RunRow = (
            i64,
            String,
            String,
            Option<String>,
            String,
            Option<String>,
            Option<String>,
        );
        let rows: Vec<RunRow> = sqlx::query_as(
            "SELECT id, pipeline_id, started_at, finished_at, status, error, stats_json \
                 FROM pipeline_runs WHERE pipeline_id = ? ORDER BY started_at DESC",
        )
        .bind(pipeline_id)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(
                |(id, pipeline_id, started_at, finished_at, status, error, stats_json)| {
                    let stats = stats_json
                        .map(|s| {
                            serde_json::from_str(&s)
                                .map_err(|e| PipelineStoreError::Corrupt(e.to_string()))
                        })
                        .transpose()?;
                    Ok(RunRecord {
                        id,
                        pipeline_id,
                        started_at,
                        finished_at,
                        status,
                        error,
                        stats,
                    })
                },
            )
            .collect()
    }
}

fn encode_spec(spec: &PipelineSpec, cipher: &SecretCipher) -> String {
    let json = serde_json::to_string(spec).expect("PipelineSpec always serializes");
    cipher.encrypt(&json)
}

fn decode_spec(
    ciphertext: &str,
    cipher: &SecretCipher,
) -> Result<PipelineSpec, PipelineStoreError> {
    let json = cipher
        .decrypt(ciphertext)
        .map_err(|e| PipelineStoreError::Corrupt(e.to_string()))?;
    serde_json::from_str(&json).map_err(|e| PipelineStoreError::Corrupt(e.to_string()))
}

fn summarize(spec: PipelineSpec, created_at: String, updated_at: String) -> PipelineSummary {
    let to_summary = |n: nexus_core::NodeSpec| NodeSummary {
        connector: n.connector,
        name: n.name,
    };
    PipelineSummary {
        pipeline_id: spec.pipeline_id,
        sources: spec.sources.into_iter().map(to_summary).collect(),
        sinks: spec.sinks.into_iter().map(to_summary).collect(),
        has_transform: spec.transform.is_some(),
        created_at,
        updated_at,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexus_core::NodeSpec;

    fn cipher() -> SecretCipher {
        SecretCipher::from_hex_key(&"ab".repeat(32)).unwrap()
    }

    fn sample_spec(id: &str) -> PipelineSpec {
        PipelineSpec {
            pipeline_id: id.to_string(),
            sources: vec![NodeSpec {
                name: None,
                connector: "postgres".to_string(),
                config: serde_json::json!({"uri": "postgres://user:pw@host/db"}),
            }],
            transform: None,
            sinks: vec![NodeSpec {
                name: None,
                connector: "sqlite".to_string(),
                config: serde_json::json!({"path": "/tmp/out.db"}),
            }],
            channel_capacity: 100,
            partitions: 1,
        }
    }

    #[test]
    fn encode_decode_round_trips_arbitrary_config_content() {
        let cipher = cipher();
        let spec = sample_spec("p1");

        let ciphertext = encode_spec(&spec, &cipher);
        let decoded = decode_spec(&ciphertext, &cipher).unwrap();

        assert_eq!(
            decoded.sources[0].config["uri"],
            "postgres://user:pw@host/db"
        );
    }

    #[tokio::test]
    async fn creating_duplicate_id_fails() {
        let store = PipelineStore::connect("sqlite::memory:").await.unwrap();
        let cipher = cipher();
        let spec = sample_spec("p1");

        store.create(&spec, &cipher).await.unwrap();
        assert!(matches!(
            store.create(&spec, &cipher).await,
            Err(PipelineStoreError::AlreadyExists(_))
        ));
    }

    #[tokio::test]
    async fn summary_never_contains_config() {
        let store = PipelineStore::connect("sqlite::memory:").await.unwrap();
        let cipher = cipher();
        store.create(&sample_spec("p1"), &cipher).await.unwrap();

        let summary = store.get_summary("p1", &cipher).await.unwrap();
        assert_eq!(summary.sources[0].connector, "postgres");
        assert!(!summary.has_transform);

        let list = store.list_summaries(&cipher).await.unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].pipeline_id, "p1");
    }

    #[tokio::test]
    async fn spec_is_encrypted_at_rest_not_plaintext() {
        let store = PipelineStore::connect("sqlite::memory:").await.unwrap();
        let cipher = cipher();
        store.create(&sample_spec("p1"), &cipher).await.unwrap();

        let (raw,): (String,) =
            sqlx::query_as("SELECT spec_ciphertext FROM pipelines WHERE id = ?")
                .bind("p1")
                .fetch_one(&store.pool)
                .await
                .unwrap();
        assert!(!raw.contains("postgres://user:pw@host/db"));
        assert!(!raw.contains("postgres"));
    }

    #[tokio::test]
    async fn update_replaces_spec() {
        let store = PipelineStore::connect("sqlite::memory:").await.unwrap();
        let cipher = cipher();
        store.create(&sample_spec("p1"), &cipher).await.unwrap();

        let mut updated = sample_spec("p1");
        updated.sinks[0].connector = "postgres".to_string();
        store.update("p1", &updated, &cipher).await.unwrap();

        let summary = store.get_summary("p1", &cipher).await.unwrap();
        assert_eq!(summary.sinks[0].connector, "postgres");
    }

    #[tokio::test]
    async fn update_on_missing_id_fails() {
        let store = PipelineStore::connect("sqlite::memory:").await.unwrap();
        let cipher = cipher();
        assert!(matches!(
            store
                .update("missing", &sample_spec("missing"), &cipher)
                .await,
            Err(PipelineStoreError::NotFound(_))
        ));
    }

    #[tokio::test]
    async fn delete_removes_pipeline() {
        let store = PipelineStore::connect("sqlite::memory:").await.unwrap();
        let cipher = cipher();
        store.create(&sample_spec("p1"), &cipher).await.unwrap();

        store.delete("p1").await.unwrap();
        assert!(matches!(
            store.get_summary("p1", &cipher).await,
            Err(PipelineStoreError::NotFound(_))
        ));
    }

    #[tokio::test]
    async fn delete_on_missing_id_fails() {
        let store = PipelineStore::connect("sqlite::memory:").await.unwrap();
        assert!(matches!(
            store.delete("missing").await,
            Err(PipelineStoreError::NotFound(_))
        ));
    }

    #[tokio::test]
    async fn run_lifecycle_success_is_recorded() {
        let store = PipelineStore::connect("sqlite::memory:").await.unwrap();
        let run_id = store.start_run("p1").await.unwrap();

        let runs_before = store.list_runs("p1").await.unwrap();
        assert_eq!(runs_before[0].status, "running");
        assert!(runs_before[0].finished_at.is_none());

        let stats = vec![nexus_core::PartitionStats {
            partition_id: "p0".to_string(),
            batches_written: 3,
            rows_written: 100,
        }];
        store.finish_run_success(run_id, &stats).await.unwrap();

        let runs = store.list_runs("p1").await.unwrap();
        assert_eq!(runs[0].status, "success");
        assert!(runs[0].finished_at.is_some());
        assert_eq!(runs[0].stats.as_ref().unwrap()[0]["rows_written"], 100);
    }

    #[tokio::test]
    async fn run_lifecycle_failure_is_recorded() {
        let store = PipelineStore::connect("sqlite::memory:").await.unwrap();
        let run_id = store.start_run("p1").await.unwrap();
        store
            .finish_run_failure(run_id, "connector unreachable")
            .await
            .unwrap();

        let runs = store.list_runs("p1").await.unwrap();
        assert_eq!(runs[0].status, "failed");
        assert_eq!(runs[0].error.as_deref(), Some("connector unreachable"));
    }

    #[tokio::test]
    async fn runs_are_scoped_per_pipeline() {
        let store = PipelineStore::connect("sqlite::memory:").await.unwrap();
        store.start_run("p1").await.unwrap();
        store.start_run("p2").await.unwrap();

        assert_eq!(store.list_runs("p1").await.unwrap().len(), 1);
        assert_eq!(store.list_runs("p2").await.unwrap().len(), 1);
    }
}
