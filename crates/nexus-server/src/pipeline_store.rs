use crate::crypto::SecretCipher;
use crate::db::{rewrite_placeholders, MetadataPool};
use nexus_core::PipelineSpec;
use serde::Serialize;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use std::borrow::Cow;
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

/// (spec_ciphertext, created_at, updated_at, last_run status, last_run started_at)
/// — the row shape shared by `get_summary`'s and `list_summaries`' LEFT JOIN
/// against the most recent `pipeline_runs` row per pipeline.
type SummaryRow = (String, String, String, Option<String>, Option<String>);

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
    /// Cron expression, if this pipeline has an automatic schedule (see
    /// `scheduler.rs`) — `None` means it only runs when explicitly
    /// triggered via `POST /pipelines/{id}/run`.
    pub schedule: Option<String>,
    /// Status of the most recent run ("running" / "success" / "failed"),
    /// `None` if it has never run — lets the Pipelines list show at a
    /// glance which scheduled/manual runs are healthy.
    pub last_run_status: Option<String>,
    pub last_run_at: Option<String>,
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
    /// dbt outcome summary (Marco 10 task #26's UI panel) — `None` when the
    /// pipeline has no `dbt` step, or that build lacks the "dbt" feature.
    pub dbt_summary: Option<serde_json::Value>,
}

/// Persists `PipelineSpec`s (encrypted at rest, see `crypto.rs`) and the
/// run history recorded each time one executes — ARCHITECTURE.md §7 /
/// IMPLEMENTATION_PLAN.md Marco 7 task #8. Backed by SQLite (default) or
/// Postgres (multi-replica deployments, see `db::MetadataPool`).
#[derive(Clone)]
pub struct PipelineStore {
    pool: MetadataPool,
}

impl PipelineStore {
    /// Exposes the backend for `scheduler.rs`'s leader election
    /// (`pg_try_advisory_lock` needs a real `PgPool`, see `db::MetadataPool`).
    pub fn pool(&self) -> &MetadataPool {
        &self.pool
    }

    /// Every query below is written with `?` placeholders as a `&'static`
    /// literal; `q()` rewrites them to `$1, $2, ...` when running against
    /// Postgres (see `db::rewrite_placeholders`). Queries that embed
    /// SQLite's `datetime('now')` function call inline (not just as a
    /// column default) need a genuinely different literal per backend —
    /// those don't go through `q()`, see the two-arm `match` at each such
    /// call site instead.
    fn q(&self, sql: &'static str) -> Cow<'static, str> {
        rewrite_placeholders(sql, self.pool.is_postgres())
    }

    pub async fn connect(database_url: &str) -> anyhow::Result<Self> {
        let is_postgres =
            database_url.starts_with("postgres://") || database_url.starts_with("postgresql://");
        // An in-memory SQLite database is private to its connection: with an
        // unbounded pool, two concurrent acquires would open *separate*
        // empty databases. Runs now execute in background supervisor tasks
        // (POST /run returns 202 immediately), so a supervisor and a
        // concurrent request can hold connections at the same time — cap
        // the pool at one connection so every user of the store shares the
        // same in-memory database. Postgres has no such concept (no
        // `:memory:`), so this branch is SQLite-only.
        let pool = if !is_postgres && database_url.contains(":memory:") {
            let options = SqliteConnectOptions::from_str(database_url)?.create_if_missing(true);
            let sqlite_pool = SqlitePoolOptions::new()
                .max_connections(1)
                .connect_with(options)
                .await?;
            MetadataPool::Sqlite(sqlite_pool)
        } else {
            MetadataPool::connect(database_url).await?
        };

        match &pool {
            MetadataPool::Sqlite(p) => {
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
                .execute(p)
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
                        stats_json TEXT,
                        dbt_summary_json TEXT
                    )
                    "#,
                )
                .execute(p)
                .await?;
            }
            MetadataPool::Postgres(p) => {
                sqlx::query(
                    r#"
                    CREATE TABLE IF NOT EXISTS pipelines (
                        id TEXT PRIMARY KEY,
                        spec_ciphertext TEXT NOT NULL,
                        created_at TEXT NOT NULL DEFAULT (to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')),
                        updated_at TEXT NOT NULL DEFAULT (to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS'))
                    )
                    "#,
                )
                .execute(p)
                .await?;

                sqlx::query(
                    r#"
                    CREATE TABLE IF NOT EXISTS pipeline_runs (
                        id BIGSERIAL PRIMARY KEY,
                        pipeline_id TEXT NOT NULL,
                        started_at TEXT NOT NULL DEFAULT (to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')),
                        finished_at TEXT,
                        status TEXT NOT NULL,
                        error TEXT,
                        stats_json TEXT,
                        dbt_summary_json TEXT
                    )
                    "#,
                )
                .execute(p)
                .await?;
            }
        }

        Ok(Self { pool })
    }

    pub async fn create(
        &self,
        spec: &PipelineSpec,
        cipher: &SecretCipher,
    ) -> Result<(), PipelineStoreError> {
        let sql = self.q("SELECT id FROM pipelines WHERE id = ?");
        let existing: Option<(String,)> = match &self.pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query_as(sqlx::AssertSqlSafe(sql))
                    .bind(&spec.pipeline_id)
                    .fetch_optional(p)
                    .await?
            }
            MetadataPool::Postgres(p) => {
                sqlx::query_as(sqlx::AssertSqlSafe(sql))
                    .bind(&spec.pipeline_id)
                    .fetch_optional(p)
                    .await?
            }
        };
        if existing.is_some() {
            return Err(PipelineStoreError::AlreadyExists(spec.pipeline_id.clone()));
        }

        let ciphertext = encode_spec(spec, cipher);
        let sql = self.q("INSERT INTO pipelines (id, spec_ciphertext) VALUES (?, ?)");
        match &self.pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query(sqlx::AssertSqlSafe(sql))
                    .bind(&spec.pipeline_id)
                    .bind(&ciphertext)
                    .execute(p)
                    .await?;
            }
            MetadataPool::Postgres(p) => {
                sqlx::query(sqlx::AssertSqlSafe(sql))
                    .bind(&spec.pipeline_id)
                    .bind(&ciphertext)
                    .execute(p)
                    .await?;
            }
        }
        Ok(())
    }

    pub async fn update(
        &self,
        id: &str,
        spec: &PipelineSpec,
        cipher: &SecretCipher,
    ) -> Result<(), PipelineStoreError> {
        let ciphertext = encode_spec(spec, cipher);
        let rows_affected = match &self.pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query(sqlx::AssertSqlSafe(self.q(
                    "UPDATE pipelines SET spec_ciphertext = ?, updated_at = datetime('now') WHERE id = ?",
                )))
                .bind(&ciphertext)
                .bind(id)
                .execute(p)
                .await?
                .rows_affected()
            }
            MetadataPool::Postgres(p) => {
                sqlx::query(sqlx::AssertSqlSafe(self.q(
                    "UPDATE pipelines SET spec_ciphertext = ?, updated_at = (to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')) WHERE id = ?",
                )))
                .bind(&ciphertext)
                .bind(id)
                .execute(p)
                .await?
                .rows_affected()
            }
        };
        if rows_affected == 0 {
            return Err(PipelineStoreError::NotFound(id.to_string()));
        }
        Ok(())
    }

    pub async fn delete(&self, id: &str) -> Result<(), PipelineStoreError> {
        let sql = self.q("DELETE FROM pipelines WHERE id = ?");
        let rows_affected = match &self.pool {
            MetadataPool::Sqlite(p) => sqlx::query(sqlx::AssertSqlSafe(sql))
                .bind(id)
                .execute(p)
                .await?
                .rows_affected(),
            MetadataPool::Postgres(p) => sqlx::query(sqlx::AssertSqlSafe(sql))
                .bind(id)
                .execute(p)
                .await?
                .rows_affected(),
        };
        if rows_affected == 0 {
            return Err(PipelineStoreError::NotFound(id.to_string()));
        }
        Ok(())
    }

    pub async fn get_summary(
        &self,
        id: &str,
        cipher: &SecretCipher,
    ) -> Result<PipelineSummary, PipelineStoreError> {
        let sql = self.q(
            "SELECT p.spec_ciphertext, p.created_at, p.updated_at, r.status, r.started_at \
             FROM pipelines p LEFT JOIN pipeline_runs r ON r.id = ( \
                 SELECT id FROM pipeline_runs WHERE pipeline_id = p.id \
                 ORDER BY started_at DESC LIMIT 1 \
             ) WHERE p.id = ?",
        );
        let row: Option<SummaryRow> = match &self.pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query_as(sqlx::AssertSqlSafe(sql))
                    .bind(id)
                    .fetch_optional(p)
                    .await?
            }
            MetadataPool::Postgres(p) => {
                sqlx::query_as(sqlx::AssertSqlSafe(sql))
                    .bind(id)
                    .fetch_optional(p)
                    .await?
            }
        };
        let (ciphertext, created_at, updated_at, last_run_status, last_run_at) =
            row.ok_or_else(|| PipelineStoreError::NotFound(id.to_string()))?;
        let spec = decode_spec(&ciphertext, cipher)?;
        Ok(summarize(
            spec,
            created_at,
            updated_at,
            last_run_status,
            last_run_at,
        ))
    }

    /// Full decrypted spec, config blobs and all — for internal use only
    /// (the scheduler needs the real connector configs to actually run a
    /// pipeline). Never expose this over the API; `get_summary` is what
    /// `GET /pipelines/{id}` uses instead.
    pub async fn get_spec(
        &self,
        id: &str,
        cipher: &SecretCipher,
    ) -> Result<PipelineSpec, PipelineStoreError> {
        let sql = self.q("SELECT spec_ciphertext FROM pipelines WHERE id = ?");
        let row: Option<(String,)> = match &self.pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query_as(sqlx::AssertSqlSafe(sql))
                    .bind(id)
                    .fetch_optional(p)
                    .await?
            }
            MetadataPool::Postgres(p) => {
                sqlx::query_as(sqlx::AssertSqlSafe(sql))
                    .bind(id)
                    .fetch_optional(p)
                    .await?
            }
        };
        let (ciphertext,) = row.ok_or_else(|| PipelineStoreError::NotFound(id.to_string()))?;
        decode_spec(&ciphertext, cipher)
    }

    pub async fn list_summaries(
        &self,
        cipher: &SecretCipher,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<PipelineSummary>, PipelineStoreError> {
        let sql = self.q(
            "SELECT p.spec_ciphertext, p.created_at, p.updated_at, r.status, r.started_at \
             FROM pipelines p LEFT JOIN pipeline_runs r ON r.id = ( \
                 SELECT id FROM pipeline_runs WHERE pipeline_id = p.id \
                 ORDER BY started_at DESC LIMIT 1 \
             ) ORDER BY p.created_at LIMIT ? OFFSET ?",
        );
        let rows: Vec<SummaryRow> = match &self.pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query_as(sqlx::AssertSqlSafe(sql))
                    .bind(limit)
                    .bind(offset)
                    .fetch_all(p)
                    .await?
            }
            MetadataPool::Postgres(p) => {
                sqlx::query_as(sqlx::AssertSqlSafe(sql))
                    .bind(limit)
                    .bind(offset)
                    .fetch_all(p)
                    .await?
            }
        };
        rows.into_iter()
            .map(
                |(ciphertext, created_at, updated_at, last_run_status, last_run_at)| {
                    let spec = decode_spec(&ciphertext, cipher)?;
                    Ok(summarize(
                        spec,
                        created_at,
                        updated_at,
                        last_run_status,
                        last_run_at,
                    ))
                },
            )
            .collect()
    }

    /// Called right before a pipeline starts executing — returns the new
    /// run's id, to be closed out via `finish_run_success`/`finish_run_failure`.
    /// SQLite exposes the id via `last_insert_rowid()`; Postgres has no such
    /// call, so that branch uses `INSERT ... RETURNING id` instead.
    pub async fn start_run(&self, pipeline_id: &str) -> Result<i64, PipelineStoreError> {
        match &self.pool {
            MetadataPool::Sqlite(p) => {
                let sql =
                    self.q("INSERT INTO pipeline_runs (pipeline_id, status) VALUES (?, 'running')");
                let result = sqlx::query(sqlx::AssertSqlSafe(sql))
                    .bind(pipeline_id)
                    .execute(p)
                    .await?;
                Ok(result.last_insert_rowid())
            }
            MetadataPool::Postgres(p) => {
                let sql = self.q(
                    "INSERT INTO pipeline_runs (pipeline_id, status) VALUES (?, 'running') RETURNING id",
                );
                let id: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(sql))
                    .bind(pipeline_id)
                    .fetch_one(p)
                    .await?;
                Ok(id)
            }
        }
    }

    pub async fn finish_run_success(
        &self,
        run_id: i64,
        stats: &[nexus_core::PartitionStats],
        dbt_summary: Option<&serde_json::Value>,
    ) -> Result<(), PipelineStoreError> {
        let stats_json =
            serde_json::to_string(stats).map_err(|e| PipelineStoreError::Corrupt(e.to_string()))?;
        let dbt_summary_json = dbt_summary
            .map(serde_json::to_string)
            .transpose()
            .map_err(|e| PipelineStoreError::Corrupt(e.to_string()))?;
        match &self.pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query(sqlx::AssertSqlSafe(self.q(
                    "UPDATE pipeline_runs SET finished_at = datetime('now'), status = 'success', \
                     stats_json = ?, dbt_summary_json = ? WHERE id = ?",
                )))
                .bind(&stats_json)
                .bind(&dbt_summary_json)
                .bind(run_id)
                .execute(p)
                .await?;
            }
            MetadataPool::Postgres(p) => {
                sqlx::query(sqlx::AssertSqlSafe(self.q(
                    "UPDATE pipeline_runs SET finished_at = (to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')), status = 'success', \
                     stats_json = ?, dbt_summary_json = ? WHERE id = ?",
                )))
                .bind(&stats_json)
                .bind(&dbt_summary_json)
                .bind(run_id)
                .execute(p)
                .await?;
            }
        }
        Ok(())
    }

    pub async fn finish_run_failure(
        &self,
        run_id: i64,
        error: &str,
    ) -> Result<(), PipelineStoreError> {
        match &self.pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query(sqlx::AssertSqlSafe(self.q(
                    "UPDATE pipeline_runs SET finished_at = datetime('now'), status = 'failed', \
                     error = ? WHERE id = ?",
                )))
                .bind(error)
                .bind(run_id)
                .execute(p)
                .await?;
            }
            MetadataPool::Postgres(p) => {
                sqlx::query(sqlx::AssertSqlSafe(self.q(
                    "UPDATE pipeline_runs SET finished_at = (to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')), status = 'failed', \
                     error = ? WHERE id = ?",
                )))
                .bind(error)
                .bind(run_id)
                .execute(p)
                .await?;
            }
        }
        Ok(())
    }

    /// Returns true if there is at least one run for `pipeline_id` whose
    /// status is still `'running'`. Used by the manual run handler to reject
    /// overlapping manual runs with 409 Conflict (A02); the scheduler already
    /// avoids overlap on its own, so this check is only applied there.
    pub async fn has_running_run(&self, pipeline_id: &str) -> Result<bool, PipelineStoreError> {
        let sql = self
            .q("SELECT 1 FROM pipeline_runs WHERE pipeline_id = ? AND status = 'running' LIMIT 1");
        let row: Option<(i32,)> = match &self.pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query_as(sqlx::AssertSqlSafe(sql))
                    .bind(pipeline_id)
                    .fetch_optional(p)
                    .await?
            }
            MetadataPool::Postgres(p) => {
                sqlx::query_as(sqlx::AssertSqlSafe(sql))
                    .bind(pipeline_id)
                    .fetch_optional(p)
                    .await?
            }
        };
        Ok(row.is_some())
    }

    /// Boot-time reaper: any run still marked 'running' when the process
    /// starts belongs to a dead process — while it lives, a run's
    /// supervisor task always records its terminal state (see lib.rs'
    /// `execute_pipeline_run`). Left alone, such a row would make the
    /// scheduler skip that pipeline forever (it never overlaps a run with
    /// itself). Returns how many rows were reaped.
    pub async fn fail_interrupted_runs(&self) -> Result<u64, PipelineStoreError> {
        let rows_affected = match &self.pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query(
                    "UPDATE pipeline_runs SET finished_at = datetime('now'), status = 'failed', \
                     error = 'server process ended before this run completed' WHERE status = 'running'",
                )
                .execute(p)
                .await?
                .rows_affected()
            }
            MetadataPool::Postgres(p) => {
                sqlx::query(
                    "UPDATE pipeline_runs SET finished_at = (to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')), status = 'failed', \
                     error = 'server process ended before this run completed' WHERE status = 'running'",
                )
                .execute(p)
                .await?
                .rows_affected()
            }
        };
        Ok(rows_affected)
    }

    pub async fn list_runs(
        &self,
        pipeline_id: &str,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<RunRecord>, PipelineStoreError> {
        type RunRow = (
            i64,
            String,
            String,
            Option<String>,
            String,
            Option<String>,
            Option<String>,
            Option<String>,
        );
        let sql = self.q(
            "SELECT id, pipeline_id, started_at, finished_at, status, error, stats_json, \
                 dbt_summary_json FROM pipeline_runs WHERE pipeline_id = ? \
                 ORDER BY started_at DESC LIMIT ? OFFSET ?",
        );
        let rows: Vec<RunRow> = match &self.pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query_as(sqlx::AssertSqlSafe(sql))
                    .bind(pipeline_id)
                    .bind(limit)
                    .bind(offset)
                    .fetch_all(p)
                    .await?
            }
            MetadataPool::Postgres(p) => {
                sqlx::query_as(sqlx::AssertSqlSafe(sql))
                    .bind(pipeline_id)
                    .bind(limit)
                    .bind(offset)
                    .fetch_all(p)
                    .await?
            }
        };

        rows.into_iter()
            .map(
                |(
                    id,
                    pipeline_id,
                    started_at,
                    finished_at,
                    status,
                    error,
                    stats_json,
                    dbt_summary_json,
                )| {
                    let stats = stats_json
                        .map(|s| {
                            serde_json::from_str(&s)
                                .map_err(|e| PipelineStoreError::Corrupt(e.to_string()))
                        })
                        .transpose()?;
                    let dbt_summary = dbt_summary_json
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
                        dbt_summary,
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

fn summarize(
    spec: PipelineSpec,
    created_at: String,
    updated_at: String,
    last_run_status: Option<String>,
    last_run_at: Option<String>,
) -> PipelineSummary {
    let to_summary = |n: nexus_core::NodeSpec| NodeSummary {
        connector: n.connector,
        name: n.name,
    };
    PipelineSummary {
        pipeline_id: spec.pipeline_id,
        has_transform: spec.transform.is_some(),
        schedule: spec.schedule,
        sources: spec.sources.into_iter().map(to_summary).collect(),
        sinks: spec.sinks.into_iter().map(to_summary).collect(),
        created_at,
        updated_at,
        last_run_status,
        last_run_at,
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
            embedding: None,
            channel_capacity: 100,
            partitions: 1,
            dbt: None,
            post_dbt_sinks: Vec::new(),
            schedule: None,
            draft: false,
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

        let list = store.list_summaries(&cipher, 100, 0).await.unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].pipeline_id, "p1");
    }

    #[tokio::test]
    async fn spec_is_encrypted_at_rest_not_plaintext() {
        let store = PipelineStore::connect("sqlite::memory:").await.unwrap();
        let cipher = cipher();
        store.create(&sample_spec("p1"), &cipher).await.unwrap();

        let MetadataPool::Sqlite(pool) = &store.pool else {
            unreachable!("this test always connects via sqlite::memory:")
        };
        let (raw,): (String,) =
            sqlx::query_as("SELECT spec_ciphertext FROM pipelines WHERE id = ?")
                .bind("p1")
                .fetch_one(pool)
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

        let runs_before = store.list_runs("p1", 100, 0).await.unwrap();
        assert_eq!(runs_before[0].status, "running");
        assert!(runs_before[0].finished_at.is_none());

        let stats = vec![nexus_core::PartitionStats {
            partition_id: "p0".to_string(),
            batches_written: 3,
            rows_written: 100,
        }];
        store
            .finish_run_success(run_id, &stats, None)
            .await
            .unwrap();

        let runs = store.list_runs("p1", 100, 0).await.unwrap();
        assert_eq!(runs[0].status, "success");
        assert!(runs[0].finished_at.is_some());
        assert_eq!(runs[0].stats.as_ref().unwrap()[0]["rows_written"], 100);
    }

    #[tokio::test]
    async fn run_lifecycle_records_dbt_summary_when_present() {
        let store = PipelineStore::connect("sqlite::memory:").await.unwrap();
        let run_id = store.start_run("p1").await.unwrap();

        let dbt_summary = serde_json::json!({
            "command": "build",
            "models_total": 2,
            "tests_total": 3,
            "tests_passed": 2,
        });
        store
            .finish_run_success(run_id, &[], Some(&dbt_summary))
            .await
            .unwrap();

        let runs = store.list_runs("p1", 100, 0).await.unwrap();
        assert_eq!(runs[0].dbt_summary.as_ref().unwrap()["tests_passed"], 2);
    }

    #[tokio::test]
    async fn run_lifecycle_failure_is_recorded() {
        let store = PipelineStore::connect("sqlite::memory:").await.unwrap();
        let run_id = store.start_run("p1").await.unwrap();
        store
            .finish_run_failure(run_id, "connector unreachable")
            .await
            .unwrap();

        let runs = store.list_runs("p1", 100, 0).await.unwrap();
        assert_eq!(runs[0].status, "failed");
        assert_eq!(runs[0].error.as_deref(), Some("connector unreachable"));
    }

    #[tokio::test]
    async fn interrupted_runs_are_reaped_as_failed() {
        let store = PipelineStore::connect("sqlite::memory:").await.unwrap();
        let stale_id = store.start_run("p1").await.unwrap();
        let failed_id = store.start_run("p1").await.unwrap();
        store.finish_run_failure(failed_id, "boom").await.unwrap();
        let success_id = store.start_run("p1").await.unwrap();
        store
            .finish_run_success(success_id, &[], None)
            .await
            .unwrap();

        let reaped = store.fail_interrupted_runs().await.unwrap();
        assert_eq!(reaped, 1, "only the still-'running' row is reaped");

        let runs = store.list_runs("p1", 100, 0).await.unwrap();
        let stale = runs.iter().find(|r| r.id == stale_id).unwrap();
        assert_eq!(stale.status, "failed");
        assert!(stale.finished_at.is_some());
        assert_eq!(
            stale.error.as_deref(),
            Some("server process ended before this run completed")
        );

        // Already-finished rows are untouched by the reaper.
        let success = runs.iter().find(|r| r.id == success_id).unwrap();
        assert_eq!(success.status, "success");
        let failed = runs.iter().find(|r| r.id == failed_id).unwrap();
        assert_eq!(failed.error.as_deref(), Some("boom"));
    }

    #[tokio::test]
    async fn runs_are_scoped_per_pipeline() {
        let store = PipelineStore::connect("sqlite::memory:").await.unwrap();
        store.start_run("p1").await.unwrap();
        store.start_run("p2").await.unwrap();

        assert_eq!(store.list_runs("p1", 100, 0).await.unwrap().len(), 1);
        assert_eq!(store.list_runs("p2", 100, 0).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn has_running_run_detects_active_run() {
        let store = PipelineStore::connect("sqlite::memory:").await.unwrap();
        assert!(!store.has_running_run("p1").await.unwrap());

        let run_id = store.start_run("p1").await.unwrap();
        assert!(store.has_running_run("p1").await.unwrap());

        store.finish_run_success(run_id, &[], None).await.unwrap();
        assert!(!store.has_running_run("p1").await.unwrap());
    }

    /// Proves the Postgres branch — most notably `start_run`'s `INSERT ...
    /// RETURNING id` path (SQLite instead uses `last_insert_rowid()`) — is
    /// behaviorally identical to the SQLite path already covered above.
    #[tokio::test]
    async fn postgres_backend_supports_full_lifecycle() {
        use testcontainers_modules::postgres;
        use testcontainers_modules::testcontainers::runners::AsyncRunner;

        let container = postgres::Postgres::default().start().await.unwrap();
        let host = container.get_host().await.unwrap();
        let port = container.get_host_port_ipv4(5432).await.unwrap();
        let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");

        let store = PipelineStore::connect(&url).await.unwrap();
        assert!(matches!(store.pool, MetadataPool::Postgres(_)));
        let cipher = cipher();

        store.create(&sample_spec("p1"), &cipher).await.unwrap();
        assert!(matches!(
            store.create(&sample_spec("p1"), &cipher).await,
            Err(PipelineStoreError::AlreadyExists(_))
        ));

        let summary = store.get_summary("p1", &cipher).await.unwrap();
        assert_eq!(summary.sources[0].connector, "postgres");

        let mut updated = sample_spec("p1");
        updated.sinks[0].connector = "postgres".to_string();
        store.update("p1", &updated, &cipher).await.unwrap();
        assert_eq!(
            store.get_summary("p1", &cipher).await.unwrap().sinks[0].connector,
            "postgres"
        );

        // `start_run` twice — RETURNING must hand back two distinct,
        // increasing ids just like SQLite's last_insert_rowid() would.
        let run_id_1 = store.start_run("p1").await.unwrap();
        let run_id_2 = store.start_run("p1").await.unwrap();
        assert!(run_id_2 > run_id_1);

        let stats = vec![nexus_core::PartitionStats {
            partition_id: "p0".to_string(),
            batches_written: 1,
            rows_written: 42,
        }];
        store
            .finish_run_success(run_id_1, &stats, None)
            .await
            .unwrap();
        store.finish_run_failure(run_id_2, "boom").await.unwrap();

        let runs = store.list_runs("p1", 100, 0).await.unwrap();
        assert_eq!(runs.len(), 2);
        let success = runs.iter().find(|r| r.id == run_id_1).unwrap();
        assert_eq!(success.status, "success");
        assert_eq!(success.stats.as_ref().unwrap()[0]["rows_written"], 42);
        let failed = runs.iter().find(|r| r.id == run_id_2).unwrap();
        assert_eq!(failed.status, "failed");
        assert_eq!(failed.error.as_deref(), Some("boom"));

        store.delete("p1").await.unwrap();
        assert!(matches!(
            store.get_summary("p1", &cipher).await,
            Err(PipelineStoreError::NotFound(_))
        ));
    }
}
