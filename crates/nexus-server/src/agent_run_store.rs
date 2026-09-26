//! `agent_runs`/`agent_steps` (ROADMAP.md Fase 31) — one row per run, one
//! row per step within it. `agent_steps` doubles as the step-by-step trace
//! (`GET /agents/{id}/runs/{run_id}`) and the source for aggregate metrics
//! (success rate, cost, latency over time) — same "one table, two uses"
//! reasoning `pipeline_run_llm_stats_store.rs` documents, no duplicated
//! data between a "trace" table and a "metrics" table.
//!
//! `#[allow(dead_code)]`: lands ahead of `agent_runner.rs` (ROADMAP.md
//! Fase 31 checklist, next step) — every method here is exercised by its
//! own tests but nothing in the crate calls it yet. Remove the allow once
//! `agent_runner.rs` exists.
#![allow(dead_code)]

use crate::db::{rewrite_placeholders, MetadataPool};
use std::borrow::Cow;

#[derive(Debug, thiserror::Error)]
pub enum AgentRunStoreError {
    #[error("agent run {0} not found")]
    RunNotFound(i64),
    #[error("agent step {0} not found")]
    StepNotFound(i64),
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunStatus {
    Running,
    WaitingApproval,
    Completed,
    Failed,
    MaxStepsReached,
}

impl RunStatus {
    fn as_str(&self) -> &'static str {
        match self {
            RunStatus::Running => "running",
            RunStatus::WaitingApproval => "waiting_approval",
            RunStatus::Completed => "completed",
            RunStatus::Failed => "failed",
            RunStatus::MaxStepsReached => "max_steps_reached",
        }
    }

    fn from_str(s: &str) -> Self {
        match s {
            "waiting_approval" => RunStatus::WaitingApproval,
            "completed" => RunStatus::Completed,
            "failed" => RunStatus::Failed,
            "max_steps_reached" => RunStatus::MaxStepsReached,
            _ => RunStatus::Running,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApprovalStatus {
    /// Not a tool-call step, or a tool-call step that never needed
    /// approval (`ApprovalMode::Auto`).
    NotApplicable,
    Pending,
    Approved,
    Rejected,
}

impl ApprovalStatus {
    fn as_str(&self) -> &'static str {
        match self {
            ApprovalStatus::NotApplicable => "not_applicable",
            ApprovalStatus::Pending => "pending",
            ApprovalStatus::Approved => "approved",
            ApprovalStatus::Rejected => "rejected",
        }
    }

    fn from_str(s: &str) -> Self {
        match s {
            "pending" => ApprovalStatus::Pending,
            "approved" => ApprovalStatus::Approved,
            "rejected" => ApprovalStatus::Rejected,
            _ => ApprovalStatus::NotApplicable,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AgentRun {
    pub id: i64,
    pub agent_id: String,
    /// The question this run was started with — needed to rebuild the
    /// tool-calling conversation history on resume after an approval
    /// (`agent_runner.rs`), since `agent_steps` alone only has the turns
    /// *after* the first user message.
    pub question: String,
    pub status: RunStatus,
    pub started_at: String,
    pub finished_at: Option<String>,
    pub total_tokens: i64,
    pub total_cost: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AgentStep {
    pub id: i64,
    pub run_id: i64,
    pub step_number: i32,
    /// "tool_call" | "final_answer" — kept as a plain string rather than an
    /// enum since `agent_runner.rs` is the only writer and the frontend
    /// trace view is the only reader, neither needs exhaustive matching.
    pub kind: String,
    pub tool: Option<String>,
    pub args: Option<String>,
    pub result: Option<String>,
    pub approval_status: ApprovalStatus,
    pub approved_by: Option<String>,
    pub approved_at: Option<String>,
}

#[derive(Clone)]
pub struct AgentRunStore {
    pool: MetadataPool,
}

impl AgentRunStore {
    fn q(&self, sql: &'static str) -> Cow<'static, str> {
        rewrite_placeholders(sql, self.pool.is_postgres())
    }

    pub async fn connect(database_url: &str) -> anyhow::Result<Self> {
        let pool = MetadataPool::connect(database_url).await?;

        match &pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query(
                    r#"
                    CREATE TABLE IF NOT EXISTS agent_runs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        agent_id TEXT NOT NULL,
                        question TEXT NOT NULL,
                        status TEXT NOT NULL,
                        started_at TEXT NOT NULL DEFAULT (datetime('now')),
                        finished_at TEXT,
                        total_tokens INTEGER NOT NULL DEFAULT 0,
                        total_cost REAL NOT NULL DEFAULT 0
                    )
                    "#,
                )
                .execute(p)
                .await?;
                sqlx::query(
                    "CREATE INDEX IF NOT EXISTS idx_agent_runs_agent_id ON agent_runs(agent_id)",
                )
                .execute(p)
                .await?;

                sqlx::query(
                    r#"
                    CREATE TABLE IF NOT EXISTS agent_steps (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        run_id INTEGER NOT NULL,
                        step_number INTEGER NOT NULL,
                        kind TEXT NOT NULL,
                        tool TEXT,
                        args TEXT,
                        result TEXT,
                        approval_status TEXT NOT NULL DEFAULT 'not_applicable',
                        approved_by TEXT,
                        approved_at TEXT
                    )
                    "#,
                )
                .execute(p)
                .await?;
                sqlx::query(
                    "CREATE INDEX IF NOT EXISTS idx_agent_steps_run_id ON agent_steps(run_id)",
                )
                .execute(p)
                .await?;
            }
            MetadataPool::Postgres(p) => {
                sqlx::query(
                    r#"
                    CREATE TABLE IF NOT EXISTS agent_runs (
                        id BIGSERIAL PRIMARY KEY,
                        agent_id TEXT NOT NULL,
                        question TEXT NOT NULL,
                        status TEXT NOT NULL,
                        started_at TEXT NOT NULL DEFAULT (to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')),
                        finished_at TEXT,
                        total_tokens BIGINT NOT NULL DEFAULT 0,
                        total_cost DOUBLE PRECISION NOT NULL DEFAULT 0
                    )
                    "#,
                )
                .execute(p)
                .await?;
                sqlx::query(
                    "CREATE INDEX IF NOT EXISTS idx_agent_runs_agent_id ON agent_runs(agent_id)",
                )
                .execute(p)
                .await?;

                sqlx::query(
                    r#"
                    CREATE TABLE IF NOT EXISTS agent_steps (
                        id BIGSERIAL PRIMARY KEY,
                        run_id BIGINT NOT NULL,
                        step_number INTEGER NOT NULL,
                        kind TEXT NOT NULL,
                        tool TEXT,
                        args TEXT,
                        result TEXT,
                        approval_status TEXT NOT NULL DEFAULT 'not_applicable',
                        approved_by TEXT,
                        approved_at TEXT
                    )
                    "#,
                )
                .execute(p)
                .await?;
                sqlx::query(
                    "CREATE INDEX IF NOT EXISTS idx_agent_steps_run_id ON agent_steps(run_id)",
                )
                .execute(p)
                .await?;
            }
        }

        Ok(Self { pool })
    }

    pub async fn start_run(&self, agent_id: &str, question: &str) -> Result<i64, sqlx::Error> {
        let sql =
            self.q("INSERT INTO agent_runs (agent_id, question, status) VALUES (?, ?, 'running')");
        let id = match &self.pool {
            MetadataPool::Sqlite(p) => sqlx::query(sqlx::AssertSqlSafe(sql))
                .bind(agent_id)
                .bind(question)
                .execute(p)
                .await?
                .last_insert_rowid(),
            MetadataPool::Postgres(p) => {
                let sql = self.q(
                    "INSERT INTO agent_runs (agent_id, question, status) VALUES (?, ?, 'running') \
                     RETURNING id",
                );
                let (id,): (i64,) = sqlx::query_as(sqlx::AssertSqlSafe(sql))
                    .bind(agent_id)
                    .bind(question)
                    .fetch_one(p)
                    .await?;
                id
            }
        };
        Ok(id)
    }

    /// Terminal update — sets `status`/`finished_at` together, the run's
    /// last write. `WaitingApproval` (mid-run, not terminal) goes through
    /// `set_status_waiting_approval` instead, which leaves `finished_at`
    /// unset.
    pub async fn finish_run(&self, run_id: i64, status: RunStatus) -> Result<(), sqlx::Error> {
        match &self.pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query(sqlx::AssertSqlSafe(self.q(
                    "UPDATE agent_runs SET status = ?, finished_at = datetime('now') WHERE id = ?",
                )))
                .bind(status.as_str())
                .bind(run_id)
                .execute(p)
                .await?;
            }
            MetadataPool::Postgres(p) => {
                sqlx::query(sqlx::AssertSqlSafe(self.q(
                    "UPDATE agent_runs SET status = ?, finished_at = (to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')) WHERE id = ?",
                )))
                .bind(status.as_str())
                .bind(run_id)
                .execute(p)
                .await?;
            }
        }
        Ok(())
    }

    pub async fn set_status_waiting_approval(&self, run_id: i64) -> Result<(), sqlx::Error> {
        let sql = self.q("UPDATE agent_runs SET status = 'waiting_approval' WHERE id = ?");
        match &self.pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query(sqlx::AssertSqlSafe(sql))
                    .bind(run_id)
                    .execute(p)
                    .await?;
            }
            MetadataPool::Postgres(p) => {
                sqlx::query(sqlx::AssertSqlSafe(sql))
                    .bind(run_id)
                    .execute(p)
                    .await?;
            }
        }
        Ok(())
    }

    pub async fn add_usage(&self, run_id: i64, tokens: u32, cost: f64) -> Result<(), sqlx::Error> {
        let sql = self.q(
            "UPDATE agent_runs SET total_tokens = total_tokens + ?, total_cost = total_cost + ? WHERE id = ?",
        );
        match &self.pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query(sqlx::AssertSqlSafe(sql))
                    .bind(tokens as i64)
                    .bind(cost)
                    .bind(run_id)
                    .execute(p)
                    .await?;
            }
            MetadataPool::Postgres(p) => {
                sqlx::query(sqlx::AssertSqlSafe(sql))
                    .bind(tokens as i64)
                    .bind(cost)
                    .bind(run_id)
                    .execute(p)
                    .await?;
            }
        }
        Ok(())
    }

    pub async fn get_run(&self, run_id: i64) -> Result<AgentRun, AgentRunStoreError> {
        let sql = self.q(
            "SELECT id, agent_id, question, status, started_at, finished_at, total_tokens, \
             total_cost FROM agent_runs WHERE id = ?",
        );
        type Row = (
            i64,
            String,
            String,
            String,
            String,
            Option<String>,
            i64,
            f64,
        );
        let row: Option<Row> = match &self.pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query_as(sqlx::AssertSqlSafe(sql))
                    .bind(run_id)
                    .fetch_optional(p)
                    .await?
            }
            MetadataPool::Postgres(p) => {
                sqlx::query_as(sqlx::AssertSqlSafe(sql))
                    .bind(run_id)
                    .fetch_optional(p)
                    .await?
            }
        };
        let (id, agent_id, question, status, started_at, finished_at, total_tokens, total_cost) =
            row.ok_or(AgentRunStoreError::RunNotFound(run_id))?;
        Ok(AgentRun {
            id,
            agent_id,
            question,
            status: RunStatus::from_str(&status),
            started_at,
            finished_at,
            total_tokens,
            total_cost,
        })
    }

    pub async fn list_runs(&self, agent_id: &str) -> Result<Vec<AgentRun>, sqlx::Error> {
        let sql = self.q(
            "SELECT id, agent_id, question, status, started_at, finished_at, total_tokens, \
             total_cost FROM agent_runs WHERE agent_id = ? ORDER BY id DESC",
        );
        type Row = (
            i64,
            String,
            String,
            String,
            String,
            Option<String>,
            i64,
            f64,
        );
        let rows: Vec<Row> = match &self.pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query_as(sqlx::AssertSqlSafe(sql))
                    .bind(agent_id)
                    .fetch_all(p)
                    .await?
            }
            MetadataPool::Postgres(p) => {
                sqlx::query_as(sqlx::AssertSqlSafe(sql))
                    .bind(agent_id)
                    .fetch_all(p)
                    .await?
            }
        };
        Ok(rows
            .into_iter()
            .map(
                |(
                    id,
                    agent_id,
                    question,
                    status,
                    started_at,
                    finished_at,
                    total_tokens,
                    total_cost,
                )| {
                    AgentRun {
                        id,
                        agent_id,
                        question,
                        status: RunStatus::from_str(&status),
                        started_at,
                        finished_at,
                        total_tokens,
                        total_cost,
                    }
                },
            )
            .collect())
    }

    /// Appends the next step (`step_number` is 1-indexed and monotonic
    /// within a run, computed here rather than left to the caller so two
    /// concurrent writers can never collide on the same number).
    pub async fn append_step(
        &self,
        run_id: i64,
        kind: &str,
        tool: Option<&str>,
        args: Option<&str>,
        result: Option<&str>,
        approval_status: ApprovalStatus,
    ) -> Result<i64, sqlx::Error> {
        let sql =
            self.q("SELECT COALESCE(MAX(step_number), 0) + 1 FROM agent_steps WHERE run_id = ?");
        let next_step: i32 = match &self.pool {
            MetadataPool::Sqlite(p) => {
                let (n,): (i32,) = sqlx::query_as(sqlx::AssertSqlSafe(sql))
                    .bind(run_id)
                    .fetch_one(p)
                    .await?;
                n
            }
            MetadataPool::Postgres(p) => {
                let (n,): (i32,) = sqlx::query_as(sqlx::AssertSqlSafe(sql))
                    .bind(run_id)
                    .fetch_one(p)
                    .await?;
                n
            }
        };

        let sql = self.q(
            "INSERT INTO agent_steps (run_id, step_number, kind, tool, args, result, approval_status) \
             VALUES (?, ?, ?, ?, ?, ?, ?)",
        );
        let id = match &self.pool {
            MetadataPool::Sqlite(p) => sqlx::query(sqlx::AssertSqlSafe(sql))
                .bind(run_id)
                .bind(next_step)
                .bind(kind)
                .bind(tool)
                .bind(args)
                .bind(result)
                .bind(approval_status.as_str())
                .execute(p)
                .await?
                .last_insert_rowid(),
            MetadataPool::Postgres(p) => {
                let sql = self.q(
                    "INSERT INTO agent_steps (run_id, step_number, kind, tool, args, result, approval_status) \
                     VALUES (?, ?, ?, ?, ?, ?, ?) RETURNING id",
                );
                let (id,): (i64,) = sqlx::query_as(sqlx::AssertSqlSafe(sql))
                    .bind(run_id)
                    .bind(next_step)
                    .bind(kind)
                    .bind(tool)
                    .bind(args)
                    .bind(result)
                    .bind(approval_status.as_str())
                    .fetch_one(p)
                    .await?;
                id
            }
        };
        Ok(id)
    }

    /// Resolves a `pending` step to `approved`/`rejected`, and — only when
    /// approved — fills in `result` with what the tool actually returned
    /// (a rejected step never runs the tool, so it has none). Used by both
    /// `POST .../approve` and `POST .../reject`.
    pub async fn resolve_step(
        &self,
        step_id: i64,
        approval_status: ApprovalStatus,
        approved_by: &str,
        result: Option<&str>,
    ) -> Result<(), AgentRunStoreError> {
        let sql = self.q(
            "UPDATE agent_steps SET approval_status = ?, approved_by = ?, \
             approved_at = datetime('now'), result = COALESCE(?, result) WHERE id = ?",
        );
        let rows_affected = match &self.pool {
            MetadataPool::Sqlite(p) => sqlx::query(sqlx::AssertSqlSafe(sql))
                .bind(approval_status.as_str())
                .bind(approved_by)
                .bind(result)
                .bind(step_id)
                .execute(p)
                .await?
                .rows_affected(),
            MetadataPool::Postgres(p) => {
                let sql = self.q(
                    "UPDATE agent_steps SET approval_status = ?, approved_by = ?, \
                     approved_at = (to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')), \
                     result = COALESCE(?, result) WHERE id = ?",
                );
                sqlx::query(sqlx::AssertSqlSafe(sql))
                    .bind(approval_status.as_str())
                    .bind(approved_by)
                    .bind(result)
                    .bind(step_id)
                    .execute(p)
                    .await?
                    .rows_affected()
            }
        };
        if rows_affected == 0 {
            return Err(AgentRunStoreError::StepNotFound(step_id));
        }
        Ok(())
    }

    pub async fn get_step(&self, step_id: i64) -> Result<AgentStep, AgentRunStoreError> {
        let sql = self.q(
            "SELECT id, run_id, step_number, kind, tool, args, result, approval_status, \
             approved_by, approved_at FROM agent_steps WHERE id = ?",
        );
        type Row = (
            i64,
            i64,
            i32,
            String,
            Option<String>,
            Option<String>,
            Option<String>,
            String,
            Option<String>,
            Option<String>,
        );
        let row: Option<Row> = match &self.pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query_as(sqlx::AssertSqlSafe(sql))
                    .bind(step_id)
                    .fetch_optional(p)
                    .await?
            }
            MetadataPool::Postgres(p) => {
                sqlx::query_as(sqlx::AssertSqlSafe(sql))
                    .bind(step_id)
                    .fetch_optional(p)
                    .await?
            }
        };
        let (
            id,
            run_id,
            step_number,
            kind,
            tool,
            args,
            result,
            approval_status,
            approved_by,
            approved_at,
        ) = row.ok_or(AgentRunStoreError::StepNotFound(step_id))?;
        Ok(AgentStep {
            id,
            run_id,
            step_number,
            kind,
            tool,
            args,
            result,
            approval_status: ApprovalStatus::from_str(&approval_status),
            approved_by,
            approved_at,
        })
    }

    pub async fn list_steps(&self, run_id: i64) -> Result<Vec<AgentStep>, sqlx::Error> {
        let sql = self.q(
            "SELECT id, run_id, step_number, kind, tool, args, result, approval_status, \
             approved_by, approved_at FROM agent_steps WHERE run_id = ? ORDER BY step_number",
        );
        type Row = (
            i64,
            i64,
            i32,
            String,
            Option<String>,
            Option<String>,
            Option<String>,
            String,
            Option<String>,
            Option<String>,
        );
        let rows: Vec<Row> = match &self.pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query_as(sqlx::AssertSqlSafe(sql))
                    .bind(run_id)
                    .fetch_all(p)
                    .await?
            }
            MetadataPool::Postgres(p) => {
                sqlx::query_as(sqlx::AssertSqlSafe(sql))
                    .bind(run_id)
                    .fetch_all(p)
                    .await?
            }
        };
        Ok(rows
            .into_iter()
            .map(
                |(
                    id,
                    run_id,
                    step_number,
                    kind,
                    tool,
                    args,
                    result,
                    approval_status,
                    approved_by,
                    approved_at,
                )| {
                    AgentStep {
                        id,
                        run_id,
                        step_number,
                        kind,
                        tool,
                        args,
                        result,
                        approval_status: ApprovalStatus::from_str(&approval_status),
                        approved_by,
                        approved_at,
                    }
                },
            )
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn start_run_and_get_run_roundtrips() {
        let store = AgentRunStore::connect("sqlite::memory:").await.unwrap();
        let run_id = store.start_run("agent-1", "test question").await.unwrap();

        let run = store.get_run(run_id).await.unwrap();
        assert_eq!(run.agent_id, "agent-1");
        assert_eq!(run.status, RunStatus::Running);
        assert!(run.finished_at.is_none());
    }

    #[tokio::test]
    async fn finish_run_sets_status_and_finished_at() {
        let store = AgentRunStore::connect("sqlite::memory:").await.unwrap();
        let run_id = store.start_run("agent-1", "test question").await.unwrap();
        store
            .finish_run(run_id, RunStatus::Completed)
            .await
            .unwrap();

        let run = store.get_run(run_id).await.unwrap();
        assert_eq!(run.status, RunStatus::Completed);
        assert!(run.finished_at.is_some());
    }

    #[tokio::test]
    async fn add_usage_accumulates() {
        let store = AgentRunStore::connect("sqlite::memory:").await.unwrap();
        let run_id = store.start_run("agent-1", "test question").await.unwrap();
        store.add_usage(run_id, 100, 0.01).await.unwrap();
        store.add_usage(run_id, 50, 0.005).await.unwrap();

        let run = store.get_run(run_id).await.unwrap();
        assert_eq!(run.total_tokens, 150);
        assert!((run.total_cost - 0.015).abs() < 1e-9);
    }

    #[tokio::test]
    async fn append_step_numbers_are_sequential_per_run() {
        let store = AgentRunStore::connect("sqlite::memory:").await.unwrap();
        let run_id = store.start_run("agent-1", "test question").await.unwrap();

        let step1 = store
            .append_step(
                run_id,
                "tool_call",
                Some("search_vectors"),
                Some("{}"),
                Some("[]"),
                ApprovalStatus::NotApplicable,
            )
            .await
            .unwrap();
        let step2 = store
            .append_step(
                run_id,
                "final_answer",
                None,
                None,
                Some("done"),
                ApprovalStatus::NotApplicable,
            )
            .await
            .unwrap();

        let s1 = store.get_step(step1).await.unwrap();
        let s2 = store.get_step(step2).await.unwrap();
        assert_eq!(s1.step_number, 1);
        assert_eq!(s2.step_number, 2);
    }

    #[tokio::test]
    async fn append_step_numbering_is_independent_per_run() {
        let store = AgentRunStore::connect("sqlite::memory:").await.unwrap();
        let run_a = store.start_run("agent-1", "test question").await.unwrap();
        let run_b = store.start_run("agent-1", "test question").await.unwrap();

        let step_a = store
            .append_step(
                run_a,
                "tool_call",
                Some("query_data"),
                None,
                None,
                ApprovalStatus::NotApplicable,
            )
            .await
            .unwrap();
        let step_b = store
            .append_step(
                run_b,
                "tool_call",
                Some("query_data"),
                None,
                None,
                ApprovalStatus::NotApplicable,
            )
            .await
            .unwrap();

        assert_eq!(store.get_step(step_a).await.unwrap().step_number, 1);
        assert_eq!(store.get_step(step_b).await.unwrap().step_number, 1);
    }

    #[tokio::test]
    async fn resolve_step_approve_sets_result_and_approver() {
        let store = AgentRunStore::connect("sqlite::memory:").await.unwrap();
        let run_id = store.start_run("agent-1", "test question").await.unwrap();
        let step_id = store
            .append_step(
                run_id,
                "tool_call",
                Some("run_pipeline"),
                Some("{}"),
                None,
                ApprovalStatus::Pending,
            )
            .await
            .unwrap();

        store
            .resolve_step(
                step_id,
                ApprovalStatus::Approved,
                "alice",
                Some("run started"),
            )
            .await
            .unwrap();

        let step = store.get_step(step_id).await.unwrap();
        assert_eq!(step.approval_status, ApprovalStatus::Approved);
        assert_eq!(step.approved_by.as_deref(), Some("alice"));
        assert_eq!(step.result.as_deref(), Some("run started"));
    }

    #[tokio::test]
    async fn resolve_step_reject_keeps_result_none() {
        let store = AgentRunStore::connect("sqlite::memory:").await.unwrap();
        let run_id = store.start_run("agent-1", "test question").await.unwrap();
        let step_id = store
            .append_step(
                run_id,
                "tool_call",
                Some("run_pipeline"),
                Some("{}"),
                None,
                ApprovalStatus::Pending,
            )
            .await
            .unwrap();

        store
            .resolve_step(step_id, ApprovalStatus::Rejected, "bob", None)
            .await
            .unwrap();

        let step = store.get_step(step_id).await.unwrap();
        assert_eq!(step.approval_status, ApprovalStatus::Rejected);
        assert!(step.result.is_none());
    }

    #[tokio::test]
    async fn resolve_missing_step_is_not_found() {
        let store = AgentRunStore::connect("sqlite::memory:").await.unwrap();
        let err = store
            .resolve_step(999, ApprovalStatus::Approved, "alice", None)
            .await
            .unwrap_err();
        assert!(matches!(err, AgentRunStoreError::StepNotFound(999)));
    }

    #[tokio::test]
    async fn list_steps_returns_run_trace_in_order() {
        let store = AgentRunStore::connect("sqlite::memory:").await.unwrap();
        let run_id = store.start_run("agent-1", "test question").await.unwrap();
        store
            .append_step(
                run_id,
                "tool_call",
                Some("query_data"),
                None,
                None,
                ApprovalStatus::NotApplicable,
            )
            .await
            .unwrap();
        store
            .append_step(
                run_id,
                "final_answer",
                None,
                None,
                Some("done"),
                ApprovalStatus::NotApplicable,
            )
            .await
            .unwrap();

        let steps = store.list_steps(run_id).await.unwrap();
        assert_eq!(steps.len(), 2);
        assert_eq!(steps[0].step_number, 1);
        assert_eq!(steps[1].step_number, 2);
    }

    #[tokio::test]
    async fn list_runs_scoped_to_agent_and_ordered_newest_first() {
        let store = AgentRunStore::connect("sqlite::memory:").await.unwrap();
        let r1 = store.start_run("agent-1", "test question").await.unwrap();
        let r2 = store.start_run("agent-1", "test question").await.unwrap();
        store.start_run("agent-2", "test question").await.unwrap();

        let runs = store.list_runs("agent-1").await.unwrap();
        assert_eq!(runs.len(), 2);
        assert_eq!(runs[0].id, r2);
        assert_eq!(runs[1].id, r1);
    }

    #[tokio::test]
    async fn get_missing_run_is_not_found() {
        let store = AgentRunStore::connect("sqlite::memory:").await.unwrap();
        let err = store.get_run(999).await.unwrap_err();
        assert!(matches!(err, AgentRunStoreError::RunNotFound(999)));
    }
}
