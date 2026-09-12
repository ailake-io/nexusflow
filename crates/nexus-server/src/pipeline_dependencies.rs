use crate::db::{rewrite_placeholders, MetadataPool};
use crate::AppState;
use nexus_core::{DependencyMode, PipelineSpec};
use std::collections::{HashMap, HashSet};

/// Validates `candidate.depends_on` against every other saved pipeline —
/// unknown upstream and cross-pipeline cycles. `PipelineSpec::validate()`
/// itself can only catch what a single spec knows about itself (charset,
/// self-dependency); this needs the full picture, so it lives here and is
/// called from the create/update handlers instead, right after
/// `spec.validate()` and before persisting.
///
/// Pure and synchronous on purpose (`all_specs` is loaded by the caller) —
/// keeps this unit-testable without a database.
pub fn check_dependencies(
    all_specs: &[PipelineSpec],
    candidate: &PipelineSpec,
) -> Result<(), String> {
    let mut graph: HashMap<&str, Vec<&str>> = HashMap::new();
    for spec in all_specs {
        let deps: Vec<&str> = if spec.pipeline_id == candidate.pipeline_id {
            // An update replaces this spec's own edges with the candidate's
            // — never both, or a removed dependency would still count
            // toward a cycle.
            candidate
                .depends_on
                .iter()
                .map(|d| d.upstream_pipeline_id.as_str())
                .collect()
        } else {
            spec.depends_on
                .iter()
                .map(|d| d.upstream_pipeline_id.as_str())
                .collect()
        };
        graph.insert(spec.pipeline_id.as_str(), deps);
    }
    // A brand-new pipeline (create, not update) isn't in `all_specs` yet.
    graph.entry(candidate.pipeline_id.as_str()).or_insert_with(|| {
        candidate
            .depends_on
            .iter()
            .map(|d| d.upstream_pipeline_id.as_str())
            .collect()
    });

    for dep in &candidate.depends_on {
        if !graph.contains_key(dep.upstream_pipeline_id.as_str()) {
            return Err(format!(
                "depends_on references unknown pipeline {:?}",
                dep.upstream_pipeline_id
            ));
        }
    }

    let mut visiting = HashSet::new();
    let mut visited = HashSet::new();
    let mut path = Vec::new();
    detect_cycle(
        candidate.pipeline_id.as_str(),
        &graph,
        &mut visiting,
        &mut visited,
        &mut path,
    )
}

fn detect_cycle<'a>(
    node: &'a str,
    graph: &HashMap<&'a str, Vec<&'a str>>,
    visiting: &mut HashSet<&'a str>,
    visited: &mut HashSet<&'a str>,
    path: &mut Vec<&'a str>,
) -> Result<(), String> {
    if visited.contains(node) {
        return Ok(());
    }
    if visiting.contains(node) {
        path.push(node);
        return Err(format!(
            "dependency cycle detected: {}",
            path.join(" -> ")
        ));
    }
    visiting.insert(node);
    path.push(node);
    if let Some(deps) = graph.get(node) {
        for &dep in deps {
            detect_cycle(dep, graph, visiting, visited, path)?;
        }
    }
    path.pop();
    visiting.remove(node);
    visited.insert(node);
    Ok(())
}

/// Persists `All`-mode satisfaction state — which upstreams have succeeded
/// since a downstream last fired. `Any`-mode dependencies never touch this
/// (each upstream success dispatches independently, no shared state to
/// track). One row per (downstream, upstream) pair; `clear` wipes a
/// downstream's rows once it fires, starting the next "round" empty — same
/// idiom as `quality_check_results`' append-then-query pattern, just with a
/// delete instead of an ever-growing history, since only the *current*
/// round's satisfaction matters.
#[derive(Clone)]
pub struct DependencyStateStore {
    pool: MetadataPool,
}

impl DependencyStateStore {
    fn q(&self, sql: &'static str) -> std::borrow::Cow<'static, str> {
        rewrite_placeholders(sql, self.pool.is_postgres())
    }

    pub async fn connect(database_url: &str) -> anyhow::Result<Self> {
        let pool = MetadataPool::connect(database_url).await?;
        let create = r#"
            CREATE TABLE IF NOT EXISTS pipeline_dependency_state (
                downstream_pipeline_id TEXT NOT NULL,
                upstream_pipeline_id TEXT NOT NULL,
                satisfied_at TEXT NOT NULL,
                PRIMARY KEY (downstream_pipeline_id, upstream_pipeline_id)
            )
        "#;
        match &pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query(create).execute(p).await?;
            }
            MetadataPool::Postgres(p) => {
                sqlx::query(create).execute(p).await?;
            }
        }
        Ok(Self { pool })
    }

    pub async fn mark_satisfied(
        &self,
        downstream_id: &str,
        upstream_id: &str,
    ) -> anyhow::Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        let sql = self.q(
            "INSERT INTO pipeline_dependency_state \
                (downstream_pipeline_id, upstream_pipeline_id, satisfied_at) \
             VALUES (?, ?, ?) \
             ON CONFLICT (downstream_pipeline_id, upstream_pipeline_id) \
             DO UPDATE SET satisfied_at = excluded.satisfied_at",
        );
        match &self.pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query(sqlx::AssertSqlSafe(sql))
                    .bind(downstream_id)
                    .bind(upstream_id)
                    .bind(&now)
                    .execute(p)
                    .await?;
            }
            MetadataPool::Postgres(p) => {
                sqlx::query(sqlx::AssertSqlSafe(sql))
                    .bind(downstream_id)
                    .bind(upstream_id)
                    .bind(&now)
                    .execute(p)
                    .await?;
            }
        }
        Ok(())
    }

    pub async fn satisfied_upstreams(&self, downstream_id: &str) -> anyhow::Result<HashSet<String>> {
        let sql = self.q(
            "SELECT upstream_pipeline_id FROM pipeline_dependency_state \
             WHERE downstream_pipeline_id = ?",
        );
        let rows: Vec<(String,)> = match &self.pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query_as(sqlx::AssertSqlSafe(sql))
                    .bind(downstream_id)
                    .fetch_all(p)
                    .await?
            }
            MetadataPool::Postgres(p) => {
                sqlx::query_as(sqlx::AssertSqlSafe(sql))
                    .bind(downstream_id)
                    .fetch_all(p)
                    .await?
            }
        };
        Ok(rows.into_iter().map(|(u,)| u).collect())
    }

    pub async fn clear(&self, downstream_id: &str) -> anyhow::Result<()> {
        let sql = self.q("DELETE FROM pipeline_dependency_state WHERE downstream_pipeline_id = ?");
        match &self.pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query(sqlx::AssertSqlSafe(sql))
                    .bind(downstream_id)
                    .execute(p)
                    .await?;
            }
            MetadataPool::Postgres(p) => {
                sqlx::query(sqlx::AssertSqlSafe(sql))
                    .bind(downstream_id)
                    .execute(p)
                    .await?;
            }
        }
        Ok(())
    }
}

/// Called from `execute_pipeline_run`'s success branch — finds every saved
/// pipeline that lists `succeeded_pipeline_id` in `depends_on` and, per its
/// `dependency_mode`, starts a run once its trigger condition is met.
/// Best-effort/fire-and-forget from the caller's perspective, same posture
/// as `data_catalog`/`pipeline_schemas`: a failure here must never fail the
/// run that just succeeded.
pub async fn trigger_downstream(state: &AppState, succeeded_pipeline_id: &str) -> anyhow::Result<()> {
    let all_specs = state.pipelines.list_all_specs(&state.secrets).await?;
    for spec in &all_specs {
        if spec.draft {
            continue;
        }
        let depends_on_this = spec
            .depends_on
            .iter()
            .any(|d| d.upstream_pipeline_id == succeeded_pipeline_id);
        if !depends_on_this {
            continue;
        }
        match spec.dependency_mode {
            DependencyMode::Any => {
                dispatch_if_not_running(state, spec).await;
            }
            DependencyMode::All => {
                state
                    .pipeline_dependency_state
                    .mark_satisfied(&spec.pipeline_id, succeeded_pipeline_id)
                    .await?;
                let satisfied = state
                    .pipeline_dependency_state
                    .satisfied_upstreams(&spec.pipeline_id)
                    .await?;
                let all_satisfied = spec
                    .depends_on
                    .iter()
                    .all(|d| satisfied.contains(&d.upstream_pipeline_id));
                if all_satisfied {
                    // Clear before dispatching, not after: if the dispatch
                    // itself fails partway, the next individual upstream
                    // success re-marks its own entry and the round can
                    // still complete — clearing after a failed dispatch
                    // would silently drop already-recorded satisfactions.
                    state
                        .pipeline_dependency_state
                        .clear(&spec.pipeline_id)
                        .await?;
                    dispatch_if_not_running(state, spec).await;
                }
            }
        }
    }
    Ok(())
}

/// Same overlap guard `POST /pipelines/{id}/run` already applies — a
/// dependency-triggered run must not stack on top of one already in
/// progress for the same pipeline.
async fn dispatch_if_not_running(state: &AppState, spec: &PipelineSpec) {
    match state.pipelines.has_running_run(&spec.pipeline_id).await {
        Ok(true) => {
            tracing::info!(
                pipeline_id = %spec.pipeline_id,
                "dependency-triggered run skipped, already running"
            );
            return;
        }
        Ok(false) => {}
        Err(e) => {
            tracing::warn!(
                pipeline_id = %spec.pipeline_id,
                error = %e,
                "failed to check for an in-progress run before dependency-triggered dispatch"
            );
            return;
        }
    }
    if let Err(e) = crate::start_pipeline_run(state, spec).await {
        tracing::warn!(
            pipeline_id = %spec.pipeline_id,
            error = %e,
            "failed to start dependency-triggered run"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec_with_deps(id: &str, deps: &[&str], mode: DependencyMode) -> PipelineSpec {
        let json = format!(
            r#"{{
                "pipeline_id": "{id}",
                "sources": [{{"connector": "postgres", "config": {{}}}}],
                "sinks": [{{"connector": "postgres", "config": {{}}}}],
                "depends_on": [{deps}],
                "dependency_mode": "{mode}"
            }}"#,
            deps = deps
                .iter()
                .map(|d| format!(r#"{{"upstream_pipeline_id": "{d}"}}"#))
                .collect::<Vec<_>>()
                .join(","),
            mode = match mode {
                DependencyMode::Any => "any",
                DependencyMode::All => "all",
            },
        );
        PipelineSpec::parse(&json).unwrap()
    }

    fn spec(id: &str) -> PipelineSpec {
        spec_with_deps(id, &[], DependencyMode::Any)
    }

    #[test]
    fn accepts_a_simple_valid_dependency() {
        let upstream = spec("a");
        let downstream = spec_with_deps("b", &["a"], DependencyMode::Any);
        let all = vec![upstream, downstream.clone()];
        check_dependencies(&all, &downstream).expect("a -> b is a valid DAG");
    }

    #[test]
    fn accepts_a_new_pipeline_not_yet_in_all_specs() {
        let upstream = spec("a");
        let candidate = spec_with_deps("b", &["a"], DependencyMode::Any);
        // Create case: `candidate` ("b") isn't in `all_specs` yet.
        check_dependencies(&[upstream], &candidate).expect("new pipeline depending on an existing one is valid");
    }

    #[test]
    fn rejects_dependency_on_unknown_pipeline() {
        let candidate = spec_with_deps("b", &["ghost"], DependencyMode::Any);
        let err = check_dependencies(&[candidate.clone()], &candidate)
            .expect_err("unknown upstream must be rejected");
        assert!(err.contains("unknown pipeline"));
    }

    #[test]
    fn rejects_direct_two_pipeline_cycle() {
        let a = spec_with_deps("a", &["b"], DependencyMode::Any);
        let b = spec_with_deps("b", &["a"], DependencyMode::Any);
        let err = check_dependencies(&[a.clone(), b], &a).expect_err("a <-> b must be rejected");
        assert!(err.contains("cycle"));
    }

    #[test]
    fn rejects_longer_cycle_through_three_pipelines() {
        let a = spec_with_deps("a", &["c"], DependencyMode::Any);
        let b = spec_with_deps("b", &["a"], DependencyMode::Any);
        let c = spec_with_deps("c", &["b"], DependencyMode::Any);
        let err = check_dependencies(&[a.clone(), b, c], &a)
            .expect_err("a -> c -> b -> a must be rejected");
        assert!(err.contains("cycle"));
    }

    #[test]
    fn accepts_diamond_shaped_dependencies() {
        // a -> b, a -> c, {b, c} -> d (All mode) — not a cycle, just a
        // diamond; must be accepted.
        let a = spec("a");
        let b = spec_with_deps("b", &["a"], DependencyMode::Any);
        let c = spec_with_deps("c", &["a"], DependencyMode::Any);
        let d = spec_with_deps("d", &["b", "c"], DependencyMode::All);
        check_dependencies(&[a, b, c, d.clone()], &d).expect("diamond dependency is not a cycle");
    }

    #[test]
    fn updating_a_pipeline_to_drop_a_dependency_cannot_still_count_toward_a_cycle() {
        // a -> b currently exists (a depends on b). Updating `a` to instead
        // depend on nothing must not still see the old a->b edge and reject
        // a legitimate, now-independent b -> a.
        let a_old = spec_with_deps("a", &["b"], DependencyMode::Any);
        let b = spec("b");
        let a_new = spec("a"); // update: a no longer depends on b
        let candidate = spec_with_deps("b", &["a"], DependencyMode::Any); // now b -> a
        check_dependencies(&[a_old, b], &a_new).expect("a with dependency removed is valid");
        check_dependencies(&[a_new, candidate.clone()], &candidate)
            .expect("b -> a is valid once a no longer depends on b");
    }

    #[tokio::test]
    async fn dependency_state_tracks_and_clears_all_mode_satisfaction() {
        let store = DependencyStateStore::connect("sqlite::memory:").await.unwrap();
        store.mark_satisfied("d", "a").await.unwrap();
        let satisfied = store.satisfied_upstreams("d").await.unwrap();
        assert_eq!(satisfied, HashSet::from(["a".to_string()]));

        store.mark_satisfied("d", "b").await.unwrap();
        let satisfied = store.satisfied_upstreams("d").await.unwrap();
        assert_eq!(satisfied, HashSet::from(["a".to_string(), "b".to_string()]));

        store.clear("d").await.unwrap();
        assert!(store.satisfied_upstreams("d").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn dependency_state_is_scoped_per_downstream() {
        let store = DependencyStateStore::connect("sqlite::memory:").await.unwrap();
        store.mark_satisfied("d1", "a").await.unwrap();
        store.mark_satisfied("d2", "a").await.unwrap();
        store.clear("d1").await.unwrap();
        assert!(store.satisfied_upstreams("d1").await.unwrap().is_empty());
        assert_eq!(
            store.satisfied_upstreams("d2").await.unwrap(),
            HashSet::from(["a".to_string()])
        );
    }
}
