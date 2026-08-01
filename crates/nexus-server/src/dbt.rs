use nexus_core::{DbtCommand, DbtConfig};

/// One model/test result from dbt's `target/run_results.json` (schema
/// `https://schemas.getdbt.com/dbt/run-results/v6.json`) — only the fields
/// this module actually reads; serde ignores the rest. Feature-gated along
/// with everything else below that only the real (feature = "dbt")
/// `run()` constructs — without the feature, nothing produces one, so the
/// type itself would just be dead weight in that build.
#[cfg(feature = "dbt")]
#[derive(Debug, Clone, serde::Deserialize)]
pub struct DbtRunResult {
    pub unique_id: String,
    /// "success"/"error" for models, "pass"/"fail"/"warn" for tests.
    pub status: String,
    pub execution_time: f64,
    #[serde(default)]
    pub message: Option<String>,
}

#[cfg(feature = "dbt")]
#[derive(Debug, Clone, Default, serde::Deserialize)]
pub struct DbtRunResults {
    #[serde(default)]
    pub results: Vec<DbtRunResult>,
    #[serde(default)]
    pub elapsed_time: f64,
}

/// Basic lineage from `target/manifest.json` — dbt already precomputes the
/// dependency graph as `parent_map` (node -> its upstream nodes), so there's
/// no need to walk each node's own `depends_on` by hand.
#[cfg(feature = "dbt")]
#[derive(Debug, Clone, Default, serde::Deserialize)]
pub struct DbtManifestSummary {
    #[serde(default)]
    pub parent_map: std::collections::HashMap<String, Vec<String>>,
}

/// Result of one dbt invocation — logged (`tracing::info!`) at the call
/// site, and returned so `run_pipeline_handler` can fold it into the run's
/// success/failure. `run_results`/`lineage` are `None` when the
/// corresponding artifact under `target/` couldn't be read or parsed —
/// dbt's own subprocess exit status is what determines success/failure,
/// these are supplementary observability, not correctness-critical.
#[derive(Debug, Clone)]
pub struct DbtOutcome {
    pub command: &'static str,
    pub stdout: String,
    pub stderr: String,
    #[cfg(feature = "dbt")]
    pub run_results: Option<DbtRunResults>,
    #[cfg(feature = "dbt")]
    pub lineage: Option<DbtManifestSummary>,
}

impl DbtOutcome {
    /// Logs this outcome for the enclosing pipeline run — kept here (not in
    /// `run_pipeline_handler`) so callers never need to know that the
    /// models/lineage counts only exist in `feature = "dbt"` builds.
    pub fn log_summary(&self) {
        #[cfg(feature = "dbt")]
        let (models_total, nodes_in_lineage) = (
            self.run_results.as_ref().map(|r| r.results.len()),
            self.lineage.as_ref().map(|l| l.parent_map.len()),
        );
        #[cfg(not(feature = "dbt"))]
        let (models_total, nodes_in_lineage): (Option<usize>, Option<usize>) = (None, None);

        tracing::info!(
            dbt_command = self.command,
            dbt_stdout = %self.stdout,
            dbt_stderr = %self.stderr,
            dbt_models_total = models_total,
            dbt_nodes_in_lineage = nodes_in_lineage,
            "dbt step finished for this run"
        );
    }
}

fn subcommand(command: DbtCommand) -> &'static str {
    match command {
        DbtCommand::Run => "run",
        DbtCommand::Build => "build",
        DbtCommand::Test => "test",
    }
}

#[cfg(feature = "dbt")]
fn read_json_artifact<T: serde::de::DeserializeOwned>(path: &std::path::Path) -> Option<T> {
    let contents = match std::fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(e) => {
            tracing::warn!(path = %path.display(), error = %e, "failed to read dbt artifact");
            return None;
        }
    };
    match serde_json::from_str(&contents) {
        Ok(parsed) => Some(parsed),
        Err(e) => {
            tracing::warn!(path = %path.display(), error = %e, "failed to parse dbt artifact");
            None
        }
    }
}

/// Runs `dbt {run,build,test}` in `config.project_dir` after a pipeline's
/// raw load succeeds (ELT mode, Marco 10) — dbt operates via SQL directly
/// against the already-loaded warehouse tables, not this pipeline's Arrow
/// batches, so this is a plain subprocess step, not a `PipelineEngine` node.
#[cfg(feature = "dbt")]
#[tracing::instrument(skip(config), fields(project_dir = %config.project_dir))]
pub async fn run(config: &DbtConfig) -> anyhow::Result<DbtOutcome> {
    let command = subcommand(config.command);

    let mut cmd = tokio::process::Command::new("dbt");
    cmd.arg(command).current_dir(&config.project_dir);
    if let Some(select) = &config.select {
        cmd.arg("--select").arg(select);
    }

    let output = cmd.output().await.map_err(|e| {
        anyhow::anyhow!(
            "failed to spawn `dbt {command}` in {:?}: {e} (is the `dbt` CLI on PATH?)",
            config.project_dir
        )
    })?;

    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    tracing::info!(
        %stdout,
        %stderr,
        success = output.status.success(),
        "dbt {command} finished"
    );

    // dbt writes both artifacts under `target/` on every invocation, success
    // or failure alike — capture them before deciding success/failure below,
    // so a broken model's own status/message (not just dbt's overall exit
    // code) ends up in the logs too.
    let target_dir = std::path::Path::new(&config.project_dir).join("target");
    let run_results: Option<DbtRunResults> =
        read_json_artifact(&target_dir.join("run_results.json"));
    let lineage: Option<DbtManifestSummary> = read_json_artifact(&target_dir.join("manifest.json"));

    if let Some(rr) = &run_results {
        let succeeded = rr
            .results
            .iter()
            .filter(|r| matches!(r.status.as_str(), "success" | "pass"))
            .count();
        let failed = rr.results.len() - succeeded;
        tracing::info!(
            models_total = rr.results.len(),
            models_succeeded = succeeded,
            models_failed = failed,
            elapsed_time = rr.elapsed_time,
            "dbt run_results.json captured"
        );
        // Aggregate counts above are enough to know *whether* something
        // broke; log each non-passing node individually so *which* one and
        // *why* also lands in the logs (still task #23 — the per-model
        // pass/fail breakdown from run_results.json is the "observabilidade"
        // this task is about, not #25's dedicated `dbt test` invocation).
        for result in &rr.results {
            if !matches!(result.status.as_str(), "success" | "pass") {
                tracing::warn!(
                    unique_id = %result.unique_id,
                    status = %result.status,
                    execution_time = result.execution_time,
                    message = result.message.as_deref().unwrap_or(""),
                    "dbt node did not succeed"
                );
            }
        }
    }
    if let Some(lin) = &lineage {
        tracing::info!(
            nodes_in_lineage = lin.parent_map.len(),
            "dbt manifest.json lineage captured"
        );
    }

    if !output.status.success() {
        anyhow::bail!("dbt {command} exited with {}: {stderr}", output.status);
    }

    Ok(DbtOutcome {
        command,
        stdout,
        stderr,
        run_results,
        lineage,
    })
}

#[cfg(not(feature = "dbt"))]
pub async fn run(config: &DbtConfig) -> anyhow::Result<DbtOutcome> {
    anyhow::bail!(
        "pipeline {:?} requests a dbt {} step, but nexus-server was built without the \
         `dbt` feature — rebuild with `--features dbt`",
        config.project_dir,
        subcommand(config.command)
    )
}

#[cfg(all(test, feature = "dbt"))]
mod tests {
    use super::*;
    use std::fs;

    /// The `dbt` CLI isn't a Rust dependency (see the module doc comment) —
    /// it has to actually be on PATH. Dev machines that opted into the
    /// `dbt` feature normally have it; a plain CI runner usually doesn't,
    /// and installing dbt-fusion there means piping a downloaded installer
    /// script into the workflow, which needs its own explicit sign-off, not
    /// something to wire in unilaterally here. Skip (not fail) when it's
    /// missing, same spirit as this repo's `require_env` pattern for
    /// ADBC-driver-dependent tests — except those *are* provisioned in CI
    /// (scripts/build-adbc-*.sh), so they panic instead of skipping.
    macro_rules! require_dbt_cli_or_skip {
        () => {
            if tokio::process::Command::new("dbt")
                .arg("--version")
                .output()
                .await
                .is_err()
            {
                eprintln!("skipping: `dbt` CLI not found on PATH");
                return;
            }
        };
    }

    /// Minimal dbt project that needs no real warehouse: a single seed-free
    /// model over dbt's built-in `dbt_utils`-free SQL, targeting DuckDB
    /// (file-based, no server) — just enough to prove the subprocess
    /// wiring end to end without any external infrastructure.
    fn write_fixture_project(dir: &std::path::Path) {
        fs::write(
            dir.join("dbt_project.yml"),
            r#"
name: 'nexus_fixture'
version: '1.0.0'
profile: 'nexus_fixture'
model-paths: ["models"]
"#,
        )
        .unwrap();
        fs::write(
            dir.join("profiles.yml"),
            r#"
nexus_fixture:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: 'nexus_fixture.duckdb'
"#,
        )
        .unwrap();
        fs::create_dir_all(dir.join("models")).unwrap();
        fs::write(
            dir.join("models").join("one.sql"),
            "select 1 as id, 'a' as label",
        )
        .unwrap();
        // A second model depending on the first — gives run_results.json
        // more than one row and manifest.json a real (non-empty) edge in
        // parent_map, so the capture assertions below prove something.
        fs::write(
            dir.join("models").join("two.sql"),
            "select * from {{ ref('one') }}",
        )
        .unwrap();
    }

    #[tokio::test]
    async fn runs_a_real_dbt_project_end_to_end() {
        require_dbt_cli_or_skip!();
        let dir = tempfile::tempdir().unwrap();
        write_fixture_project(dir.path());

        let config = DbtConfig {
            project_dir: dir.path().to_string_lossy().into_owned(),
            command: DbtCommand::Run,
            select: None,
        };

        // `--profiles-dir` isn't part of DbtConfig (that would leak
        // deployment-specific detail into the pipeline schema) — dbt reads
        // DBT_PROFILES_DIR itself, so point it at the fixture's own
        // profiles.yml instead of the developer's real ~/.dbt/.
        std::env::set_var("DBT_PROFILES_DIR", dir.path());
        let outcome = run(&config).await.expect("dbt run succeeds");
        std::env::remove_var("DBT_PROFILES_DIR");

        assert_eq!(outcome.command, "run");
        assert!(dir.path().join("nexus_fixture.duckdb").exists());

        let run_results = outcome.run_results.expect("run_results.json was captured");
        assert_eq!(run_results.results.len(), 2, "both models ran");
        assert!(run_results.results.iter().all(|r| r.status == "success"));

        let lineage = outcome.lineage.expect("manifest.json was captured");
        assert_eq!(
            lineage.parent_map.get("model.nexus_fixture.two").unwrap(),
            &vec!["model.nexus_fixture.one".to_string()],
            "model two's real lineage traces back to model one"
        );
    }

    #[tokio::test]
    async fn surfaces_dbt_failure_as_an_error() {
        require_dbt_cli_or_skip!();
        let dir = tempfile::tempdir().unwrap();
        write_fixture_project(dir.path());
        fs::write(
            dir.path().join("models").join("broken.sql"),
            "select * from a_table_that_does_not_exist",
        )
        .unwrap();

        let config = DbtConfig {
            project_dir: dir.path().to_string_lossy().into_owned(),
            command: DbtCommand::Run,
            select: None,
        };

        std::env::set_var("DBT_PROFILES_DIR", dir.path());
        let err = run(&config).await.expect_err("broken model fails the run");
        std::env::remove_var("DBT_PROFILES_DIR");

        assert!(err.to_string().contains("dbt run exited"));
    }
}

#[cfg(all(test, not(feature = "dbt")))]
mod feature_disabled_tests {
    use super::*;

    #[tokio::test]
    async fn returns_a_clear_error_when_the_feature_is_off() {
        let config = DbtConfig {
            project_dir: "/tmp/does-not-matter".to_string(),
            command: DbtCommand::Run,
            select: None,
        };
        let err = run(&config)
            .await
            .expect_err("dbt feature is off in this build");
        assert!(err.to_string().contains("without the `dbt` feature"));
    }
}
