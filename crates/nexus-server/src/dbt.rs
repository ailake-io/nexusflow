use nexus_core::{DbtCommand, DbtConfig};

/// Result of one dbt invocation — logged (`tracing::info!`) at the call
/// site, and returned so `run_pipeline_handler` can fold it into the run's
/// success/failure and (task #23) parse `manifest.json`/`run_results.json`
/// out of `config.project_dir/target/`.
#[derive(Debug, Clone)]
pub struct DbtOutcome {
    pub command: &'static str,
    pub stdout: String,
    pub stderr: String,
}

fn subcommand(command: DbtCommand) -> &'static str {
    match command {
        DbtCommand::Run => "run",
        DbtCommand::Build => "build",
        DbtCommand::Test => "test",
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

    if !output.status.success() {
        anyhow::bail!("dbt {command} exited with {}: {stderr}", output.status);
    }

    Ok(DbtOutcome {
        command,
        stdout,
        stderr,
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
