use nexus_core::PythonTransformSpec;

/// Default per-run timeout when `PythonTransformSpec.timeout_seconds` is
/// unset — same order of magnitude as dbt's own default (300s), but
/// smaller since a cleaning script over one already-in-memory batch is
/// normally much cheaper than a full dbt run.
#[cfg(feature = "python-transform")]
const DEFAULT_TIMEOUT_SECONDS: u64 = 60;

#[cfg(feature = "python-transform")]
const MAX_OUTPUT_BYTES: usize = 1_048_576; // 1 MiB per stream, same cap as dbt.rs

/// The fixed harness script — embedded in the binary so the runtime image
/// only needs `python3` + the pre-installed packages on PATH, no extra
/// file to ship/mount. Not user-editable; `spec.script` is written
/// alongside it as a separate `script.py` the harness imports.
#[cfg(feature = "python-transform")]
const HARNESS: &str = include_str!("python_harness.py");

#[cfg(feature = "python-transform")]
fn truncate_utf8(bytes: &[u8], max_len: usize) -> String {
    if bytes.len() <= max_len {
        return String::from_utf8_lossy(bytes).into_owned();
    }
    let s = String::from_utf8_lossy(bytes);
    let mut boundary = max_len;
    while boundary > 0 && !s.is_char_boundary(boundary) {
        boundary -= 1;
    }
    let mut out = s[..boundary].to_string();
    out.push_str("\n…[truncated]");
    out
}

/// Runs `spec.script`'s `transform(df)` over `batches` in an isolated
/// `python3` subprocess (same isolation model as the dbt stage in
/// `dbt.rs`: no sandboxing beyond process boundary + timeout, gated
/// behind the same `Role::Write` bar as any other pipeline edit). Data
/// crosses the process boundary as Parquet (not JSON — CLAUDE.md §8 rule
/// 1's zero-copy intent stays in spirit: no manual field-by-field
/// serialization, just Arrow's own on-disk columnar format), round-
/// tripped through a per-run temp directory that's removed afterward.
#[cfg(feature = "python-transform")]
pub async fn apply(
    schema: arrow_schema::SchemaRef,
    batches: Vec<arrow_array::RecordBatch>,
    spec: &PythonTransformSpec,
) -> anyhow::Result<Vec<arrow_array::RecordBatch>> {
    let timeout_seconds = spec.timeout_seconds.unwrap_or(DEFAULT_TIMEOUT_SECONDS);

    let unique = format!(
        "{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default()
    );
    let tmp_dir = std::env::temp_dir().join(format!("nexusflow-py-{unique}"));
    std::fs::create_dir_all(&tmp_dir)
        .map_err(|e| anyhow::anyhow!("failed to create python transform temp dir: {e}"))?;
    // Always attempt cleanup on the way out, success or failure — a leaked
    // temp dir shouldn't accumulate across runs, but a cleanup failure
    // itself must never mask the real result below.
    let cleanup = |tmp_dir: &std::path::Path| {
        if let Err(e) = std::fs::remove_dir_all(tmp_dir) {
            tracing::warn!(path = %tmp_dir.display(), error = %e, "failed to clean up python transform temp dir");
        }
    };

    let result = run_in_temp_dir(&tmp_dir, schema, batches, spec, timeout_seconds).await;
    cleanup(&tmp_dir);
    result
}

#[cfg(feature = "python-transform")]
async fn run_in_temp_dir(
    tmp_dir: &std::path::Path,
    schema: arrow_schema::SchemaRef,
    batches: Vec<arrow_array::RecordBatch>,
    spec: &PythonTransformSpec,
    timeout_seconds: u64,
) -> anyhow::Result<Vec<arrow_array::RecordBatch>> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::arrow::ArrowWriter;
    use std::time::Duration;

    let in_path = tmp_dir.join("in.parquet");
    let out_path = tmp_dir.join("out.parquet");
    let script_path = tmp_dir.join("script.py");
    let harness_path = tmp_dir.join("harness.py");

    std::fs::write(&script_path, &spec.script)
        .map_err(|e| anyhow::anyhow!("failed to write python script: {e}"))?;
    std::fs::write(&harness_path, HARNESS)
        .map_err(|e| anyhow::anyhow!("failed to write python harness: {e}"))?;

    {
        let file = std::fs::File::create(&in_path)
            .map_err(|e| anyhow::anyhow!("failed to create python transform input file: {e}"))?;
        let mut writer = ArrowWriter::try_new(file, schema, None)
            .map_err(|e| anyhow::anyhow!("parquet writer init failed: {e}"))?;
        for batch in &batches {
            writer
                .write(batch)
                .map_err(|e| anyhow::anyhow!("parquet write failed: {e}"))?;
        }
        writer
            .close()
            .map_err(|e| anyhow::anyhow!("parquet close failed: {e}"))?;
    }

    let mut cmd = tokio::process::Command::new("python3");
    cmd.arg(&harness_path)
        .arg(&in_path)
        .arg(&out_path)
        .arg(&script_path)
        .current_dir(tmp_dir)
        .kill_on_drop(true);

    let output = tokio::time::timeout(Duration::from_secs(timeout_seconds), cmd.output())
        .await
        .map_err(|_| anyhow::anyhow!("python transform timed out after {timeout_seconds}s"))?
        .map_err(|e| anyhow::anyhow!("failed to spawn `python3`: {e} (is python3 on PATH?)"))?;

    let stdout = truncate_utf8(&output.stdout, MAX_OUTPUT_BYTES);
    let stderr = truncate_utf8(&output.stderr, MAX_OUTPUT_BYTES);
    tracing::info!(%stdout, %stderr, success = output.status.success(), "python transform finished");

    if !output.status.success() {
        anyhow::bail!("python transform script failed: {stderr}");
    }

    let file = std::fs::File::open(&out_path)
        .map_err(|e| anyhow::anyhow!("failed to open python transform output file: {e}"))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| anyhow::anyhow!("parquet reader build failed: {e}"))?
        .build()
        .map_err(|e| anyhow::anyhow!("parquet reader build failed: {e}"))?;
    reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| anyhow::anyhow!("parquet read failed: {e}"))
}

#[cfg(not(feature = "python-transform"))]
pub async fn apply(
    _schema: arrow_schema::SchemaRef,
    _batches: Vec<arrow_array::RecordBatch>,
    _spec: &PythonTransformSpec,
) -> anyhow::Result<Vec<arrow_array::RecordBatch>> {
    anyhow::bail!(
        "pipeline requests a python transform step, but nexus-server was built without \
         the `python-transform` feature — rebuild with `--features python-transform`"
    )
}

#[cfg(all(test, feature = "python-transform"))]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    /// python3 isn't a Rust dependency (see the module doc comment) — it
    /// has to actually be on PATH. Skip (not fail) when it's missing, same
    /// spirit as dbt.rs's `require_dbt_cli_or_skip!()`.
    macro_rules! require_python3_or_skip {
        () => {
            if tokio::process::Command::new("python3")
                .arg("--version")
                .output()
                .await
                .is_err()
            {
                eprintln!("skipping: `python3` not found on PATH");
                return;
            }
        };
    }

    fn sample_batch() -> (arrow_schema::SchemaRef, RecordBatch) {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        (schema, batch)
    }

    #[tokio::test]
    async fn runs_a_real_script_end_to_end() {
        require_python3_or_skip!();
        let (schema, batch) = sample_batch();
        let spec = PythonTransformSpec {
            script: "def transform(df):\n    df['x'] = df['x'] * 2\n    return df\n".to_string(),
            timeout_seconds: None,
        };

        let result = apply(schema, vec![batch], &spec)
            .await
            .expect("script runs");
        assert_eq!(result.len(), 1);
        let col = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(col.values(), &[2, 4, 6]);
    }

    #[tokio::test]
    async fn surfaces_a_script_exception_as_an_error() {
        require_python3_or_skip!();
        let (schema, batch) = sample_batch();
        let spec = PythonTransformSpec {
            script: "def transform(df):\n    raise ValueError('boom')\n".to_string(),
            timeout_seconds: None,
        };

        let err = apply(schema, vec![batch], &spec)
            .await
            .expect_err("a raising script fails the stage");
        assert!(err.to_string().contains("boom"));
    }

    #[tokio::test]
    async fn missing_transform_function_is_a_clear_error() {
        require_python3_or_skip!();
        let (schema, batch) = sample_batch();
        let spec = PythonTransformSpec {
            script: "x = 1\n".to_string(),
            timeout_seconds: None,
        };

        let err = apply(schema, vec![batch], &spec)
            .await
            .expect_err("a script with no transform() fails the stage");
        assert!(err.to_string().contains("transform"));
    }
}

#[cfg(all(test, not(feature = "python-transform")))]
mod feature_disabled_tests {
    use super::*;

    #[tokio::test]
    async fn returns_a_clear_error_when_the_feature_is_off() {
        let schema: arrow_schema::SchemaRef = std::sync::Arc::new(arrow_schema::Schema::empty());
        let spec = PythonTransformSpec {
            script: "def transform(df):\n    return df\n".to_string(),
            timeout_seconds: None,
        };
        let err = apply(schema, vec![], &spec)
            .await
            .expect_err("python-transform feature is off in this build");
        assert!(err
            .to_string()
            .contains("without the `python-transform` feature"));
    }
}
