// `#[allow(dead_code)]`: lands ahead of `agent_tools.rs`'s `generate_chart`
// consuming it (ROADMAP.md Fase 31) — exercised by this module's own tests,
// nothing else calls `render` yet. Remove once `agent_runner.rs` wires the
// whole tool chain together.
#![allow(dead_code)]

/// Same default as `python_transform::DEFAULT_TIMEOUT_SECONDS` — a
/// cleaning script and a chart-rendering script are the same order of
/// magnitude of cost (one already-in-memory batch, no network I/O of
/// their own).
#[cfg(feature = "python-viz")]
const DEFAULT_TIMEOUT_SECONDS: u64 = 60;

#[cfg(feature = "python-viz")]
const MAX_OUTPUT_BYTES: usize = 1_048_576; // 1 MiB per stream, same cap as python_transform.rs

#[cfg(feature = "python-viz")]
const HARNESS: &str = include_str!("python_viz_harness.py");

/// One rendered chart — raw bytes plus the MIME type the harness reported,
/// so the HTTP layer (`GET .../visualization`, the agent's `GenerateChart`
/// tool result) can set `Content-Type` correctly regardless of which
/// plotting library produced it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenderedChart {
    pub bytes: Vec<u8>,
    pub content_type: String,
}

#[cfg(feature = "python-viz")]
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

/// Runs `spec.script`'s `visualize(df)` over `batches` in an isolated
/// `python3` subprocess — same isolation model as `python_transform::apply`
/// (process boundary + timeout only, `Role::Write`/`Role::Execute` bar
/// enforced by the caller), same per-run temp directory lifecycle.
#[cfg(feature = "python-viz")]
pub async fn render(
    schema: arrow_schema::SchemaRef,
    batches: Vec<arrow_array::RecordBatch>,
    script: &str,
    timeout_seconds: Option<u64>,
) -> anyhow::Result<RenderedChart> {
    let timeout_seconds = timeout_seconds.unwrap_or(DEFAULT_TIMEOUT_SECONDS);

    // Same uniqueness reasoning as `python_transform::apply` — PID +
    // nanosecond timestamp alone can collide under a coarse clock tick, an
    // atomic counter can't.
    static CALL_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let call_id = CALL_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let unique = format!(
        "{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default(),
        call_id
    );
    let tmp_dir = std::env::temp_dir().join(format!("nexusflow-viz-{unique}"));
    std::fs::create_dir_all(&tmp_dir)
        .map_err(|e| anyhow::anyhow!("failed to create python viz temp dir: {e}"))?;
    let cleanup = |tmp_dir: &std::path::Path| {
        if let Err(e) = std::fs::remove_dir_all(tmp_dir) {
            tracing::warn!(path = %tmp_dir.display(), error = %e, "failed to clean up python viz temp dir");
        }
    };

    let result = run_in_temp_dir(&tmp_dir, schema, batches, timeout_seconds, script).await;
    cleanup(&tmp_dir);
    result
}

#[cfg(feature = "python-viz")]
async fn run_in_temp_dir(
    tmp_dir: &std::path::Path,
    schema: arrow_schema::SchemaRef,
    batches: Vec<arrow_array::RecordBatch>,
    timeout_seconds: u64,
    script: &str,
) -> anyhow::Result<RenderedChart> {
    use parquet::arrow::ArrowWriter;
    use std::time::Duration;

    let in_path = tmp_dir.join("in.parquet");
    let out_path = tmp_dir.join("out.bin");
    let contenttype_path = tmp_dir.join("out.contenttype");
    let script_path = tmp_dir.join("script.py");
    let harness_path = tmp_dir.join("harness.py");

    std::fs::write(&script_path, script)
        .map_err(|e| anyhow::anyhow!("failed to write python viz script: {e}"))?;
    std::fs::write(&harness_path, HARNESS)
        .map_err(|e| anyhow::anyhow!("failed to write python viz harness: {e}"))?;

    {
        let file = std::fs::File::create(&in_path)
            .map_err(|e| anyhow::anyhow!("failed to create python viz input file: {e}"))?;
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
        .arg(&contenttype_path)
        .arg(&script_path)
        .current_dir(tmp_dir)
        .kill_on_drop(true);

    let output = tokio::time::timeout(Duration::from_secs(timeout_seconds), cmd.output())
        .await
        .map_err(|_| anyhow::anyhow!("python viz timed out after {timeout_seconds}s"))?
        .map_err(|e| anyhow::anyhow!("failed to spawn `python3`: {e} (is python3 on PATH?)"))?;

    let stdout = truncate_utf8(&output.stdout, MAX_OUTPUT_BYTES);
    let stderr = truncate_utf8(&output.stderr, MAX_OUTPUT_BYTES);
    tracing::info!(%stdout, %stderr, success = output.status.success(), "python viz finished");

    if !output.status.success() {
        anyhow::bail!("python viz script failed: {stderr}");
    }

    let bytes = std::fs::read(&out_path)
        .map_err(|e| anyhow::anyhow!("failed to read python viz output file: {e}"))?;
    let content_type = std::fs::read_to_string(&contenttype_path)
        .map_err(|e| anyhow::anyhow!("failed to read python viz content-type file: {e}"))?
        .trim()
        .to_string();

    Ok(RenderedChart {
        bytes,
        content_type,
    })
}

#[cfg(not(feature = "python-viz"))]
pub async fn render(
    _schema: arrow_schema::SchemaRef,
    _batches: Vec<arrow_array::RecordBatch>,
    _script: &str,
    _timeout_seconds: Option<u64>,
) -> anyhow::Result<RenderedChart> {
    anyhow::bail!(
        "agent requests the generate_chart tool, but nexus-server was built without the \
         `python-viz` feature — rebuild with `--features python-viz`"
    )
}

#[cfg(all(test, feature = "python-viz"))]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    /// Same skip-not-fail posture as `python_transform.rs`'s own macro —
    /// `python3` plus matplotlib have to actually be importable, which a
    /// developer's local machine often won't have (production image
    /// installs it, see Dockerfile).
    macro_rules! require_python3_matplotlib_or_skip {
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
            if !tokio::process::Command::new("python3")
                .args(["-c", "import pandas, pyarrow, matplotlib"])
                .output()
                .await
                .map(|o| o.status.success())
                .unwrap_or(false)
            {
                eprintln!(
                    "skipping: `python3` lacks `pandas`/`pyarrow`/`matplotlib` (see Dockerfile)"
                );
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
    async fn renders_a_matplotlib_figure_as_png() {
        require_python3_matplotlib_or_skip!();
        let (schema, batch) = sample_batch();
        let script = "import matplotlib\nmatplotlib.use('Agg')\nimport matplotlib.pyplot as plt\n\
                     def visualize(df):\n    fig, ax = plt.subplots()\n    ax.plot(df['x'])\n    return fig\n";

        let chart = render(schema, vec![batch], script, None)
            .await
            .expect("script runs");
        assert_eq!(chart.content_type, "image/png");
        // PNG magic bytes.
        assert_eq!(
            &chart.bytes[..8],
            &[0x89, b'P', b'N', b'G', b'\r', b'\n', 0x1a, b'\n']
        );
    }

    #[tokio::test]
    async fn renders_a_tuple_return_as_is() {
        require_python3_matplotlib_or_skip!();
        let (schema, batch) = sample_batch();
        let script = "def visualize(df):\n    return (b'<svg></svg>', 'image/svg+xml')\n";

        let chart = render(schema, vec![batch], script, None)
            .await
            .expect("script runs");
        assert_eq!(chart.content_type, "image/svg+xml");
        assert_eq!(chart.bytes, b"<svg></svg>");
    }

    #[tokio::test]
    async fn surfaces_a_script_exception_as_an_error() {
        require_python3_matplotlib_or_skip!();
        let (schema, batch) = sample_batch();
        let script = "def visualize(df):\n    raise ValueError('boom')\n";

        let err = render(schema, vec![batch], script, None).await.unwrap_err();
        assert!(err.to_string().contains("python viz script failed"));
    }

    #[tokio::test]
    async fn surfaces_a_bad_return_type_as_an_error() {
        require_python3_matplotlib_or_skip!();
        let (schema, batch) = sample_batch();
        let script = "def visualize(df):\n    return 42\n";

        let err = render(schema, vec![batch], script, None).await.unwrap_err();
        assert!(err.to_string().contains("python viz script failed"));
    }
}
