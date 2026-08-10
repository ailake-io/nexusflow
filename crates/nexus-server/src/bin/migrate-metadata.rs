//! CLI for `nexus_server::migrate::run` — see that module's doc comment for
//! what this does and does not do (byte-for-byte `spec_ciphertext` copy,
//! same `NEXUS_ENCRYPTION_KEY` required on both sides).
//!
//! Usage:
//! ```text
//! migrate-metadata \
//!   --auth-sqlite sqlite://nexusflow-auth.db --auth-postgres postgres://user:pass@host/db \
//!   --pipelines-sqlite sqlite://nexusflow-pipelines.db --pipelines-postgres postgres://user:pass@host/db \
//!   --checkpoint-sqlite sqlite://nexusflow.db --checkpoint-postgres postgres://user:pass@host/db
//! ```

use std::collections::HashMap;

const REQUIRED_FLAGS: &[&str] = &[
    "--auth-sqlite",
    "--auth-postgres",
    "--pipelines-sqlite",
    "--pipelines-postgres",
    "--checkpoint-sqlite",
    "--checkpoint-postgres",
];

fn parse_args() -> anyhow::Result<HashMap<String, String>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut flags = HashMap::new();
    let mut i = 0;
    while i < args.len() {
        let flag = &args[i];
        let value = args
            .get(i + 1)
            .ok_or_else(|| anyhow::anyhow!("flag {flag:?} needs a value"))?;
        flags.insert(flag.clone(), value.clone());
        i += 2;
    }
    let missing: Vec<&&str> = REQUIRED_FLAGS
        .iter()
        .filter(|f| !flags.contains_key(**f))
        .collect();
    if !missing.is_empty() {
        anyhow::bail!(
            "missing required flags: {}\n\nUsage: migrate-metadata {}",
            missing
                .iter()
                .map(|f| f.to_string())
                .collect::<Vec<_>>()
                .join(", "),
            REQUIRED_FLAGS
                .iter()
                .map(|f| format!("{f} <url>"))
                .collect::<Vec<_>>()
                .join(" "),
        );
    }
    Ok(flags)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let flags = parse_args()?;

    let summary = nexus_server::migrate::run(
        &flags["--auth-sqlite"],
        &flags["--auth-postgres"],
        &flags["--pipelines-sqlite"],
        &flags["--pipelines-postgres"],
        &flags["--checkpoint-sqlite"],
        &flags["--checkpoint-postgres"],
    )
    .await?;

    println!("Migration complete:");
    println!("  users:          {}", summary.users);
    println!("  audit_log:      {}", summary.audit_log);
    println!("  pipelines:      {}", summary.pipelines);
    println!("  pipeline_runs:  {}", summary.pipeline_runs);
    println!("  checkpoints:    {}", summary.checkpoints);
    println!(
        "\nPoint NEXUS_AUTH_DB/NEXUS_PIPELINES_DB/NEXUS_CHECKPOINT_DB at the postgres:// URLs \
         above and restart nexus-server with the SAME NEXUS_ENCRYPTION_KEY as before."
    );

    Ok(())
}
