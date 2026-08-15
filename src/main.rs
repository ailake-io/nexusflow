#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Handled before telemetry/run() so `--version` never needs
    // NEXUS_JWT_SECRET/NEXUS_ENCRYPTION_KEY — release.yml's smoke-test step
    // just extracts the tarball and runs this with no env vars set.
    if std::env::args().nth(1).as_deref() == Some("--version") {
        println!("nexusflow {}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }
    nexus_server::telemetry::init()?;
    nexus_server::run().await
}
