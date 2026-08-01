#[tokio::main]
async fn main() -> anyhow::Result<()> {
    nexus_server::telemetry::init()?;
    nexus_server::run().await
}
