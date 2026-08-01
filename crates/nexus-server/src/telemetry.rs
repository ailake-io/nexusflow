use opentelemetry::KeyValue;
use opentelemetry_otlp::{SpanExporter, WithExportConfig};
use opentelemetry_sdk::trace::{
    span_processor_with_async_runtime::BatchSpanProcessor, SdkTracerProvider,
};
use opentelemetry_sdk::Resource;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

/// Initializes the global tracing subscriber — JSON logs to stdout always
/// (so a plain `docker logs`/self-host deployment still gets structured
/// output, ARCHITECTURE.md §9), plus OTel span export to an OTLP/HTTP
/// collector when `NEXUS_OTLP_ENDPOINT` is set. Off by default: this is
/// observability infra, not security, so a missing endpoint is a no-op
/// (same posture as the Slack webhook in alerts.rs), not a startup failure.
pub fn init() -> anyhow::Result<()> {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let json_layer = tracing_subscriber::fmt::layer().json();

    match std::env::var("NEXUS_OTLP_ENDPOINT") {
        Ok(endpoint) => {
            let exporter = SpanExporter::builder()
                .with_http()
                .with_endpoint(endpoint)
                .build()?;
            // The default (non-async) BatchSpanProcessor spawns a plain OS
            // thread and panics the first time it tries to poll our async
            // (reqwest/tokio-based) HTTP exporter from there — "no reactor
            // running". The `_with_async_runtime` variant instead drives the
            // batch loop as a real tokio task via `runtime::Tokio`.
            let batch_processor =
                BatchSpanProcessor::builder(exporter, opentelemetry_sdk::runtime::Tokio).build();
            let provider = SdkTracerProvider::builder()
                .with_span_processor(batch_processor)
                .with_resource(
                    Resource::builder()
                        .with_attribute(KeyValue::new("service.name", "nexus-server"))
                        .build(),
                )
                .build();

            let tracer = opentelemetry::trace::TracerProvider::tracer(&provider, "nexus-server");
            opentelemetry::global::set_tracer_provider(provider);

            tracing_subscriber::registry()
                .with(env_filter)
                .with(json_layer)
                .with(tracing_opentelemetry::layer().with_tracer(tracer))
                .try_init()?;
        }
        Err(_) => {
            tracing_subscriber::registry()
                .with(env_filter)
                .with(json_layer)
                .try_init()?;
            tracing::warn!(
                "NEXUS_OTLP_ENDPOINT not set — traces stay local (JSON logs only), not \
                 exported to an OTel collector"
            );
        }
    }

    Ok(())
}
