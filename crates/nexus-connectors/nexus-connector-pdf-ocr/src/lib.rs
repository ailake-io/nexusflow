//! PDF OCR connector — extracts text from scanned/image-based PDFs
//! (`pdftoppm` + `tesseract`, see `source.rs`'s doc comment). Follows the
//! same two-registration contract as `nexus-connector-excel`:
//! `submit_connector!` for the catalog/license gate,
//! `submit_source_builder!` for the actual `Box<dyn Source>` construction
//! `nexus-server`'s closed `build_source` match can't host directly.
//!
//! Source-only — there's no meaningful "write OCR'd text back into a PDF"
//! sink, unlike csv/excel's read+write symmetry.

mod config;
mod pagerange;
mod source;
mod tsv;

pub use config::PdfOcrConnectorConfig;
pub use source::PdfOcrSource;

use futures::future::BoxFuture;
use nexus_core::{ConnectorCapability, NexusError, Source};

nexus_core::submit_connector!(
    "pdf-ocr",
    ConnectorCapability::Bridged,
    PdfOcrConnectorConfig
);
// `path` is documented (config.rs) as a local filesystem path exactly like
// csv/excel's own — opts into nexus-core's `LocalPathConnector` registry so
// an absolute path (e.g. one just returned by `POST /system/upload`) isn't
// rejected by dag.rs's SSRF-style guard.
nexus_core::submit_local_path_connector!("pdf-ocr");

fn validate_pdf_ocr_config(cfg: &serde_json::Value) -> Result<(), NexusError> {
    serde_json::from_value::<PdfOcrConnectorConfig>(cfg.clone())
        .map(|_| ())
        .map_err(|e| NexusError::Serialization(e.to_string()))
}

nexus_core::submit_source_builder!(
    "pdf-ocr",
    validate_pdf_ocr_config,
    |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Source>, NexusError>> {
        Box::pin(async move {
            let parsed: PdfOcrConnectorConfig = serde_json::from_value(cfg)
                .map_err(|e| NexusError::Serialization(e.to_string()))?;
            let source = PdfOcrSource::connect(&parsed).await?;
            Ok(Box::new(source) as Box<dyn Source>)
        })
    }
);
