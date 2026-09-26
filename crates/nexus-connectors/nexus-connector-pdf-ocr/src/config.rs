use schemars::JsonSchema;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct PdfOcrConnectorConfig {
    /// Path to a single PDF file, or a directory containing one or more
    /// PDF files (every `.pdf` directly inside it is processed) — a local
    /// filesystem path, typically populated via the Canvas's upload
    /// button/dropzone (`POST /system/upload`) rather than typed by hand.
    pub path: String,
    /// Tesseract language code(s) — `"eng"`, `"por"`, or `"por+eng"` for a
    /// multi-language document. Needs the matching `tesseract-ocr-<lang>`
    /// package installed in the runtime image.
    #[serde(default = "default_language")]
    pub language: String,
    /// Rasterization resolution (dots per inch) passed to `pdftoppm -r` —
    /// higher improves OCR accuracy on small text at the cost of time.
    #[serde(default = "default_dpi")]
    pub dpi: u32,
    /// Restrict OCR to specific pages, e.g. `"1-5,8,10-12"`. Empty/absent
    /// processes every page in the document.
    #[serde(default)]
    pub page_range: Option<String>,
    /// Words scoring below this Tesseract confidence (0-100) count toward
    /// a page's `low_confidence_word_count` output column.
    #[serde(default = "default_low_confidence_threshold")]
    pub low_confidence_threshold: u32,
}

fn default_language() -> String {
    "por+eng".to_string()
}

fn default_dpi() -> u32 {
    300
}

fn default_low_confidence_threshold() -> u32 {
    60
}
