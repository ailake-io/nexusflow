//! OCR via CLI, not FFI: `pdftoppm` (poppler-utils) rasterizes the wanted
//! pages of a PDF to PNG, `tesseract` OCRs each PNG. Same "shell out to a
//! binary pre-installed in the runtime image" posture nexusflow already
//! uses for `dbt`/`python-transform` (CLAUDE.md §4.4/§8) — real FFI
//! bindings (`leptess`/`pdfium-render`) are more fragile to vendor/build
//! than a documented `apt-get install` line.
//!
//! Whole-document, not streaming — same trade-off `nexus-connector-csv`'s
//! `CsvSource` and `nexus-connector-excel`'s `ExcelSource` already make (a
//! PDF, like a spreadsheet, isn't line-delimited).

use crate::config::PdfOcrConnectorConfig;
use crate::pagerange::parse_page_range;
use crate::tsv::parse_tesseract_tsv;
use arrow_array::{ArrayRef, Float32Array, RecordBatch, StringArray, UInt32Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use nexus_core::{NexusError, Source};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::process::Command;

pub struct PdfOcrSource {
    schema: SchemaRef,
    batch: RecordBatch,
}

struct PageRow {
    file_name: String,
    page_number: u32,
    text: String,
    char_count: u32,
    avg_confidence: f32,
    low_confidence_word_count: u32,
}

fn build_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("file_name", DataType::Utf8, false),
        Field::new("page_number", DataType::UInt32, false),
        Field::new("text", DataType::Utf8, false),
        Field::new("char_count", DataType::UInt32, false),
        Field::new("avg_confidence", DataType::Float32, false),
        Field::new("low_confidence_word_count", DataType::UInt32, false),
    ]))
}

fn rows_to_batch(schema: SchemaRef, rows: &[PageRow]) -> Result<RecordBatch, NexusError> {
    let file_name: ArrayRef = Arc::new(StringArray::from_iter_values(
        rows.iter().map(|r| r.file_name.as_str()),
    ));
    let page_number: ArrayRef = Arc::new(UInt32Array::from_iter_values(
        rows.iter().map(|r| r.page_number),
    ));
    let text: ArrayRef = Arc::new(StringArray::from_iter_values(
        rows.iter().map(|r| r.text.as_str()),
    ));
    let char_count: ArrayRef = Arc::new(UInt32Array::from_iter_values(
        rows.iter().map(|r| r.char_count),
    ));
    let avg_confidence: ArrayRef = Arc::new(Float32Array::from_iter_values(
        rows.iter().map(|r| r.avg_confidence),
    ));
    let low_confidence_word_count: ArrayRef = Arc::new(UInt32Array::from_iter_values(
        rows.iter().map(|r| r.low_confidence_word_count),
    ));
    RecordBatch::try_new(
        schema,
        vec![
            file_name,
            page_number,
            text,
            char_count,
            avg_confidence,
            low_confidence_word_count,
        ],
    )
    .map_err(|e| NexusError::Schema(e.to_string()))
}

impl PdfOcrSource {
    pub async fn connect(cfg: &PdfOcrConnectorConfig) -> Result<Self, NexusError> {
        let path = PathBuf::from(&cfg.path);
        let pdf_files = list_pdf_files(&path).await?;
        if pdf_files.is_empty() {
            return Err(NexusError::Connector(format!(
                "pdf-ocr: no .pdf files found under {path:?}"
            )));
        }

        let mut rows = Vec::new();
        for pdf_path in pdf_files {
            rows.extend(process_pdf(&pdf_path, cfg).await?);
        }

        let schema = build_schema();
        let batch = rows_to_batch(schema.clone(), &rows)?;
        Ok(Self { schema, batch })
    }
}

#[async_trait]
impl Source for PdfOcrSource {
    async fn read_batches(
        &mut self,
    ) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError> {
        Ok(Box::pin(stream::once(async { Ok(self.batch.clone()) })))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// `path` pointing at a single `.pdf` processes just that file; pointing
/// at a directory processes every `.pdf` directly inside it (no recursion
/// into subdirectories — matches the flat batch-upload directory shape
/// `POST /system/upload` produces for a multi-file selection).
async fn list_pdf_files(path: &Path) -> Result<Vec<PathBuf>, NexusError> {
    let metadata = tokio::fs::metadata(path)
        .await
        .map_err(|e| NexusError::Connector(format!("pdf-ocr: path {path:?} not found: {e}")))?;
    if metadata.is_file() {
        return Ok(vec![path.to_path_buf()]);
    }

    let mut entries = tokio::fs::read_dir(path)
        .await
        .map_err(|e| NexusError::Connector(format!("pdf-ocr: could not list {path:?}: {e}")))?;
    let mut files = Vec::new();
    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|e| NexusError::Connector(format!("pdf-ocr: readdir error under {path:?}: {e}")))?
    {
        let entry_path = entry.path();
        let is_pdf = entry_path
            .extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| ext.eq_ignore_ascii_case("pdf"))
            .unwrap_or(false);
        if is_pdf {
            files.push(entry_path);
        }
    }
    files.sort();
    Ok(files)
}

/// `pdftoppm -png` names each output file `{prefix}-{page}.png`, zero-padded
/// to the document's total page count — the real page number, unaffected
/// by `-f`/`-l` (rendering pages 5-8 still produces `page-05.png`, not
/// `page-01.png`), so it's read back from the filename rather than the
/// loop index.
fn page_number_from_filename(png_path: &Path) -> Result<u32, NexusError> {
    let stem = png_path.file_stem().and_then(|s| s.to_str()).unwrap_or("");
    let digits: String = stem
        .chars()
        .rev()
        .take_while(|c| c.is_ascii_digit())
        .collect::<String>()
        .chars()
        .rev()
        .collect();
    digits.parse().map_err(|_| {
        NexusError::Connector(format!(
            "pdf-ocr: unexpected rendered page filename {png_path:?}"
        ))
    })
}

async fn process_pdf(
    pdf_path: &Path,
    cfg: &PdfOcrConnectorConfig,
) -> Result<Vec<PageRow>, NexusError> {
    let file_name = pdf_path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("unknown.pdf")
        .to_string();

    let wanted_pages: Option<BTreeSet<u32>> = match cfg.page_range.as_deref() {
        Some(spec) if !spec.trim().is_empty() => {
            Some(parse_page_range(spec).map_err(NexusError::Connector)?)
        }
        _ => None,
    };

    let tmp_dir = tempfile::tempdir().map_err(|e| {
        NexusError::Connector(format!("pdf-ocr: could not create scratch dir: {e}"))
    })?;
    let prefix = tmp_dir.path().join("page");

    let mut cmd = Command::new("pdftoppm");
    cmd.arg("-r").arg(cfg.dpi.to_string()).arg("-png");
    if let Some(pages) = &wanted_pages {
        // Bounded to the requested set's own min..max — cheaper than
        // rendering the whole document when the range is narrow, without
        // needing one pdftoppm invocation per contiguous sub-range for a
        // comma-separated set (a handful of extra rendered-but-unused
        // pages inside the bound costs far less than re-invoking the
        // rasterizer once per segment).
        let min = *pages
            .iter()
            .next()
            .expect("non-empty, checked by parse_page_range");
        let max = *pages
            .iter()
            .next_back()
            .expect("non-empty, checked by parse_page_range");
        cmd.arg("-f")
            .arg(min.to_string())
            .arg("-l")
            .arg(max.to_string());
    }
    cmd.arg(pdf_path).arg(&prefix);

    let output = cmd.output().await.map_err(|e| {
        NexusError::Connector(format!(
            "pdf-ocr: pdftoppm failed to start ({e}) — is poppler-utils installed in this image?"
        ))
    })?;
    if !output.status.success() {
        return Err(NexusError::Connector(format!(
            "pdf-ocr: pdftoppm failed for {file_name}: {}",
            String::from_utf8_lossy(&output.stderr)
        )));
    }

    let mut png_files: Vec<PathBuf> = std::fs::read_dir(tmp_dir.path())
        .map_err(|e| NexusError::Connector(format!("pdf-ocr: could not list rendered pages: {e}")))?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|ext| ext.to_str()) == Some("png"))
        .collect();
    png_files.sort();

    let mut rows = Vec::new();
    for png_path in png_files {
        let page_number = page_number_from_filename(&png_path)?;
        if let Some(pages) = &wanted_pages {
            if !pages.contains(&page_number) {
                continue;
            }
        }

        let tsv_output = Command::new("tesseract")
            .arg(&png_path)
            .arg("stdout")
            .arg("-l")
            .arg(&cfg.language)
            .arg("tsv")
            .output()
            .await
            .map_err(|e| {
                NexusError::Connector(format!(
                    "pdf-ocr: tesseract failed to start ({e}) — is tesseract-ocr installed in this image?"
                ))
            })?;
        if !tsv_output.status.success() {
            return Err(NexusError::Connector(format!(
                "pdf-ocr: tesseract failed for {file_name} page {page_number}: {}",
                String::from_utf8_lossy(&tsv_output.stderr)
            )));
        }

        let tsv_text = String::from_utf8_lossy(&tsv_output.stdout);
        let result = parse_tesseract_tsv(&tsv_text, cfg.low_confidence_threshold);
        rows.push(PageRow {
            file_name: file_name.clone(),
            page_number,
            char_count: result.text.chars().count() as u32,
            text: result.text,
            avg_confidence: result.avg_confidence,
            low_confidence_word_count: result.low_confidence_word_count,
        });
    }
    Ok(rows)
    // `tmp_dir` drops here — removes every rendered PNG along with it.
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_page_number_from_zero_padded_filename() {
        assert_eq!(
            page_number_from_filename(Path::new("/tmp/page-05.png")).unwrap(),
            5
        );
        assert_eq!(
            page_number_from_filename(Path::new("/tmp/page-1.png")).unwrap(),
            1
        );
        assert_eq!(
            page_number_from_filename(Path::new("/tmp/page-123.png")).unwrap(),
            123
        );
    }

    #[test]
    fn rejects_a_filename_with_no_trailing_digits() {
        assert!(page_number_from_filename(Path::new("/tmp/page.png")).is_err());
    }
}
