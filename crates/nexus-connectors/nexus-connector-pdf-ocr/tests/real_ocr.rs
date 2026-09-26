//! Real subprocess integration test — needs pdftoppm/tesseract installed
//! (they are on this dev machine; `#[ignore]` so CI/a fresh clone without
//! them doesn't fail). Run explicitly with `cargo test -- --ignored`.

use nexus_connector_pdf_ocr::{PdfOcrConnectorConfig, PdfOcrSource};
use nexus_core::Source;

fn write_minimal_pdf(path: &std::path::Path, text: &str) {
    let content = format!("BT /F1 24 Tf 10 40 Td ({text}) Tj ET");
    let objects = [
        "<< /Type /Catalog /Pages 2 0 R >>".to_string(),
        "<< /Type /Pages /Kids [3 0 R] /Count 1 >>".to_string(),
        "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 200 100] /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>".to_string(),
        "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>".to_string(),
        format!("<< /Length {} >>\nstream\n{content}\nendstream", content.len()),
    ];

    let mut out: Vec<u8> = b"%PDF-1.4\n".to_vec();
    let mut offsets = Vec::new();
    for (i, obj) in objects.iter().enumerate() {
        offsets.push(out.len());
        out.extend_from_slice(format!("{} 0 obj\n{obj}\nendobj\n", i + 1).as_bytes());
    }
    let xref_offset = out.len();
    out.extend_from_slice(format!("xref\n0 {}\n", objects.len() + 1).as_bytes());
    out.extend_from_slice(b"0000000000 65535 f \n");
    for off in &offsets {
        out.extend_from_slice(format!("{off:010} 00000 n \n").as_bytes());
    }
    out.extend_from_slice(format!("trailer\n<< /Size {} /Root 1 0 R >>\n", objects.len() + 1).as_bytes());
    out.extend_from_slice(format!("startxref\n{xref_offset}\n%%EOF").as_bytes());

    std::fs::write(path, out).unwrap();
}

#[tokio::test]
#[ignore]
async fn extracts_text_from_a_real_pdf_via_pdftoppm_and_tesseract() {
    let dir = tempfile::tempdir().unwrap();
    let pdf_path = dir.path().join("hello.pdf");
    write_minimal_pdf(&pdf_path, "Hello World");

    let cfg = PdfOcrConnectorConfig {
        path: pdf_path.to_string_lossy().to_string(),
        language: "eng".to_string(),
        dpi: 300,
        page_range: None,
        low_confidence_threshold: 60,
    };

    let mut source = PdfOcrSource::connect(&cfg).await.expect("connect should succeed");
    let mut stream = source.read_batches().await.expect("read_batches should succeed");

    use futures::StreamExt;
    let batch = stream.next().await.expect("one batch").expect("batch ok");
    assert_eq!(batch.num_rows(), 1);

    let text_col = batch
        .column_by_name("text")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap();
    let text = text_col.value(0);
    assert!(
        text.contains("Hello") && text.contains("World"),
        "expected OCR'd text to contain 'Hello World', got: {text:?}"
    );

    let confidence_col = batch
        .column_by_name("avg_confidence")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow_array::Float32Array>()
        .unwrap();
    assert!(confidence_col.value(0) > 80.0, "expected high confidence on clean rendered text");
}
