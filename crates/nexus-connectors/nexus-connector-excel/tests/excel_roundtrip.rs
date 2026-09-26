use arrow_array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use futures::StreamExt;
use nexus_connector_excel::{
    ExcelConnectorConfig, ExcelDataType, ExcelFieldSpec, ExcelSink, ExcelSource, StorageType,
};
use nexus_core::{CheckpointCursor, Opcode, Sink, Source, OPCODE_COLUMN};
use rust_xlsxwriter::Workbook;
use std::sync::Arc;

fn base_config(path: &std::path::Path) -> ExcelConnectorConfig {
    ExcelConnectorConfig {
        storage: StorageType::Local,
        path: path.to_string_lossy().to_string(),
        bucket: None,
        region: None,
        access_key_id: None,
        secret_access_key: None,
        endpoint: None,
        sheet_name: None,
        sheet_index: 0,
        has_header: true,
        fields: vec![
            ExcelFieldSpec {
                name: "id".to_string(),
                data_type: ExcelDataType::Int64,
                nullable: false,
            },
            ExcelFieldSpec {
                name: "name".to_string(),
                data_type: ExcelDataType::Utf8,
                nullable: false,
            },
            ExcelFieldSpec {
                name: "amount".to_string(),
                data_type: ExcelDataType::Float64,
                nullable: false,
            },
        ],
        schema_sample_rows: 100,
        primary_key: Some("id".to_string()),
        storage_options: Default::default(),
        timeout_seconds: 30,
    }
}

#[tokio::test]
async fn reads_a_real_xlsx_fixture_with_explicit_fields() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fixture.xlsx");

    let mut wb = Workbook::new();
    let ws = wb.add_worksheet();
    ws.write_string(0, 0, "id").unwrap();
    ws.write_string(0, 1, "name").unwrap();
    ws.write_string(0, 2, "amount").unwrap();
    ws.write_number(1, 0, 1.0).unwrap();
    ws.write_string(1, 1, "Alice").unwrap();
    ws.write_number(1, 2, 10.5).unwrap();
    ws.write_number(2, 0, 2.0).unwrap();
    ws.write_string(2, 1, "Bob").unwrap();
    ws.write_number(2, 2, 20.0).unwrap();
    wb.save(&path).unwrap();

    let cfg = base_config(&path);
    let mut source = ExcelSource::connect(&cfg).await.expect("connects");
    assert_eq!(source.schema().fields().len(), 3);

    let mut stream = source.read_batches().await.expect("reads");
    let batch = stream.next().await.expect("one batch").expect("ok");
    assert_eq!(batch.num_rows(), 2);

    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ids.values(), &[1, 2]);

    let names = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "Alice");
    assert_eq!(names.value(1), "Bob");

    let amounts = batch
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(amounts.values(), &[10.5, 20.0]);
}

#[tokio::test]
async fn infers_a_schema_when_fields_is_empty() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fixture.xlsx");

    let mut wb = Workbook::new();
    let ws = wb.add_worksheet();
    ws.write_string(0, 0, "id").unwrap();
    ws.write_string(0, 1, "name").unwrap();
    ws.write_number(1, 0, 1.0).unwrap();
    ws.write_string(1, 1, "Alice").unwrap();
    wb.save(&path).unwrap();

    let mut cfg = base_config(&path);
    cfg.fields = vec![];
    let source = ExcelSource::connect(&cfg).await.expect("connects");
    let schema = source.schema();
    assert_eq!(schema.field(0).name(), "id");
    assert_eq!(schema.field(1).name(), "name");
}

fn record_batch(rows: &[(i64, &str, f64)]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("amount", DataType::Float64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

fn cdc_delete_batch(ids: &[i64]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("amount", DataType::Float64, false),
        Field::new(OPCODE_COLUMN, DataType::Utf8, false),
    ]));
    let n = ids.len();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(StringArray::from(vec![""; n])),
            Arc::new(Float64Array::from(vec![0.0; n])),
            Arc::new(StringArray::from(vec![Opcode::Delete.as_str(); n])),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn sink_write_then_source_read_round_trips_upserts_and_deletes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("out.xlsx");
    let cfg = base_config(&path);

    let mut sink = ExcelSink::connect(&cfg).expect("connects");
    sink.write_batch(record_batch(&[(1, "Alice", 10.5), (2, "Bob", 20.0)]))
        .await
        .expect("first write");
    sink.commit_checkpoint(CheckpointCursor::new("p0"))
        .await
        .expect("commits");

    let mut source = ExcelSource::connect(&cfg).await.expect("reads back");
    let mut stream = source.read_batches().await.expect("reads");
    let batch = stream.next().await.expect("one batch").expect("ok");
    assert_eq!(batch.num_rows(), 2);

    // Delete id=1 via a CDC batch — the sink should read the existing
    // file, drop that row, and rewrite with only id=2 left.
    let mut sink = ExcelSink::connect(&cfg).expect("reconnects");
    sink.write_batch(cdc_delete_batch(&[1]))
        .await
        .expect("delete write");

    let mut source = ExcelSource::connect(&cfg).await.expect("reads back again");
    let mut stream = source.read_batches().await.expect("reads");
    let batch = stream.next().await.expect("one batch").expect("ok");
    assert_eq!(batch.num_rows(), 1);
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ids.values(), &[2]);
}
