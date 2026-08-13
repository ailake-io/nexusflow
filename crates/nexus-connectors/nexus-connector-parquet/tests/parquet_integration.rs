//! Real end-to-end: same pipeline shape as the other Marco 6 formats —
//! writes a batch, reads it back, validates schema/data, then a CDC delete
//! batch confirms row removal via the read-filter-rewrite path. Plain local
//! file, no container. See IMPLEMENTATION_PLAN.md Marco 6.

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use futures::StreamExt;
use nexus_connector_parquet::{
    ParquetCompression, ParquetConnectorConfig, ParquetSink, ParquetSource, StorageType,
};
use nexus_core::{Sink, Source};
use std::sync::Arc;

#[tokio::test]
async fn writes_reads_back_and_deletes_via_rewrite() {
    let dir = tempfile::tempdir().expect("tempdir creates");
    let path = dir.path().join("orders.parquet");

    let cfg = ParquetConnectorConfig {
        path: path.to_str().unwrap().to_string(),
        primary_key: "id".to_string(),
        storage: StorageType::Local,
        compression: ParquetCompression::Snappy,
        ..Default::default()
    };

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("status", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["pending", "pending", "pending"])),
        ],
    )
    .unwrap();

    let mut sink = ParquetSink::connect(&cfg).expect("sink connects");
    sink.write_batch(batch).await.expect("writes batch");

    // --- read back and validate schema/data ---
    let mut source = ParquetSource::connect(&cfg).await.expect("source connects");
    let schema = source.schema();
    assert!(schema.field_with_name("id").is_ok());
    assert!(schema.field_with_name("status").is_ok());

    let mut stream = source.read_batches().await.expect("reads batches");
    let mut ids = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch.expect("batch reads ok");
        let id_col = batch
            .column(batch.schema().index_of("id").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .clone();
        ids.extend((0..id_col.len()).map(|i| id_col.value(i)));
    }
    drop(stream);
    ids.sort();
    assert_eq!(ids, vec![1, 2, 3]);

    // --- CDC upsert: update id=2's status ---
    let update_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("status", DataType::Utf8, false),
    ]));
    let update_batch = RecordBatch::try_new(
        update_schema,
        vec![
            Arc::new(Int64Array::from(vec![2i64])),
            Arc::new(StringArray::from(vec!["paid"])),
        ],
    )
    .unwrap();
    sink.write_batch(update_batch)
        .await
        .expect("writes update batch");

    // --- CDC delete: id=1 must be removed, not kept ---
    let delete_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("status", DataType::Utf8, false),
        Field::new(nexus_core::OPCODE_COLUMN, DataType::Utf8, false),
    ]));
    let delete_batch = RecordBatch::try_new(
        delete_schema,
        vec![
            Arc::new(Int64Array::from(vec![1i64])),
            Arc::new(StringArray::from(vec!["pending"])),
            Arc::new(StringArray::from(vec!["D"])),
        ],
    )
    .unwrap();
    sink.write_batch(delete_batch)
        .await
        .expect("writes delete batch");

    // --- re-read: id=1 gone, id=2 updated, id=3 unchanged ---
    let mut source = ParquetSource::connect(&cfg)
        .await
        .expect("source reconnects");
    let mut stream = source.read_batches().await.expect("reads batches");
    let mut rows: Vec<(i64, String)> = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch.expect("batch reads ok");
        let id_col = batch
            .column(batch.schema().index_of("id").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .clone();
        let status_col = batch
            .column(batch.schema().index_of("status").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .clone();
        for i in 0..id_col.len() {
            rows.push((id_col.value(i), status_col.value(i).to_string()));
        }
    }
    rows.sort_by_key(|(id, _)| *id);
    assert_eq!(
        rows,
        vec![(2, "paid".to_string()), (3, "pending".to_string()),],
        "id=1 must have been deleted, id=2 updated, id=3 unchanged: {rows:?}"
    );
}

#[cfg(test)]
mod config_helpers {
    use nexus_connector_parquet::{ParquetConnectorConfig, StorageType};

    #[test]
    fn legacy_uri_takes_precedence() {
        let cfg = ParquetConnectorConfig {
            uri: Some("s3://legacy-bucket/key.parquet".to_string()),
            path: "/local/file.parquet".to_string(),
            storage: StorageType::Local,
            bucket: None,
            ..Default::default()
        };
        assert_eq!(cfg.uri().unwrap(), "s3://legacy-bucket/key.parquet");
    }

    #[test]
    fn local_storage_uses_path() {
        let cfg = ParquetConnectorConfig {
            path: "/data/orders.parquet".to_string(),
            storage: StorageType::Local,
            ..Default::default()
        };
        assert_eq!(cfg.uri().unwrap(), "/data/orders.parquet");
    }

    #[test]
    fn s3_uri_is_built_from_parts() {
        let cfg = ParquetConnectorConfig {
            path: "/folder/orders.parquet".to_string(),
            storage: StorageType::S3,
            bucket: Some("my-bucket".to_string()),
            ..Default::default()
        };
        assert_eq!(cfg.uri().unwrap(), "s3://my-bucket/folder/orders.parquet");
    }

    #[test]
    fn gcs_uri_is_built_from_parts() {
        let cfg = ParquetConnectorConfig {
            path: "folder/orders.parquet".to_string(),
            storage: StorageType::Gcs,
            bucket: Some("my-bucket".to_string()),
            ..Default::default()
        };
        assert_eq!(cfg.uri().unwrap(), "gs://my-bucket/folder/orders.parquet");
    }

    #[test]
    fn azure_uri_is_built_from_parts() {
        let cfg = ParquetConnectorConfig {
            path: "/folder/orders.parquet".to_string(),
            storage: StorageType::Azure,
            bucket: Some("my-container".to_string()),
            ..Default::default()
        };
        assert_eq!(
            cfg.uri().unwrap(),
            "az://my-container/folder/orders.parquet"
        );
    }

    #[test]
    fn cloud_storage_requires_bucket() {
        let cfg = ParquetConnectorConfig {
            path: "orders.parquet".to_string(),
            storage: StorageType::S3,
            bucket: None,
            ..Default::default()
        };
        assert!(cfg.uri().is_err());
    }

    #[test]
    fn s3_storage_options_map_credentials() {
        let cfg = ParquetConnectorConfig {
            storage: StorageType::S3,
            access_key_id: Some("AKIA".to_string()),
            secret_access_key: Some("secret".to_string()),
            region: Some("us-east-1".to_string()),
            endpoint: Some("http://localhost:9000".to_string()),
            ..Default::default()
        };
        let opts = cfg.storage_options();
        assert_eq!(opts.get("aws_access_key_id"), Some(&"AKIA".to_string()));
        assert_eq!(
            opts.get("aws_secret_access_key"),
            Some(&"secret".to_string())
        );
        assert_eq!(opts.get("aws_region"), Some(&"us-east-1".to_string()));
        assert_eq!(
            opts.get("aws_endpoint"),
            Some(&"http://localhost:9000".to_string())
        );
    }
}
