//! Real end-to-end: chunk → embed(CPU) → Milvus, including CDC delete.
//! Milvus standalone needs etcd + object storage (minio) — 3 containers,
//! same testcontainers pattern as the Marco 4 Debezium test. See
//! IMPLEMENTATION_PLAN.md Marco 5.

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use milvus::client::Client as MilvusClient;
use milvus::index::{IndexParams, IndexType, MetricType};
use milvus::schema::{CollectionSchemaBuilder, FieldSchema};
use nexus_ai::chunking::{chunk_recursive_character, RecursiveCharacterConfig};
use nexus_ai::embedding::{
    append_embedding_column, EmbeddingModel, EmbeddingModelConfig, ModelConfig,
};
use nexus_connector_milvus::{MilvusConnectorConfig, MilvusSink};
use nexus_core::Sink;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

fn unique_suffix() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
        .to_string()
}

#[tokio::test]
async fn text_chunk_embed_milvus_end_to_end() {
    let suffix = unique_suffix();
    let network = format!("nexus-milvus-{suffix}");
    let etcd_name = format!("nexus-etcd-{suffix}");
    let minio_name = format!("nexus-minio-{suffix}");
    let milvus_name = format!("nexus-milvus-node-{suffix}");

    let _etcd = GenericImage::new("quay.io/coreos/etcd", "v3.5.18")
        // etcd becomes ready in under a second here — fast enough that
        // testcontainers' log-follow attaches after "ready to serve client
        // requests" already scrolled by, so a message-based wait misses it
        // and hangs to the full startup_timeout. A short fixed wait is more
        // reliable for a container this fast.
        .with_wait_for(WaitFor::seconds(3))
        .with_env_var("ETCD_AUTO_COMPACTION_MODE", "revision")
        .with_env_var("ETCD_AUTO_COMPACTION_RETENTION", "1000")
        .with_env_var("ETCD_QUOTA_BACKEND_BYTES", "4294967296")
        .with_env_var("ETCD_SNAPSHOT_COUNT", "50000")
        .with_cmd([
            "etcd",
            "-advertise-client-urls=http://127.0.0.1:2379",
            "-listen-client-urls",
            "http://0.0.0.0:2379",
            "--data-dir",
            "/etcd",
        ])
        .with_network(&network)
        .with_container_name(&etcd_name)
        .with_startup_timeout(std::time::Duration::from_secs(120))
        .start()
        .await
        .expect("etcd starts");

    let _minio = GenericImage::new("minio/minio", "latest")
        // Same log-follow race as etcd above — minio is ready before
        // testcontainers starts tailing stdout.
        .with_wait_for(WaitFor::seconds(3))
        .with_env_var("MINIO_ACCESS_KEY", "minioadmin")
        .with_env_var("MINIO_SECRET_KEY", "minioadmin")
        .with_cmd([
            "minio",
            "server",
            "/minio_data",
            "--console-address",
            ":9001",
        ])
        .with_network(&network)
        .with_container_name(&minio_name)
        .with_startup_timeout(std::time::Duration::from_secs(120))
        .start()
        .await
        .expect("minio starts");

    let milvus_node = GenericImage::new("milvusdb/milvus", "latest")
        .with_exposed_port(19530.tcp())
        .with_wait_for(WaitFor::message_on_stdout("Proxy successfully started"))
        .with_env_var("ETCD_ENDPOINTS", format!("{etcd_name}:2379"))
        .with_env_var("MINIO_ADDRESS", format!("{minio_name}:9000"))
        .with_env_var("MINIO_ACCESS_KEY_ID", "minioadmin")
        .with_env_var("MINIO_SECRET_ACCESS_KEY", "minioadmin")
        .with_cmd(["milvus", "run", "standalone"])
        .with_network(&network)
        .with_container_name(&milvus_name)
        .with_startup_timeout(std::time::Duration::from_secs(120))
        .start()
        .await
        .expect("milvus starts");
    let grpc_port = milvus_node
        .get_host_port_ipv4(19530)
        .await
        .expect("milvus grpc port");
    let url = format!("http://127.0.0.1:{grpc_port}");

    // --- create the collection (schema comes from Milvus, not local config) ---
    let setup_client = MilvusClient::new(url.clone())
        .await
        .expect("milvus client connects");

    // "Proxy successfully started" (the container's WaitFor) only means the
    // gRPC proxy accepted the port — RootCoord (which actually serves
    // CreateCollection) registers itself with etcd a moment later, and on
    // this memory-constrained host that gap is wide enough for the first
    // call to hit a client-side "Cancelled: Timeout expired" instead of a
    // real server error. Same shape as the ChromaDB gotcha (container log
    // line != fully ready): retry the first DDL call a few times instead of
    // widening the container-level wait.
    let mut last_err = None;
    for attempt in 0..10 {
        let mut builder = CollectionSchemaBuilder::new("docs", "nexusflow marco 5 test");
        builder.add_field(FieldSchema::new_primary_int64("id", "", false));
        builder.add_field(FieldSchema::new_varchar("chunk", "", 512));
        builder.add_field(FieldSchema::new_float_vector("embedding", "", 384));
        let schema = builder.build().expect("schema builds");
        match setup_client.create_collection(schema, None).await {
            Ok(_) => {
                last_err = None;
                break;
            }
            Err(e) => {
                last_err = Some(e);
                tokio::time::sleep(std::time::Duration::from_millis(1000 * (attempt + 1))).await;
            }
        }
    }
    if let Some(e) = last_err {
        panic!("creates collection (after retries): {e}");
    }

    // --- chunk ---
    let source_text = "NexusFlow moves data at high speed.\n\n\
        It builds an AI lakehouse from any source.\n\n\
        The weather today is sunny with a light breeze.";
    let chunks = chunk_recursive_character(
        source_text,
        &RecursiveCharacterConfig {
            chunk_size: 60,
            overlap: 0,
            ..RecursiveCharacterConfig::default()
        },
    );
    assert!(
        chunks.len() >= 2,
        "expected multiple chunks, got {chunks:?}"
    );

    // --- embed ---
    let embedding_cfg = EmbeddingModelConfig {
        model: ModelConfig {
            repo_id: "sentence-transformers/all-MiniLM-L6-v2".to_string(),
            revision: "main".to_string(),
            filename: "onnx/model.onnx".to_string(),
        },
        tokenizer_filename: "tokenizer.json".to_string(),
        dimension: 384,
        max_length: 128,
    };
    let mut model = EmbeddingModel::load(&embedding_cfg)
        .await
        .expect("embedding model loads");
    let embeddings = model.embed_batch(&chunks).expect("embeds chunks");

    // --- build the RecordBatch nexus-server would hand to the sink ---
    let batch_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("chunk", DataType::Utf8, false),
    ]));
    let ids: Vec<i64> = (1..=chunks.len() as i64).collect();
    let batch = RecordBatch::try_new(
        batch_schema,
        vec![
            Arc::new(Int64Array::from(ids.clone())),
            Arc::new(StringArray::from(chunks.clone())),
        ],
    )
    .unwrap();
    let batch = append_embedding_column(&batch, &embeddings, 384, "embedding").unwrap();

    // --- write to milvus ---
    let sink_cfg = MilvusConnectorConfig {
        url: url.clone(),
        collection: "docs".to_string(),
        primary_key: "id".to_string(),
        embedding_column: "embedding".to_string(),
        dimension: 384,
    };
    let mut sink = MilvusSink::connect(&sink_cfg).await.expect("sink connects");
    sink.write_batch(batch).await.expect("writes batch");

    // --- index + load so scalar query can see the data ---
    let docs = setup_client
        .get_collection("docs")
        .await
        .expect("opens collection");
    // The sink itself never flushes (Milvus rate-limits it aggressively) —
    // only the test's own verification queries need it, and only once each.
    docs.flush().await.expect("flushes after initial write");
    docs.create_index(
        "embedding",
        IndexParams::new(
            "embedding_idx".to_string(),
            IndexType::Flat,
            MetricType::L2,
            HashMap::new(),
        ),
    )
    .await
    .expect("creates index");
    docs.load(1).await.expect("loads collection");

    let rows = docs
        .query("id > 0", Vec::<String>::new())
        .await
        .expect("queries rows");
    let id_column = rows
        .iter()
        .find(|c| c.name == "id")
        .expect("id column present");
    assert_eq!(id_column.len(), chunks.len());

    // --- CDC delete: a batch carrying __opcode = "D" for id 1 must remove it ---
    let delete_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("chunk", DataType::Utf8, false),
        Field::new(nexus_core::OPCODE_COLUMN, DataType::Utf8, false),
    ]));
    let delete_batch = RecordBatch::try_new(
        delete_schema,
        vec![
            Arc::new(Int64Array::from(vec![1i64])),
            Arc::new(StringArray::from(vec![chunks[0].clone()])),
            Arc::new(StringArray::from(vec!["D"])),
        ],
    )
    .unwrap();
    let delete_batch =
        append_embedding_column(&delete_batch, &embeddings[..1], 384, "embedding").unwrap();
    sink.write_batch(delete_batch)
        .await
        .expect("writes delete batch");

    // Milvus's default flush rate limit is 1 per 10s (collection-scoped) —
    // space this one out from the first `docs.flush()` above.
    tokio::time::sleep(std::time::Duration::from_secs(11)).await;
    docs.flush().await.expect("flushes after delete");
    let remaining = docs
        .query("id == 1", Vec::<String>::new())
        .await
        .expect("queries rows");
    let remaining_ids = remaining.iter().find(|c| c.name == "id");
    let remaining_count = remaining_ids.map(|c| c.len()).unwrap_or(0);
    assert_eq!(
        remaining_count, 0,
        "id=1 must have been deleted, not upserted"
    );
}
