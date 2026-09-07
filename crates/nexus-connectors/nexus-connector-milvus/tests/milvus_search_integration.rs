//! `MilvusSearchClient` (LLMOPS_IMPLEMENTATION_PLAN.md Marco L7 follow-up
//! — RAG multi-vetor) against a real Milvus standalone (etcd + minio +
//! milvus, same 3-container setup as `milvus_integration.rs`'s own sink
//! test).

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use milvus::client::Client as MilvusClient;
use milvus::index::{IndexParams, IndexType, MetricType};
use milvus::schema::{CollectionSchemaBuilder, FieldSchema};
use nexus_ai::embedding::{
    append_embedding_column, EmbeddingModel, EmbeddingModelConfig, ModelConfig,
};
use nexus_connector_milvus::{MilvusConnectorConfig, MilvusSearchClient, MilvusSink};
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
async fn search_returns_the_most_similar_row_first() {
    let suffix = unique_suffix();
    let network = format!("nexus-milvus-search-{suffix}");
    let etcd_name = format!("nexus-etcd-search-{suffix}");
    let minio_name = format!("nexus-minio-search-{suffix}");
    let milvus_name = format!("nexus-milvus-node-search-{suffix}");

    let _etcd = GenericImage::new("quay.io/coreos/etcd", "v3.5.18")
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

    let setup_client = MilvusClient::new(url.clone())
        .await
        .expect("milvus client connects");

    // Same RootCoord-registration race `milvus_integration.rs` documents —
    // retry the first DDL call instead of widening the container-level wait.
    let mut last_err = None;
    for attempt in 0..10 {
        let mut builder = CollectionSchemaBuilder::new("docs", "nexusflow rag search test");
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
    let model = EmbeddingModel::load(&embedding_cfg)
        .await
        .expect("embedding model loads");

    let texts = vec![
        "NexusFlow moves data at high speed across many connectors.".to_string(),
        "The weather today is sunny with a light breeze.".to_string(),
    ];
    let embeddings = model.embed_batch(&texts).expect("embeds chunks");

    let batch_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("chunk", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        batch_schema,
        vec![
            Arc::new(Int64Array::from(vec![1i64, 2i64])),
            Arc::new(StringArray::from(texts.clone())),
        ],
    )
    .unwrap();
    let batch = append_embedding_column(&batch, &embeddings, 384, "embedding").unwrap();

    let sink_cfg = MilvusConnectorConfig {
        url: Some(url.clone()),
        host: None,
        port: None,
        api_key: None,
        collection: Some("docs".to_string()),
        collection_name: None,
        primary_key: "id".to_string(),
        embedding_column: "embedding".to_string(),
        dimension: 384,
        timeout_seconds: 30,
    };
    let mut sink = MilvusSink::connect(&sink_cfg).await.expect("sink connects");
    sink.write_batch(batch).await.expect("writes batch");

    // Index + load — `MilvusSearchClient::search` (like any Milvus search)
    // needs both before the collection is queryable by vector similarity.
    let docs = setup_client
        .get_collection("docs")
        .await
        .expect("opens collection");
    docs.flush().await.expect("flushes after write");
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

    let question_vector = model
        .embed_batch(&["What does NexusFlow do with data?".to_string()])
        .expect("embeds question")
        .remove(0);

    let search_client = MilvusSearchClient::connect(&sink_cfg)
        .await
        .expect("search client connects");
    let hits = search_client
        .search(question_vector, "embedding", "chunk", 2)
        .await
        .expect("search succeeds");

    assert_eq!(hits.len(), 2);
    assert_eq!(hits[0].0, "1", "the NexusFlow row must rank first");
    assert_eq!(hits[0].1, texts[0]);
}
