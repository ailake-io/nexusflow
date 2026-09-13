//! `ChromaSearchClient` (LLMOPS_IMPLEMENTATION_PLAN.md Marco L7 follow-up
//! — RAG multi-vetor) against a real ChromaDB, same container setup as
//! `chromadb_integration.rs`'s own sink test.

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use nexus_ai::embedding::{
    append_embedding_column, EmbeddingModel, EmbeddingModelConfig, ModelConfig,
};
use nexus_connector_chromadb::{ChromaConnectorConfig, ChromaSearchClient, ChromaSink};
use nexus_core::Sink;
use std::sync::Arc;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::GenericImage;

#[tokio::test]
async fn search_returns_the_most_similar_row_first() {
    let chroma_node = GenericImage::new("chromadb/chroma", "latest")
        .with_exposed_port(8000.tcp())
        .with_wait_for(WaitFor::message_on_stdout("Connect to Chroma at:"))
        .start()
        .await
        .expect("chroma starts");
    let host_port = chroma_node
        .get_host_port_ipv4(8000)
        .await
        .expect("chroma host port");
    let host = format!("http://127.0.0.1:{host_port}");

    let http = reqwest::Client::new();
    let mut create_response = None;
    for _ in 0..20 {
        match http
            .post(format!(
                "{host}/api/v2/tenants/default_tenant/databases/default_database/collections"
            ))
            .json(&serde_json::json!({ "name": "docs" }))
            .send()
            .await
        {
            Ok(resp) => {
                create_response = Some(resp);
                break;
            }
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(300)).await,
        }
    }
    create_response
        .expect("creates collection")
        .error_for_status()
        .expect("collection creation succeeds");

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

    let sink_cfg = ChromaConnectorConfig {
        host: host.clone(),
        port: None,
        api_key: None,
        tenant: "default_tenant".to_string(),
        database: "default_database".to_string(),
        collection: "docs".to_string(),
        primary_key: "id".to_string(),
        embedding_column: "embedding".to_string(),
        dimension: 384,
        timeout_seconds: 30,
        max_concurrent_requests: 8,
    };
    let mut sink = ChromaSink::connect(&sink_cfg).await.expect("sink connects");
    sink.write_batch(batch).await.expect("writes batch");

    let question_vector = model
        .embed_batch(&["What does NexusFlow do with data?".to_string()])
        .expect("embeds question")
        .remove(0);

    let search_client = ChromaSearchClient::connect(&sink_cfg)
        .await
        .expect("search client connects");
    let hits = search_client
        .search(question_vector, "chunk", 2)
        .await
        .expect("search succeeds");

    assert_eq!(hits.len(), 2);
    assert_eq!(hits[0].0, "1", "the NexusFlow row must rank first");
    assert_eq!(hits[0].1, texts[0]);
}
