//! Real end-to-end: chunk → embed(CPU) → ChromaDB, including CDC delete.
//! ChromaDB has an official Docker image, so — unlike Pinecone — this gets
//! a real integration test. See IMPLEMENTATION_PLAN.md Marco 5.

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use nexus_ai::chunking::{chunk_recursive_character, RecursiveCharacterConfig};
use nexus_ai::embedding::{
    append_embedding_column, EmbeddingModel, EmbeddingModelConfig, ModelConfig,
};
use nexus_connector_chromadb::{ChromaConnectorConfig, ChromaSink};
use nexus_core::Sink;
use serde_json::Value;
use std::sync::Arc;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::GenericImage;

#[tokio::test]
async fn text_chunk_embed_chromadb_end_to_end() {
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

    // --- create the collection (default_tenant/default_database already
    // exist in the image) ---
    let http = reqwest::Client::new();
    // The "Connect to Chroma at:" log line prints slightly before the TCP
    // listener actually accepts connections — retry through the resulting
    // ConnectionReset instead of racing it with a fixed sleep.
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

    // --- write to chromadb ---
    let sink_cfg = ChromaConnectorConfig {
        host: host.clone(),
        tenant: "default_tenant".to_string(),
        database: "default_database".to_string(),
        collection: "docs".to_string(),
        primary_key: "id".to_string(),
        embedding_column: "embedding".to_string(),
        dimension: 384,
        timeout_seconds: 30,
    };
    let mut sink = ChromaSink::connect(&sink_cfg).await.expect("sink connects");
    sink.write_batch(batch).await.expect("writes batch");

    let collection: Value = http
        .get(format!(
            "{host}/api/v2/tenants/default_tenant/databases/default_database/collections/docs"
        ))
        .send()
        .await
        .expect("gets collection")
        .json()
        .await
        .expect("parses collection");
    let collection_id = collection["id"].as_str().unwrap();
    let collection_url = format!(
        "{host}/api/v2/tenants/default_tenant/databases/default_database/collections/{collection_id}"
    );

    let ids_json: Vec<String> = ids.iter().map(|id| id.to_string()).collect();
    let got: Value = http
        .post(format!("{collection_url}/get"))
        .json(&serde_json::json!({ "ids": ids_json }))
        .send()
        .await
        .expect("queries rows")
        .json()
        .await
        .expect("parses rows");
    assert_eq!(
        got["ids"].as_array().expect("ids array").len(),
        chunks.len()
    );

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

    let remaining: Value = http
        .post(format!("{collection_url}/get"))
        .json(&serde_json::json!({ "ids": ["1"] }))
        .send()
        .await
        .expect("queries rows")
        .json()
        .await
        .expect("parses rows");
    assert_eq!(
        remaining["ids"].as_array().expect("ids array").len(),
        0,
        "id=1 must have been deleted, not upserted"
    );
}
