#![cfg(feature = "cpu")]

//! Real end-to-end validation of the `ort` inference wrapper: downloads a
//! small real sentence-transformer ONNX model from the Hugging Face Hub,
//! embeds a few sentences, and checks a semantic sanity property (similar
//! sentences must be closer than unrelated ones) — not just that the code
//! compiles. See IMPLEMENTATION_PLAN.md Marco 5.

use nexus_ai::embedding::{EmbeddingModel, EmbeddingModelConfig, ModelConfig};

fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    let dot: f32 = a.iter().zip(b).map(|(x, y)| x * y).sum();
    let norm_a = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    dot / (norm_a * norm_b)
}

#[tokio::test]
async fn embeds_sentences_with_correct_semantic_similarity() {
    let cfg = EmbeddingModelConfig {
        model: ModelConfig {
            repo_id: "sentence-transformers/all-MiniLM-L6-v2".to_string(),
            revision: "main".to_string(),
            filename: "onnx/model.onnx".to_string(),
        },
        tokenizer_filename: "tokenizer.json".to_string(),
        dimension: 384,
        max_length: 128,
    };

    let model = EmbeddingModel::load(&cfg).await.expect("model loads");

    let texts = vec![
        "The cat sits on the mat.".to_string(),
        "A cat is sitting on a mat.".to_string(),
        "The stock market crashed today.".to_string(),
    ];
    let embeddings = model.embed_batch(&texts).expect("embeds batch");

    assert_eq!(embeddings.len(), 3);
    for embedding in &embeddings {
        assert_eq!(embedding.len(), 384);
    }

    let similar = cosine_similarity(&embeddings[0], &embeddings[1]);
    let unrelated = cosine_similarity(&embeddings[0], &embeddings[2]);
    assert!(
        similar > unrelated,
        "semantically similar sentences must score higher than unrelated ones: \
         similar={similar}, unrelated={unrelated}"
    );
}
