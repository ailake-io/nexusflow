use crate::embedding::common::EmbeddingError;
use crate::embedding::model::{resolve_model_path, ModelConfig};
use ort::session::builder::SessionBuilder;
use ort::session::Session;
use ort::value::Tensor;
use tokenizers::Tokenizer;

fn ort_err(e: impl std::fmt::Display) -> EmbeddingError {
    EmbeddingError::Ort(e.to_string())
}

/// Registers the GPU execution provider selected by Cargo feature, if any
/// (`cuda`/`metal` are additive on top of `cpu`, see nexus-ai/Cargo.toml).
/// ONNX Runtime falls back to its default CPU EP when neither is enabled,
/// or transparently at runtime if the registered EP fails to initialize on
/// a given machine (e.g. `cuda` compiled in but no NVIDIA driver present).
#[cfg(feature = "cuda")]
fn with_accelerated_execution_provider(
    builder: SessionBuilder,
) -> Result<SessionBuilder, EmbeddingError> {
    builder
        .with_execution_providers([ort::ep::CUDA::default().build()])
        .map_err(ort_err)
}

#[cfg(all(feature = "metal", not(feature = "cuda")))]
fn with_accelerated_execution_provider(
    builder: SessionBuilder,
) -> Result<SessionBuilder, EmbeddingError> {
    builder
        .with_execution_providers([ort::ep::CoreML::default().build()])
        .map_err(ort_err)
}

#[cfg(not(any(feature = "cuda", feature = "metal")))]
fn with_accelerated_execution_provider(
    builder: SessionBuilder,
) -> Result<SessionBuilder, EmbeddingError> {
    Ok(builder)
}

/// Pinned ONNX model + its tokenizer — both resolved (downloaded/cached) via
/// the same HF Hub repo+revision, per ARCHITECTURE.md §8.
#[derive(Debug, Clone)]
pub struct EmbeddingModelConfig {
    pub model: ModelConfig,
    /// Tokenizer file name in the same repo/revision as `model` (e.g.
    /// `"tokenizer.json"`).
    pub tokenizer_filename: String,
    /// Output embedding width — declared explicitly rather than probed at
    /// runtime, since it drives the `FixedSizeList` column's arrow type.
    pub dimension: usize,
    /// Truncate/pad token sequences to this length.
    pub max_length: usize,
}

/// Loaded ONNX session + tokenizer, ready to embed batches of text. See
/// ARCHITECTURE.md §4.3/§8 — chunking and embedding are pure/no-I/O once
/// the model is loaded; only `load` itself does I/O (download + cache).
pub struct EmbeddingModel {
    session: Session,
    tokenizer: Tokenizer,
    dimension: usize,
    max_length: usize,
}

impl EmbeddingModel {
    pub async fn load(cfg: &EmbeddingModelConfig) -> Result<Self, EmbeddingError> {
        let model_path = resolve_model_path(&cfg.model).await?;
        let tokenizer_path = resolve_model_path(&ModelConfig {
            filename: cfg.tokenizer_filename.clone(),
            ..cfg.model.clone()
        })
        .await?;

        let tokenizer = Tokenizer::from_file(&tokenizer_path)
            .map_err(|e| EmbeddingError::Tokenizer(e.to_string()))?;
        let mut builder =
            with_accelerated_execution_provider(Session::builder().map_err(ort_err)?)?;
        let session = builder.commit_from_file(&model_path).map_err(ort_err)?;

        Ok(Self {
            session,
            tokenizer,
            dimension: cfg.dimension,
            max_length: cfg.max_length,
        })
    }

    /// Tokenizes, runs one forward pass, and returns one embedding vector per
    /// input text (mean-pooled over token embeddings when the model outputs
    /// per-token hidden states; used as-is when it already outputs a single
    /// pooled vector per input).
    pub fn embed_batch(&mut self, texts: &[String]) -> Result<Vec<Vec<f32>>, EmbeddingError> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }

        let encodings = self
            .tokenizer
            .encode_batch(texts.to_vec(), true)
            .map_err(|e| EmbeddingError::Tokenizer(e.to_string()))?;

        let seq_len = encodings
            .iter()
            .map(|e| e.get_ids().len())
            .max()
            .unwrap_or(0)
            .min(self.max_length);

        let batch = encodings.len();
        let mut input_ids = vec![0i64; batch * seq_len];
        let mut attention_mask = vec![0i64; batch * seq_len];
        let mut token_type_ids = vec![0i64; batch * seq_len];

        for (row, encoding) in encodings.iter().enumerate() {
            let ids = encoding.get_ids();
            let mask = encoding.get_attention_mask();
            let types = encoding.get_type_ids();
            for col in 0..seq_len {
                let idx = row * seq_len + col;
                input_ids[idx] = ids.get(col).copied().unwrap_or(0) as i64;
                attention_mask[idx] = mask.get(col).copied().unwrap_or(0) as i64;
                token_type_ids[idx] = types.get(col).copied().unwrap_or(0) as i64;
            }
        }

        let shape = [batch, seq_len];
        let input_ids_tensor = Tensor::from_array((shape, input_ids)).map_err(ort_err)?;
        let attention_mask_tensor =
            Tensor::from_array((shape, attention_mask.clone())).map_err(ort_err)?;
        let token_type_ids_tensor = Tensor::from_array((shape, token_type_ids)).map_err(ort_err)?;

        let outputs = self
            .session
            .run(ort::inputs![
                "input_ids" => input_ids_tensor,
                "attention_mask" => attention_mask_tensor,
                "token_type_ids" => token_type_ids_tensor,
            ])
            .map_err(ort_err)?;

        let (output_shape, output_data) =
            outputs[0].try_extract_tensor::<f32>().map_err(ort_err)?;

        match output_shape.len() {
            // Already pooled: [batch, hidden].
            2 => Ok(output_data
                .chunks_exact(self.dimension)
                .map(|row| row.to_vec())
                .collect()),
            // Per-token hidden states: [batch, seq_len, hidden] — mean-pool
            // over real (non-padding) tokens using the attention mask.
            3 => {
                let hidden = output_shape[2] as usize;
                if hidden != self.dimension {
                    return Err(EmbeddingError::UnexpectedOutputShape(format!(
                        "expected hidden size {}, got {hidden}",
                        self.dimension
                    )));
                }
                let mut pooled = Vec::with_capacity(batch);
                for row in 0..batch {
                    let mut sums = vec![0f32; hidden];
                    let mut count = 0f32;
                    for col in 0..seq_len {
                        if attention_mask[row * seq_len + col] == 0 {
                            continue;
                        }
                        count += 1.0;
                        let base = (row * seq_len + col) * hidden;
                        for h in 0..hidden {
                            sums[h] += output_data[base + h];
                        }
                    }
                    if count > 0.0 {
                        for v in &mut sums {
                            *v /= count;
                        }
                    }
                    pooled.push(sums);
                }
                Ok(pooled)
            }
            other => Err(EmbeddingError::UnexpectedOutputShape(format!(
                "expected a rank-2 or rank-3 output tensor, got rank {other}"
            ))),
        }
    }
}
