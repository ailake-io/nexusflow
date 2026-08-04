//! ONNX embedding pipeline. Gated behind the `cpu` feature (and later
//! `cuda`/`metal`/`api`) — see ARCHITECTURE.md §8 and CLAUDE.md §8.5.
//! The `model` submodule resolves which local file backs a given Hugging
//! Face repo (downloading + caching on first use). `inference` wraps the
//! `ort` session + tokenizer to turn text into embeddings and append them to
//! a `RecordBatch` — see ARCHITECTURE.md §4.3 and IMPLEMENTATION_PLAN.md
//! Marco 5.

mod inference;
mod model;
#[cfg(feature = "cpu")]
mod pipeline;

pub use inference::{
    append_embedding_column, EmbeddingError, EmbeddingModel, EmbeddingModelConfig,
};
pub use model::{resolve_model_path, ModelConfig, ModelError};
#[cfg(feature = "cpu")]
pub use pipeline::apply_embedding;
