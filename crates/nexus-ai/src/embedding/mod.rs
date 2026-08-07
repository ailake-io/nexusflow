//! Two independent embedding backends behind separate Cargo features (see
//! nexus-ai/Cargo.toml, CLAUDE.md §4.3/§8.5): a local ONNX model (`cpu`,
//! plus `cuda`/`metal` on top) via `inference`, or an external HTTP API
//! (`api`) via `api_client`. `common` holds the shared error type and the
//! arrow-append helper neither backend-specific module needs `ort` or
//! `reqwest` for. `pipeline` orchestrates chunking + whichever backend the
//! DAG spec's `EmbeddingModelSpec` selects — see ARCHITECTURE.md §4.3 and
//! IMPLEMENTATION_PLAN.md Marco 5.

#[cfg(feature = "api")]
mod api_client;
mod common;
#[cfg(feature = "cpu")]
mod inference;
// HF Hub resolution — only ever needed by the local-ONNX path.
#[cfg(feature = "cpu")]
mod model;
#[cfg(any(feature = "cpu", feature = "api"))]
mod pipeline;

#[cfg(feature = "api")]
pub use api_client::{ApiEmbeddingConfig, ApiEmbeddingModel};
pub use common::{append_embedding_column, EmbeddingError};
#[cfg(feature = "cpu")]
pub use inference::{EmbeddingModel, EmbeddingModelConfig};
#[cfg(feature = "cpu")]
pub use model::{resolve_model_path, ModelConfig, ModelError};
#[cfg(any(feature = "cpu", feature = "api"))]
pub use pipeline::apply_embedding;
