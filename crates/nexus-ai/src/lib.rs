//! Pipeline de embeddings (chunking + ONNX inference). Ver ARCHITECTURE.md §8
//! e IMPLEMENTATION_PLAN.md Marco 5.
//!
//! `chunking` é puro (sem I/O, sem ONNX) — ver módulo. `embedding` depende
//! da decisão de ciclo de vida do modelo ONNX (resolvida: HF Hub em runtime
//! + cache local, ver ARCHITECTURE.md §8) e chega em seguida.

pub mod chunking;
#[cfg(feature = "cpu")]
pub mod embedding;

pub use chunking::{
    chunk_fixed_window, chunk_recursive_character, chunk_semantic, FixedWindowConfig,
    RecursiveCharacterConfig, SemanticChunkConfig,
};
