//! LLM node — calls an OpenAI-compatible chat completions endpoint and
//! appends the response as a new column. See
//! `docs/LLMOPS_IMPLEMENTATION_PLAN.md` Marco L1 and ARCHITECTURE.md §8.3
//! for the embedding pipeline this mirrors.
//!
//! `client` talks to the HTTP API (mirrors `embedding::api_client`),
//! `common` holds the shared error type and the arrow-append helper,
//! `pipeline` orchestrates prompt interpolation + the HTTP call per row.

mod client;
mod common;
mod pipeline;

pub use client::{LlmClient, LlmClientConfig, LlmResponse};
pub use common::{append_text_column, LlmError};
pub use pipeline::{
    apply_llm, build_prompt, load_llm_backend, LlmApplyResult, LlmBackend, LlmCallStats,
};
