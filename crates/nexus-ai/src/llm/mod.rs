//! LLM node — calls an LLM API and appends the response as a new column.
//! See `docs/LLMOPS_IMPLEMENTATION_PLAN.md` Marco L1 and ARCHITECTURE.md
//! §8.3 for the embedding pipeline this mirrors.
//!
//! Two backends: `client` talks to any OpenAI-compatible endpoint (OpenAI,
//! Ollama, Kimi/Moonshot, and most others that mimic that shape — mirrors
//! `embedding::api_client`); `anthropic_client` talks to Anthropic's own
//! native Messages API, which isn't OpenAI-shaped. `common` holds the
//! shared error type and the arrow-append helper; `pipeline` dispatches to
//! whichever backend the spec selects and orchestrates prompt
//! interpolation + the HTTP call per row.

mod anthropic_client;
mod client;
mod common;
mod pipeline;

pub use anthropic_client::{AnthropicClient, AnthropicClientConfig};
pub use client::{LlmClient, LlmClientConfig, LlmResponse};
pub use common::{append_text_column, cache_key, LlmCache, LlmError};
pub use pipeline::{
    apply_llm, build_prompt, load_llm_backend, LlmApplyResult, LlmBackend, LlmCallStats,
};
