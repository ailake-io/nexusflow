//! Golden-dataset evaluation (LLMOPS_IMPLEMENTATION_PLAN.md Marco L7): a
//! fixed set of question/expected-answer pairs re-run every time a
//! pipeline's `llm` node runs, scored against the *current* prompt
//! version/model. The point is regression detection — swap a prompt
//! version (Marco L4) and see whether the average score moved, instead of
//! finding out from a user that answers got worse.
//!
//! Scoring is direct comparison (token-set similarity), not an "LLM as
//! judge" second call — deterministic, no extra API dependency in every
//! test/CI run, and already enough to make a prompt regression visible. An
//! LLM-judge variant would reuse the same `LlmBackend` this module already
//! takes, just swap `score_answer` for a call through it — left for when
//! direct comparison proves insufficient.

use crate::llm::pipeline::{build_prompt, LlmBackend};
use nexus_core::LlmNodeSpec;

/// A score below this is `passed: false`. Picked as the midpoint of the
/// Jaccard range (identical bag-of-words = 1.0, disjoint = 0.0) — no data
/// yet to tune it further, revisit once real golden datasets exist.
pub const EVAL_PASS_THRESHOLD: f64 = 0.5;

/// One golden case's result: `answer` is kept (not just the score) so a
/// failure is inspectable without re-running the call.
#[derive(Debug, Clone)]
pub struct LlmEvalOutcome {
    pub eval_name: String,
    pub score: f64,
    pub passed: bool,
    pub answer: String,
}

/// Token-set (Jaccard) similarity between `expected` and `actual`, both
/// lowercased and split on non-alphanumeric runs. Pure and deterministic —
/// no LLM call, so a prompt-version comparison test never depends on model
/// nondeterminism to prove the mechanism works.
pub fn score_answer(expected: &str, actual: &str) -> f64 {
    fn tokens(s: &str) -> std::collections::HashSet<String> {
        s.to_lowercase()
            .split(|c: char| !c.is_alphanumeric())
            .filter(|w| !w.is_empty())
            .map(|w| w.to_string())
            .collect()
    }
    let expected_tokens = tokens(expected);
    let actual_tokens = tokens(actual);
    if expected_tokens.is_empty() && actual_tokens.is_empty() {
        return 1.0;
    }
    let union = expected_tokens.union(&actual_tokens).count();
    if union == 0 {
        return 0.0;
    }
    let intersection = expected_tokens.intersection(&actual_tokens).count();
    intersection as f64 / union as f64
}

/// Runs every case in `spec.eval` as one ad-hoc call each (not through
/// `apply_llm` — there's no `RecordBatch` here, each case supplies its own
/// placeholder values directly, same spirit as `rag.rs`'s ad-hoc RAG call).
/// A call error becomes a failed outcome (score 0.0) rather than aborting
/// the rest of the dataset — one bad case shouldn't hide the other four.
pub async fn run_eval_cases(
    spec: &LlmNodeSpec,
    template: &str,
    backend: &LlmBackend,
) -> Vec<LlmEvalOutcome> {
    let mut outcomes = Vec::with_capacity(spec.eval.len());
    for case in &spec.eval {
        let values: Vec<(&str, String)> = case
            .inputs
            .iter()
            .map(|(k, v)| (k.as_str(), v.clone()))
            .collect();
        let prompt = build_prompt(template, &values);
        let response = match backend {
            LlmBackend::Api(client) => client.call(&prompt, spec.max_tokens, spec.temperature).await,
            LlmBackend::Anthropic(client) => {
                client.call(&prompt, spec.max_tokens, spec.temperature).await
            }
        };
        let outcome = match response {
            Ok(resp) => {
                let score = score_answer(&case.expected_answer, &resp.text);
                LlmEvalOutcome {
                    eval_name: case.name.clone(),
                    score,
                    passed: score >= EVAL_PASS_THRESHOLD,
                    answer: resp.text,
                }
            }
            Err(e) => LlmEvalOutcome {
                eval_name: case.name.clone(),
                score: 0.0,
                passed: false,
                answer: format!("error: {e}"),
            },
        };
        outcomes.push(outcome);
    }
    outcomes
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::llm::pipeline::load_llm_backend;
    use nexus_core::{LlmEvalCase, LlmModelConfig, PromptRef};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[test]
    fn identical_answers_score_a_perfect_match() {
        assert_eq!(score_answer("the sky is blue", "the sky is blue"), 1.0);
    }

    #[test]
    fn disjoint_answers_score_zero() {
        assert_eq!(score_answer("the sky is blue", "cats like fish"), 0.0);
    }

    #[test]
    fn partial_overlap_scores_between_zero_and_one() {
        let score = score_answer("the sky is blue", "the sky is grey");
        assert!(score > 0.0 && score < 1.0, "score was {score}");
    }

    fn spec_with_eval(base_url: String, eval: Vec<LlmEvalCase>) -> LlmNodeSpec {
        LlmNodeSpec {
            prompt: PromptRef {
                name: "eval-prompt".to_string(),
                version: None,
            },
            input_columns: vec![],
            output_column: "answer".to_string(),
            model: LlmModelConfig::Api {
                base_url,
                model: "gpt-test".to_string(),
                api_key_env: None,
                cost_per_1k_prompt_tokens: None,
                cost_per_1k_completion_tokens: None,
            },
            max_tokens: None,
            temperature: None,
            log_full_content: false,
            cache: None,
            eval,
        }
    }

    #[tokio::test]
    async fn scores_matching_and_mismatching_cases_differently() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "choices": [{"message": {"content": "paris is the capital of france"}}],
                "usage": {"prompt_tokens": 4, "completion_tokens": 6}
            })))
            .mount(&server)
            .await;

        let mut good_inputs = std::collections::BTreeMap::new();
        good_inputs.insert("question".to_string(), "what is the capital of france?".to_string());
        let mut bad_inputs = std::collections::BTreeMap::new();
        bad_inputs.insert("question".to_string(), "what is the capital of japan?".to_string());

        let spec = spec_with_eval(
            server.uri(),
            vec![
                LlmEvalCase {
                    name: "good-case".to_string(),
                    inputs: good_inputs,
                    expected_answer: "paris is the capital of france".to_string(),
                },
                LlmEvalCase {
                    name: "bad-case".to_string(),
                    inputs: bad_inputs,
                    expected_answer: "tokyo is the capital of japan".to_string(),
                },
            ],
        );
        let backend = load_llm_backend(&spec);

        let outcomes = run_eval_cases(&spec, "Answer: {question}", &backend).await;
        assert_eq!(outcomes.len(), 2);

        let good = outcomes.iter().find(|o| o.eval_name == "good-case").unwrap();
        assert!(good.passed, "score was {}", good.score);
        assert_eq!(good.score, 1.0);

        let bad = outcomes.iter().find(|o| o.eval_name == "bad-case").unwrap();
        assert!(!bad.passed, "score was {}", bad.score);
    }
}
