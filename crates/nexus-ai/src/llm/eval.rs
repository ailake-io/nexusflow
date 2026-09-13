//! Golden-dataset evaluation (LLMOPS_IMPLEMENTATION_PLAN.md Marco L7): a
//! fixed set of question/expected-answer pairs re-run every time a
//! pipeline's `llm` node runs, scored against the *current* prompt
//! version/model. The point is regression detection — swap a prompt
//! version (Marco L4) and see whether the average score moved, instead of
//! finding out from a user that answers got worse.
//!
//! Two scoring modes (`LlmNodeSpec.eval_scoring`,
//! `nexus_core::EvalScoringMode`): `TokenSimilarity` (default) is direct
//! comparison — deterministic, no extra API call. `LlmJudge` reuses the
//! same `LlmBackend` for a *second* call that grades the answer against
//! the expected one — catches a correct-but-differently-worded answer
//! that token similarity would score low, at roughly double the per-case
//! cost. A judge call that fails or returns an unparseable score falls
//! back to `score_answer` rather than zeroing the case — a broken judge
//! response shouldn't read as "the answer was wrong."

use crate::llm::pipeline::{build_prompt, LlmBackend};
use nexus_core::{EvalScoringMode, LlmNodeSpec};

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

/// Fixed grading prompt (LLMOPS_IMPLEMENTATION_PLAN.md Marco L7 follow-up)
/// — not versioned via `PromptTemplateStore` like the answer-generation
/// prompt; v1 doesn't need that, it's an internal implementation detail of
/// scoring, not something a pipeline author configures.
fn build_judge_prompt(expected: &str, actual: &str) -> String {
    format!(
        "You are grading whether a candidate answer matches a reference \
         answer's meaning.\n\nReference answer: {expected}\n\
         Candidate answer: {actual}\n\n\
         Score how well the candidate matches the reference, from 0 (completely \
         wrong or unrelated) to 10 (fully correct and equivalent). \
         Respond with ONLY the number, nothing else."
    )
}

/// Extracts a 0-10 grade from a judge response and normalizes it to
/// `[0.0, 1.0]` — pure, testable without a network call. `None` when the
/// response doesn't start with a parseable number (the caller falls back
/// to `score_answer` in that case).
fn parse_judge_score(text: &str) -> Option<f64> {
    let leading_number: String = text
        .trim()
        .chars()
        .take_while(|c| c.is_ascii_digit() || *c == '.')
        .collect();
    let raw: f64 = leading_number.parse().ok()?;
    Some((raw / 10.0).clamp(0.0, 1.0))
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
            LlmBackend::Api(client) => {
                client
                    .call(&prompt, spec.max_tokens, spec.temperature)
                    .await
            }
            LlmBackend::Anthropic(client) => {
                client
                    .call(&prompt, spec.max_tokens, spec.temperature)
                    .await
            }
        };
        let outcome = match response {
            Ok(resp) => {
                let score = match spec.eval_scoring {
                    EvalScoringMode::TokenSimilarity => {
                        score_answer(&case.expected_answer, &resp.text)
                    }
                    EvalScoringMode::LlmJudge => {
                        judge_score(&case.expected_answer, &resp.text, backend, spec.max_tokens)
                            .await
                            .unwrap_or_else(|| score_answer(&case.expected_answer, &resp.text))
                    }
                };
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

/// Makes the second, grading call for `EvalScoringMode::LlmJudge` — `None`
/// on any failure (call error or unparseable response), letting the caller
/// fall back to `score_answer` instead of treating a broken judge as a
/// zero score.
async fn judge_score(
    expected: &str,
    actual: &str,
    backend: &LlmBackend,
    max_tokens: Option<u32>,
) -> Option<f64> {
    let prompt = build_judge_prompt(expected, actual);
    let response = match backend {
        LlmBackend::Api(client) => client.call(&prompt, max_tokens, Some(0.0)).await,
        LlmBackend::Anthropic(client) => client.call(&prompt, max_tokens, Some(0.0)).await,
    };
    parse_judge_score(&response.ok()?.text)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::llm::pipeline::load_llm_backend;
    use nexus_core::{EvalScoringMode, LlmEvalCase, LlmModelConfig, PromptRef};
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

    #[test]
    fn parse_judge_score_reads_a_bare_number() {
        assert_eq!(parse_judge_score("8"), Some(0.8));
    }

    #[test]
    fn parse_judge_score_ignores_trailing_text() {
        assert_eq!(parse_judge_score("7 out of 10"), Some(0.7));
    }

    #[test]
    fn parse_judge_score_clamps_out_of_range_values() {
        assert_eq!(parse_judge_score("15"), Some(1.0));
    }

    #[test]
    fn parse_judge_score_returns_none_for_non_numeric_text() {
        assert_eq!(parse_judge_score("that looks about right"), None);
    }

    fn spec_with_eval(base_url: String, eval: Vec<LlmEvalCase>) -> LlmNodeSpec {
        spec_with_eval_scoring(base_url, eval, EvalScoringMode::TokenSimilarity)
    }

    fn spec_with_eval_scoring(
        base_url: String,
        eval: Vec<LlmEvalCase>,
        eval_scoring: EvalScoringMode,
    ) -> LlmNodeSpec {
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
            eval_scoring,
        }
    }

    #[tokio::test]
    async fn scores_matching_and_mismatching_cases_differently() {
        use wiremock::matchers::body_string_contains;

        let server = MockServer::start().await;
        // Two mocks, routed by question content — "bad-case" gets an
        // answer sharing essentially no tokens with what it expected, so
        // its score lands unambiguously below the pass threshold instead
        // of depending on exactly how much two arbitrary phrases happen to
        // overlap (a prior version of this test picked phrases that
        // overlapped by *precisely* half their tokens — 0.5, the pass
        // threshold itself — so `bad-case` silently passed instead of
        // failing, undetected until a full, honest `cargo test` run).
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .and(body_string_contains("france"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "choices": [{"message": {"content": "paris is the capital of france"}}],
                "usage": {"prompt_tokens": 4, "completion_tokens": 6}
            })))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .and(body_string_contains("japan"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "choices": [{"message": {"content": "i have no idea"}}],
                "usage": {"prompt_tokens": 4, "completion_tokens": 4}
            })))
            .mount(&server)
            .await;

        let mut good_inputs = std::collections::BTreeMap::new();
        good_inputs.insert(
            "question".to_string(),
            "what is the capital of france?".to_string(),
        );
        let mut bad_inputs = std::collections::BTreeMap::new();
        bad_inputs.insert(
            "question".to_string(),
            "what is the capital of japan?".to_string(),
        );

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

        let good = outcomes
            .iter()
            .find(|o| o.eval_name == "good-case")
            .unwrap();
        assert!(good.passed, "score was {}", good.score);
        assert_eq!(good.score, 1.0);

        let bad = outcomes.iter().find(|o| o.eval_name == "bad-case").unwrap();
        assert!(!bad.passed, "score was {}", bad.score);
    }

    #[tokio::test]
    async fn llm_judge_mode_scores_from_the_grading_call_not_token_overlap() {
        use wiremock::matchers::body_string_contains;

        let server = MockServer::start().await;
        // Answer call: some paraphrase token-similarity would score low on
        // (no words in common with the expected answer), but the judge
        // considers equivalent — proves LlmJudge isn't secretly falling
        // back to `score_answer`.
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .and(body_string_contains("Answer:"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "choices": [{"message": {"content": "it's the city of light, obviously"}}],
                "usage": {"prompt_tokens": 4, "completion_tokens": 6}
            })))
            .mount(&server)
            .await;
        // Judge call: matched by a phrase unique to `build_judge_prompt`.
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .and(body_string_contains("grading whether"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "choices": [{"message": {"content": "9"}}],
                "usage": {"prompt_tokens": 20, "completion_tokens": 1}
            })))
            .mount(&server)
            .await;

        let mut inputs = std::collections::BTreeMap::new();
        inputs.insert(
            "question".to_string(),
            "what is the capital of france?".to_string(),
        );
        let spec = spec_with_eval_scoring(
            server.uri(),
            vec![LlmEvalCase {
                name: "judged-case".to_string(),
                inputs,
                expected_answer: "paris is the capital of france".to_string(),
            }],
            EvalScoringMode::LlmJudge,
        );
        let backend = load_llm_backend(&spec);

        let outcomes = run_eval_cases(&spec, "Answer: {question}", &backend).await;
        assert_eq!(outcomes.len(), 1);
        assert_eq!(outcomes[0].score, 0.9);
        assert!(outcomes[0].passed);
    }

    #[tokio::test]
    async fn llm_judge_mode_falls_back_to_token_similarity_on_an_unparseable_grade() {
        use wiremock::matchers::body_string_contains;

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .and(body_string_contains("Answer:"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "choices": [{"message": {"content": "paris is the capital of france"}}],
                "usage": {"prompt_tokens": 4, "completion_tokens": 6}
            })))
            .mount(&server)
            .await;
        // Judge responds with prose instead of a number — must not read as
        // a score of 0.0.
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .and(body_string_contains("grading whether"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "choices": [{"message": {"content": "looks correct to me"}}],
                "usage": {"prompt_tokens": 20, "completion_tokens": 4}
            })))
            .mount(&server)
            .await;

        let mut inputs = std::collections::BTreeMap::new();
        inputs.insert(
            "question".to_string(),
            "what is the capital of france?".to_string(),
        );
        let spec = spec_with_eval_scoring(
            server.uri(),
            vec![LlmEvalCase {
                name: "fallback-case".to_string(),
                inputs,
                // Identical to the mocked answer — `score_answer` gives
                // exactly 1.0, unambiguous evidence the fallback ran.
                expected_answer: "paris is the capital of france".to_string(),
            }],
            EvalScoringMode::LlmJudge,
        );
        let backend = load_llm_backend(&spec);

        let outcomes = run_eval_cases(&spec, "Answer: {question}", &backend).await;
        assert_eq!(outcomes.len(), 1);
        assert_eq!(
            outcomes[0].score, 1.0,
            "an unparseable judge grade must fall back to score_answer, not zero out"
        );
    }
}
