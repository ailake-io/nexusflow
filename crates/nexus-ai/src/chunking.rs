//! Pure, no-I/O chunking strategies — testable without GPU/network, per
//! ARCHITECTURE.md §8. Three strategies per IMPLEMENTATION_PLAN.md Marco 5:
//! fixed-size window, recursive character, and semantic (similarity-based).

/// Fixed-size window over Unicode scalar values (not bytes, to never split a
/// multi-byte character), with optional overlap between consecutive chunks.
#[derive(Debug, Clone)]
pub struct FixedWindowConfig {
    pub chunk_size: usize,
    pub overlap: usize,
}

pub fn chunk_fixed_window(text: &str, cfg: &FixedWindowConfig) -> Vec<String> {
    if cfg.chunk_size == 0 || text.is_empty() {
        return Vec::new();
    }
    let chars: Vec<char> = text.chars().collect();
    let step = cfg.chunk_size.saturating_sub(cfg.overlap).max(1);
    let mut chunks = Vec::new();
    let mut start = 0;
    loop {
        let end = (start + cfg.chunk_size).min(chars.len());
        chunks.push(chars[start..end].iter().collect());
        if end == chars.len() {
            break;
        }
        start += step;
    }
    chunks
}

/// LangChain-style recursive splitter: tries each separator in priority
/// order, recursing into any piece still over `chunk_size` with the
/// remaining separators, falling back to a hard character split once
/// separators are exhausted.
#[derive(Debug, Clone)]
pub struct RecursiveCharacterConfig {
    pub chunk_size: usize,
    pub overlap: usize,
    pub separators: Vec<String>,
}

impl Default for RecursiveCharacterConfig {
    fn default() -> Self {
        Self {
            chunk_size: 1000,
            overlap: 100,
            separators: vec![
                "\n\n".to_string(),
                "\n".to_string(),
                ". ".to_string(),
                " ".to_string(),
                String::new(),
            ],
        }
    }
}

pub fn chunk_recursive_character(text: &str, cfg: &RecursiveCharacterConfig) -> Vec<String> {
    if text.is_empty() || cfg.chunk_size == 0 {
        return Vec::new();
    }
    let chunks = recursive_split(text, &cfg.separators, cfg.chunk_size);
    apply_overlap(chunks, cfg.overlap.min(cfg.chunk_size.saturating_sub(1)))
}

fn recursive_split(text: &str, separators: &[String], chunk_size: usize) -> Vec<String> {
    if text.chars().count() <= chunk_size {
        return vec![text.to_string()];
    }
    let Some((sep, rest_seps)) = separators.split_first() else {
        return hard_char_split(text, chunk_size);
    };
    if sep.is_empty() {
        return hard_char_split(text, chunk_size);
    }

    let pieces: Vec<&str> = text.split(sep.as_str()).collect();
    if pieces.len() == 1 {
        // Separator doesn't occur in this text — fall through to the next one.
        return recursive_split(text, rest_seps, chunk_size);
    }

    let mut chunks = Vec::new();
    let mut current = String::new();
    for piece in pieces {
        if piece.chars().count() > chunk_size {
            if !current.is_empty() {
                chunks.push(std::mem::take(&mut current));
            }
            chunks.extend(recursive_split(piece, rest_seps, chunk_size));
            continue;
        }
        let candidate_len = current.chars().count()
            + if current.is_empty() {
                0
            } else {
                sep.chars().count()
            }
            + piece.chars().count();
        if candidate_len > chunk_size && !current.is_empty() {
            chunks.push(std::mem::take(&mut current));
        }
        if !current.is_empty() {
            current.push_str(sep);
        }
        current.push_str(piece);
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}

fn hard_char_split(text: &str, chunk_size: usize) -> Vec<String> {
    if chunk_size == 0 {
        return Vec::new();
    }
    text.chars()
        .collect::<Vec<_>>()
        .chunks(chunk_size)
        .map(|c| c.iter().collect())
        .collect()
}

fn apply_overlap(chunks: Vec<String>, overlap: usize) -> Vec<String> {
    if overlap == 0 || chunks.len() < 2 {
        return chunks;
    }
    let mut result = Vec::with_capacity(chunks.len());
    let mut prev_tail: Option<Vec<char>> = None;
    for chunk in chunks {
        let chars: Vec<char> = chunk.chars().collect();
        let with_overlap: String = match &prev_tail {
            Some(tail) => tail.iter().chain(chars.iter()).collect(),
            None => chars.iter().collect(),
        };
        let tail_start = chars.len().saturating_sub(overlap);
        prev_tail = Some(chars[tail_start..].to_vec());
        result.push(with_overlap);
    }
    result
}

/// Similarity-based chunking: groups consecutive sentences together until
/// the embedding similarity between neighbors drops below `similarity_threshold`
/// (a topic shift), then starts a new chunk. Takes the embedding function as
/// a parameter rather than depending on `embedding` directly, so it stays a
/// pure, ONNX-free function — see ARCHITECTURE.md §8.
#[derive(Debug, Clone, Copy)]
pub struct SemanticChunkConfig {
    pub similarity_threshold: f32,
}

pub fn chunk_semantic<F>(sentences: &[String], embed: F, cfg: &SemanticChunkConfig) -> Vec<String>
where
    F: Fn(&str) -> Vec<f32>,
{
    let Some((first, rest)) = sentences.split_first() else {
        return Vec::new();
    };

    let mut chunks = Vec::new();
    let mut current = vec![first.clone()];
    let mut prev_embedding = embed(first);

    for sentence in rest {
        let embedding = embed(sentence);
        if cosine_similarity(&prev_embedding, &embedding) < cfg.similarity_threshold {
            chunks.push(current.join(" "));
            current = Vec::new();
        }
        current.push(sentence.clone());
        prev_embedding = embedding;
    }
    if !current.is_empty() {
        chunks.push(current.join(" "));
    }
    chunks
}

fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    let dot: f32 = a.iter().zip(b).map(|(x, y)| x * y).sum();
    let norm_a = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm_a == 0.0 || norm_b == 0.0 {
        0.0
    } else {
        dot / (norm_a * norm_b)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_window_splits_without_overlap() {
        let chunks = chunk_fixed_window(
            "abcdefghij",
            &FixedWindowConfig {
                chunk_size: 4,
                overlap: 0,
            },
        );
        assert_eq!(chunks, vec!["abcd", "efgh", "ij"]);
    }

    #[test]
    fn fixed_window_applies_overlap() {
        let chunks = chunk_fixed_window(
            "abcdefghij",
            &FixedWindowConfig {
                chunk_size: 4,
                overlap: 2,
            },
        );
        assert_eq!(chunks, vec!["abcd", "cdef", "efgh", "ghij"]);
    }

    #[test]
    fn fixed_window_handles_text_shorter_than_chunk() {
        let chunks = chunk_fixed_window(
            "ab",
            &FixedWindowConfig {
                chunk_size: 10,
                overlap: 0,
            },
        );
        assert_eq!(chunks, vec!["ab"]);
    }

    #[test]
    fn fixed_window_multibyte_chars_stay_intact() {
        let chunks = chunk_fixed_window(
            "áéíóú",
            &FixedWindowConfig {
                chunk_size: 2,
                overlap: 0,
            },
        );
        assert_eq!(chunks, vec!["áé", "íó", "ú"]);
    }

    #[test]
    fn empty_text_yields_no_chunks() {
        assert!(chunk_fixed_window(
            "",
            &FixedWindowConfig {
                chunk_size: 4,
                overlap: 0
            }
        )
        .is_empty());
        assert!(chunk_recursive_character("", &RecursiveCharacterConfig::default()).is_empty());
    }

    #[test]
    fn recursive_character_prefers_paragraph_boundary() {
        let cfg = RecursiveCharacterConfig {
            chunk_size: 21,
            overlap: 0,
            separators: vec!["\n\n".to_string(), " ".to_string(), String::new()],
        };
        let text = "first paragraph here\n\nsecond paragraph here";
        let chunks = chunk_recursive_character(text, &cfg);
        assert_eq!(
            chunks,
            vec!["first paragraph here", "second paragraph here"]
        );
    }

    #[test]
    fn recursive_character_falls_back_to_hard_split_with_no_separators() {
        let cfg = RecursiveCharacterConfig {
            chunk_size: 5,
            overlap: 0,
            separators: vec![String::new()],
        };
        let chunks = chunk_recursive_character("abcdefghijklmno", &cfg);
        assert_eq!(chunks, vec!["abcde", "fghij", "klmno"]);
    }

    #[test]
    fn recursive_character_merges_short_pieces_up_to_chunk_size() {
        let cfg = RecursiveCharacterConfig {
            chunk_size: 11,
            overlap: 0,
            separators: vec![" ".to_string(), String::new()],
        };
        let chunks = chunk_recursive_character("aa bb cc dd ee", &cfg);
        // Greedy merge: "aa bb cc" (8 chars) + " dd" would be 11, + " ee" would
        // overflow — verify no chunk exceeds chunk_size and content survives.
        assert!(chunks.iter().all(|c| c.chars().count() <= 11));
        assert_eq!(chunks.join(" "), "aa bb cc dd ee");
    }

    #[test]
    fn recursive_character_applies_overlap_between_chunks() {
        let cfg = RecursiveCharacterConfig {
            chunk_size: 5,
            overlap: 2,
            separators: vec![String::new()],
        };
        let chunks = chunk_recursive_character("abcdefghij", &cfg);
        assert_eq!(chunks[0], "abcde");
        // Second chunk carries the last 2 chars of the first as a prefix.
        assert_eq!(chunks[1], "defghij");
    }

    #[test]
    fn semantic_chunking_breaks_on_low_similarity() {
        // Sentences embedded onto simple 2D vectors: "a"/"b" cluster near
        // (1,0), "c" is orthogonal (0,1) — must start a new chunk at "c".
        let embed = |s: &str| -> Vec<f32> {
            match s {
                "a" => vec![1.0, 0.0],
                "b" => vec![0.9, 0.1],
                "c" => vec![0.0, 1.0],
                _ => vec![0.0, 0.0],
            }
        };
        let sentences: Vec<String> = ["a", "b", "c"].iter().map(|s| s.to_string()).collect();
        let chunks = chunk_semantic(
            &sentences,
            embed,
            &SemanticChunkConfig {
                similarity_threshold: 0.5,
            },
        );
        assert_eq!(chunks, vec!["a b", "c"]);
    }

    #[test]
    fn semantic_chunking_keeps_similar_sentences_together() {
        let embed = |_: &str| vec![1.0, 0.0];
        let sentences: Vec<String> = ["a", "b", "c"].iter().map(|s| s.to_string()).collect();
        let chunks = chunk_semantic(
            &sentences,
            embed,
            &SemanticChunkConfig {
                similarity_threshold: 0.9,
            },
        );
        assert_eq!(chunks, vec!["a b c"]);
    }

    #[test]
    fn semantic_chunking_empty_input_yields_no_chunks() {
        let chunks = chunk_semantic(
            &[],
            |_| vec![],
            &SemanticChunkConfig {
                similarity_threshold: 0.5,
            },
        );
        assert!(chunks.is_empty());
    }
}
