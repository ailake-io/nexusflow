//! Parses `tesseract <image> stdout -l <lang> tsv` output. TSV mode is
//! used instead of plain `tesseract <image> stdout -l <lang>` specifically
//! so a single tesseract invocation per page yields both the recognized
//! text *and* per-word confidence — running tesseract twice per page (once
//! for text, once for confidence) would double OCR time for no reason.
//!
//! Real tesseract TSV columns (tab-separated, one header row):
//! `level page_num block_num par_num line_num word_num left top width
//! height conf text` — `level == 5` marks a word row (the only rows with
//! real `text`/`conf`); levels 1-4 are page/block/paragraph/line summary
//! rows with `conf == -1` and empty `text`.

/// One page's OCR result, reconstructed from its TSV output.
#[derive(Debug, Clone, PartialEq)]
pub struct PageOcrResult {
    pub text: String,
    /// Mean confidence (0-100) across every recognized word — `0.0` for a
    /// page tesseract found no words on at all (a blank/near-blank page).
    pub avg_confidence: f32,
    pub low_confidence_word_count: u32,
}

const WORD_LEVEL: &str = "5";

pub fn parse_tesseract_tsv(tsv: &str, low_confidence_threshold: u32) -> PageOcrResult {
    let mut text = String::new();
    let mut last_line_key: Option<(i64, i64, i64)> = None;
    let mut confidences: Vec<f32> = Vec::new();
    let mut low_confidence_word_count = 0u32;

    let mut lines = tsv.lines();
    lines.next(); // header row

    for line in lines {
        let cols: Vec<&str> = line.split('\t').collect();
        if cols.len() < 12 || cols[0] != WORD_LEVEL {
            continue;
        }
        let word = cols[11];
        if word.is_empty() {
            continue;
        }
        let block_num: i64 = cols[2].parse().unwrap_or(0);
        let par_num: i64 = cols[3].parse().unwrap_or(0);
        let line_num: i64 = cols[4].parse().unwrap_or(0);
        let conf: f32 = cols[10].parse().unwrap_or(-1.0);

        let line_key = (block_num, par_num, line_num);
        match last_line_key {
            Some(prev) if prev == line_key => text.push(' '),
            Some(_) => text.push('\n'),
            None => {}
        }
        text.push_str(word);
        last_line_key = Some(line_key);

        if conf >= 0.0 {
            confidences.push(conf);
            if (conf as u32) < low_confidence_threshold {
                low_confidence_word_count += 1;
            }
        }
    }

    let avg_confidence = if confidences.is_empty() {
        0.0
    } else {
        confidences.iter().sum::<f32>() / confidences.len() as f32
    };

    PageOcrResult {
        text,
        avg_confidence,
        low_confidence_word_count,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HEADER: &str = "level\tpage_num\tblock_num\tpar_num\tline_num\tword_num\tleft\ttop\twidth\theight\tconf\ttext";

    fn word_row(block: i64, par: i64, line: i64, word_num: i64, conf: f32, text: &str) -> String {
        format!("5\t1\t{block}\t{par}\t{line}\t{word_num}\t0\t0\t10\t10\t{conf}\t{text}")
    }

    #[test]
    fn joins_words_on_the_same_line_with_spaces() {
        let tsv = format!(
            "{HEADER}\n{}\n{}\n{}",
            word_row(1, 1, 1, 1, 95.0, "Hello"),
            word_row(1, 1, 1, 2, 90.0, "world"),
            word_row(1, 1, 1, 3, 88.0, "!"),
        );
        let result = parse_tesseract_tsv(&tsv, 60);
        assert_eq!(result.text, "Hello world !");
    }

    #[test]
    fn starts_a_new_line_when_line_num_changes() {
        let tsv = format!(
            "{HEADER}\n{}\n{}",
            word_row(1, 1, 1, 1, 95.0, "First"),
            word_row(1, 1, 2, 1, 95.0, "Second"),
        );
        let result = parse_tesseract_tsv(&tsv, 60);
        assert_eq!(result.text, "First\nSecond");
    }

    #[test]
    fn skips_non_word_level_rows() {
        let tsv = format!(
            "{HEADER}\n1\t1\t0\t0\t0\t0\t0\t0\t100\t100\t-1\t\n{}",
            word_row(1, 1, 1, 1, 92.0, "Text"),
        );
        let result = parse_tesseract_tsv(&tsv, 60);
        assert_eq!(result.text, "Text");
        assert_eq!(result.avg_confidence, 92.0);
    }

    #[test]
    fn computes_average_confidence_across_words_only() {
        let tsv = format!(
            "{HEADER}\n{}\n{}",
            word_row(1, 1, 1, 1, 80.0, "A"),
            word_row(1, 1, 1, 2, 60.0, "B"),
        );
        let result = parse_tesseract_tsv(&tsv, 60);
        assert_eq!(result.avg_confidence, 70.0);
    }

    #[test]
    fn counts_words_below_the_confidence_threshold() {
        let tsv = format!(
            "{HEADER}\n{}\n{}\n{}",
            word_row(1, 1, 1, 1, 90.0, "Good"),
            word_row(1, 1, 1, 2, 40.0, "Bad"),
            word_row(1, 1, 1, 3, 59.0, "AlsoBad"),
        );
        let result = parse_tesseract_tsv(&tsv, 60);
        assert_eq!(result.low_confidence_word_count, 2);
    }

    #[test]
    fn blank_page_yields_empty_text_and_zero_confidence() {
        let tsv = format!("{HEADER}\n1\t1\t0\t0\t0\t0\t0\t0\t100\t100\t-1\t");
        let result = parse_tesseract_tsv(&tsv, 60);
        assert_eq!(result.text, "");
        assert_eq!(result.avg_confidence, 0.0);
        assert_eq!(result.low_confidence_word_count, 0);
    }
}
