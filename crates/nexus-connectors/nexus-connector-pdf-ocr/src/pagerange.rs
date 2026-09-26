use std::collections::BTreeSet;

/// Parses a page-range string like `"1-5,8,10-12"` into a sorted set of
/// 1-based page numbers. Only called for a non-empty `page_range` — an
/// absent/empty config value means "no restriction" and is handled by the
/// caller before this ever runs, so an empty *input string* here is always
/// a real config mistake, not "no restriction".
pub fn parse_page_range(spec: &str) -> Result<BTreeSet<u32>, String> {
    let mut pages = BTreeSet::new();
    for part in spec.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        if let Some((start, end)) = part.split_once('-') {
            let start: u32 = start
                .trim()
                .parse()
                .map_err(|_| format!("invalid page range segment: {part:?}"))?;
            let end: u32 = end
                .trim()
                .parse()
                .map_err(|_| format!("invalid page range segment: {part:?}"))?;
            if start == 0 || end < start {
                return Err(format!("invalid page range segment: {part:?}"));
            }
            pages.extend(start..=end);
        } else {
            let page: u32 = part
                .parse()
                .map_err(|_| format!("invalid page number: {part:?}"))?;
            if page == 0 {
                return Err(format!("invalid page number: {part:?}"));
            }
            pages.insert(page);
        }
    }
    if pages.is_empty() {
        return Err(format!("page_range {spec:?} parsed to no pages"));
    }
    Ok(pages)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_mixed_ranges_and_single_pages() {
        let pages = parse_page_range("1-5,8,10-12").unwrap();
        assert_eq!(
            pages.into_iter().collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5, 8, 10, 11, 12]
        );
    }

    #[test]
    fn parses_a_single_page_number() {
        let pages = parse_page_range("3").unwrap();
        assert_eq!(pages.into_iter().collect::<Vec<_>>(), vec![3]);
    }

    #[test]
    fn rejects_zero_as_a_page_number() {
        assert!(parse_page_range("0").is_err());
        assert!(parse_page_range("0-3").is_err());
    }

    #[test]
    fn rejects_a_backwards_range() {
        assert!(parse_page_range("5-2").is_err());
    }

    #[test]
    fn rejects_garbage() {
        assert!(parse_page_range("abc").is_err());
        assert!(parse_page_range("1-abc").is_err());
    }

    #[test]
    fn rejects_a_blank_spec() {
        assert!(parse_page_range("").is_err());
        assert!(parse_page_range("  ").is_err());
    }

    #[test]
    fn deduplicates_overlapping_ranges() {
        let pages = parse_page_range("1-3,2-4").unwrap();
        assert_eq!(pages.into_iter().collect::<Vec<_>>(), vec![1, 2, 3, 4]);
    }
}
