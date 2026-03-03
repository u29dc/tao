//! Wikilink parsing and extraction primitives.

use thiserror::Error;

/// Parsed wikilink token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WikiLink {
    /// Raw source token including brackets.
    pub raw: String,
    /// Target path or note token.
    pub target: String,
    /// Optional display text after `|`.
    pub display: Option<String>,
    /// Optional heading fragment after `#`.
    pub heading: Option<String>,
    /// Optional block fragment after `#^`.
    pub block: Option<String>,
    /// True when target contains explicit path separators.
    pub has_explicit_path: bool,
}

/// Parse one wikilink token from raw text.
pub fn parse_wikilink(raw: &str) -> Result<WikiLink, WikiLinkParseError> {
    let trimmed = raw.trim();
    let inner = strip_wikilink_wrappers(trimmed);
    if inner.is_empty() {
        return Err(WikiLinkParseError::Empty);
    }

    let (target_with_fragment, display) = split_display(inner);
    let (target, heading, block) = split_fragments(target_with_fragment);

    if target.is_empty() {
        return Err(WikiLinkParseError::MissingTarget {
            raw: trimmed.to_string(),
        });
    }

    Ok(WikiLink {
        raw: trimmed.to_string(),
        target: target.to_string(),
        display,
        heading,
        block,
        has_explicit_path: target.contains('/'),
    })
}

/// Extract all valid wikilinks from a markdown string.
#[must_use]
pub fn extract_wikilinks(markdown: &str) -> Vec<WikiLink> {
    let mut links = Vec::new();
    let mut cursor = 0;

    while let Some(start_offset) = markdown[cursor..].find("[[") {
        let start = cursor + start_offset;
        let rest = &markdown[(start + 2)..];
        let Some(end_offset) = rest.find("]]") else {
            break;
        };

        let end = start + 2 + end_offset + 2;
        let raw = &markdown[start..end];
        if let Ok(link) = parse_wikilink(raw) {
            links.push(link);
        }

        cursor = end;
    }

    links
}

fn strip_wikilink_wrappers(value: &str) -> &str {
    value
        .strip_prefix("[[")
        .and_then(|value| value.strip_suffix("]]"))
        .unwrap_or(value)
        .trim()
}

fn split_display(value: &str) -> (&str, Option<String>) {
    if let Some(index) = value.find('|') {
        let target = value[..index].trim();
        let display = value[(index + 1)..].trim();
        let display = (!display.is_empty()).then(|| display.to_string());
        (target, display)
    } else {
        (value.trim(), None)
    }
}

fn split_fragments(value: &str) -> (&str, Option<String>, Option<String>) {
    if let Some(index) = value.find("#^") {
        let target = value[..index].trim();
        let block = value[(index + 2)..].trim();
        let block = (!block.is_empty()).then(|| block.to_string());
        return (target, None, block);
    }

    if let Some(index) = value.find('#') {
        let target = value[..index].trim();
        let heading = value[(index + 1)..].trim();
        let heading = (!heading.is_empty()).then(|| heading.to_string());
        return (target, heading, None);
    }

    (value.trim(), None, None)
}

/// Wikilink parser failures.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum WikiLinkParseError {
    /// Raw token content was empty.
    #[error("wikilink token is empty")]
    Empty,
    /// Raw token lacked a target value.
    #[error("wikilink token has no target: {raw}")]
    MissingTarget {
        /// Raw token string.
        raw: String,
    },
}

#[cfg(test)]
mod tests {
    use super::{WikiLinkParseError, extract_wikilinks, parse_wikilink};

    #[test]
    fn parse_supports_target_display_heading_and_block_forms() {
        let plain = parse_wikilink("[[note]]").expect("parse plain");
        assert_eq!(plain.target, "note");
        assert_eq!(plain.display, None);
        assert_eq!(plain.heading, None);
        assert_eq!(plain.block, None);

        let display = parse_wikilink("[[folder/note|Daily Note]]").expect("parse display");
        assert_eq!(display.target, "folder/note");
        assert_eq!(display.display.as_deref(), Some("Daily Note"));
        assert!(display.has_explicit_path);

        let heading = parse_wikilink("[[note#summary]]").expect("parse heading");
        assert_eq!(heading.target, "note");
        assert_eq!(heading.heading.as_deref(), Some("summary"));
        assert_eq!(heading.block, None);

        let block = parse_wikilink("[[note#^block-id]]").expect("parse block");
        assert_eq!(block.target, "note");
        assert_eq!(block.block.as_deref(), Some("block-id"));
        assert_eq!(block.heading, None);
    }

    #[test]
    fn extract_wikilinks_parses_multiple_tokens() {
        let markdown =
            "see [[note-one]] and [[folder/note-two|second]] then [[note-three#details]]";
        let links = extract_wikilinks(markdown);

        assert_eq!(links.len(), 3);
        assert_eq!(links[0].target, "note-one");
        assert_eq!(links[1].display.as_deref(), Some("second"));
        assert_eq!(links[2].heading.as_deref(), Some("details"));
    }

    #[test]
    fn parse_rejects_empty_target() {
        let error = parse_wikilink("[[]]").expect_err("empty link should fail");
        assert_eq!(error, WikiLinkParseError::Empty);
    }
}
