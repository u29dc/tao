//! Markdown parse entrypoints and ingest-oriented parse models.

use std::path::Path;

use thiserror::Error;

mod render_cache;

pub use render_cache::{
    CacheInsertOutcome, RenderCachePolicy, RenderCachePolicyError, RenderedHtmlCache,
};

/// Input payload for markdown parser entrypoints.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarkdownParseRequest {
    /// Canonical normalized note path.
    pub normalized_path: String,
    /// Raw markdown text read from disk.
    pub raw: String,
}

/// Heading token extracted from markdown text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeadingToken {
    /// Heading level from 1 to 6.
    pub level: u8,
    /// Heading text without leading `#` prefix.
    pub text: String,
    /// 1-based line number within the markdown body block.
    pub line: usize,
}

/// Structured markdown parse result for downstream index pipelines.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarkdownParseResult {
    /// Canonical normalized note path.
    pub normalized_path: String,
    /// Derived title from first heading or file name.
    pub title: String,
    /// Optional front matter body (without `---` fences).
    pub front_matter: Option<String>,
    /// Markdown body content without front matter fences.
    pub body: String,
    /// Parsed heading tokens in source order.
    pub headings: Vec<HeadingToken>,
}

/// Stateless markdown parser used by ingest pipelines.
#[derive(Debug, Default, Clone, Copy)]
pub struct MarkdownParser;

impl MarkdownParser {
    /// Parse markdown text into a structured result suitable for indexing.
    pub fn parse(
        &self,
        request: MarkdownParseRequest,
    ) -> Result<MarkdownParseResult, MarkdownParseError> {
        if request.normalized_path.trim().is_empty() {
            return Err(MarkdownParseError::EmptyPath);
        }

        let (front_matter, body) = split_front_matter(&request.raw)?;
        let headings = collect_headings(&body);
        let title = headings
            .first()
            .map(|heading| heading.text.clone())
            .unwrap_or_else(|| derive_title(&request.normalized_path));

        Ok(MarkdownParseResult {
            normalized_path: request.normalized_path,
            title,
            front_matter,
            body,
            headings,
        })
    }
}

fn split_front_matter(raw: &str) -> Result<(Option<String>, String), MarkdownParseError> {
    let lines: Vec<&str> = raw.lines().collect();
    if lines.first() != Some(&"---") {
        return Ok((None, raw.to_string()));
    }

    let closing_index = lines
        .iter()
        .enumerate()
        .skip(1)
        .find_map(|(index, line)| (*line == "---").then_some(index))
        .ok_or(MarkdownParseError::UnclosedFrontMatter)?;

    let front_matter = lines[1..closing_index].join("\n");
    let body = if closing_index + 1 < lines.len() {
        lines[(closing_index + 1)..].join("\n")
    } else {
        String::new()
    };

    Ok((Some(front_matter), body))
}

fn collect_headings(body: &str) -> Vec<HeadingToken> {
    body.lines()
        .enumerate()
        .filter_map(|(index, line)| {
            parse_heading(line).map(|(level, text)| HeadingToken {
                level,
                text,
                line: index + 1,
            })
        })
        .collect()
}

fn parse_heading(line: &str) -> Option<(u8, String)> {
    let trimmed = line.trim_start();
    let level = trimmed.chars().take_while(|ch| *ch == '#').count();
    if !(1..=6).contains(&level) {
        return None;
    }

    let text = trimmed[level..].trim_start();
    if text.is_empty() {
        return None;
    }

    Some((level as u8, text.to_string()))
}

fn derive_title(normalized_path: &str) -> String {
    Path::new(normalized_path)
        .file_stem()
        .and_then(|stem| stem.to_str())
        .filter(|stem| !stem.is_empty())
        .unwrap_or("untitled")
        .to_string()
}

/// Errors returned by markdown parser entrypoints.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum MarkdownParseError {
    /// Input path is empty after trimming.
    #[error("normalized path must not be empty")]
    EmptyPath,
    /// Front matter opening fence was found without a closing fence.
    #[error("front matter fence is not closed")]
    UnclosedFrontMatter,
}

#[cfg(test)]
mod tests {
    use super::{MarkdownParseError, MarkdownParseRequest, MarkdownParser};

    #[test]
    fn parse_extracts_front_matter_and_headings() {
        let parser = MarkdownParser;
        let input = MarkdownParseRequest {
            normalized_path: "notes/today.md".to_string(),
            raw: "---\ntags: [daily]\n---\n# Day Title\n\n## Plan\nWork".to_string(),
        };

        let parsed = parser.parse(input).expect("parse markdown");

        assert_eq!(parsed.title, "Day Title");
        assert_eq!(parsed.front_matter, Some("tags: [daily]".to_string()));
        assert_eq!(parsed.body, "# Day Title\n\n## Plan\nWork");
        assert_eq!(parsed.headings.len(), 2);
        assert_eq!(parsed.headings[0].level, 1);
        assert_eq!(parsed.headings[0].text, "Day Title");
        assert_eq!(parsed.headings[1].level, 2);
        assert_eq!(parsed.headings[1].text, "Plan");
    }

    #[test]
    fn parse_falls_back_to_file_stem_for_title() {
        let parser = MarkdownParser;
        let input = MarkdownParseRequest {
            normalized_path: "journal/2026-03-03.md".to_string(),
            raw: "no headings in this file".to_string(),
        };

        let parsed = parser.parse(input).expect("parse markdown");
        assert_eq!(parsed.title, "2026-03-03");
        assert_eq!(parsed.headings, Vec::new());
    }

    #[test]
    fn parse_rejects_unclosed_front_matter() {
        let parser = MarkdownParser;
        let input = MarkdownParseRequest {
            normalized_path: "note.md".to_string(),
            raw: "---\nkey: value".to_string(),
        };

        let error = parser
            .parse(input)
            .expect_err("unclosed front matter should fail");
        assert_eq!(error, MarkdownParseError::UnclosedFrontMatter);
    }

    #[test]
    fn parse_rejects_empty_path() {
        let parser = MarkdownParser;
        let input = MarkdownParseRequest {
            normalized_path: "   ".to_string(),
            raw: "# Title".to_string(),
        };

        let error = parser.parse(input).expect_err("empty path should fail");
        assert_eq!(error, MarkdownParseError::EmptyPath);
    }
}
