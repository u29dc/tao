//! One source-aware Markdown parse shared by indexing and document reads.

use std::collections::BTreeSet;
use std::ops::Range;
use std::path::Path;

use pulldown_cmark::{Event, LinkType, Options, Parser, Tag, TagEnd};
use serde::{Deserialize, Serialize};
use tao_sdk_properties::{FrontMatterStatus, extract_front_matter};
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
    /// One captured revision of the original source.
    pub raw: String,
}

/// Half-open original-source byte range with physical one-based line locations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceSpan {
    /// First source byte, inclusive.
    pub start: usize,
    /// Last source byte, exclusive.
    pub end: usize,
    /// Physical source line containing the first byte.
    pub line: usize,
    /// Physical source line containing the last byte.
    pub end_line: usize,
}

/// Heading token extracted from real Markdown structure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeadingToken {
    /// Heading level from one to six.
    pub level: u8,
    /// Visible inline heading text, without markup delimiters.
    pub text: String,
    /// Physical one-based source line, including frontmatter.
    pub line: usize,
    /// Original source location.
    pub span: SourceSpan,
}

/// One Markdown task-list occurrence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskToken {
    /// Supported task state: open, done, or cancelled.
    pub state: String,
    /// First-line task payload, preserving inline source syntax.
    pub text: String,
    /// Physical one-based source line.
    pub line: usize,
    /// Original first-line task source location.
    pub span: SourceSpan,
}

/// Syntax used to express a link destination.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LinkSyntax {
    /// Obsidian-style discovery link.
    Wiki,
    /// Explicit Markdown destination or reference-style link.
    Markdown,
}

/// One occurrence, before syntax-specific target resolution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParsedLinkOccurrence {
    /// Exact source expression, including an embed marker when present.
    pub raw: String,
    /// Parser destination; percent escapes remain undecoded.
    pub target: String,
    /// Destination syntax determines path resolution semantics.
    pub syntax: LinkSyntax,
    /// Whether this is an image or embed.
    pub is_embed: bool,
    /// Original source expression location.
    pub span: SourceSpan,
}

/// Structured facts from one captured source revision.
#[derive(Debug, Clone, PartialEq)]
pub struct MarkdownParseResult {
    /// Canonical normalized note path.
    pub normalized_path: String,
    /// First real heading, otherwise the file stem.
    pub title: String,
    /// Optional frontmatter content without fence markers.
    pub front_matter: Option<String>,
    /// Shared frontmatter parse/projection diagnostic status.
    pub front_matter_status: FrontMatterStatus,
    /// Source body without recognized frontmatter fences.
    pub body: String,
    /// Byte offset where the body begins in the original source.
    pub body_offset: usize,
    /// Real headings in source order.
    pub headings: Vec<HeadingToken>,
    /// Real task-list items in source order.
    pub tasks: Vec<TaskToken>,
    /// Link occurrences in source order, without deduplication.
    pub links: Vec<ParsedLinkOccurrence>,
    /// Unique eligible block identifiers.
    pub block_ids: Vec<String>,
}

/// Stateless Markdown parser used by indexing and document reads.
#[derive(Debug, Default, Clone, Copy)]
pub struct MarkdownParser;

impl MarkdownParser {
    /// Parse one captured revision using one Markdown event stream.
    pub fn parse(
        &self,
        request: MarkdownParseRequest,
    ) -> Result<MarkdownParseResult, MarkdownParseError> {
        if request.normalized_path.trim().is_empty() {
            return Err(MarkdownParseError::EmptyPath);
        }
        let extraction = extract_front_matter(&request.raw);
        let body_offset = request.raw.len().saturating_sub(extraction.body.len());
        let body_line = request.raw[..body_offset]
            .bytes()
            .filter(|byte| *byte == b'\n')
            .count()
            + 1;
        let structure = parse_body(&extraction.body, body_offset, body_line);
        let title = structure
            .headings
            .iter()
            .find(|heading| !heading.text.trim().is_empty())
            .map(|heading| heading.text.clone())
            .unwrap_or_else(|| derive_title(&request.normalized_path));
        // An unclosed fence remains source body; do not claim it was extracted.
        let front_matter = if body_offset == 0 {
            None
        } else {
            extraction.raw
        };
        Ok(MarkdownParseResult {
            normalized_path: request.normalized_path,
            title,
            front_matter,
            front_matter_status: extraction.status,
            body: extraction.body,
            body_offset,
            headings: structure.headings,
            tasks: structure.tasks,
            links: structure.links,
            block_ids: structure.block_ids,
        })
    }
}

/// Body-only structural parse, for callers that already own extracted source bytes.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarkdownBodyStructure {
    /// Headings in source order.
    pub headings: Vec<HeadingToken>,
    /// Task occurrences in source order.
    pub tasks: Vec<TaskToken>,
    /// Link occurrences in source order.
    pub links: Vec<ParsedLinkOccurrence>,
    /// Unique eligible block identifiers.
    pub block_ids: Vec<String>,
}

/// Parse Markdown body once, retaining locations relative to the original capture.
#[must_use]
pub fn parse_body(body: &str, byte_offset: usize, first_line: usize) -> MarkdownBodyStructure {
    let options = Options::ENABLE_TABLES
        | Options::ENABLE_TASKLISTS
        | Options::ENABLE_FOOTNOTES
        | Options::ENABLE_STRIKETHROUGH
        | Options::ENABLE_WIKILINKS;
    let mut result = MarkdownBodyStructure::default();
    let mut eligible = body.as_bytes().to_vec();
    let line_starts = std::iter::once(0)
        .chain(body.match_indices('\n').map(|(index, _)| index + 1))
        .collect::<Vec<_>>();
    let span_for = |range: Range<usize>| SourceSpan {
        start: byte_offset + range.start,
        end: byte_offset + range.end,
        line: first_line
            + line_starts
                .partition_point(|start| *start <= range.start)
                .saturating_sub(1),
        end_line: first_line
            + line_starts
                .partition_point(|start| *start <= range.end.saturating_sub(1).max(range.start))
                .saturating_sub(1),
    };
    let events = Parser::new_ext(body, options)
        .into_offset_iter()
        .collect::<Vec<_>>();
    for (event, range) in &events {
        if matches!(
            event,
            Event::Start(Tag::CodeBlock(_))
                | Event::Code(_)
                | Event::Html(_)
                | Event::InlineHtml(_)
        ) {
            blank_range(&mut eligible, range.clone());
        }
    }
    let mut comment_ranges = Vec::new();
    let mut cursor = 0;
    while let Some(start) = find_pair(&eligible, cursor, b"%%") {
        if is_escaped(body.as_bytes(), start) {
            cursor = start + 2;
            continue;
        }
        let end = find_pair(&eligible, start + 2, b"%%").map_or(body.len(), |end| end + 2);
        comment_ranges.push(start..end);
        blank_range(&mut eligible, start..end);
        cursor = end;
    }
    let inside_comment = |offset: usize| {
        let index = comment_ranges.partition_point(|range| range.end <= offset);
        comment_ranges
            .get(index)
            .is_some_and(|range| range.contains(&offset))
    };
    let overlapping_comments = |range: &Range<usize>| {
        let start = comment_ranges.partition_point(|comment| comment.end <= range.start);
        let end = comment_ranges.partition_point(|comment| comment.start < range.end);
        &comment_ranges[start..end]
    };
    let overlaps_comment = |range: &Range<usize>| !overlapping_comments(range).is_empty();
    let mut current_heading: Option<(u8, Range<usize>, String)> = None;
    for (event, range) in events {
        match event {
            Event::Start(Tag::Heading { level, .. }) => {
                if !inside_comment(range.start) {
                    current_heading = Some((level as u8, range, String::new()));
                }
            }
            Event::End(TagEnd::Heading(_)) => {
                if let Some((level, range, text)) = current_heading.take() {
                    let span = span_for(range);
                    result.headings.push(HeadingToken {
                        level,
                        text: text.trim().to_string(),
                        line: span.line,
                        span,
                    });
                }
            }
            Event::Start(Tag::CodeBlock(_)) => blank_range(&mut eligible, range),
            Event::Code(text) => {
                blank_range(&mut eligible, range.clone());
                if !overlaps_comment(&range)
                    && let Some((_, _, heading)) = current_heading.as_mut()
                {
                    heading.push_str(&text);
                }
            }
            Event::Html(_) | Event::InlineHtml(_) => blank_range(&mut eligible, range),
            Event::Text(text) => {
                if let Some((_, _, heading)) = current_heading.as_mut() {
                    if !overlaps_comment(&range) {
                        heading.push_str(&text);
                    } else if !inside_comment(range.start)
                        || !inside_comment(range.end.saturating_sub(1))
                    {
                        // Text events spanning comment boundaries use unchanged source
                        // slices; entity/escape events are emitted separately by the parser.
                        let mut position = range.start;
                        for comment in overlapping_comments(&range) {
                            if comment.start > position {
                                heading.push_str(&body[position..comment.start]);
                            }
                            position = comment.end.min(range.end);
                        }
                        if position < range.end {
                            heading.push_str(&body[position..range.end]);
                        }
                    }
                }
            }
            Event::SoftBreak | Event::HardBreak => {
                if let Some((_, _, heading)) = current_heading.as_mut() {
                    heading.push(' ');
                }
            }
            Event::Start(Tag::Item) => {
                if inside_comment(range.start) {
                    continue;
                }
                let first = body[range.clone()]
                    .split('\n')
                    .next()
                    .unwrap_or_default()
                    .trim_end_matches('\r');
                let mut visible = first.as_bytes().to_vec();
                for comment in overlapping_comments(&(range.start..range.start + first.len())) {
                    let start = comment.start.max(range.start);
                    let end = comment.end.min(range.start + first.len());
                    if start < end {
                        blank_range(&mut visible, start - range.start..end - range.start);
                    }
                }
                let first = std::str::from_utf8(&visible).expect("mask preserves UTF-8");
                if let Some((state, text)) = parse_task_item(first) {
                    let span = span_for(range.start..range.start + first.len());
                    result.tasks.push(TaskToken {
                        state: state.to_string(),
                        text: text.to_string(),
                        line: span.line,
                        span,
                    });
                }
            }
            Event::Start(Tag::Link {
                link_type,
                dest_url,
                ..
            })
            | Event::Start(Tag::Image {
                link_type,
                dest_url,
                ..
            }) => {
                if overlaps_comment(&range) {
                    continue;
                }
                let raw = &body[range.clone()];
                result.links.push(ParsedLinkOccurrence {
                    raw: raw.to_string(),
                    target: dest_url.into_string(),
                    syntax: if matches!(link_type, LinkType::WikiLink { .. }) {
                        LinkSyntax::Wiki
                    } else {
                        LinkSyntax::Markdown
                    },
                    is_embed: raw.starts_with('!'),
                    span: span_for(range),
                });
            }
            _ => {}
        }
    }
    let mut blocks = BTreeSet::new();
    for (line_index, line) in body.lines().enumerate() {
        let start = line_starts[line_index];
        let end = start + line.len();
        let masked = std::str::from_utf8(&eligible[start..end]).expect("mask preserves UTF-8");
        let trimmed = masked.trim_end();
        if let Some(caret) = trimmed.rfind('^') {
            let id = &trimmed[caret + 1..];
            if !id.is_empty()
                && !is_escaped(body.as_bytes(), start + caret)
                && trimmed[..caret]
                    .chars()
                    .last()
                    .is_none_or(char::is_whitespace)
                && id
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
            {
                blocks.insert(id.to_string());
            }
        }
    }
    result.block_ids = blocks.into_iter().collect();
    result
}

fn parse_task_item(line: &str) -> Option<(&'static str, &str)> {
    let line = line.trim_start();
    let marker_end = if matches!(line.as_bytes().first(), Some(b'-' | b'*' | b'+')) {
        1
    } else {
        let digits = line.bytes().take_while(u8::is_ascii_digit).count();
        if digits == 0 || !matches!(line.as_bytes().get(digits), Some(b'.' | b')')) {
            return None;
        }
        digits + 1
    };
    let after_marker = &line[marker_end..];
    if !after_marker.starts_with(char::is_whitespace) {
        return None;
    }
    let checkbox = after_marker.trim_start();
    let state = if checkbox.starts_with("[ ]") {
        "open"
    } else if checkbox.starts_with("[x]") || checkbox.starts_with("[X]") {
        "done"
    } else if checkbox.starts_with("[-]") {
        "cancelled"
    } else {
        return None;
    };
    let text = &checkbox[3..];
    if !text.is_empty() && !text.starts_with(char::is_whitespace) {
        return None;
    }
    Some((state, text.trim()))
}

fn blank_range(bytes: &mut [u8], range: Range<usize>) {
    for byte in &mut bytes[range] {
        if !matches!(*byte, b'\r' | b'\n') {
            *byte = b' ';
        }
    }
}
fn find_pair(bytes: &[u8], start: usize, pair: &[u8; 2]) -> Option<usize> {
    bytes
        .get(start..)?
        .windows(2)
        .position(|window| window == pair)
        .map(|offset| start + offset)
}
fn is_escaped(bytes: &[u8], index: usize) -> bool {
    bytes[..index]
        .iter()
        .rev()
        .take_while(|byte| **byte == b'\\')
        .count()
        % 2
        == 1
}
fn derive_title(path: &str) -> String {
    Path::new(path)
        .file_stem()
        .and_then(|stem| stem.to_str())
        .filter(|stem| !stem.is_empty())
        .unwrap_or("untitled")
        .to_string()
}

/// Parser input errors. Metadata failures remain diagnostics on usable source text.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum MarkdownParseError {
    /// Missing source identity.
    #[error("normalized path must not be empty")]
    EmptyPath,
    /// Retained compatibility error; malformed fences are returned as diagnostics.
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
    fn parse_preserves_front_matter_body_line_endings_and_trailing_newline() {
        let parser = MarkdownParser;
        let input = MarkdownParseRequest {
            normalized_path: "notes/today.md".to_string(),
            raw: "---\r\ntags: x\r\n---\r\n# Title\r\nBody\r\n".to_string(),
        };

        let parsed = parser.parse(input).expect("parse markdown");

        assert_eq!(parsed.title, "Title");
        assert_eq!(parsed.front_matter, Some("tags: x".to_string()));
        assert_eq!(parsed.body, "# Title\r\nBody\r\n");
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
    fn parse_ignores_hash_prefixed_text_without_heading_space() {
        let parser = MarkdownParser;
        let input = MarkdownParseRequest {
            normalized_path: "note.md".to_string(),
            raw: "#todo item\n###identifier".to_string(),
        };

        let parsed = parser.parse(input).expect("parse markdown");
        assert_eq!(parsed.title, "note");
        assert_eq!(parsed.headings, Vec::new());
    }

    #[test]
    fn parse_rejects_unclosed_front_matter() {
        let parser = MarkdownParser;
        let input = MarkdownParseRequest {
            normalized_path: "note.md".to_string(),
            raw: "---\nkey: value".to_string(),
        };

        let parsed = parser
            .parse(input)
            .expect("unclosed front matter should be tolerated");
        assert_eq!(parsed.front_matter, None);
        assert_eq!(parsed.body, "---\nkey: value");
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

#[cfg(test)]
mod structural_tests {
    use super::{LinkSyntax, MarkdownParseRequest, MarkdownParser, parse_body};

    #[test]
    fn one_parse_excludes_code_comments_and_preserves_occurrences() {
        let raw = "---\r\nexample: |\r\n  - [ ] yaml task\r\n---\r\n```md\r\n# Fake\r\n- [ ] code task\r\n[[code]] ^code\r\n```\r\n<!-- [[comment]] ^comment -->\r\n\\[[escaped]]\r\n# Real *title*\r\nSetext\r\n======\r\n[[target]] [[target]] ![[asset.png]]\r\n[md](target.md#heading)\r\nreal ^block\r\n`fake ^inline`\r\n%% [[hidden]]\r\n# Hidden heading\r\n%%\r\n";
        let parsed = MarkdownParser
            .parse(MarkdownParseRequest {
                normalized_path: "test.md".into(),
                raw: raw.into(),
            })
            .unwrap();
        assert_eq!(parsed.title, "Real title");
        assert_eq!(
            parsed
                .headings
                .iter()
                .map(|h| h.text.as_str())
                .collect::<Vec<_>>(),
            ["Real title", "Setext"]
        );
        assert_eq!(parsed.headings[0].line, 12);
        assert!(parsed.tasks.is_empty());
        assert_eq!(parsed.links.len(), 4);
        assert_eq!(parsed.links[0].raw, "[[target]]");
        assert_eq!(parsed.links[1].raw, "[[target]]");
        assert!(parsed.links[2].is_embed);
        assert_eq!(parsed.links[3].syntax, LinkSyntax::Markdown);
        for link in &parsed.links {
            assert_eq!(&raw[link.span.start..link.span.end], link.raw);
        }
        assert_eq!(parsed.block_ids, ["block"]);
    }

    #[test]
    fn parses_actual_list_items_and_original_lines() {
        let parsed = MarkdownParser.parse(MarkdownParseRequest {
            normalized_path: "task.md".into(),
            raw: "---\ntitle: Tasks\n---\n- [ ] open\n* [X] done\n+ [-] cancelled\n1. [ ] ordered\n> - [ ] quoted\n- [ ]\n\n    - [ ] indented code\n".into(),
        }).unwrap();
        assert_eq!(
            parsed
                .tasks
                .iter()
                .map(|task| (task.state.as_str(), task.text.as_str(), task.line))
                .collect::<Vec<_>>(),
            [
                ("open", "open", 4),
                ("done", "done", 5),
                ("cancelled", "cancelled", 6),
                ("open", "ordered", 7),
                ("open", "quoted", 8),
                ("open", "", 9)
            ]
        );
        // Standalone indented code is never a task.
        assert!(parse_body("    - [ ] code\n", 0, 1).tasks.is_empty());
    }

    #[test]
    fn code_fence_comments_do_not_hide_later_structure() {
        let parsed = parse_body("```\n%%\n```\n[[real]]\n", 0, 1);
        assert_eq!(parsed.links.len(), 1);
        assert_eq!(parsed.links[0].target, "real");
    }
}

#[cfg(test)]
mod comment_tests {
    #[test]
    fn inline_comments_do_not_erase_visible_heading_or_task() {
        let parsed = super::parse_body(
            "# Hello %%hidden%% world\n- [ ] visible %%hidden%% task\n",
            0,
            1,
        );
        assert_eq!(parsed.headings.len(), 1);
        assert_eq!(parsed.headings[0].text, "Hello  world");
        assert_eq!(parsed.tasks.len(), 1);
        assert!(parsed.tasks[0].text.contains("visible"));
        assert!(!parsed.tasks[0].text.contains("hidden"));
        let with_code = super::parse_body("- [ ] run `command` %%private%% safely\n", 0, 1);
        assert!(with_code.tasks[0].text.contains("`command`"));
        assert!(!with_code.tasks[0].text.contains("private"));
    }
}

#[cfg(test)]
mod arbitrary_source_tests {
    use super::{MarkdownParseRequest, MarkdownParser, SourceSpan};

    fn assert_span(raw: &str, span: SourceSpan) {
        assert!(span.start <= span.end && span.end <= raw.len());
        assert!(raw.is_char_boundary(span.start));
        assert!(raw.is_char_boundary(span.end));
        assert_eq!(
            span.line,
            raw[..span.start].bytes().filter(|b| *b == b'\n').count() + 1
        );
        let last = span.end.saturating_sub(1).max(span.start);
        assert_eq!(
            span.end_line,
            raw.as_bytes()[..last]
                .iter()
                .filter(|b| **b == b'\n')
                .count()
                + 1
        );
    }

    #[test]
    fn deterministic_malformed_unicode_sources_keep_valid_original_spans() {
        // Deterministic grammar fragments exercise unfinished delimiters, nesting,
        // CRLF, escapes, non-ASCII bytes and unsupported/control characters.
        let fragments = [
            "é",
            "e\u{301}",
            "漢字",
            "🧭",
            "\0",
            "\r\n",
            "\n",
            " ",
            "\t",
            "# ",
            "##",
            "---\n",
            "```md\n",
            "~~~\n",
            "`",
            "``",
            "\\",
            "%%",
            "<!--",
            "-->",
            "<div>",
            "</div>",
            "[[",
            "]]",
            "![[",
            "|alias",
            "#^id",
            "[label](",
            ")",
            "<target>",
            "%ff",
            "&amp;",
            "- [ ] ",
            "1. [X] ",
            "+ [-] ",
            "> ",
            "^block",
            "*text*",
            "[ref]: file.md#section\n",
        ];
        let mut state = 0x6874_616f_7061_7273_u64;
        for case in 0..512 {
            let mut raw = if case % 3 == 0 {
                "---\ntitle: Test\n---\n".to_string()
            } else {
                String::new()
            };
            for _ in 0..(case % 96 + 1) {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                raw.push_str(fragments[(state as usize) % fragments.len()]);
            }
            let parsed = MarkdownParser
                .parse(MarkdownParseRequest {
                    normalized_path: "unicode.md".into(),
                    raw: raw.clone(),
                })
                .unwrap();
            assert_eq!(&raw[parsed.body_offset..], parsed.body);
            for heading in &parsed.headings {
                assert_span(&raw, heading.span);
            }
            for task in &parsed.tasks {
                assert_span(&raw, task.span);
            }
            let mut previous = 0;
            for link in &parsed.links {
                assert_span(&raw, link.span);
                assert_eq!(&raw[link.span.start..link.span.end], link.raw);
                assert!(previous <= link.span.start);
                previous = link.span.start;
            }
        }
    }

    #[test]
    fn many_comments_preserve_surrounding_structure() {
        let raw = (0..2048).map(|index| format!("# Heading {index} %%secret%% visible\n- [ ] task %%secret%% {index}\n[[note-{index}]]\n")).collect::<String>();
        let parsed = MarkdownParser
            .parse(MarkdownParseRequest {
                normalized_path: "many.md".into(),
                raw,
            })
            .unwrap();
        assert_eq!(parsed.headings.len(), 2048);
        assert_eq!(parsed.tasks.len(), 2048);
        assert_eq!(parsed.links.len(), 2048);
        assert!(
            parsed
                .headings
                .iter()
                .all(|heading| !heading.text.contains("secret"))
        );
    }
}
