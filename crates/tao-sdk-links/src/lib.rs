//! Markdown/wikilink parsing and deterministic link-resolution primitives.

use std::collections::{BTreeSet, HashMap, HashSet};

use pulldown_cmark::{Event, Options, Parser, Tag, TagEnd};
use tao_sdk_core::{cmp_normalized_paths, normalize_path_like};
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
    let markdown = markdown_without_code_contexts(markdown);
    let markdown = markdown.as_str();
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

/// Parsed markdown inline link or embed target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarkdownLink {
    /// Raw destination token as emitted by markdown parser.
    pub raw_target: String,
    /// Normalized vault-relative target path candidate (fragment/query removed).
    pub target: String,
    /// True when source token was an image/embed (`![alt](target)`).
    pub is_embed: bool,
}

/// Extract markdown links and embeds from markdown text.
///
/// This parser supports inline markdown links/embeds and excludes external URL schemes.
#[must_use]
pub fn extract_markdown_links(markdown: &str) -> Vec<MarkdownLink> {
    let parser = Parser::new_ext(markdown, markdown_parser_options());
    let mut links = Vec::new();

    for event in parser {
        match event {
            Event::Start(Tag::Link { dest_url, .. }) => {
                if let Some(target) = normalize_markdown_target(dest_url.as_ref()) {
                    links.push(MarkdownLink {
                        raw_target: dest_url.into_string(),
                        target,
                        is_embed: false,
                    });
                }
            }
            Event::Start(Tag::Image { dest_url, .. }) => {
                if let Some(target) = normalize_markdown_target(dest_url.as_ref()) {
                    links.push(MarkdownLink {
                        raw_target: dest_url.into_string(),
                        target,
                        is_embed: true,
                    });
                }
            }
            _ => {}
        }
    }

    links
}

/// Deterministic resolution result for a wikilink target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinkResolution {
    /// Selected resolved path when resolution succeeded.
    pub resolved_path: Option<String>,
    /// Sorted candidate paths considered by resolver.
    pub matched_candidates: Vec<String>,
    /// True when multiple candidates matched and tie-breakers selected one path.
    pub is_ambiguous: bool,
}

/// Controls how link targets and candidate paths are compared.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum LinkCasePolicy {
    /// Preserve path casing for comparisons.
    #[default]
    Sensitive,
    /// Lowercase paths before comparing.
    Insensitive,
}

/// Precomputed lookup table for deterministic link resolution.
#[derive(Debug, Clone)]
pub struct LinkResolutionIndex {
    case_policy: LinkCasePolicy,
    by_normalized_target: HashMap<String, Vec<String>>,
    by_basename: HashMap<String, Vec<String>>,
}

impl Default for LinkResolutionIndex {
    fn default() -> Self {
        Self::new(&[])
    }
}

impl LinkResolutionIndex {
    /// Build one resolution index from normalized vault-relative candidate paths.
    #[must_use]
    pub fn new(candidates: &[String]) -> Self {
        Self::with_case_policy(candidates, LinkCasePolicy::Sensitive)
    }

    /// Build one resolution index with explicit path case matching semantics.
    #[must_use]
    pub fn with_case_policy(candidates: &[String], case_policy: LinkCasePolicy) -> Self {
        let mut by_normalized_target = HashMap::<String, Vec<String>>::new();
        let mut by_basename = HashMap::<String, Vec<String>>::new();

        for candidate in candidates {
            let normalized_candidate = normalize_path_like(candidate.trim());
            let normalized_key = resolution_lookup_key(&normalized_candidate, case_policy);
            by_normalized_target
                .entry(normalized_key)
                .or_default()
                .push(candidate.clone());

            let basename_key = basename_lookup_key(&normalized_candidate, case_policy);
            by_basename
                .entry(basename_key)
                .or_default()
                .push(candidate.clone());
        }

        for candidates in by_normalized_target
            .values_mut()
            .chain(by_basename.values_mut())
        {
            candidates.sort_by(|left, right| cmp_normalized_paths(left, right));
            candidates.dedup();
        }

        Self {
            case_policy,
            by_normalized_target,
            by_basename,
        }
    }

    /// Resolve raw target using precomputed candidate lookup tables.
    #[must_use]
    pub fn resolve(&self, raw_target: &str, source_path: Option<&str>) -> LinkResolution {
        let target = parse_wikilink(raw_target)
            .map(|link| link.target)
            .unwrap_or_else(|_| strip_wikilink_wrappers(raw_target).to_string());
        let Some(target) = normalize_resolution_target(&target) else {
            return LinkResolution {
                resolved_path: None,
                matched_candidates: Vec::new(),
                is_ambiguous: false,
            };
        };

        let mut matched_candidates = Vec::<String>::new();
        if target.contains('/') {
            let source_dir = source_path.map(parent_dir);
            let mut seen = HashSet::<String>::new();
            for variant in resolve_target_variants(&target, source_dir.as_deref()) {
                let key = resolution_lookup_key(&variant, self.case_policy);
                if let Some(candidates) = self.by_normalized_target.get(&key) {
                    for candidate in candidates {
                        if seen.insert(candidate.clone()) {
                            matched_candidates.push(candidate.clone());
                        }
                    }
                }
            }
        } else {
            let key = basename_lookup_key(&target, self.case_policy);
            if let Some(candidates) = self.by_basename.get(&key) {
                matched_candidates.extend(candidates.iter().cloned());
            }
        }

        finish_resolution(matched_candidates, source_path, self.case_policy)
    }
}

/// Heading fragment resolution result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeadingResolution {
    /// Normalized heading slug when fragment exists on the resolved target file.
    pub resolved_heading_slug: Option<String>,
    /// True when heading requirement is satisfied.
    pub is_resolved: bool,
}

/// Block fragment resolution result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockResolution {
    /// Normalized block id when fragment exists on the resolved target file.
    pub resolved_block_id: Option<String>,
    /// True when block requirement is satisfied.
    pub is_resolved: bool,
}

/// Resolve raw target against candidate normalized paths using deterministic tie-breakers.
#[must_use]
pub fn resolve_target(
    raw_target: &str,
    source_path: Option<&str>,
    candidates: &[String],
) -> LinkResolution {
    resolve_target_with_case_policy(
        raw_target,
        source_path,
        candidates,
        LinkCasePolicy::Sensitive,
    )
}

/// Resolve raw target against candidate normalized paths with explicit case semantics.
#[must_use]
pub fn resolve_target_with_case_policy(
    raw_target: &str,
    source_path: Option<&str>,
    candidates: &[String],
    case_policy: LinkCasePolicy,
) -> LinkResolution {
    let target = parse_wikilink(raw_target)
        .map(|link| link.target)
        .unwrap_or_else(|_| strip_wikilink_wrappers(raw_target).to_string());
    let Some(target) = normalize_resolution_target(&target) else {
        return LinkResolution {
            resolved_path: None,
            matched_candidates: Vec::new(),
            is_ambiguous: false,
        };
    };

    let mut matched_candidates = Vec::new();
    if target.contains('/') {
        let source_dir = source_path.map(parent_dir);
        let resolved_variants = resolve_target_variants(&target, source_dir.as_deref());
        for candidate in candidates {
            if resolved_variants.iter().any(|resolved_target| {
                normalized_candidate_equals_target(candidate, resolved_target, case_policy)
            }) {
                matched_candidates.push(candidate.clone());
            }
        }
    } else {
        let target_basename = basename_without_extension(&target);
        for candidate in candidates {
            let candidate_trimmed = candidate.trim();
            if candidate_trimmed.contains('\\') {
                let normalized_candidate = normalize_path_like(candidate_trimmed);
                if case_policy_eq(
                    &basename_without_extension(&normalized_candidate),
                    &target_basename,
                    case_policy,
                ) {
                    matched_candidates.push(candidate.clone());
                }
                continue;
            }

            let candidate_without_extension = strip_markdown_extension(candidate_trimmed);
            let candidate_basename = candidate_without_extension
                .rsplit('/')
                .next()
                .unwrap_or(candidate_without_extension);
            if case_policy_eq(candidate_basename, &target_basename, case_policy) {
                matched_candidates.push(candidate.clone());
            }
        }
    }

    finish_resolution(matched_candidates, source_path, case_policy)
}

/// Convert heading text or heading fragment value into an Obsidian-style slug.
#[must_use]
pub fn slugify_heading(value: &str) -> String {
    let mut slug = String::new();
    let mut previous_was_separator = false;

    for character in value.trim().chars() {
        if character.is_ascii_alphanumeric() {
            slug.push(character.to_ascii_lowercase());
            previous_was_separator = false;
            continue;
        }

        if character.is_alphanumeric() {
            for lower in character.to_lowercase() {
                slug.push(lower);
            }
            previous_was_separator = false;
            continue;
        }

        if !previous_was_separator && !slug.is_empty() {
            slug.push('-');
            previous_was_separator = true;
        }
    }

    while slug.ends_with('-') {
        let _ = slug.pop();
    }

    slug
}

/// Resolve heading fragment against target heading slug index.
#[must_use]
pub fn resolve_heading_target(
    heading_fragment: Option<&str>,
    resolved_path: Option<&str>,
    heading_index: &HashMap<String, Vec<String>>,
) -> HeadingResolution {
    let Some(fragment) = heading_fragment else {
        return HeadingResolution {
            resolved_heading_slug: None,
            is_resolved: true,
        };
    };
    let Some(path) = resolved_path else {
        return HeadingResolution {
            resolved_heading_slug: None,
            is_resolved: false,
        };
    };

    let requested_slug = slugify_heading(fragment);
    if requested_slug.is_empty() {
        return HeadingResolution {
            resolved_heading_slug: None,
            is_resolved: false,
        };
    }

    let matched = heading_index
        .get(path)
        .and_then(|headings| {
            headings
                .iter()
                .find(|heading| heading.eq_ignore_ascii_case(&requested_slug))
        })
        .cloned();

    HeadingResolution {
        resolved_heading_slug: matched.clone(),
        is_resolved: matched.is_some(),
    }
}

/// Extract unique block identifiers from markdown body text.
#[must_use]
pub fn extract_block_ids(markdown: &str) -> Vec<String> {
    let markdown = markdown_without_code_contexts(markdown);
    let mut block_ids = BTreeSet::new();

    for line in markdown.lines() {
        let trimmed = line.trim_end();
        let Some(caret_index) = trimmed.rfind('^') else {
            continue;
        };

        let boundary_ok = trimmed[..caret_index]
            .chars()
            .last()
            .is_none_or(char::is_whitespace);
        if !boundary_ok {
            continue;
        }

        let candidate = trimmed[(caret_index + 1)..].trim();
        if candidate.is_empty() {
            continue;
        }
        if candidate
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_'))
        {
            block_ids.insert(candidate.to_string());
        }
    }

    block_ids.into_iter().collect()
}

fn markdown_parser_options() -> Options {
    Options::ENABLE_TABLES
        | Options::ENABLE_TASKLISTS
        | Options::ENABLE_FOOTNOTES
        | Options::ENABLE_STRIKETHROUGH
}

fn markdown_without_code_contexts(markdown: &str) -> String {
    let mut sanitized = markdown.as_bytes().to_vec();
    let parser = Parser::new_ext(markdown, markdown_parser_options()).into_offset_iter();
    let mut in_code_block = false;

    for (event, range) in parser {
        match event {
            Event::Code(_) => blank_range_preserving_lines(&mut sanitized, range),
            Event::Start(Tag::CodeBlock(_)) => in_code_block = true,
            Event::End(TagEnd::CodeBlock) => in_code_block = false,
            Event::Text(_) if in_code_block => blank_range_preserving_lines(&mut sanitized, range),
            _ => {}
        }
    }

    String::from_utf8(sanitized).expect("sanitized markdown remains utf-8")
}

fn blank_range_preserving_lines(markdown: &mut [u8], range: std::ops::Range<usize>) {
    for byte in &mut markdown[range] {
        if !matches!(*byte, b'\n' | b'\r') {
            *byte = b' ';
        }
    }
}

/// Resolve block fragment against target block id index.
#[must_use]
pub fn resolve_block_target(
    block_fragment: Option<&str>,
    resolved_path: Option<&str>,
    block_index: &HashMap<String, Vec<String>>,
) -> BlockResolution {
    let Some(fragment) = block_fragment else {
        return BlockResolution {
            resolved_block_id: None,
            is_resolved: true,
        };
    };
    let Some(path) = resolved_path else {
        return BlockResolution {
            resolved_block_id: None,
            is_resolved: false,
        };
    };

    let requested = fragment.trim().trim_start_matches('^');
    if requested.is_empty() {
        return BlockResolution {
            resolved_block_id: None,
            is_resolved: false,
        };
    }

    let matched = block_index
        .get(path)
        .and_then(|blocks| {
            blocks
                .iter()
                .find(|block_id| block_id.eq_ignore_ascii_case(requested))
        })
        .cloned();

    BlockResolution {
        resolved_block_id: matched.clone(),
        is_resolved: matched.is_some(),
    }
}

fn normalize_markdown_target(raw: &str) -> Option<String> {
    let trimmed = raw
        .trim()
        .strip_prefix('<')
        .and_then(|value| value.strip_suffix('>'))
        .unwrap_or(raw.trim());
    if trimmed.is_empty() {
        return None;
    }
    if is_external_target(trimmed) {
        return None;
    }

    let without_fragment = trimmed.split_once('#').map_or(trimmed, |(path, _)| path);
    let without_query = without_fragment
        .split_once('?')
        .map_or(without_fragment, |(path, _)| path);
    let decoded = decode_percent(without_query);
    normalize_resolution_target(&decoded)
}

fn normalize_resolution_target(raw: &str) -> Option<String> {
    let normalized = normalize_path_like(raw);
    (!normalized.is_empty()).then_some(normalized)
}

fn is_external_target(target: &str) -> bool {
    if target.starts_with("//") {
        return true;
    }
    let Some(colon_index) = target.find(':') else {
        return false;
    };
    if colon_index == 1 {
        let bytes = target.as_bytes();
        let drive = bytes[0].is_ascii_alphabetic();
        let has_windows_separator = bytes
            .get(2)
            .is_some_and(|byte| *byte == b'/' || *byte == b'\\');
        if drive && has_windows_separator {
            return false;
        }
    }
    if colon_index == 0 {
        return false;
    }
    target[..colon_index]
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || matches!(character, '+' | '-' | '.'))
}

fn decode_percent(input: &str) -> String {
    let mut bytes = Vec::<u8>::with_capacity(input.len());
    let mut index = 0_usize;
    let source = input.as_bytes();

    while index < source.len() {
        if source[index] == b'%' && index + 2 < source.len() {
            let hi = source[index + 1] as char;
            let lo = source[index + 2] as char;
            if let (Some(hi), Some(lo)) = (hex_value(hi), hex_value(lo)) {
                bytes.push((hi << 4) | lo);
                index += 3;
                continue;
            }
        }
        bytes.push(source[index]);
        index += 1;
    }

    String::from_utf8_lossy(&bytes).to_string()
}

fn hex_value(value: char) -> Option<u8> {
    match value {
        '0'..='9' => Some((value as u8) - b'0'),
        'a'..='f' => Some((value as u8) - b'a' + 10),
        'A'..='F' => Some((value as u8) - b'A' + 10),
        _ => None,
    }
}

fn resolve_target_variants(target: &str, source_dir: Option<&str>) -> Vec<String> {
    let mut variants = vec![collapse_dot_segments(target)];
    if let Some(source_dir) = source_dir {
        for ancestor in ancestor_dirs(source_dir) {
            let combined = if ancestor.is_empty() {
                target.to_string()
            } else {
                format!("{ancestor}/{target}")
            };
            variants.push(collapse_dot_segments(&combined));
        }
    }
    variants.sort();
    variants.dedup();
    variants
}

fn ancestor_dirs(path: &str) -> Vec<String> {
    let normalized = normalize_path_like(path);
    if normalized.is_empty() {
        return vec![String::new()];
    }

    let parts = normalized
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    let mut ancestors = Vec::with_capacity(parts.len() + 1);
    ancestors.push(String::new());
    for index in 1..=parts.len() {
        ancestors.push(parts[..index].join("/"));
    }
    ancestors
}

fn collapse_dot_segments(path: &str) -> String {
    let normalized = normalize_path_like(path);
    let mut parts = Vec::<&str>::new();
    for segment in normalized.split('/') {
        if segment.is_empty() || segment == "." {
            continue;
        }
        if segment == ".." {
            let _ = parts.pop();
            continue;
        }
        parts.push(segment);
    }
    parts.join("/")
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

fn normalized_candidate_equals_target(
    candidate: &str,
    target: &str,
    case_policy: LinkCasePolicy,
) -> bool {
    if candidate.contains('\\') || target.contains('\\') {
        let normalized_candidate = normalize_path_like(candidate);
        let normalized_target = normalize_path_like(target);
        let candidate_without_ext = strip_markdown_extension(&normalized_candidate);
        let target_without_ext = strip_markdown_extension(&normalized_target);
        return case_policy_eq(candidate_without_ext, target_without_ext, case_policy);
    }

    let candidate_without_ext = strip_markdown_extension(candidate.trim());
    let target_without_ext = strip_markdown_extension(target.trim());
    case_policy_eq(candidate_without_ext, target_without_ext, case_policy)
}

fn strip_markdown_extension(path: &str) -> &str {
    path.strip_suffix(".md")
        .or_else(|| path.strip_suffix(".MD"))
        .unwrap_or(path)
}

fn basename_without_extension(path: &str) -> String {
    let path = strip_markdown_extension(path);
    path.rsplit('/').next().unwrap_or(path).to_string()
}

fn resolution_lookup_key(path: &str, case_policy: LinkCasePolicy) -> String {
    apply_case_policy(strip_markdown_extension(path.trim()), case_policy)
}

fn basename_lookup_key(path: &str, case_policy: LinkCasePolicy) -> String {
    apply_case_policy(&basename_without_extension(path), case_policy)
}

fn apply_case_policy(value: &str, case_policy: LinkCasePolicy) -> String {
    match case_policy {
        LinkCasePolicy::Sensitive => value.to_string(),
        LinkCasePolicy::Insensitive => value.to_lowercase(),
    }
}

fn case_policy_eq(left: &str, right: &str, case_policy: LinkCasePolicy) -> bool {
    match case_policy {
        LinkCasePolicy::Sensitive => left == right,
        LinkCasePolicy::Insensitive => left.to_lowercase() == right.to_lowercase(),
    }
}

fn finish_resolution(
    mut matched_candidates: Vec<String>,
    source_path: Option<&str>,
    case_policy: LinkCasePolicy,
) -> LinkResolution {
    if matched_candidates.is_empty() {
        return LinkResolution {
            resolved_path: None,
            matched_candidates,
            is_ambiguous: false,
        };
    }

    let source_dir = source_path.map(parent_dir);
    matched_candidates
        .sort_by(|left, right| compare_candidates(left, right, source_dir.as_deref(), case_policy));
    matched_candidates.dedup();

    let resolved_path = matched_candidates.first().cloned();
    LinkResolution {
        resolved_path,
        is_ambiguous: matched_candidates.len() > 1,
        matched_candidates,
    }
}

fn parent_dir(path: &str) -> String {
    let normalized = normalize_path_like(path);
    let mut parts: Vec<&str> = normalized.split('/').collect();
    if parts.len() <= 1 {
        return String::new();
    }
    let _ = parts.pop();
    parts.join("/")
}

fn compare_candidates(
    left: &str,
    right: &str,
    source_dir: Option<&str>,
    case_policy: LinkCasePolicy,
) -> std::cmp::Ordering {
    let left_dir = parent_dir(left);
    let right_dir = parent_dir(right);

    let left_same_folder =
        source_dir.is_some_and(|dir| case_policy_eq(&left_dir, dir, case_policy));
    let right_same_folder =
        source_dir.is_some_and(|dir| case_policy_eq(&right_dir, dir, case_policy));
    if left_same_folder != right_same_folder {
        return right_same_folder.cmp(&left_same_folder);
    }

    if let Some(source_dir) = source_dir {
        let left_distance = relative_distance(source_dir, left, case_policy);
        let right_distance = relative_distance(source_dir, right, case_policy);
        if left_distance != right_distance {
            return left_distance.cmp(&right_distance);
        }
    }

    cmp_normalized_paths(left, right)
}

fn relative_distance(source_dir: &str, candidate_path: &str, case_policy: LinkCasePolicy) -> usize {
    let from: Vec<&str> = source_dir
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect();
    let candidate_parent = parent_dir(candidate_path);
    let to: Vec<&str> = candidate_parent
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect();

    let common_prefix = from
        .iter()
        .zip(to.iter())
        .take_while(|(left, right)| case_policy_eq(left, right, case_policy))
        .count();

    (from.len() - common_prefix) + (to.len() - common_prefix)
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
    use std::collections::HashMap;

    use super::{
        LinkCasePolicy, LinkResolutionIndex, WikiLinkParseError, extract_block_ids,
        extract_markdown_links, extract_wikilinks, parse_wikilink, resolve_block_target,
        resolve_heading_target, resolve_target, resolve_target_with_case_policy, slugify_heading,
    };

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
    fn extract_wikilinks_ignores_code_contexts() {
        let markdown = r#"
see [[real-note]]
`[[inline-code]]`
```
[[fenced-code]]
```
and [[second-real]]
"#;
        let links = extract_wikilinks(markdown);

        assert_eq!(
            links
                .iter()
                .map(|link| link.target.as_str())
                .collect::<Vec<_>>(),
            vec!["real-note", "second-real"]
        );
    }

    #[test]
    fn extract_markdown_links_supports_inline_and_embed_forms() {
        let markdown = r#"
[inline](notes/a.md)
![image](assets/logo.png)
[external](https://example.com)
"#;
        let links = extract_markdown_links(markdown);
        assert_eq!(links.len(), 2);
        assert_eq!(links[0].target, "notes/a.md");
        assert!(!links[0].is_embed);
        assert_eq!(links[1].target, "assets/logo.png");
        assert!(links[1].is_embed);
    }

    #[test]
    fn extract_markdown_links_decodes_percent_and_preserves_relative_segments() {
        let markdown = r#"[attachment](../docs/Tax%20Return%202025.pdf#page=1)"#;
        let links = extract_markdown_links(markdown);
        assert_eq!(links.len(), 1);
        assert_eq!(links[0].target, "../docs/Tax Return 2025.pdf");
    }

    #[test]
    fn parse_rejects_empty_target() {
        let error = parse_wikilink("[[]]").expect_err("empty link should fail");
        assert_eq!(error, WikiLinkParseError::Empty);
    }

    #[test]
    fn resolve_target_prefers_same_folder_then_distance_then_lexical_order() {
        let candidates = vec![
            "notes/project/alpha.md".to_string(),
            "notes/project/zeta.md".to_string(),
            "archive/project/alpha.md".to_string(),
        ];

        let resolution = resolve_target("[[alpha]]", Some("notes/project/today.md"), &candidates);

        assert_eq!(
            resolution.resolved_path.as_deref(),
            Some("notes/project/alpha.md")
        );
        assert!(resolution.is_ambiguous);
        assert_eq!(
            resolution.matched_candidates,
            vec!["notes/project/alpha.md", "archive/project/alpha.md"]
        );
    }

    #[test]
    fn resolve_target_handles_explicit_paths() {
        let candidates = vec![
            "notes/project/alpha.md".to_string(),
            "notes/project/beta.md".to_string(),
        ];

        let resolution = resolve_target(
            "[[notes/project/beta]]",
            Some("notes/project/today.md"),
            &candidates,
        );

        assert_eq!(
            resolution.resolved_path.as_deref(),
            Some("notes/project/beta.md")
        );
        assert!(!resolution.is_ambiguous);
        assert_eq!(resolution.matched_candidates, vec!["notes/project/beta.md"]);
    }

    #[test]
    fn resolve_target_handles_relative_non_markdown_targets() {
        let candidates = vec![
            "notes/attachments/company-deck.pdf".to_string(),
            "notes/attachments/other.pdf".to_string(),
        ];
        let resolution = resolve_target(
            "../attachments/company-deck.pdf",
            Some("notes/current/source.md"),
            &candidates,
        );
        assert_eq!(
            resolution.resolved_path.as_deref(),
            Some("notes/attachments/company-deck.pdf")
        );
        assert!(!resolution.is_ambiguous);
    }

    #[test]
    fn resolve_target_handles_ancestor_relative_explicit_paths() {
        let candidates = vec!["WORK/13-RELATIONS/Contents/Media/foo.jpg".to_string()];
        let resolution = resolve_target(
            "[[Contents/Media/foo.jpg]]",
            Some("WORK/13-RELATIONS/Contents/post.md"),
            &candidates,
        );

        assert_eq!(
            resolution.resolved_path.as_deref(),
            Some("WORK/13-RELATIONS/Contents/Media/foo.jpg")
        );
        assert!(!resolution.is_ambiguous);
        assert_eq!(
            resolution.matched_candidates,
            vec!["WORK/13-RELATIONS/Contents/Media/foo.jpg"]
        );
    }

    #[test]
    fn resolve_target_respects_sensitive_case_policy() {
        let candidates = vec!["foo.md".to_string()];
        let index = LinkResolutionIndex::new(&candidates);

        let indexed = index.resolve("[[Foo]]", None);
        let linear = resolve_target("[[Foo]]", None, &candidates);

        assert_eq!(indexed.resolved_path, None);
        assert_eq!(indexed, linear);
    }

    #[test]
    fn resolve_target_supports_insensitive_case_policy() {
        let candidates = vec!["foo.md".to_string()];
        let index = LinkResolutionIndex::with_case_policy(&candidates, LinkCasePolicy::Insensitive);

        let indexed = index.resolve("[[Foo]]", None);
        let linear = resolve_target_with_case_policy(
            "[[Foo]]",
            None,
            &candidates,
            LinkCasePolicy::Insensitive,
        );

        assert_eq!(indexed.resolved_path.as_deref(), Some("foo.md"));
        assert_eq!(indexed, linear);
    }

    #[test]
    fn resolve_target_keeps_case_collisions_distinct_in_sensitive_mode() {
        let candidates = vec!["Foo.md".to_string(), "foo.md".to_string()];
        let index = LinkResolutionIndex::new(&candidates);

        let upper = index.resolve("[[Foo]]", None);
        let lower = index.resolve("[[foo]]", None);

        assert_eq!(upper.resolved_path.as_deref(), Some("Foo.md"));
        assert_eq!(upper.matched_candidates, vec!["Foo.md"]);
        assert_eq!(lower.resolved_path.as_deref(), Some("foo.md"));
        assert_eq!(lower.matched_candidates, vec!["foo.md"]);
    }

    #[test]
    fn resolution_index_matches_linear_resolver() {
        let candidates = vec![
            "notes/project/alpha.md".to_string(),
            "notes/project/zeta.md".to_string(),
            "archive/project/alpha.md".to_string(),
            "assets/company-deck.pdf".to_string(),
        ];
        let index = LinkResolutionIndex::new(&candidates);

        for raw_target in [
            "[[alpha]]",
            "[[notes/project/zeta]]",
            "../assets/company-deck.pdf",
            "[[missing]]",
        ] {
            let indexed = index.resolve(raw_target, Some("notes/project/today.md"));
            let linear = resolve_target(raw_target, Some("notes/project/today.md"), &candidates);
            assert_eq!(indexed, linear);
        }
    }

    #[test]
    fn slugify_heading_normalizes_spaces_case_and_punctuation() {
        assert_eq!(slugify_heading("Project Plan"), "project-plan");
        assert_eq!(slugify_heading("  API: v2.0 Scope  "), "api-v2-0-scope");
        assert_eq!(slugify_heading("Überblick"), "überblick");
    }

    #[test]
    fn resolve_heading_target_matches_indexed_slugs() {
        let mut heading_index = HashMap::new();
        heading_index.insert(
            "notes/a.md".to_string(),
            vec!["project-plan".to_string(), "status".to_string()],
        );

        let found =
            resolve_heading_target(Some("Project Plan"), Some("notes/a.md"), &heading_index);
        assert!(found.is_resolved);
        assert_eq!(found.resolved_heading_slug.as_deref(), Some("project-plan"));

        let missing = resolve_heading_target(Some("Unknown"), Some("notes/a.md"), &heading_index);
        assert!(!missing.is_resolved);
        assert_eq!(missing.resolved_heading_slug, None);
    }

    #[test]
    fn extract_block_ids_finds_unique_markdown_block_markers() {
        let markdown = "line one ^block-a\nline two ^block-b\nline three ^block-a\nignored^inline";
        let blocks = extract_block_ids(markdown);
        assert_eq!(blocks, vec!["block-a", "block-b"]);
    }

    #[test]
    fn extract_block_ids_ignores_code_contexts() {
        let markdown = r#"
line one ^block-a
`inline ^inline-code`
```
fenced ^fenced-code
```
line two ^block-b
"#;
        let blocks = extract_block_ids(markdown);

        assert_eq!(blocks, vec!["block-a", "block-b"]);
    }

    #[test]
    fn resolve_block_target_matches_indexed_block_ids() {
        let mut block_index = HashMap::new();
        block_index.insert(
            "notes/a.md".to_string(),
            vec!["intro".to_string(), "block-1".to_string()],
        );

        let found = resolve_block_target(Some("block-1"), Some("notes/a.md"), &block_index);
        assert!(found.is_resolved);
        assert_eq!(found.resolved_block_id.as_deref(), Some("block-1"));

        let missing = resolve_block_target(Some("missing"), Some("notes/a.md"), &block_index);
        assert!(!missing.is_resolved);
        assert_eq!(missing.resolved_block_id, None);
    }
}
