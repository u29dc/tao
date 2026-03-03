//! Wikilink parsing and extraction primitives.

use std::collections::HashMap;

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

/// Heading fragment resolution result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeadingResolution {
    /// Normalized heading slug when fragment exists on the resolved target file.
    pub resolved_heading_slug: Option<String>,
    /// True when heading requirement is satisfied.
    pub is_resolved: bool,
}

/// Resolve raw target against candidate normalized paths using deterministic tie-breakers.
#[must_use]
pub fn resolve_target(
    raw_target: &str,
    source_path: Option<&str>,
    candidates: &[String],
) -> LinkResolution {
    let target = parse_wikilink(raw_target)
        .map(|link| link.target)
        .unwrap_or_else(|_| strip_wikilink_wrappers(raw_target).to_string());
    let target = normalize_path_like(&target);

    let mut matched_candidates: Vec<String> = if target.contains('/') {
        candidates
            .iter()
            .filter(|candidate| normalized_candidate_equals_target(candidate, &target))
            .cloned()
            .collect()
    } else {
        let target_basename = basename_without_extension(&target);
        candidates
            .iter()
            .filter(|candidate| {
                basename_without_extension(&normalize_path_like(candidate))
                    .eq_ignore_ascii_case(&target_basename)
            })
            .cloned()
            .collect()
    };

    if matched_candidates.is_empty() {
        return LinkResolution {
            resolved_path: None,
            matched_candidates,
            is_ambiguous: false,
        };
    }

    let source_dir = source_path.map(parent_dir);
    matched_candidates
        .sort_by(|left, right| compare_candidates(left, right, source_dir.as_deref()));

    let resolved_path = matched_candidates.first().cloned();
    LinkResolution {
        resolved_path,
        is_ambiguous: matched_candidates.len() > 1,
        matched_candidates,
    }
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

fn normalize_path_like(value: &str) -> String {
    value.replace('\\', "/").trim().to_string()
}

fn normalized_candidate_equals_target(candidate: &str, target: &str) -> bool {
    let candidate = normalize_path_like(candidate);
    let candidate_without_ext = strip_markdown_extension(&candidate);
    let target_without_ext = strip_markdown_extension(target);
    candidate_without_ext.eq_ignore_ascii_case(target_without_ext)
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

fn parent_dir(path: &str) -> String {
    let normalized = normalize_path_like(path);
    let mut parts: Vec<&str> = normalized.split('/').collect();
    if parts.len() <= 1 {
        return String::new();
    }
    let _ = parts.pop();
    parts.join("/")
}

fn compare_candidates(left: &str, right: &str, source_dir: Option<&str>) -> std::cmp::Ordering {
    let left_dir = parent_dir(left);
    let right_dir = parent_dir(right);

    let left_same_folder = source_dir.is_some_and(|dir| left_dir.eq_ignore_ascii_case(dir));
    let right_same_folder = source_dir.is_some_and(|dir| right_dir.eq_ignore_ascii_case(dir));
    if left_same_folder != right_same_folder {
        return right_same_folder.cmp(&left_same_folder);
    }

    if let Some(source_dir) = source_dir {
        let left_distance = relative_distance(source_dir, left);
        let right_distance = relative_distance(source_dir, right);
        if left_distance != right_distance {
            return left_distance.cmp(&right_distance);
        }
    }

    normalize_path_like(left)
        .to_lowercase()
        .cmp(&normalize_path_like(right).to_lowercase())
}

fn relative_distance(source_dir: &str, candidate_path: &str) -> usize {
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
        .take_while(|(left, right)| left.eq_ignore_ascii_case(right))
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
        WikiLinkParseError, extract_wikilinks, parse_wikilink, resolve_heading_target,
        resolve_target, slugify_heading,
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
}
