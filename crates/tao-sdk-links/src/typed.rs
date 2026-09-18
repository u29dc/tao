//! Typed destinations keep syntax and fragment semantics separate from file lookup.
use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use tao_sdk_core::normalize_path_like;
use tao_sdk_markdown::{LinkSyntax, ParsedLinkOccurrence, SourceSpan};

use super::{
    LinkResolution, LinkResolutionIndex, WikiLink, apply_case_policy, finish_resolution,
    is_external_target, parent_dir, parse_wikilink, resolution_lookup_key, resolve_block_target,
    resolve_heading_target,
};

/// Destination fragment with its original semantic domain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum LinkFragment {
    /// Markdown heading text or slug.
    Heading(String),
    /// Explicit block identifier.
    Block(String),
    /// Physical one-based PDF page ordinal, never a printed page label.
    Page(u32),
    /// An explicitly supplied but invalid PDF page ordinal.
    InvalidPage(String),
}

/// Parsed target; `path` is decoded exactly once and never reparsed as link syntax.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LinkTarget {
    /// NFC vault path token, without the leading root slash.
    pub path: String,
    /// Explicit anchor or page locator.
    pub fragment: Option<LinkFragment>,
    /// Resolution semantics selected by source syntax.
    pub syntax: LinkSyntax,
    /// The original path started with a vault-root slash.
    pub rooted: bool,
    /// A parse failure retained as evidence rather than silently discarded.
    pub invalid_reason: Option<String>,
}

/// Link occurrence classification, independent of target extension.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LinkKind {
    /// Wikilink occurrence.
    Wikilink,
    /// Markdown link occurrence.
    Markdown,
    /// Image or embedded-file occurrence in either syntax.
    Embed,
}

/// Full occurrence evidence suitable for canonical index storage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LinkOccurrence {
    /// Compatibility representation with raw expression and parsed fragments.
    pub link: WikiLink,
    /// Syntax-aware destination for resolution.
    pub target: LinkTarget,
    /// Source link classification.
    pub kind: LinkKind,
    /// Physical source coordinates.
    pub span: SourceSpan,
}

/// Rule that produced a resolution; ambiguity candidates remain separate evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResolutionRule {
    /// No eligible file matched.
    NotFound,
    /// Explicit destination within the current document.
    SameDocument,
    /// Explicit vault-root destination.
    VaultRoot,
    /// Exact path relative to the source document.
    SourceRelative,
    /// Wiki basename/ancestor discovery with deterministic tie-breaking.
    WikiDiscovery,
    /// Explicit note alias, after ordinary path/basename lookup failed.
    Alias,
    /// Invalid destination syntax or path traversal beyond the vault root.
    InvalidTarget,
}

/// Independent fragment state. Pending PDF metadata does not erase file resolution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FragmentStatus {
    /// No fragment requested.
    NotRequested,
    /// Fragment exists in the resolved file.
    Resolved,
    /// Target file does not exist.
    MissingDocument,
    /// Heading does not exist.
    BadAnchor,
    /// Block does not exist.
    BadBlock,
    /// Invalid or out-of-range physical page.
    BadPage,
    /// File exists but its PDF page count is not yet available.
    Pending,
}

/// Interpret a parsed occurrence without another Markdown parse or percent decode.
#[must_use]
pub fn parse_link_occurrence(occurrence: &ParsedLinkOccurrence) -> Option<LinkOccurrence> {
    let target = parse_link_target(&occurrence.target, occurrence.syntax)?;
    let mut link = if occurrence.syntax == LinkSyntax::Wiki {
        parse_wikilink(&occurrence.raw).unwrap_or_else(|_| WikiLink {
            raw: occurrence.raw.clone(),
            target: target.path.clone(),
            display: None,
            heading: None,
            block: None,
            has_explicit_path: target.path.contains('/'),
        })
    } else {
        WikiLink {
            raw: occurrence.raw.clone(),
            target: target.path.clone(),
            display: None,
            heading: None,
            block: None,
            has_explicit_path: true,
        }
    };
    link.raw.clone_from(&occurrence.raw);
    link.target.clone_from(&target.path);
    link.heading = match &target.fragment {
        Some(LinkFragment::Heading(value)) => Some(value.clone()),
        _ => None,
    };
    link.block = match &target.fragment {
        Some(LinkFragment::Block(value)) => Some(value.clone()),
        _ => None,
    };
    Some(LinkOccurrence {
        link,
        target,
        kind: if occurrence.is_embed {
            LinkKind::Embed
        } else if occurrence.syntax == LinkSyntax::Wiki {
            LinkKind::Wikilink
        } else {
            LinkKind::Markdown
        },
        span: occurrence.span,
    })
}

/// Parse the syntax boundary once, preserving encoded literal delimiters in paths.
#[must_use]
pub fn parse_link_target(raw: &str, syntax: LinkSyntax) -> Option<LinkTarget> {
    let raw = raw.trim();
    let wiki;
    let destination = if syntax == LinkSyntax::Wiki {
        wiki = super::strip_wikilink_wrappers(raw);
        wiki.split_once('|')
            .map_or(wiki, |(target, _)| target)
            .trim()
    } else {
        if is_external_target(raw) {
            return None;
        }
        raw.strip_prefix('<')
            .and_then(|value| value.strip_suffix('>'))
            .unwrap_or(raw)
    };
    let (path, fragment) = destination
        .split_once('#')
        .map_or((destination, None), |(path, fragment)| {
            (path, Some(fragment))
        });
    let path = if syntax == LinkSyntax::Markdown {
        path.split_once('?').map_or(path, |(path, _)| path)
    } else {
        path
    };
    let rooted = path.starts_with('/') || path.starts_with('\\');
    let mut invalid_reason = None;
    let decode = |value: &str| -> Result<String, String> {
        if syntax == LinkSyntax::Wiki {
            Ok(value.to_string())
        } else {
            decode_destination(value)
        }
    };
    let decoded = decode(path).unwrap_or_else(|reason| {
        invalid_reason = Some(reason);
        path.to_string()
    });
    let path = normalize_path_like(&decoded);
    if path.contains('\0') || path.contains('\n') || path.contains('\r') {
        invalid_reason = Some("invalid path control character".to_string());
    }
    let fragment = fragment.map(|raw| {
        let value = decode(raw).unwrap_or_else(|reason| {
            invalid_reason = Some(reason);
            raw.to_string()
        });
        if path.to_ascii_lowercase().ends_with(".pdf") && value.starts_with("page=") {
            let page = &value[5..];
            if page.bytes().all(|byte| byte.is_ascii_digit())
                && let Ok(page) = page.parse::<u32>()
                && page > 0
            {
                return LinkFragment::Page(page);
            }
            LinkFragment::InvalidPage(page.to_string())
        } else if let Some(block) = value.strip_prefix('^') {
            LinkFragment::Block(block.to_string())
        } else {
            LinkFragment::Heading(value)
        }
    });
    if path.is_empty() && fragment.is_none() {
        return None;
    }
    Some(LinkTarget {
        path,
        fragment,
        syntax,
        rooted,
        invalid_reason,
    })
}

fn decode_destination(value: &str) -> Result<String, String> {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut cursor = 0;
    while cursor < bytes.len() {
        if bytes[cursor] == b'%' {
            let Some(pair) = bytes.get(cursor + 1..cursor + 3) else {
                return Err("incomplete percent escape".to_string());
            };
            let (Some(hi), Some(lo)) = (
                super::hex_value(pair[0] as char),
                super::hex_value(pair[1] as char),
            ) else {
                return Err("invalid percent escape".to_string());
            };
            out.push((hi << 4) | lo);
            cursor += 3;
        } else {
            out.push(bytes[cursor]);
            cursor += 1;
        }
    }
    String::from_utf8(out).map_err(|_| "percent-decoded path is not UTF-8".to_string())
}

pub(super) fn unresolved() -> LinkResolution {
    LinkResolution {
        resolved_path: None,
        matched_candidates: Vec::new(),
        is_ambiguous: false,
        rule: ResolutionRule::NotFound,
    }
}

impl LinkResolutionIndex {
    /// Resolve a typed destination without interpreting its already-decoded path again.
    #[must_use]
    pub fn resolve_link(&self, target: &LinkTarget, source_path: Option<&str>) -> LinkResolution {
        if target.invalid_reason.is_some() {
            return LinkResolution {
                rule: ResolutionRule::InvalidTarget,
                ..unresolved()
            };
        }
        if target.path.is_empty() {
            return self.resolve_exact(
                source_path.unwrap_or_default(),
                source_path,
                ResolutionRule::SameDocument,
                true,
            );
        }
        if target.syntax == LinkSyntax::Wiki
            && !target.rooted
            && !target.path.starts_with("./")
            && !target.path.starts_with("../")
        {
            let result = self.resolve_wiki_path(&target.path, source_path);
            if result.resolved_path.is_some() {
                return result;
            }
            let key = apply_case_policy(&normalize_path_like(&target.path), self.case_policy);
            let mut aliases = finish_resolution(
                self.by_alias.get(&key).cloned().unwrap_or_default(),
                source_path,
                self.case_policy,
            );
            if aliases.resolved_path.is_some() {
                aliases.rule = ResolutionRule::Alias;
            }
            return aliases;
        }
        let combined = if target.rooted {
            target.path.clone()
        } else if let Some(source) = source_path {
            let parent = parent_dir(source);
            if parent.is_empty() {
                target.path.clone()
            } else {
                format!("{parent}/{}", target.path)
            }
        } else {
            target.path.clone()
        };
        let Some(path) = bounded_dot_segments(&combined) else {
            return LinkResolution {
                rule: ResolutionRule::InvalidTarget,
                ..unresolved()
            };
        };
        self.resolve_exact(
            &path,
            source_path,
            if target.rooted {
                ResolutionRule::VaultRoot
            } else {
                ResolutionRule::SourceRelative
            },
            target.syntax == LinkSyntax::Markdown,
        )
    }

    fn resolve_exact(
        &self,
        path: &str,
        source: Option<&str>,
        rule: ResolutionRule,
        exact_extension: bool,
    ) -> LinkResolution {
        let normalized = normalize_path_like(path);
        let candidates = if exact_extension {
            self.by_exact_path
                .get(&apply_case_policy(&normalized, self.case_policy))
        } else {
            self.by_normalized_target
                .get(&resolution_lookup_key(&normalized, self.case_policy))
        };
        let mut result = finish_resolution(
            candidates.cloned().unwrap_or_default(),
            source,
            self.case_policy,
        );
        result.rule = if result.resolved_path.is_some() {
            rule
        } else {
            ResolutionRule::NotFound
        };
        result
    }
}

fn bounded_dot_segments(path: &str) -> Option<String> {
    let mut parts = Vec::new();
    for segment in path.split('/') {
        match segment {
            "" | "." => {}
            ".." => {
                parts.pop()?;
            }
            part => parts.push(part),
        }
    }
    Some(parts.join("/"))
}

/// Validate a fragment independently from file resolution. Page counts are physical.
#[must_use]
pub fn validate_fragment(
    fragment: Option<&LinkFragment>,
    resolved_path: Option<&str>,
    headings: &HashMap<String, Vec<String>>,
    blocks: &HashMap<String, Vec<String>>,
    page_count: Option<u32>,
) -> FragmentStatus {
    let Some(fragment) = fragment else {
        return FragmentStatus::NotRequested;
    };
    if resolved_path.is_none() {
        return FragmentStatus::MissingDocument;
    }
    match fragment {
        LinkFragment::Heading(value) => {
            if value.is_empty()
                || resolve_heading_target(Some(value), resolved_path, headings).is_resolved
            {
                FragmentStatus::Resolved
            } else {
                FragmentStatus::BadAnchor
            }
        }
        LinkFragment::Block(value) => {
            if resolve_block_target(Some(value), resolved_path, blocks).is_resolved {
                FragmentStatus::Resolved
            } else {
                FragmentStatus::BadBlock
            }
        }
        LinkFragment::Page(page) => match page_count {
            Some(count) if *page > 0 && *page <= count => FragmentStatus::Resolved,
            Some(_) => FragmentStatus::BadPage,
            None => FragmentStatus::Pending,
        },
        LinkFragment::InvalidPage(_) => FragmentStatus::BadPage,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tao_sdk_markdown::parse_body;

    fn index() -> LinkResolutionIndex {
        LinkResolutionIndex::new(
            &[
                "target.md",
                "a/target.md",
                "sub/target.md",
                "a/hash#name.md",
                "a/hash%23name.md",
                "café.md",
                "a/source.md",
                "doc.PDF",
            ]
            .map(str::to_string),
        )
    }

    #[test]
    fn markdown_resolution_is_exact_and_decodes_once() {
        let index = index();
        for (raw, expected) in [
            ("/target.md", Some("target.md")),
            ("target.md", Some("a/target.md")),
            ("sub/target.md", None),
            ("../target.md", Some("target.md")),
            ("../../target.md", None),
            ("hash%23name.md#heading", Some("a/hash#name.md")),
            ("hash%2523name.md", Some("a/hash%23name.md")),
            ("target", None),
        ] {
            let target = parse_link_target(raw, LinkSyntax::Markdown).unwrap();
            assert_eq!(
                index
                    .resolve_link(&target, Some("a/source.md"))
                    .resolved_path
                    .as_deref(),
                expected,
                "{raw}"
            );
        }
        assert_eq!(
            parse_link_target("hash%23name.md#heading", LinkSyntax::Markdown)
                .unwrap()
                .fragment,
            Some(LinkFragment::Heading("heading".into()))
        );
    }

    #[test]
    fn same_note_and_pdf_pages_preserve_independent_status() {
        let index = index();
        let blocks = HashMap::from([("a/source.md".into(), vec!["block".into()])]);
        let headings = HashMap::from([("a/source.md".into(), vec!["heading".into()])]);
        for raw in ["[[#heading]]", "[[#^block]]"] {
            let target = parse_link_target(raw, LinkSyntax::Wiki).unwrap();
            let resolved = index.resolve_link(&target, Some("a/source.md"));
            assert_eq!(resolved.resolved_path.as_deref(), Some("a/source.md"));
            assert_eq!(resolved.rule, ResolutionRule::SameDocument);
            assert_eq!(
                validate_fragment(
                    target.fragment.as_ref(),
                    resolved.resolved_path.as_deref(),
                    &headings,
                    &blocks,
                    None
                ),
                FragmentStatus::Resolved
            );
        }
        let top = parse_link_target("#", LinkSyntax::Markdown).unwrap();
        let resolved = index.resolve_link(&top, Some("a/source.md"));
        assert_eq!(
            validate_fragment(
                top.fragment.as_ref(),
                resolved.resolved_path.as_deref(),
                &headings,
                &blocks,
                None
            ),
            FragmentStatus::Resolved
        );
        let target = parse_link_target("doc.PDF#page=12", LinkSyntax::Markdown).unwrap();
        assert_eq!(target.fragment, Some(LinkFragment::Page(12)));
        let resolved = index.resolve_link(&target, Some("source.md"));
        assert_eq!(resolved.resolved_path.as_deref(), Some("doc.PDF"));
        assert_eq!(
            validate_fragment(
                target.fragment.as_ref(),
                resolved.resolved_path.as_deref(),
                &headings,
                &blocks,
                None
            ),
            FragmentStatus::Pending
        );
        assert_eq!(
            validate_fragment(
                target.fragment.as_ref(),
                resolved.resolved_path.as_deref(),
                &headings,
                &blocks,
                Some(11)
            ),
            FragmentStatus::BadPage
        );
        assert_eq!(
            validate_fragment(
                target.fragment.as_ref(),
                resolved.resolved_path.as_deref(),
                &headings,
                &blocks,
                Some(12)
            ),
            FragmentStatus::Resolved
        );
        assert!(matches!(
            parse_link_target("[[doc.PDF#page=0]]", LinkSyntax::Wiki)
                .unwrap()
                .fragment,
            Some(LinkFragment::InvalidPage(_))
        ));
    }

    #[test]
    fn occurrences_keep_duplicates_embeds_and_original_offsets() {
        let raw = "[[target]] [[target]] ![[doc.PDF#page=1]] [x](target.md#missing)";
        let parsed = parse_body(raw, 0, 1);
        let links = parsed
            .links
            .iter()
            .filter_map(parse_link_occurrence)
            .collect::<Vec<_>>();
        assert_eq!(links.len(), 4);
        assert_ne!(links[0].span, links[1].span);
        assert_eq!(links[2].kind, LinkKind::Embed);
        assert_eq!(links[2].target.fragment, Some(LinkFragment::Page(1)));
        assert_eq!(
            links[3].target.fragment,
            Some(LinkFragment::Heading("missing".into()))
        );
    }

    #[test]
    fn unicode_and_case_ambiguity_are_deterministic() {
        assert_eq!(
            index()
                .resolve("[[cafe\u{301}]]", None)
                .resolved_path
                .as_deref(),
            Some("café.md")
        );
        let first = LinkResolutionIndex::with_case_policy(
            &["foo.md".into(), "Foo.md".into()],
            super::super::LinkCasePolicy::Insensitive,
        )
        .resolve("foo", None);
        let second = LinkResolutionIndex::with_case_policy(
            &["Foo.md".into(), "foo.md".into()],
            super::super::LinkCasePolicy::Insensitive,
        )
        .resolve("foo", None);
        assert_eq!(first, second);
        assert!(first.is_ambiguous);
        assert_eq!(first.matched_candidates.len(), 2);
    }

    #[test]
    fn aliases_preserve_complete_values_and_path_precedence() {
        let index = index().with_aliases(&[
            ("Smith, John".into(), "a/target.md".into()),
            ("Smith, John".into(), "target.md".into()),
            ("target".into(), "café.md".into()),
        ]);
        let alias = index.resolve("[[Smith, John]]", Some("a/source.md"));
        assert_eq!(alias.rule, ResolutionRule::Alias);
        assert!(alias.is_ambiguous);
        assert_eq!(alias.resolved_path.as_deref(), Some("a/target.md"));
        assert_eq!(
            index
                .resolve("[[target]]", Some("a/source.md"))
                .resolved_path
                .as_deref(),
            Some("a/target.md")
        );
    }

    #[test]
    fn invalid_decoding_is_evidence_not_a_lossy_filename() {
        for raw in ["bad%", "bad%GG", "bad%ff", "bad%00"] {
            let target = parse_link_target(raw, LinkSyntax::Markdown).unwrap();
            assert!(target.invalid_reason.is_some());
            assert_eq!(
                index().resolve_link(&target, None).rule,
                ResolutionRule::InvalidTarget
            );
        }
    }

    #[test]
    fn arbitrary_unicode_targets_resolve_without_losing_decode_evidence() {
        let fragments = [
            "é",
            "e\u{301}",
            "漢字",
            "🧭",
            "%",
            "%ff",
            "%2F",
            "%2523",
            "%23",
            "#",
            "^",
            "|",
            "[[",
            "]]",
            "../",
            "./",
            "/",
            "\\",
            "?",
            "\0",
            " ",
            "file.md",
            "asset.pdf#page=0",
        ];
        let index = index();
        let mut state = 0x7461_6f6c_696e_6b73_u64;
        for case in 0..1024 {
            let mut raw = String::new();
            for _ in 0..(case % 24 + 1) {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                raw.push_str(fragments[(state as usize) % fragments.len()]);
            }
            for syntax in [LinkSyntax::Wiki, LinkSyntax::Markdown] {
                if let Some(target) = parse_link_target(&raw, syntax) {
                    let resolution = index.resolve_link(&target, Some("a/source.md"));
                    assert_eq!(resolution, index.resolve_link(&target, Some("a/source.md")));
                    if target.invalid_reason.is_some() {
                        assert_eq!(resolution.rule, ResolutionRule::InvalidTarget);
                        assert!(resolution.resolved_path.is_none());
                    }
                    assert_eq!(
                        resolution.is_ambiguous,
                        resolution.matched_candidates.len() > 1
                    );
                }
            }
        }
    }
}
