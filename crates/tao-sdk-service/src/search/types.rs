//! Public graph-aware search request and response contracts.

use super::VaultSearchError;
use serde::Serialize;
use serde_json::{Map as JsonMap, Value as JsonValue};
use std::path::PathBuf;

/// Search surface selector for high-level vault search.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SearchKind {
    /// Let the service search all indexed surfaces and rank canonical candidates.
    Auto,
    /// Search every supported indexed surface.
    All,
    /// Search indexed Markdown and extracted TXT/PDF document text.
    Docs,
    /// Search the indexed file inventory.
    Files,
    /// Search base definitions and base row values.
    Bases,
    /// Search frontmatter/property rows.
    Properties,
    /// Search extracted task rows.
    Tasks,
    /// Search graph link targets and paths.
    Graph,
}

impl SearchKind {
    /// Parse one public search kind label.
    pub fn parse(raw: &str) -> Result<Self, VaultSearchError> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "auto" => Ok(Self::Auto),
            "all" => Ok(Self::All),
            "docs" => Ok(Self::Docs),
            "files" => Ok(Self::Files),
            "bases" => Ok(Self::Bases),
            "properties" => Ok(Self::Properties),
            "tasks" => Ok(Self::Tasks),
            "graph" => Ok(Self::Graph),
            other => Err(VaultSearchError::InvalidKind {
                value: other.to_string(),
            }),
        }
    }

    /// Public label used in JSON output.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::All => "all",
            Self::Docs => "docs",
            Self::Files => "files",
            Self::Bases => "bases",
            Self::Properties => "properties",
            Self::Tasks => "tasks",
            Self::Graph => "graph",
        }
    }
}

/// Request payload for graph-aware vault search.
#[derive(Debug, Clone)]
pub struct VaultSearchRequest {
    /// Canonical vault root.
    pub vault_root: PathBuf,
    /// Optional text query.
    pub query: Option<String>,
    /// Optional path root for context mode.
    pub path: Option<String>,
    /// Surface selector.
    pub kind: SearchKind,
    /// Optional path prefix.
    pub scope: Option<String>,
    /// Extension filters without leading dots.
    pub extensions: Vec<String>,
    /// Include context expansion.
    pub include_context: bool,
    /// Graph context depth.
    pub depth: u32,
    /// Result limit per section.
    pub limit: u32,
    /// Include bounded content excerpts.
    pub include_content: bool,
    /// Include local frontmatter/property values.
    pub include_pii: bool,
}

/// Top-level search response payload.
#[derive(Debug, Clone, Serialize)]
pub struct VaultSearchResult {
    /// Original query string, empty for path-only context mode.
    pub query: String,
    /// Search mode label.
    pub mode: String,
    /// Canonical path candidates deduped across surfaces.
    pub candidates: Vec<SearchCandidate>,
    /// File inventory matches.
    pub files: Vec<SearchFileMatch>,
    /// Indexed document matches.
    pub docs: Vec<SearchDocMatch>,
    /// Frontmatter/property matches.
    pub properties: Vec<SearchPropertyMatch>,
    /// Extracted task matches.
    pub tasks: Vec<SearchTaskMatch>,
    /// Graph/link matches.
    pub graph: Vec<SearchGraphMatch>,
    /// Optional context expansion. Empty sections are still present.
    pub context: SearchContext,
    /// Total canonical candidates before limit truncation.
    pub total: u64,
    /// Per-section result limit.
    pub limit: u32,
    /// True when response content was shortened to its shared byte budget.
    pub content_truncated: bool,
    /// Text coverage of all indexed inventory in the requested scope, including nonmatches.
    pub content_coverage: SearchContentCoverage,
}

/// Scoped text-coverage counters. State counters can overlap (for example stale and partial).
#[derive(Debug, Clone, Default, Serialize)]
pub struct SearchContentCoverage {
    /// Inventory files within the request scope and extension filters.
    pub total_files: u64,
    /// Files with a published text revision.
    pub searchable_files: u64,
    /// Markdown inventory files.
    pub markdown_files: u64,
    /// Non-Markdown files with a served extraction.
    pub extracted_files: u64,
    /// Supported files awaiting an initial or replacement extraction.
    pub pending_files: u64,
    /// Supported files with a failed capture/extraction or no usable Markdown revision.
    pub failed_files: u64,
    /// Supported files with incomplete extracted coverage.
    pub partial_files: u64,
    /// Served extracted revisions older than the desired revision or extractor.
    pub stale_files: u64,
    /// Inventory-only formats without a text extractor.
    pub unsupported_files: u64,
    /// Files with canonical-source diagnostics.
    pub diagnostic_files: u64,
    /// All supported text files have complete, current coverage with no source diagnostic.
    pub complete: bool,
}

/// One deduped canonical candidate.
#[derive(Debug, Clone, Serialize)]
pub struct SearchCandidate {
    /// Normalized vault-relative path.
    pub path: String,
    /// Display title derived from note/file path.
    pub title: String,
    /// Whether the candidate is a markdown note.
    pub is_markdown: bool,
    /// Lowercase extension without leading dot.
    pub extension: String,
    /// Relative fixed-point ranking score; larger values rank first.
    pub score: i64,
    /// Matched surfaces.
    pub kinds: Vec<String>,
    /// Match reasons.
    pub reasons: Vec<String>,
    /// Resolved inbound link count.
    pub inbound_links: u64,
    /// Resolved outgoing link count.
    pub outgoing_links: u64,
}

/// One file inventory match.
#[derive(Debug, Clone, Serialize)]
pub struct SearchFileMatch {
    /// Extension-derived document/image/audio/video/archive/code/other classification.
    pub file_group: String,
    /// Stable file id.
    pub file_id: String,
    /// Normalized vault-relative path.
    pub path: String,
    /// Lowercase extension without leading dot.
    pub extension: String,
    /// Size in bytes.
    pub size: u64,
    /// Last modified unix timestamp in milliseconds.
    pub modified_unix_ms: i64,
    /// Indexed timestamp.
    pub indexed_at: String,
    /// Whether this row is markdown.
    pub is_markdown: bool,
    /// Resolved inbound link count.
    pub inbound_links: u64,
    /// Resolved outgoing link count.
    pub outgoing_links: u64,
    /// Whether the file has any resolved inbound link.
    pub linked: bool,
    /// Match reasons.
    pub matched_in: Vec<String>,
    /// Relative fixed-point ranking score; larger values rank first.
    pub score: i64,
}

/// One markdown document match.
#[derive(Debug, Clone, Serialize)]
pub struct SearchDocMatch {
    /// Stable file id.
    pub file_id: String,
    /// Normalized vault-relative path.
    pub path: String,
    /// Display title.
    pub title: String,
    /// Indexed timestamp.
    pub indexed_at: String,
    /// Matching document surfaces.
    pub matched_in: Vec<String>,
    /// Optional bounded excerpt.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub excerpt: Option<String>,
    /// Source locator for extracted text or a PDF page.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub locator: Option<JsonValue>,
    /// Captured source revision used for this result.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub revision: Option<String>,
    /// Extracted source coverage.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub coverage: Option<String>,
    /// The served extraction belongs to a previous source revision.
    pub stale: bool,
    /// Relative fixed-point ranking score; larger values rank first.
    pub score: i64,
}

/// One property/frontmatter match.
#[derive(Debug, Clone, Serialize)]
pub struct SearchPropertyMatch {
    /// Stable property id.
    pub property_id: String,
    /// Owning file id.
    pub file_id: String,
    /// Owning file path.
    pub path: String,
    /// Property key.
    pub key: String,
    /// Property value type.
    pub value_type: String,
    /// Property value, or `"redacted"` when --no-pii is set.
    pub value: JsonValue,
    /// Updated timestamp.
    pub updated_at: String,
    /// Relative fixed-point ranking score; larger values rank first.
    pub score: i64,
}

/// One extracted task match.
#[derive(Debug, Clone, Serialize)]
pub struct SearchTaskMatch {
    /// Stable task id.
    pub task_id: String,
    /// Owning file id.
    pub file_id: String,
    /// Owning file path.
    pub path: String,
    /// One-based line number.
    pub line: i64,
    /// Task state.
    pub state: String,
    /// Task text.
    pub text: String,
    /// Updated timestamp.
    pub updated_at: String,
    /// Relative fixed-point ranking score; larger values rank first.
    pub score: i64,
}

/// One graph/link match.
#[derive(Debug, Clone, Serialize)]
pub struct SearchGraphMatch {
    /// Stable link id.
    pub link_id: String,
    /// Source path.
    pub source_path: String,
    /// Resolved target path when available.
    pub target_path: Option<String>,
    /// Raw target token.
    pub raw_target: String,
    /// Link source field.
    pub source_field: String,
    /// Whether the link resolved.
    pub resolved: bool,
    /// Stable unresolved reason.
    pub unresolved_reason: Option<String>,
    /// Relative fixed-point ranking score; larger values rank first.
    pub score: i64,
}

/// Context expansion payload.
#[derive(Debug, Clone, Serialize)]
pub struct SearchContext {
    /// Selected root candidate.
    pub root: Option<SearchRoot>,
    /// Root properties.
    pub properties: Vec<SearchPropertyMatch>,
    /// Matching base rows for the root.
    pub base_rows: Vec<SearchBaseRowMatch>,
    /// One-hop link panels and grouped paths.
    pub links: SearchContextLinks,
    /// Graph walk rows.
    pub walk: Vec<SearchWalkStep>,
    /// Date-sorted inferred timeline.
    pub timeline: Vec<SearchTimelineEntry>,
    /// Attached or related non-markdown files.
    pub attachments: Vec<SearchFileMatch>,
    /// Related counts by coarse kind.
    pub related_counts: JsonMap<String, JsonValue>,
    /// Ambiguous near-top candidates.
    pub ambiguity: Vec<SearchCandidate>,
}

impl Default for SearchContext {
    fn default() -> Self {
        Self {
            root: None,
            properties: Vec::new(),
            base_rows: Vec::new(),
            links: SearchContextLinks::default(),
            walk: Vec::new(),
            timeline: Vec::new(),
            attachments: Vec::new(),
            related_counts: JsonMap::new(),
            ambiguity: Vec::new(),
        }
    }
}

/// Selected root context metadata.
#[derive(Debug, Clone, Serialize)]
pub struct SearchRoot {
    /// Stable file id.
    pub file_id: String,
    /// Normalized path.
    pub path: String,
    /// Display title.
    pub title: String,
    /// Whether root is markdown.
    pub is_markdown: bool,
    /// Extension.
    pub extension: String,
    /// File size.
    pub size: u64,
    /// Last modified unix timestamp in milliseconds.
    pub modified_unix_ms: i64,
    /// Indexed timestamp.
    pub indexed_at: String,
    /// Frontmatter text, or `"redacted"` when --no-pii is set.
    pub front_matter: Option<String>,
    /// Optional bounded body excerpt.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body_excerpt: Option<String>,
    /// Exact revision underlying the served text, if one has been published.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub revision: Option<String>,
    /// Explicit published text coverage.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub coverage: Option<String>,
    /// Whether the served revision precedes the desired source revision.
    pub stale: bool,
}

/// One base row match.
#[derive(Debug, Clone, Serialize)]
pub struct SearchBaseRowMatch {
    /// Base id.
    pub base_id: String,
    /// Base file path.
    pub base_path: String,
    /// View name.
    pub view_name: String,
    /// Row file id.
    pub file_id: String,
    /// Row file path.
    pub path: String,
    /// Projected base row values.
    pub values: JsonMap<String, JsonValue>,
    /// Relative fixed-point ranking score; larger values rank first.
    pub score: i64,
}

/// One-hop links.
#[derive(Debug, Clone, Default, Serialize)]
pub struct SearchContextLinks {
    /// Outgoing link rows.
    pub outgoing: Vec<SearchGraphMatch>,
    /// Incoming link rows.
    pub incoming: Vec<SearchGraphMatch>,
    /// Related paths grouped by coarse vault role.
    pub grouped: JsonMap<String, JsonValue>,
}

/// Graph walk step.
#[derive(Debug, Clone, Serialize)]
pub struct SearchWalkStep {
    /// Traversal depth.
    pub depth: u32,
    /// Direction label.
    pub direction: String,
    /// Edge type label.
    pub edge_type: String,
    /// Source path.
    pub source_path: String,
    /// Target path when resolved.
    pub target_path: Option<String>,
    /// Raw target token.
    pub raw_target: String,
    /// Whether the edge resolved.
    pub resolved: bool,
}

/// Timeline entry inferred from paths/properties.
#[derive(Debug, Clone, Serialize)]
pub struct SearchTimelineEntry {
    /// Date token in YYYY-MM-DD form when inferred.
    pub date: String,
    /// Source kind.
    pub kind: String,
    /// Related path.
    pub path: String,
    /// Human label.
    pub label: String,
}
