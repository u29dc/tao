//! High-level graph-aware vault search orchestration.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use rusqlite::Connection;
use serde::Serialize;
use serde_json::{Map as JsonMap, Value as JsonValue, json};
use tao_sdk_bases::{BaseCoercionMode, BaseTableQueryPlanner, BaseViewRegistry};
use tao_sdk_core::note_title_from_path;
use tao_sdk_markdown::{MarkdownParseRequest, MarkdownParser};
use tao_sdk_search::{SearchQueryRequest, SearchQueryService};
use tao_sdk_storage::{
    BasesRepository, FilesRepository, LinkWithPaths, LinksRepository, PropertiesRepository,
    SearchIndexRepository,
};
use thiserror::Error;

use crate::{
    BacklinkGraphService, BaseTableExecutionOptions, BaseTableExecutorService, GraphWalkDirection,
    GraphWalkEdgeType, GraphWalkRequest,
};

/// Search surface selector for high-level vault search.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SearchKind {
    /// Let the service search all indexed surfaces and rank canonical candidates.
    Auto,
    /// Search every supported indexed surface.
    All,
    /// Search markdown document title/path/body index.
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

    fn includes(self, surface: SearchKind) -> bool {
        matches!(self, SearchKind::Auto | SearchKind::All) || self == surface
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
    /// Markdown document matches.
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
    /// Ranking score.
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
    /// Ranking score.
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
    /// Ranking score.
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
    /// Ranking score.
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
    /// Ranking score.
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
    /// Ranking score.
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
    /// Ranking score.
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

/// High-level vault search service.
#[derive(Debug, Default, Clone, Copy)]
pub struct VaultSearchService;

impl VaultSearchService {
    /// Execute one graph-aware vault search over indexed state.
    pub fn search(
        &self,
        connection: &Connection,
        request: VaultSearchRequest,
    ) -> Result<VaultSearchResult, VaultSearchError> {
        validate_request(&request)?;
        let limit = request.limit.clamp(1, 100);
        let depth = request.depth.min(4);
        let query = request
            .query
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToString::to_string);
        let needle = query.as_deref().map(SearchNeedle::new);
        let root_path = request
            .path
            .as_deref()
            .map(normalize_path_input)
            .transpose()?;
        let scope = request.scope.as_deref().map(normalize_scope).transpose()?;
        let extensions = normalize_extensions(&request.extensions);

        let files = FilesRepository::list_all(connection)
            .map_err(|source| VaultSearchError::Files { source })?;
        let file_by_id = files
            .iter()
            .map(|file| (file.file_id.clone(), file.clone()))
            .collect::<HashMap<_, _>>();
        let file_by_path = files
            .iter()
            .map(|file| (file.normalized_path.clone(), file.clone()))
            .collect::<HashMap<_, _>>();
        let link_counts = link_counts(connection)?;

        let mut candidates = CandidateSet::new(&file_by_path, &link_counts);
        if let Some(path) = &root_path
            && let Some(file) = file_by_path.get(path)
        {
            candidates.add(
                &file.normalized_path,
                SearchKind::Files,
                120,
                "path-root".to_string(),
            );
        }

        let mut docs = Vec::<SearchDocMatch>::new();
        let mut file_matches = Vec::<SearchFileMatch>::new();
        let mut properties = Vec::<SearchPropertyMatch>::new();
        let mut tasks = Vec::<SearchTaskMatch>::new();
        let mut graph = Vec::<SearchGraphMatch>::new();
        let mut base_rows = Vec::<SearchBaseRowMatch>::new();

        if let Some(needle) = needle.as_ref() {
            if request.kind.includes(SearchKind::Docs) {
                docs = search_docs(
                    connection,
                    &request.vault_root,
                    needle,
                    scope.as_deref(),
                    &extensions,
                    limit,
                    request.include_content,
                    &mut candidates,
                )?;
            }

            if request.kind.includes(SearchKind::Files) {
                file_matches = search_files(
                    &files,
                    needle,
                    scope.as_deref(),
                    &extensions,
                    &link_counts,
                    limit,
                    &mut candidates,
                );
            }

            if request.kind.includes(SearchKind::Properties) {
                properties = search_properties(
                    connection,
                    needle,
                    scope.as_deref(),
                    &extensions,
                    request.include_pii,
                    limit,
                    &mut candidates,
                )?;
            }

            if request.kind.includes(SearchKind::Tasks) {
                tasks = search_tasks(
                    connection,
                    needle,
                    scope.as_deref(),
                    &extensions,
                    limit,
                    &mut candidates,
                )?;
            }

            if request.kind.includes(SearchKind::Graph) {
                graph = search_graph(
                    connection,
                    needle,
                    scope.as_deref(),
                    &extensions,
                    &file_by_id,
                    limit,
                    &mut candidates,
                )?;
            }

            if request.kind.includes(SearchKind::Bases) {
                base_rows = search_bases(
                    connection,
                    needle,
                    scope.as_deref(),
                    &extensions,
                    limit,
                    &mut candidates,
                )?;
            }
        }

        let mut candidate_rows = candidates.finish();
        sort_candidates(&mut candidate_rows);
        let total = candidate_rows.len() as u64;
        candidate_rows.truncate(limit as usize);

        let context = if request.include_context || root_path.is_some() {
            build_context(
                connection,
                &request,
                root_path.as_deref(),
                &candidate_rows,
                &file_by_path,
                &file_by_id,
                &link_counts,
                &base_rows,
                depth,
                limit,
            )?
        } else {
            SearchContext::default()
        };

        Ok(VaultSearchResult {
            query: query.unwrap_or_default(),
            mode: request.kind.label().to_string(),
            candidates: candidate_rows,
            files: file_matches,
            docs,
            properties,
            tasks,
            graph,
            context,
            total,
            limit,
        })
    }
}

#[derive(Debug, Clone)]
struct SearchNeedle {
    raw: String,
    normalized: String,
    compact: String,
    tokens: Vec<String>,
}

impl SearchNeedle {
    fn new(raw: &str) -> Self {
        let normalized = normalize_text(raw);
        let compact = compact_text(raw);
        let tokens = normalized
            .split_whitespace()
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        Self {
            raw: raw.to_string(),
            normalized,
            compact,
            tokens,
        }
    }
}

#[derive(Debug, Clone, Default)]
struct CandidateSet {
    rows: HashMap<String, CandidateAccumulator>,
    files: HashMap<String, tao_sdk_storage::FileRecord>,
    link_counts: HashMap<String, LinkCount>,
}

impl CandidateSet {
    fn new(
        files: &HashMap<String, tao_sdk_storage::FileRecord>,
        link_counts: &HashMap<String, LinkCount>,
    ) -> Self {
        Self {
            rows: HashMap::new(),
            files: files.clone(),
            link_counts: link_counts.clone(),
        }
    }

    fn add(&mut self, path: &str, kind: SearchKind, score: i64, reason: String) {
        let entry = self
            .rows
            .entry(path.to_string())
            .or_insert_with(|| CandidateAccumulator {
                path: path.to_string(),
                score: canonical_path_boost(path),
                kinds: HashSet::new(),
                reasons: HashSet::new(),
            });
        entry.score += score;
        entry.kinds.insert(kind.label().to_string());
        entry.reasons.insert(reason);
    }

    fn finish(self) -> Vec<SearchCandidate> {
        self.rows
            .into_values()
            .map(|row| {
                let file = self.files.get(&row.path);
                let counts = self.link_counts.get(&row.path).copied().unwrap_or_default();
                let mut kinds = row.kinds.into_iter().collect::<Vec<_>>();
                let mut reasons = row.reasons.into_iter().collect::<Vec<_>>();
                kinds.sort();
                reasons.sort();
                SearchCandidate {
                    title: note_title_from_path(&row.path),
                    extension: extension_for_path(&row.path),
                    is_markdown: file.is_some_and(|file| file.is_markdown),
                    path: row.path,
                    score: row.score,
                    kinds,
                    reasons,
                    inbound_links: counts.inbound,
                    outgoing_links: counts.outgoing,
                }
            })
            .collect()
    }
}

#[derive(Debug, Clone)]
struct CandidateAccumulator {
    path: String,
    score: i64,
    kinds: HashSet<String>,
    reasons: HashSet<String>,
}

#[derive(Debug, Clone, Copy, Default)]
struct LinkCount {
    inbound: u64,
    outgoing: u64,
}

fn validate_request(request: &VaultSearchRequest) -> Result<(), VaultSearchError> {
    let has_query = request
        .query
        .as_deref()
        .map(str::trim)
        .is_some_and(|value| !value.is_empty());
    let has_path = request
        .path
        .as_deref()
        .map(str::trim)
        .is_some_and(|value| !value.is_empty());
    match (has_query, has_path) {
        (true, true) => Err(VaultSearchError::InvalidRequest(
            "provide either a query or --path, not both".to_string(),
        )),
        (false, false) => Err(VaultSearchError::InvalidRequest(
            "provide a query or --path".to_string(),
        )),
        _ => Ok(()),
    }
}

#[allow(clippy::too_many_arguments)]
fn search_docs(
    connection: &Connection,
    vault_root: &Path,
    needle: &SearchNeedle,
    scope: Option<&str>,
    extensions: &HashSet<String>,
    limit: u32,
    include_content: bool,
    candidates: &mut CandidateSet,
) -> Result<Vec<SearchDocMatch>, VaultSearchError> {
    let mut by_path = HashMap::<String, SearchDocMatch>::new();
    if !needle.raw.trim().is_empty() {
        let page = SearchQueryService
            .query(
                vault_root,
                connection,
                SearchQueryRequest {
                    query: needle.raw.clone(),
                    limit: u64::from(limit.clamp(1, 100)),
                    offset: 0,
                },
            )
            .map_err(|source| VaultSearchError::Docs { source })?;
        for item in page.items {
            if !path_allowed(&item.path, scope, extensions) {
                continue;
            }
            let matched_in = if item.matched_in.is_empty() {
                vec!["fts".to_string()]
            } else {
                item.matched_in
            };
            let score = score_for_match(&item.path, &matched_in, needle, 35);
            candidates.add(
                &item.path,
                SearchKind::Docs,
                score,
                format!("docs:{}", matched_in.join(",")),
            );
            by_path.insert(
                item.path.clone(),
                SearchDocMatch {
                    file_id: item.file_id,
                    path: item.path,
                    title: item.title,
                    indexed_at: item.indexed_at,
                    matched_in,
                    excerpt: None,
                    score,
                },
            );
        }
    }

    for row in SearchIndexRepository::list_all(connection)
        .map_err(|source| VaultSearchError::SearchIndex { source })?
    {
        if !path_allowed(&row.normalized_path, scope, extensions) {
            continue;
        }
        let mut matched_in = Vec::<String>::new();
        let mut score = 0_i64;
        if let Some(path_score) = text_match_score(&row.normalized_path, needle) {
            matched_in.push("path".to_string());
            score += 45 + path_score;
        }
        if let Some(title_score) = text_match_score(&row.title_lc, needle) {
            matched_in.push("title".to_string());
            score += 55 + title_score;
        }
        if let Some(content_score) = text_match_score(&row.content_lc, needle) {
            matched_in.push("content".to_string());
            score += 15 + content_score;
        }
        if matched_in.is_empty() {
            continue;
        }
        score += canonical_path_boost(&row.normalized_path);
        candidates.add(
            &row.normalized_path,
            SearchKind::Docs,
            score,
            format!("docs:{}", matched_in.join(",")),
        );
        by_path
            .entry(row.normalized_path.clone())
            .and_modify(|existing| {
                existing.score = existing.score.max(score);
                merge_strings(&mut existing.matched_in, &matched_in);
                if include_content && existing.excerpt.is_none() {
                    existing.excerpt = excerpt_for(&row.content_lc, needle);
                }
            })
            .or_insert_with(|| SearchDocMatch {
                file_id: row.file_id,
                title: note_title_from_path(&row.normalized_path),
                path: row.normalized_path,
                indexed_at: row.updated_at,
                matched_in,
                excerpt: include_content
                    .then(|| excerpt_for(&row.content_lc, needle))
                    .flatten(),
                score,
            });
    }

    let mut rows = by_path.into_values().collect::<Vec<_>>();
    rows.sort_by(compare_score_path);
    rows.truncate(limit as usize);
    Ok(rows)
}

fn search_files(
    files: &[tao_sdk_storage::FileRecord],
    needle: &SearchNeedle,
    scope: Option<&str>,
    extensions: &HashSet<String>,
    link_counts: &HashMap<String, LinkCount>,
    limit: u32,
    candidates: &mut CandidateSet,
) -> Vec<SearchFileMatch> {
    let mut rows = Vec::<SearchFileMatch>::new();
    for file in files {
        if !path_allowed(&file.normalized_path, scope, extensions) {
            continue;
        }
        let mut matched_in = Vec::new();
        let mut score = 0_i64;
        if let Some(path_score) = text_match_score(&file.normalized_path, needle) {
            matched_in.push("path".to_string());
            score += 45 + path_score;
        }
        let title = note_title_from_path(&file.normalized_path);
        if let Some(title_score) = text_match_score(&title, needle) {
            matched_in.push("title".to_string());
            score += 50 + title_score;
        }
        if matched_in.is_empty() {
            continue;
        }
        score += canonical_path_boost(&file.normalized_path);
        candidates.add(
            &file.normalized_path,
            SearchKind::Files,
            score,
            format!("files:{}", matched_in.join(",")),
        );
        let counts = link_counts
            .get(&file.normalized_path)
            .copied()
            .unwrap_or_default();
        rows.push(SearchFileMatch {
            file_id: file.file_id.clone(),
            path: file.normalized_path.clone(),
            extension: extension_for_path(&file.normalized_path),
            size: file.size_bytes,
            modified_unix_ms: file.modified_unix_ms,
            indexed_at: file.indexed_at.clone(),
            is_markdown: file.is_markdown,
            inbound_links: counts.inbound,
            outgoing_links: counts.outgoing,
            linked: counts.inbound > 0,
            matched_in,
            score,
        });
    }
    rows.sort_by(compare_score_path);
    rows.truncate(limit as usize);
    rows
}

fn search_properties(
    connection: &Connection,
    needle: &SearchNeedle,
    scope: Option<&str>,
    extensions: &HashSet<String>,
    include_pii: bool,
    limit: u32,
    candidates: &mut CandidateSet,
) -> Result<Vec<SearchPropertyMatch>, VaultSearchError> {
    let mut statement = connection
        .prepare(
            r#"
SELECT
  p.property_id,
  p.file_id,
  f.normalized_path AS file_path,
  p.key,
  p.value_type,
  p.value_json,
  p.updated_at
FROM properties p
JOIN files f ON f.file_id = p.file_id
ORDER BY f.normalized_path ASC, p.key ASC
"#,
        )
        .map_err(|source| VaultSearchError::Sql {
            operation: "prepare_search_properties",
            source,
        })?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>("property_id")?,
                row.get::<_, String>("file_id")?,
                row.get::<_, String>("file_path")?,
                row.get::<_, String>("key")?,
                row.get::<_, String>("value_type")?,
                row.get::<_, String>("value_json")?,
                row.get::<_, String>("updated_at")?,
            ))
        })
        .map_err(|source| VaultSearchError::Sql {
            operation: "query_search_properties",
            source,
        })?;
    let mut matches = Vec::new();
    for row in rows {
        let (property_id, file_id, path, key, value_type, value_json, updated_at) =
            row.map_err(|source| VaultSearchError::Sql {
                operation: "map_search_properties",
                source,
            })?;
        if !path_allowed(&path, scope, extensions) {
            continue;
        }
        let key_score = text_match_score(&key, needle).unwrap_or(0);
        let value_score = text_match_score(&value_json, needle).unwrap_or(0);
        if key_score == 0 && value_score == 0 {
            continue;
        }
        let score = 30 + key_score + value_score + canonical_path_boost(&path);
        candidates.add(
            &path,
            SearchKind::Properties,
            score,
            format!("property:{key}"),
        );
        matches.push(SearchPropertyMatch {
            property_id,
            file_id,
            path,
            key,
            value_type,
            value: pii_value(&value_json, include_pii),
            updated_at,
            score,
        });
    }
    matches.sort_by(compare_score_path);
    matches.truncate(limit as usize);
    Ok(matches)
}

fn search_tasks(
    connection: &Connection,
    needle: &SearchNeedle,
    scope: Option<&str>,
    extensions: &HashSet<String>,
    limit: u32,
    candidates: &mut CandidateSet,
) -> Result<Vec<SearchTaskMatch>, VaultSearchError> {
    let mut statement = connection
        .prepare(
            r#"
SELECT task_id, file_id, file_path, line_number, state, text, updated_at
FROM tasks
ORDER BY file_path ASC, line_number ASC
"#,
        )
        .map_err(|source| VaultSearchError::Sql {
            operation: "prepare_search_tasks",
            source,
        })?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>("task_id")?,
                row.get::<_, String>("file_id")?,
                row.get::<_, String>("file_path")?,
                row.get::<_, i64>("line_number")?,
                row.get::<_, String>("state")?,
                row.get::<_, String>("text")?,
                row.get::<_, String>("updated_at")?,
            ))
        })
        .map_err(|source| VaultSearchError::Sql {
            operation: "query_search_tasks",
            source,
        })?;
    let mut matches = Vec::new();
    for row in rows {
        let (task_id, file_id, path, line, state, text, updated_at) =
            row.map_err(|source| VaultSearchError::Sql {
                operation: "map_search_tasks",
                source,
            })?;
        if !path_allowed(&path, scope, extensions) {
            continue;
        }
        let score = text_match_score(&text, needle).unwrap_or(0)
            + text_match_score(&path, needle).unwrap_or(0);
        if score == 0 {
            continue;
        }
        let score = 20 + score + canonical_path_boost(&path);
        candidates.add(&path, SearchKind::Tasks, score, "task".to_string());
        matches.push(SearchTaskMatch {
            task_id,
            file_id,
            path,
            line,
            state,
            text,
            updated_at,
            score,
        });
    }
    matches.sort_by(compare_score_path);
    matches.truncate(limit as usize);
    Ok(matches)
}

fn search_graph(
    connection: &Connection,
    needle: &SearchNeedle,
    scope: Option<&str>,
    extensions: &HashSet<String>,
    files: &HashMap<String, tao_sdk_storage::FileRecord>,
    limit: u32,
    candidates: &mut CandidateSet,
) -> Result<Vec<SearchGraphMatch>, VaultSearchError> {
    let links = LinksRepository::list_all_with_paths(connection)
        .map_err(|source| VaultSearchError::Links { source })?;
    let mut matches = Vec::new();
    for link in links {
        let candidate_path = link
            .resolved_path
            .as_deref()
            .unwrap_or(link.source_path.as_str());
        if !path_allowed(candidate_path, scope, extensions) {
            continue;
        }
        let score = text_match_score(&link.raw_target, needle).unwrap_or(0)
            + text_match_score(&link.source_path, needle).unwrap_or(0)
            + link
                .resolved_path
                .as_deref()
                .and_then(|path| text_match_score(path, needle))
                .unwrap_or(0);
        if score == 0 {
            continue;
        }
        let score = 25 + score + canonical_path_boost(candidate_path);
        candidates.add(
            candidate_path,
            SearchKind::Graph,
            score,
            "graph-link".to_string(),
        );
        if let Some(file) = files.get(&link.source_file_id) {
            candidates.add(
                &file.normalized_path,
                SearchKind::Graph,
                10,
                "graph-source".to_string(),
            );
        }
        matches.push(graph_match(link, score));
    }
    matches.sort_by(compare_score_source);
    matches.truncate(limit as usize);
    Ok(matches)
}

fn search_bases(
    connection: &Connection,
    needle: &SearchNeedle,
    scope: Option<&str>,
    extensions: &HashSet<String>,
    limit: u32,
    candidates: &mut CandidateSet,
) -> Result<Vec<SearchBaseRowMatch>, VaultSearchError> {
    let bases = BasesRepository::list_with_paths(connection)
        .map_err(|source| VaultSearchError::Bases { source })?;
    let mut matches = Vec::new();
    for base in bases {
        let document = match tao_sdk_bases::decode_base_config_json(&base.config_json) {
            Ok(document) => document,
            Err(_) => continue,
        };
        if let Some(score) = text_match_score(&base.file_path, needle) {
            candidates.add(
                &base.file_path,
                SearchKind::Bases,
                25 + score,
                "base-file".to_string(),
            );
        }
        let registry = match BaseViewRegistry::from_document(&document) {
            Ok(registry) => registry,
            Err(_) => continue,
        };
        for view in document.views {
            let plan = match BaseTableQueryPlanner.compile(
                &registry,
                &tao_sdk_bases::TableQueryPlanRequest {
                    view_name: view.name.clone(),
                    page: 1,
                    page_size: limit.clamp(50, 200),
                },
            ) {
                Ok(plan) => plan,
                Err(_) => continue,
            };
            let page = BaseTableExecutorService
                .execute_with_options(
                    connection,
                    &plan,
                    BaseTableExecutionOptions {
                        include_summaries: false,
                        coercion_mode: BaseCoercionMode::Permissive,
                    },
                )
                .map_err(|source| VaultSearchError::BaseExecute {
                    source: Box::new(source),
                })?;
            for row in page.rows {
                if !path_allowed(&row.file_path, scope, extensions) {
                    continue;
                }
                let values_text = serde_json::to_string(&row.values).unwrap_or_default();
                let score = text_match_score(&row.file_path, needle).unwrap_or(0)
                    + text_match_score(&values_text, needle).unwrap_or(0);
                if score == 0 {
                    continue;
                }
                let score = 35 + score + canonical_path_boost(&row.file_path);
                candidates.add(
                    &row.file_path,
                    SearchKind::Bases,
                    score,
                    "base-row".to_string(),
                );
                matches.push(SearchBaseRowMatch {
                    base_id: base.base_id.clone(),
                    base_path: base.file_path.clone(),
                    view_name: view.name.clone(),
                    file_id: row.file_id,
                    path: row.file_path,
                    values: row.values,
                    score,
                });
            }
        }
    }
    matches.sort_by(compare_score_path);
    matches.truncate(limit as usize);
    Ok(matches)
}

#[allow(clippy::too_many_arguments)]
fn build_context(
    connection: &Connection,
    request: &VaultSearchRequest,
    root_path: Option<&str>,
    candidates: &[SearchCandidate],
    file_by_path: &HashMap<String, tao_sdk_storage::FileRecord>,
    file_by_id: &HashMap<String, tao_sdk_storage::FileRecord>,
    link_counts: &HashMap<String, LinkCount>,
    base_rows: &[SearchBaseRowMatch],
    depth: u32,
    limit: u32,
) -> Result<SearchContext, VaultSearchError> {
    let selected_path = root_path
        .map(ToString::to_string)
        .or_else(|| candidates.first().map(|candidate| candidate.path.clone()));
    let Some(selected_path) = selected_path else {
        return Ok(SearchContext::default());
    };
    let Some(file) = file_by_path.get(&selected_path) else {
        return Ok(SearchContext {
            ambiguity: candidates.iter().take(3).cloned().collect(),
            ..SearchContext::default()
        });
    };

    let root = root_for_file(
        &request.vault_root,
        file,
        request.include_content,
        request.include_pii,
    );
    let properties = root_properties(connection, &file.file_id, request.include_pii)?;
    let outgoing = BacklinkGraphService
        .outgoing_for_path(connection, &selected_path)
        .map_err(|source| VaultSearchError::Graph { source })?
        .into_iter()
        .take(limit as usize)
        .map(|edge| graph_match_from_edge(edge, 0))
        .collect::<Vec<_>>();
    let incoming = BacklinkGraphService
        .backlinks_for_path(connection, &selected_path)
        .map_err(|source| VaultSearchError::Graph { source })?
        .into_iter()
        .take(limit as usize)
        .map(|edge| graph_match_from_edge(edge, 0))
        .collect::<Vec<_>>();

    let walk_steps = BacklinkGraphService
        .walk(
            connection,
            &GraphWalkRequest {
                path: selected_path.clone(),
                depth,
                limit: limit.saturating_mul(10).min(200),
                include_unresolved: true,
                include_folders: false,
            },
        )
        .map_err(|source| VaultSearchError::Graph { source })?
        .into_iter()
        .map(|step| SearchWalkStep {
            depth: step.depth,
            direction: match step.direction {
                GraphWalkDirection::Outgoing => "outgoing".to_string(),
                GraphWalkDirection::Incoming => "incoming".to_string(),
            },
            edge_type: match step.edge_type {
                GraphWalkEdgeType::Wikilink => "wikilink".to_string(),
                GraphWalkEdgeType::FolderParent => "folder_parent".to_string(),
                GraphWalkEdgeType::FolderSibling => "folder_sibling".to_string(),
            },
            source_path: step.source_path,
            target_path: step.target_path,
            raw_target: step.raw_target,
            resolved: step.resolved,
        })
        .collect::<Vec<_>>();

    let mut related_paths = Vec::<String>::new();
    for item in outgoing.iter().chain(incoming.iter()) {
        related_paths.push(item.source_path.clone());
        if let Some(path) = &item.target_path {
            related_paths.push(path.clone());
        }
    }
    for step in &walk_steps {
        related_paths.push(step.source_path.clone());
        if let Some(path) = &step.target_path {
            related_paths.push(path.clone());
        }
    }
    related_paths.sort();
    related_paths.dedup();

    let attachments = related_paths
        .iter()
        .filter_map(|path| file_by_path.get(path))
        .filter(|file| !file.is_markdown)
        .take(limit as usize)
        .map(|file| file_match_from_record(file, link_counts, Vec::new(), 0))
        .collect::<Vec<_>>();
    let grouped = grouped_paths(&related_paths);
    let related_counts = grouped
        .iter()
        .map(|(key, value)| {
            (
                key.clone(),
                JsonValue::Number(serde_json::Number::from(
                    value.as_array().map_or(0, Vec::len) as u64,
                )),
            )
        })
        .collect::<JsonMap<_, _>>();
    let timeline = timeline_entries(&selected_path, &related_paths, &properties, limit);
    let base_rows = base_rows
        .iter()
        .filter(|row| row.path == selected_path)
        .take(limit as usize)
        .cloned()
        .collect::<Vec<_>>();
    let ambiguity = if root_path.is_none() {
        ambiguous_candidates(candidates)
    } else {
        Vec::new()
    };

    let _ = file_by_id;
    Ok(SearchContext {
        root: Some(root),
        properties,
        base_rows,
        links: SearchContextLinks {
            outgoing,
            incoming,
            grouped,
        },
        walk: walk_steps,
        timeline,
        attachments,
        related_counts,
        ambiguity,
    })
}

fn root_for_file(
    vault_root: &Path,
    file: &tao_sdk_storage::FileRecord,
    include_content: bool,
    include_pii: bool,
) -> SearchRoot {
    let mut front_matter = None;
    let mut body_excerpt = None;
    if file.is_markdown
        && let Ok(raw) = fs::read_to_string(vault_root.join(&file.normalized_path))
        && let Ok(parsed) = MarkdownParser.parse(MarkdownParseRequest {
            normalized_path: file.normalized_path.clone(),
            raw,
        })
    {
        front_matter = if include_pii {
            parsed.front_matter
        } else {
            parsed.front_matter.map(|_| "redacted".to_string())
        };
        if include_content {
            body_excerpt = Some(bound_excerpt(&parsed.body));
        }
    }
    SearchRoot {
        file_id: file.file_id.clone(),
        path: file.normalized_path.clone(),
        title: note_title_from_path(&file.normalized_path),
        is_markdown: file.is_markdown,
        extension: extension_for_path(&file.normalized_path),
        size: file.size_bytes,
        modified_unix_ms: file.modified_unix_ms,
        indexed_at: file.indexed_at.clone(),
        front_matter,
        body_excerpt,
    }
}

fn root_properties(
    connection: &Connection,
    file_id: &str,
    include_pii: bool,
) -> Result<Vec<SearchPropertyMatch>, VaultSearchError> {
    let rows = PropertiesRepository::list_for_file_with_path(connection, file_id)
        .map_err(|source| VaultSearchError::Properties { source })?;
    Ok(rows
        .into_iter()
        .map(|row| SearchPropertyMatch {
            property_id: row.property_id,
            file_id: row.file_id,
            path: row.file_path,
            key: row.key,
            value_type: row.value_type,
            value: pii_value(&row.value_json, include_pii),
            updated_at: row.updated_at,
            score: 0,
        })
        .collect())
}

fn link_counts(connection: &Connection) -> Result<HashMap<String, LinkCount>, VaultSearchError> {
    let links = LinksRepository::list_all_with_paths(connection)
        .map_err(|source| VaultSearchError::Links { source })?;
    let mut counts = HashMap::<String, LinkCount>::new();
    for link in links {
        if !link.is_unresolved {
            counts.entry(link.source_path.clone()).or_default().outgoing += 1;
            if let Some(path) = link.resolved_path {
                counts.entry(path).or_default().inbound += 1;
            }
        }
    }
    Ok(counts)
}

fn graph_match(link: LinkWithPaths, score: i64) -> SearchGraphMatch {
    SearchGraphMatch {
        link_id: link.link_id,
        source_path: link.source_path,
        target_path: link.resolved_path,
        raw_target: link.raw_target,
        source_field: link.source_field,
        resolved: !link.is_unresolved,
        unresolved_reason: link.unresolved_reason,
        score,
    }
}

fn graph_match_from_edge(edge: crate::LinkGraphEdge, score: i64) -> SearchGraphMatch {
    SearchGraphMatch {
        link_id: edge.link_id,
        source_path: edge.source_path,
        target_path: edge.resolved_path,
        raw_target: edge.raw_target,
        source_field: edge.source_field,
        resolved: !edge.is_unresolved,
        unresolved_reason: edge.unresolved_reason,
        score,
    }
}

fn file_match_from_record(
    file: &tao_sdk_storage::FileRecord,
    link_counts: &HashMap<String, LinkCount>,
    matched_in: Vec<String>,
    score: i64,
) -> SearchFileMatch {
    let counts = link_counts
        .get(&file.normalized_path)
        .copied()
        .unwrap_or_default();
    SearchFileMatch {
        file_id: file.file_id.clone(),
        path: file.normalized_path.clone(),
        extension: extension_for_path(&file.normalized_path),
        size: file.size_bytes,
        modified_unix_ms: file.modified_unix_ms,
        indexed_at: file.indexed_at.clone(),
        is_markdown: file.is_markdown,
        inbound_links: counts.inbound,
        outgoing_links: counts.outgoing,
        linked: counts.inbound > 0,
        matched_in,
        score,
    }
}

fn normalize_path_input(raw: &str) -> Result<String, VaultSearchError> {
    let normalized = raw.trim().trim_matches('/').replace('\\', "/");
    if normalized.is_empty() || normalized.starts_with("../") || normalized.contains("/../") {
        return Err(VaultSearchError::InvalidRequest(format!(
            "invalid --path '{raw}': expected vault-relative path"
        )));
    }
    Ok(normalized)
}

fn normalize_scope(raw: &str) -> Result<String, VaultSearchError> {
    let normalized = raw.trim().trim_matches('/').replace('\\', "/");
    if normalized == "." {
        return Ok(String::new());
    }
    if normalized.starts_with("../") || normalized.contains("/../") {
        return Err(VaultSearchError::InvalidRequest(format!(
            "invalid --scope '{raw}': expected vault-relative prefix"
        )));
    }
    Ok(normalized)
}

fn normalize_extensions(raw: &[String]) -> HashSet<String> {
    raw.iter()
        .flat_map(|entry| entry.split(','))
        .map(str::trim)
        .map(|entry| entry.trim_start_matches('.').to_ascii_lowercase())
        .filter(|entry| !entry.is_empty())
        .collect()
}

fn path_allowed(path: &str, scope: Option<&str>, extensions: &HashSet<String>) -> bool {
    if let Some(scope) = scope
        && !scope.is_empty()
        && path != scope
        && !path.starts_with(&format!("{scope}/"))
    {
        return false;
    }
    extensions.is_empty() || extensions.contains(&extension_for_path(path))
}

fn extension_for_path(path: &str) -> String {
    Path::new(path)
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase()
}

fn normalize_text(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut last_space = true;
    for ch in value.chars().flat_map(char::to_lowercase) {
        if ch.is_alphanumeric() {
            out.push(ch);
            last_space = false;
        } else if !last_space {
            out.push(' ');
            last_space = true;
        }
    }
    out.trim().to_string()
}

fn compact_text(value: &str) -> String {
    value
        .chars()
        .flat_map(char::to_lowercase)
        .filter(|ch| ch.is_alphanumeric())
        .collect()
}

fn text_match_score(value: &str, needle: &SearchNeedle) -> Option<i64> {
    if needle.compact.is_empty() {
        return None;
    }
    let normalized = normalize_text(value);
    let compact = compact_text(value);
    if compact == needle.compact {
        return Some(80);
    }
    if normalized == needle.normalized {
        return Some(70);
    }
    if compact.contains(&needle.compact) {
        return Some(45);
    }
    if !needle.tokens.is_empty()
        && needle
            .tokens
            .iter()
            .all(|token| normalized.contains(token) || compact.contains(token))
    {
        return Some(30);
    }
    None
}

fn score_for_match(path: &str, matched_in: &[String], needle: &SearchNeedle, base: i64) -> i64 {
    let mut score = base + canonical_path_boost(path);
    if matched_in.iter().any(|item| item == "title") {
        score += 45;
    }
    if matched_in.iter().any(|item| item == "path") {
        score += 35;
    }
    if matched_in.iter().any(|item| item == "content") {
        score += 10;
    }
    if let Some(path_score) = text_match_score(path, needle) {
        score += path_score;
    }
    score
}

fn canonical_path_boost(path: &str) -> i64 {
    let lower = path.to_ascii_lowercase();
    let mut boost = 0;
    if lower.contains("contacts") || lower.contains("-con-") {
        boost += 30;
    }
    if lower.contains("companies") || lower.contains("-com-") {
        boost += 25;
    }
    if lower.contains("meetings") || lower.contains("communications") {
        boost += 15;
    }
    if lower.contains("index") || lower.contains("contents") || lower.contains("hub") {
        boost -= 20;
    }
    boost
}

fn pii_value(raw: &str, include_pii: bool) -> JsonValue {
    if !include_pii {
        return JsonValue::String("redacted".to_string());
    }
    serde_json::from_str(raw).unwrap_or_else(|_| JsonValue::String(raw.to_string()))
}

fn excerpt_for(content: &str, needle: &SearchNeedle) -> Option<String> {
    let lower = content.to_ascii_lowercase();
    let index = lower
        .find(&needle.raw.to_ascii_lowercase())
        .or_else(|| lower.find(&needle.compact))
        .or_else(|| needle.tokens.first().and_then(|token| lower.find(token)))?;
    let start = index.saturating_sub(120);
    let end = (index + 240).min(content.len());
    Some(content[start..end].replace('\n', " "))
}

fn bound_excerpt(content: &str) -> String {
    content
        .chars()
        .take(800)
        .collect::<String>()
        .replace('\n', " ")
}

fn merge_strings(target: &mut Vec<String>, source: &[String]) {
    for item in source {
        if !target.contains(item) {
            target.push(item.clone());
        }
    }
    target.sort();
}

fn sort_candidates(rows: &mut [SearchCandidate]) {
    rows.sort_by(|left, right| {
        right
            .score
            .cmp(&left.score)
            .then_with(|| right.inbound_links.cmp(&left.inbound_links))
            .then_with(|| left.path.cmp(&right.path))
    });
}

fn compare_score_path<T: SearchPathScore>(left: &T, right: &T) -> Ordering {
    right
        .score()
        .cmp(&left.score())
        .then_with(|| left.path().cmp(right.path()))
}

fn compare_score_source(left: &SearchGraphMatch, right: &SearchGraphMatch) -> Ordering {
    right
        .score
        .cmp(&left.score)
        .then_with(|| left.source_path.cmp(&right.source_path))
        .then_with(|| left.raw_target.cmp(&right.raw_target))
}

trait SearchPathScore {
    fn path(&self) -> &str;
    fn score(&self) -> i64;
}

impl SearchPathScore for SearchDocMatch {
    fn path(&self) -> &str {
        &self.path
    }

    fn score(&self) -> i64 {
        self.score
    }
}

impl SearchPathScore for SearchFileMatch {
    fn path(&self) -> &str {
        &self.path
    }

    fn score(&self) -> i64 {
        self.score
    }
}

impl SearchPathScore for SearchPropertyMatch {
    fn path(&self) -> &str {
        &self.path
    }

    fn score(&self) -> i64 {
        self.score
    }
}

impl SearchPathScore for SearchTaskMatch {
    fn path(&self) -> &str {
        &self.path
    }

    fn score(&self) -> i64 {
        self.score
    }
}

impl SearchPathScore for SearchBaseRowMatch {
    fn path(&self) -> &str {
        &self.path
    }

    fn score(&self) -> i64 {
        self.score
    }
}

fn grouped_paths(paths: &[String]) -> JsonMap<String, JsonValue> {
    let mut groups = HashMap::<String, Vec<String>>::new();
    for path in paths {
        groups
            .entry(group_for_path(path).to_string())
            .or_default()
            .push(path.clone());
    }
    let mut out = JsonMap::new();
    for (key, mut values) in groups {
        values.sort();
        values.dedup();
        out.insert(key, json!(values));
    }
    out
}

fn group_for_path(path: &str) -> &'static str {
    let lower = path.to_ascii_lowercase();
    if !lower.ends_with(".md") {
        "attachments"
    } else if lower.contains("contacts") {
        "contacts"
    } else if lower.contains("companies") {
        "companies"
    } else if lower.contains("meetings") {
        "meetings"
    } else if lower.contains("communications") {
        "communications"
    } else if lower.contains("notebook") {
        "notebook"
    } else if lower.contains("briefings") {
        "briefings"
    } else if lower.contains("tasks") || lower.contains("todo") {
        "tasks"
    } else {
        "notes"
    }
}

fn timeline_entries(
    root_path: &str,
    related_paths: &[String],
    properties: &[SearchPropertyMatch],
    limit: u32,
) -> Vec<SearchTimelineEntry> {
    let mut entries = Vec::<SearchTimelineEntry>::new();
    if let Some(date) = infer_date(root_path) {
        entries.push(SearchTimelineEntry {
            date,
            kind: "path".to_string(),
            path: root_path.to_string(),
            label: note_title_from_path(root_path),
        });
    }
    for path in related_paths {
        if let Some(date) = infer_date(path) {
            entries.push(SearchTimelineEntry {
                date,
                kind: group_for_path(path).to_string(),
                path: path.clone(),
                label: note_title_from_path(path),
            });
        }
    }
    for property in properties {
        if property.key.to_ascii_lowercase().contains("date")
            && let Some(raw) = property.value.as_str()
            && let Some(date) = infer_date(raw)
        {
            entries.push(SearchTimelineEntry {
                date,
                kind: format!("property:{}", property.key),
                path: property.path.clone(),
                label: property.key.clone(),
            });
        }
    }
    entries.sort_by(|left, right| {
        left.date
            .cmp(&right.date)
            .then_with(|| left.path.cmp(&right.path))
            .then_with(|| left.kind.cmp(&right.kind))
    });
    entries.dedup_by(|left, right| {
        left.date == right.date && left.path == right.path && left.kind == right.kind
    });
    entries.truncate(limit as usize);
    entries
}

fn infer_date(value: &str) -> Option<String> {
    let bytes = value.as_bytes();
    if bytes.len() < 10 {
        return None;
    }
    for index in 0..=(bytes.len() - 10) {
        let year = &value[index..index + 4];
        let sep1 = bytes[index + 4] as char;
        let month = &value[index + 5..index + 7];
        let sep2 = bytes[index + 7] as char;
        let day = &value[index + 8..index + 10];
        if year.chars().all(|ch| ch.is_ascii_digit())
            && month.chars().all(|ch| ch.is_ascii_digit())
            && day.chars().all(|ch| ch.is_ascii_digit())
            && matches!(sep1, '-' | '_')
            && matches!(sep2, '-' | '_')
        {
            return Some(format!("{year}-{month}-{day}"));
        }
    }
    None
}

fn ambiguous_candidates(candidates: &[SearchCandidate]) -> Vec<SearchCandidate> {
    let Some(top) = candidates.first() else {
        return Vec::new();
    };
    candidates
        .iter()
        .skip(1)
        .filter(|candidate| top.score.saturating_sub(candidate.score) <= 10)
        .take(4)
        .cloned()
        .collect()
}

/// Search operation failures.
#[derive(Debug, Error)]
pub enum VaultSearchError {
    /// Search kind was invalid.
    #[error(
        "unsupported search kind '{value}'; expected auto|all|docs|files|bases|properties|tasks|graph"
    )]
    InvalidKind {
        /// Invalid value.
        value: String,
    },
    /// Request shape was invalid.
    #[error("{0}")]
    InvalidRequest(String),
    /// File repository failed.
    #[error("files repository search failed: {source}")]
    Files {
        /// Source error.
        #[source]
        source: tao_sdk_storage::FilesRepositoryError,
    },
    /// Search index repository failed.
    #[error("search index query failed: {source}")]
    SearchIndex {
        /// Source error.
        #[source]
        source: tao_sdk_storage::SearchIndexRepositoryError,
    },
    /// FTS docs query failed.
    #[error("docs search failed: {source}")]
    Docs {
        /// Source error.
        #[source]
        source: tao_sdk_search::SearchQueryError,
    },
    /// Property repository failed.
    #[error("properties query failed: {source}")]
    Properties {
        /// Source error.
        #[source]
        source: tao_sdk_storage::PropertiesRepositoryError,
    },
    /// Link repository failed.
    #[error("links query failed: {source}")]
    Links {
        /// Source error.
        #[source]
        source: tao_sdk_storage::LinksRepositoryError,
    },
    /// Base repository failed.
    #[error("bases query failed: {source}")]
    Bases {
        /// Source error.
        #[source]
        source: tao_sdk_storage::BasesRepositoryError,
    },
    /// Base execution failed.
    #[error("base execution failed: {source}")]
    BaseExecute {
        /// Source error.
        #[source]
        source: Box<crate::BaseTableExecutorError>,
    },
    /// Graph service failed.
    #[error("graph context failed: {source}")]
    Graph {
        /// Source error.
        #[source]
        source: crate::LinkGraphServiceError,
    },
    /// Raw SQL failed.
    #[error("search SQL operation '{operation}' failed: {source}")]
    Sql {
        /// Operation name.
        operation: &'static str,
        /// Source error.
        #[source]
        source: rusqlite::Error,
    },
}
