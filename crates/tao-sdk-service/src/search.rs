//! High-level graph-aware vault search orchestration.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use rusqlite::{Connection, params_from_iter, types::Value};
use serde::Serialize;
use serde_json::{Map as JsonMap, Value as JsonValue, json};
use tao_sdk_core::note_title_from_path;
use tao_sdk_markdown::{MarkdownParseRequest, MarkdownParser};
use tao_sdk_storage::{
    FilesRepository, PropertiesRepository, SearchAliasRepository, SearchSegmentMatch,
    SearchSegmentQuery, SearchSegmentRepository,
};
use tao_sdk_vault::CasePolicy;
use thiserror::Error;

use crate::{BacklinkGraphService, GraphWalkDirection, GraphWalkEdgeType, GraphWalkRequest};

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
    /// Execute one graph-aware vault search over indexed state, repairing stale search corpus state.
    pub fn search(
        &self,
        connection: &mut Connection,
        request: VaultSearchRequest,
        case_policy: CasePolicy,
    ) -> Result<VaultSearchResult, VaultSearchError> {
        let search_status = crate::SearchCorpusService
            .status(connection)
            .map_err(|source| VaultSearchError::SearchCorpus {
                source: Box::new(source),
            })?;
        if search_status.search_index_stale {
            crate::SearchCorpusService
                .rebuild_atomic(connection, case_policy)
                .map_err(|source| VaultSearchError::SearchCorpus {
                    source: Box::new(source),
                })?;
        }
        self.search_current(connection, request)
    }

    /// Execute one graph-aware vault search over already-fresh indexed state.
    pub fn search_current(
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

        let mut candidates = CandidateSet::new();
        if let Some(path) = &root_path
            && let Some(file) = FilesRepository::get_by_normalized_path(connection, path)
                .map_err(|source| VaultSearchError::Files { source })?
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
        let mut indexed_total = None::<u64>;

        if let Some(needle) = needle.as_ref() {
            let indexed = search_indexed_corpus(
                connection,
                &request.vault_root,
                needle,
                request.kind,
                scope.as_deref(),
                &extensions,
                limit,
                request.include_content,
                request.include_pii,
                &mut candidates,
            )?;
            docs = indexed.docs;
            file_matches = indexed.files;
            properties = indexed.properties;
            tasks = indexed.tasks;
            graph = indexed.graph;
            base_rows = indexed.base_rows;
            indexed_total = Some(indexed.total);
        }

        let candidate_paths = candidates.paths();
        let candidate_files = files_by_paths(connection, &candidate_paths)?;
        let link_counts = link_counts_for_paths(connection, &candidate_paths)?;
        let mut candidate_rows = candidates.finish(&candidate_files, &link_counts);
        sort_candidates(&mut candidate_rows);
        let total = indexed_total.unwrap_or(candidate_rows.len() as u64);
        candidate_rows.truncate(limit as usize);

        let context = if request.include_context || root_path.is_some() {
            build_context(
                connection,
                &request,
                root_path.as_deref(),
                &candidate_rows,
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
}

impl CandidateSet {
    fn new() -> Self {
        Self::default()
    }

    fn add(&mut self, path: &str, kind: SearchKind, score: i64, reason: String) {
        let entry = self
            .rows
            .entry(path.to_string())
            .or_insert_with(|| CandidateAccumulator {
                path: path.to_string(),
                score: canonical_entity_path_boost(path),
                kinds: HashSet::new(),
                reasons: HashSet::new(),
            });
        entry.score += score;
        entry.kinds.insert(kind.label().to_string());
        entry.reasons.insert(reason);
    }

    fn paths(&self) -> Vec<String> {
        let mut paths = self.rows.keys().cloned().collect::<Vec<_>>();
        paths.sort();
        paths
    }

    fn finish(
        self,
        files: &HashMap<String, tao_sdk_storage::FileRecord>,
        link_counts: &HashMap<String, LinkCount>,
    ) -> Vec<SearchCandidate> {
        self.rows
            .into_values()
            .map(|row| {
                let file = files.get(&row.path);
                let counts = link_counts.get(&row.path).copied().unwrap_or_default();
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

#[derive(Debug, Default)]
struct IndexedSearchResult {
    docs: Vec<SearchDocMatch>,
    files: Vec<SearchFileMatch>,
    properties: Vec<SearchPropertyMatch>,
    tasks: Vec<SearchTaskMatch>,
    graph: Vec<SearchGraphMatch>,
    base_rows: Vec<SearchBaseRowMatch>,
    total: u64,
}

#[derive(Debug, Clone)]
struct PendingFileMatch {
    file_id: String,
    path: String,
    extension: String,
    size: u64,
    modified_unix_ms: i64,
    indexed_at: String,
    is_markdown: bool,
    matched_in: Vec<String>,
    score: i64,
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
fn search_indexed_corpus(
    connection: &Connection,
    vault_root: &Path,
    needle: &SearchNeedle,
    kind: SearchKind,
    scope: Option<&str>,
    extensions: &HashSet<String>,
    limit: u32,
    include_content: bool,
    include_pii: bool,
    candidates: &mut CandidateSet,
) -> Result<IndexedSearchResult, VaultSearchError> {
    let surfaces = surfaces_for_kind(kind);
    let extension_filters = sorted_extensions(extensions);
    let query_limit = indexed_candidate_window(limit);
    let fts_query = tao_sdk_search::parser::build_fts_query(&needle.normalized);
    let segment_query = SearchSegmentQuery {
        fts_query,
        surfaces: surfaces.clone(),
        scope: scope.map(ToString::to_string),
        extensions: extension_filters.clone(),
        limit: query_limit,
    };

    let alias_matches = SearchAliasRepository::query(
        connection,
        &needle.normalized,
        &needle.compact,
        &surfaces,
        scope,
        &extension_filters,
        query_limit,
    )
    .map_err(|source| VaultSearchError::SearchAliases { source })?;

    let segment_matches = SearchSegmentRepository::query(connection, &segment_query)
        .map_err(|source| VaultSearchError::SearchSegments { source })?;

    let mut total = SearchSegmentRepository::count_distinct_paths(connection, &segment_query)
        .map_err(|source| VaultSearchError::SearchSegments { source })?;
    let alias_paths = SearchAliasRepository::distinct_paths(
        connection,
        &needle.normalized,
        &needle.compact,
        &surfaces,
        scope,
        &extension_filters,
    )
    .map_err(|source| VaultSearchError::SearchAliases { source })?;
    if !alias_paths.is_empty() {
        let alias_overlap = SearchSegmentRepository::count_matching_paths_subset(
            connection,
            &segment_query,
            &alias_paths,
        )
        .map_err(|source| VaultSearchError::SearchSegments { source })?;
        total = total.saturating_add(
            u64::try_from(alias_paths.len())
                .unwrap_or(u64::MAX)
                .saturating_sub(alias_overlap),
        );
    }

    for alias in alias_matches {
        if !path_allowed(&alias.normalized_path, scope, extensions) {
            continue;
        }
        if let Some(surface_kind) = kind_for_surface(&alias.surface) {
            candidates.add(
                &alias.normalized_path,
                surface_kind,
                120 + alias.weight + canonical_entity_path_boost(&alias.normalized_path),
                format!("alias:{}", alias.source),
            );
        }
    }

    let mut docs = HashMap::<String, SearchDocMatch>::new();
    let mut files = HashMap::<String, PendingFileMatch>::new();
    let mut properties = HashMap::<String, SearchPropertyMatch>::new();
    let mut tasks = HashMap::<String, SearchTaskMatch>::new();
    let mut graph = HashMap::<String, SearchGraphMatch>::new();
    let mut base_rows = HashMap::<String, SearchBaseRowMatch>::new();

    for segment in segment_matches {
        if !path_allowed(&segment.normalized_path, scope, extensions) {
            continue;
        }
        let Some(surface_kind) = kind_for_surface(&segment.surface) else {
            continue;
        };
        let payload = segment_payload(&segment)?;
        let matched_in = matched_in_for_segment(&segment, &payload, needle);
        let score = indexed_segment_score(&segment, &payload, needle);
        candidates.add(
            &segment.normalized_path,
            surface_kind,
            score,
            format!("{}:{}", segment.surface, matched_in.join(",")),
        );

        match segment.surface.as_str() {
            "docs" => {
                let path = payload_string(&payload, "path")
                    .unwrap_or_else(|| segment.normalized_path.clone());
                let row = SearchDocMatch {
                    file_id: payload_string(&payload, "file_id")
                        .unwrap_or_else(|| segment.file_id.clone()),
                    title: payload_string(&payload, "title")
                        .unwrap_or_else(|| note_title_from_path(&path)),
                    indexed_at: payload_string(&payload, "indexed_at")
                        .unwrap_or_else(|| segment.updated_at.clone()),
                    excerpt: if include_content {
                        excerpt_for_file(vault_root, &path, needle)
                    } else {
                        None
                    },
                    path: path.clone(),
                    matched_in,
                    score,
                };
                upsert_best(&mut docs, path, row);
            }
            "files" => {
                let path = payload_string(&payload, "path")
                    .unwrap_or_else(|| segment.normalized_path.clone());
                let row = PendingFileMatch {
                    file_id: payload_string(&payload, "file_id")
                        .unwrap_or_else(|| segment.file_id.clone()),
                    extension: payload_string(&payload, "extension")
                        .unwrap_or_else(|| segment.extension.clone()),
                    size: payload_u64(&payload, "size").unwrap_or_default(),
                    modified_unix_ms: payload_i64(&payload, "modified_unix_ms").unwrap_or_default(),
                    indexed_at: payload_string(&payload, "indexed_at")
                        .unwrap_or_else(|| segment.updated_at.clone()),
                    is_markdown: payload_bool(&payload, "is_markdown").unwrap_or(false),
                    path: path.clone(),
                    matched_in,
                    score,
                };
                upsert_best(&mut files, path, row);
            }
            "properties" => {
                let property_id = payload_string(&payload, "property_id")
                    .or_else(|| segment.record_id.clone())
                    .unwrap_or_else(|| segment.segment_id.clone());
                let value_json = payload_string(&payload, "value_json").unwrap_or_default();
                let row = SearchPropertyMatch {
                    property_id: property_id.clone(),
                    file_id: payload_string(&payload, "file_id")
                        .unwrap_or_else(|| segment.file_id.clone()),
                    path: payload_string(&payload, "path")
                        .unwrap_or_else(|| segment.normalized_path.clone()),
                    key: payload_string(&payload, "key").unwrap_or_else(|| segment.field.clone()),
                    value_type: payload_string(&payload, "value_type").unwrap_or_default(),
                    value: pii_value(&value_json, include_pii),
                    updated_at: payload_string(&payload, "updated_at")
                        .unwrap_or_else(|| segment.updated_at.clone()),
                    score,
                };
                upsert_best(&mut properties, property_id, row);
            }
            "tasks" => {
                let task_id = payload_string(&payload, "task_id")
                    .or_else(|| segment.record_id.clone())
                    .unwrap_or_else(|| segment.segment_id.clone());
                let row = SearchTaskMatch {
                    task_id: task_id.clone(),
                    file_id: payload_string(&payload, "file_id")
                        .unwrap_or_else(|| segment.file_id.clone()),
                    path: payload_string(&payload, "path")
                        .unwrap_or_else(|| segment.normalized_path.clone()),
                    line: payload_i64(&payload, "line").unwrap_or_default(),
                    state: payload_string(&payload, "state").unwrap_or_default(),
                    text: payload_string(&payload, "text").unwrap_or_default(),
                    updated_at: payload_string(&payload, "updated_at")
                        .unwrap_or_else(|| segment.updated_at.clone()),
                    score,
                };
                upsert_best(&mut tasks, task_id, row);
            }
            "graph" => {
                let link_id = payload_string(&payload, "link_id")
                    .or_else(|| segment.record_id.clone())
                    .unwrap_or_else(|| segment.segment_id.clone());
                let row = SearchGraphMatch {
                    link_id: link_id.clone(),
                    source_path: payload_string(&payload, "source_path")
                        .unwrap_or_else(|| segment.normalized_path.clone()),
                    target_path: payload_string(&payload, "target_path"),
                    raw_target: payload_string(&payload, "raw_target").unwrap_or_default(),
                    source_field: payload_string(&payload, "source_field").unwrap_or_default(),
                    resolved: payload_bool(&payload, "resolved").unwrap_or(false),
                    unresolved_reason: payload_string(&payload, "unresolved_reason"),
                    score,
                };
                upsert_best_graph(&mut graph, link_id, row);
            }
            "bases" if segment.field == "base_row" => {
                let record_id = segment
                    .record_id
                    .clone()
                    .unwrap_or_else(|| segment.segment_id.clone());
                let values = payload
                    .get("values")
                    .and_then(JsonValue::as_object)
                    .cloned()
                    .unwrap_or_default();
                let row = SearchBaseRowMatch {
                    base_id: payload_string(&payload, "base_id").unwrap_or_default(),
                    base_path: payload_string(&payload, "base_path").unwrap_or_default(),
                    view_name: payload_string(&payload, "view_name").unwrap_or_default(),
                    file_id: payload_string(&payload, "file_id")
                        .unwrap_or_else(|| segment.file_id.clone()),
                    path: payload_string(&payload, "path")
                        .unwrap_or_else(|| segment.normalized_path.clone()),
                    values,
                    score,
                };
                upsert_best(&mut base_rows, record_id, row);
            }
            "bases" => {}
            _ => {}
        }
    }

    let file_paths = files.keys().cloned().collect::<Vec<_>>();
    let link_counts = link_counts_for_paths(connection, &file_paths)?;
    let mut docs = docs.into_values().collect::<Vec<_>>();
    let mut files = files
        .into_values()
        .map(|file| file_match_from_pending(file, &link_counts))
        .collect::<Vec<_>>();
    let mut properties = properties.into_values().collect::<Vec<_>>();
    let mut tasks = tasks.into_values().collect::<Vec<_>>();
    let mut graph = graph.into_values().collect::<Vec<_>>();
    let mut base_rows = base_rows.into_values().collect::<Vec<_>>();

    docs.sort_by(compare_score_path);
    files.sort_by(compare_score_path);
    properties.sort_by(compare_score_path);
    tasks.sort_by(compare_score_path);
    graph.sort_by(compare_score_source);
    base_rows.sort_by(compare_score_path);

    let limit_usize = limit as usize;
    docs.truncate(limit_usize);
    files.truncate(limit_usize);
    properties.truncate(limit_usize);
    tasks.truncate(limit_usize);
    graph.truncate(limit_usize);
    base_rows.truncate(limit_usize);

    Ok(IndexedSearchResult {
        docs,
        files,
        properties,
        tasks,
        graph,
        base_rows,
        total,
    })
}

fn base_rows_for_path(
    connection: &Connection,
    selected_path: &str,
    limit: u32,
) -> Result<Vec<SearchBaseRowMatch>, VaultSearchError> {
    let rows = SearchSegmentRepository::base_rows_for_path(connection, selected_path, limit)
        .map_err(|source| VaultSearchError::SearchSegments { source })?;
    rows.into_iter()
        .filter(|segment| segment.field == "base_row")
        .map(|segment| {
            let payload = segment_payload(&segment)?;
            Ok(SearchBaseRowMatch {
                base_id: payload_string(&payload, "base_id").unwrap_or_default(),
                base_path: payload_string(&payload, "base_path").unwrap_or_default(),
                view_name: payload_string(&payload, "view_name").unwrap_or_default(),
                file_id: payload_string(&payload, "file_id")
                    .unwrap_or_else(|| segment.file_id.clone()),
                path: payload_string(&payload, "path")
                    .unwrap_or_else(|| segment.normalized_path.clone()),
                values: payload
                    .get("values")
                    .and_then(JsonValue::as_object)
                    .cloned()
                    .unwrap_or_default(),
                score: 0,
            })
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn build_context(
    connection: &Connection,
    request: &VaultSearchRequest,
    root_path: Option<&str>,
    candidates: &[SearchCandidate],
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
    let Some(file) = FilesRepository::get_by_normalized_path(connection, &selected_path)
        .map_err(|source| VaultSearchError::Files { source })?
    else {
        return Ok(SearchContext {
            ambiguity: candidates.iter().take(3).cloned().collect(),
            ..SearchContext::default()
        });
    };

    let root = root_for_file(
        &request.vault_root,
        &file,
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

    let related_files = files_by_paths(connection, &related_paths)?;
    let mut context_link_counts = link_counts.clone();
    context_link_counts.extend(link_counts_for_paths(connection, &related_paths)?);
    let attachments = related_paths
        .iter()
        .filter_map(|path| related_files.get(path))
        .filter(|file| !file.is_markdown)
        .take(limit as usize)
        .map(|file| file_match_from_record(file, &context_link_counts, Vec::new(), 0))
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
    let mut context_base_rows = base_rows
        .iter()
        .filter(|row| row.path == selected_path)
        .take(limit as usize)
        .cloned()
        .collect::<Vec<_>>();
    if context_base_rows.is_empty() {
        context_base_rows = base_rows_for_path(connection, &selected_path, limit)?;
    }
    let ambiguity = if root_path.is_none() {
        ambiguous_candidates(candidates)
    } else {
        Vec::new()
    };

    Ok(SearchContext {
        root: Some(root),
        properties,
        base_rows: context_base_rows,
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

fn files_by_paths(
    connection: &Connection,
    paths: &[String],
) -> Result<HashMap<String, tao_sdk_storage::FileRecord>, VaultSearchError> {
    let mut unique_paths = paths.to_vec();
    unique_paths.sort();
    unique_paths.dedup();
    if unique_paths.is_empty() {
        return Ok(HashMap::new());
    }

    let placeholders = vec!["?"; unique_paths.len()].join(", ");
    let sql = format!(
        r#"
SELECT
  file_id,
  normalized_path,
  match_key,
  absolute_path,
  size_bytes,
  modified_unix_ms,
  hash_blake3,
  is_markdown,
  indexed_at
FROM files
WHERE normalized_path IN ({placeholders})
ORDER BY normalized_path ASC
"#
    );
    let mut statement = connection
        .prepare(&sql)
        .map_err(|source| VaultSearchError::Sql {
            operation: "prepare_files_by_paths",
            source,
        })?;
    let params = unique_paths
        .iter()
        .map(|path| Value::Text(path.clone()))
        .collect::<Vec<_>>();
    let rows = statement
        .query_map(params_from_iter(params.iter()), row_to_file_record)
        .map_err(|source| VaultSearchError::Sql {
            operation: "query_files_by_paths",
            source,
        })?;
    let mut files = HashMap::new();
    for row in rows {
        let file = row.map_err(|source| VaultSearchError::Sql {
            operation: "map_files_by_paths",
            source,
        })?;
        files.insert(file.normalized_path.clone(), file);
    }
    Ok(files)
}

fn link_counts_for_paths(
    connection: &Connection,
    paths: &[String],
) -> Result<HashMap<String, LinkCount>, VaultSearchError> {
    let mut unique_paths = paths.to_vec();
    unique_paths.sort();
    unique_paths.dedup();
    if unique_paths.is_empty() {
        return Ok(HashMap::new());
    }

    let placeholders = vec!["?"; unique_paths.len()].join(", ");
    let sql = format!(
        r#"
WITH selected AS (
  SELECT file_id, normalized_path
  FROM files
  WHERE normalized_path IN ({placeholders})
),
outgoing AS (
  SELECT source_file_id AS file_id, COUNT(*) AS outgoing
  FROM links
  WHERE is_unresolved = 0
    AND source_file_id IN (SELECT file_id FROM selected)
  GROUP BY source_file_id
),
incoming AS (
  SELECT resolved_file_id AS file_id, COUNT(*) AS incoming
  FROM links
  WHERE is_unresolved = 0
    AND resolved_file_id IN (SELECT file_id FROM selected)
  GROUP BY resolved_file_id
)
SELECT
  selected.normalized_path,
  COALESCE(incoming.incoming, 0) AS incoming,
  COALESCE(outgoing.outgoing, 0) AS outgoing
FROM selected
LEFT JOIN incoming ON incoming.file_id = selected.file_id
LEFT JOIN outgoing ON outgoing.file_id = selected.file_id
ORDER BY selected.normalized_path ASC
"#
    );
    let params = unique_paths
        .iter()
        .map(|path| Value::Text(path.clone()))
        .collect::<Vec<_>>();
    let mut statement = connection
        .prepare(&sql)
        .map_err(|source| VaultSearchError::Sql {
            operation: "prepare_link_counts_for_paths",
            source,
        })?;
    let rows = statement
        .query_map(params_from_iter(params.iter()), |row| {
            Ok((
                row.get::<_, String>("normalized_path")?,
                LinkCount {
                    inbound: row.get("incoming")?,
                    outgoing: row.get("outgoing")?,
                },
            ))
        })
        .map_err(|source| VaultSearchError::Sql {
            operation: "query_link_counts_for_paths",
            source,
        })?;
    let mut counts = HashMap::<String, LinkCount>::new();
    for row in rows {
        let (path, count) = row.map_err(|source| VaultSearchError::Sql {
            operation: "map_link_counts_for_paths",
            source,
        })?;
        counts.insert(path, count);
    }
    Ok(counts)
}

fn row_to_file_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<tao_sdk_storage::FileRecord> {
    Ok(tao_sdk_storage::FileRecord {
        file_id: row.get("file_id")?,
        normalized_path: row.get("normalized_path")?,
        match_key: row.get("match_key")?,
        absolute_path: row.get("absolute_path")?,
        size_bytes: row.get("size_bytes")?,
        modified_unix_ms: row.get("modified_unix_ms")?,
        hash_blake3: row.get("hash_blake3")?,
        is_markdown: row.get::<_, i64>("is_markdown")? != 0,
        indexed_at: row.get("indexed_at")?,
    })
}

fn surfaces_for_kind(kind: SearchKind) -> Vec<String> {
    match kind {
        SearchKind::Auto | SearchKind::All => Vec::new(),
        SearchKind::Docs => vec!["docs".to_string()],
        SearchKind::Files => vec!["files".to_string()],
        SearchKind::Bases => vec!["bases".to_string()],
        SearchKind::Properties => vec!["properties".to_string()],
        SearchKind::Tasks => vec!["tasks".to_string()],
        SearchKind::Graph => vec!["graph".to_string()],
    }
}

fn kind_for_surface(surface: &str) -> Option<SearchKind> {
    match surface {
        "docs" => Some(SearchKind::Docs),
        "files" => Some(SearchKind::Files),
        "bases" => Some(SearchKind::Bases),
        "properties" => Some(SearchKind::Properties),
        "tasks" => Some(SearchKind::Tasks),
        "graph" => Some(SearchKind::Graph),
        _ => None,
    }
}

fn sorted_extensions(extensions: &HashSet<String>) -> Vec<String> {
    let mut values = extensions.iter().cloned().collect::<Vec<_>>();
    values.sort();
    values
}

fn indexed_candidate_window(limit: u32) -> u32 {
    limit.saturating_mul(30).clamp(200, 5_000)
}

fn segment_payload(segment: &SearchSegmentMatch) -> Result<JsonValue, VaultSearchError> {
    serde_json::from_str(&segment.payload_json).map_err(|source| VaultSearchError::Payload {
        segment_id: segment.segment_id.clone(),
        source,
    })
}

fn indexed_segment_score(
    segment: &SearchSegmentMatch,
    payload: &JsonValue,
    needle: &SearchNeedle,
) -> i64 {
    let mut score = segment.weight + segment.rank_score + field_score(&segment.field);
    score += canonical_entity_path_boost(&segment.normalized_path);
    score += text_match_score(&segment.normalized_path, needle).unwrap_or(0);
    if let Some(title) = payload_string(payload, "title") {
        score += text_match_score(&title, needle).unwrap_or(0);
    }
    if let Some(path) = payload_string(payload, "path") {
        score += text_match_score(&path, needle).unwrap_or(0);
    }
    score
}

fn field_score(field: &str) -> i64 {
    match field {
        "document" => 20,
        "file" => 18,
        "base_row" => 16,
        "base" => 12,
        "link" => 8,
        "task" => 6,
        _ => 10,
    }
}

fn matched_in_for_segment(
    segment: &SearchSegmentMatch,
    payload: &JsonValue,
    needle: &SearchNeedle,
) -> Vec<String> {
    let mut matched = Vec::new();
    if text_match_score(&segment.normalized_path, needle).is_some() {
        matched.push("path".to_string());
    }
    if let Some(title) = payload_string(payload, "title")
        && text_match_score(&title, needle).is_some()
    {
        matched.push("title".to_string());
    }
    if matched.is_empty() {
        matched.push(
            match segment.surface.as_str() {
                "docs" => "content",
                "files" => "file",
                "properties" => "property",
                "tasks" => "task",
                "graph" => "link",
                "bases" if segment.field == "base_row" => "base_row",
                "bases" => "base",
                _ => segment.field.as_str(),
            }
            .to_string(),
        );
    }
    matched.sort();
    matched.dedup();
    matched
}

fn payload_string(payload: &JsonValue, key: &str) -> Option<String> {
    payload
        .get(key)
        .and_then(JsonValue::as_str)
        .map(ToString::to_string)
}

fn payload_i64(payload: &JsonValue, key: &str) -> Option<i64> {
    payload.get(key).and_then(JsonValue::as_i64)
}

fn payload_u64(payload: &JsonValue, key: &str) -> Option<u64> {
    payload.get(key).and_then(JsonValue::as_u64)
}

fn payload_bool(payload: &JsonValue, key: &str) -> Option<bool> {
    payload.get(key).and_then(JsonValue::as_bool)
}

fn upsert_best<T: SearchPathScore + Clone>(rows: &mut HashMap<String, T>, key: String, row: T) {
    rows.entry(key)
        .and_modify(|existing| {
            if row.score() > existing.score() {
                *existing = row.clone();
            }
        })
        .or_insert(row);
}

fn upsert_best_graph(
    rows: &mut HashMap<String, SearchGraphMatch>,
    key: String,
    row: SearchGraphMatch,
) {
    rows.entry(key)
        .and_modify(|existing| {
            if row.score > existing.score {
                *existing = row.clone();
            }
        })
        .or_insert(row);
}

fn file_match_from_pending(
    file: PendingFileMatch,
    link_counts: &HashMap<String, LinkCount>,
) -> SearchFileMatch {
    let counts = link_counts.get(&file.path).copied().unwrap_or_default();
    SearchFileMatch {
        file_id: file.file_id,
        path: file.path,
        extension: file.extension,
        size: file.size,
        modified_unix_ms: file.modified_unix_ms,
        indexed_at: file.indexed_at,
        is_markdown: file.is_markdown,
        inbound_links: counts.inbound,
        outgoing_links: counts.outgoing,
        linked: counts.inbound > 0,
        matched_in: file.matched_in,
        score: file.score,
    }
}

fn excerpt_for_file(vault_root: &Path, path: &str, needle: &SearchNeedle) -> Option<String> {
    let content = fs::read_to_string(vault_root.join(path)).ok()?;
    excerpt_for(&content, needle)
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

const SEARCH_CONTACT_PATH_BOOST: i64 = 30;
const SEARCH_COMPANY_PATH_BOOST: i64 = 25;
const SEARCH_INTERACTION_PATH_BOOST: i64 = 15;
const SEARCH_INDEX_PATH_PENALTY: i64 = -20;

/// Prefer canonical CRM entity notes over hub/index pages when relevance ties.
fn canonical_entity_path_boost(path: &str) -> i64 {
    let lower = path.to_ascii_lowercase();
    let mut boost = 0;
    if lower.contains("contacts") || lower.contains("-con-") {
        boost += SEARCH_CONTACT_PATH_BOOST;
    }
    if lower.contains("companies") || lower.contains("-com-") {
        boost += SEARCH_COMPANY_PATH_BOOST;
    }
    if lower.contains("meetings") || lower.contains("communications") {
        boost += SEARCH_INTERACTION_PATH_BOOST;
    }
    if lower.contains("index") || lower.contains("contents") || lower.contains("hub") {
        boost += SEARCH_INDEX_PATH_PENALTY;
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
    let start = previous_char_boundary(content, index.saturating_sub(120));
    let end = next_char_boundary(content, (index + 240).min(content.len()));
    Some(content[start..end].replace('\n', " "))
}

fn previous_char_boundary(content: &str, mut index: usize) -> usize {
    while index > 0 && !content.is_char_boundary(index) {
        index -= 1;
    }
    index
}

fn next_char_boundary(content: &str, mut index: usize) -> usize {
    while index < content.len() && !content.is_char_boundary(index) {
        index += 1;
    }
    index
}

fn bound_excerpt(content: &str) -> String {
    content
        .chars()
        .take(800)
        .collect::<String>()
        .replace('\n', " ")
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

impl SearchPathScore for PendingFileMatch {
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
    /// Unified search segment repository failed.
    #[error("search segment query failed: {source}")]
    SearchSegments {
        /// Source error.
        #[source]
        source: tao_sdk_storage::SearchSegmentRepositoryError,
    },
    /// Unified search alias repository failed.
    #[error("search alias query failed: {source}")]
    SearchAliases {
        /// Source error.
        #[source]
        source: tao_sdk_storage::SearchAliasRepositoryError,
    },
    /// Search corpus freshness check or repair failed.
    #[error("search corpus refresh failed: {source}")]
    SearchCorpus {
        /// Source error.
        #[source]
        source: Box<crate::SearchCorpusError>,
    },
    /// Stored search segment payload was invalid.
    #[error("search segment '{segment_id}' payload is invalid: {source}")]
    Payload {
        /// Segment id.
        segment_id: String,
        /// Source error.
        #[source]
        source: serde_json::Error,
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
