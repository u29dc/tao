//! High-level graph-aware vault search orchestration.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::path::Path;

use rusqlite::{Connection, params_from_iter, types::Value};
use serde_json::{Map as JsonMap, Value as JsonValue, json};
use tao_sdk_core::note_title_from_path;
use tao_sdk_storage::{
    ContentRepository, DocumentsRepository, FilesRepository, LinksRepository, PropertiesRepository,
    SearchAliasRepository, SearchSegmentCandidate, SearchSegmentMatch, SearchSegmentQuery,
    SearchSegmentRepository,
};
use tao_sdk_vault::CasePolicy;
use thiserror::Error;

use crate::{BacklinkGraphService, GraphWalkDirection, GraphWalkEdgeType, GraphWalkRequest};

mod types;
pub use types::*;
mod coverage;
use coverage::search_content_coverage;

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
        match self.search_current(connection, request.clone()) {
            Err(VaultSearchError::InvalidPayload { .. } | VaultSearchError::Payload { .. }) => {
                crate::SearchCorpusService
                    .rebuild_atomic(connection, case_policy)
                    .map_err(|source| VaultSearchError::SearchCorpus {
                        source: Box::new(source),
                    })?;
                self.search_current(connection, request)
            }
            result => result,
        }
    }

    /// Execute one graph-aware vault search over already-fresh indexed state.
    pub fn search_current(
        &self,
        connection: &Connection,
        request: VaultSearchRequest,
    ) -> Result<VaultSearchResult, VaultSearchError> {
        let transaction =
            if connection.is_autocommit() {
                Some(connection.unchecked_transaction().map_err(|source| {
                    VaultSearchError::Sql {
                        operation: "begin_search_snapshot",
                        source,
                    }
                })?)
            } else {
                None
            };
        let result = self.search_snapshot(connection, request);
        if let Some(transaction) = transaction {
            transaction
                .commit()
                .map_err(|source| VaultSearchError::Sql {
                    operation: "end_search_snapshot",
                    source,
                })?;
        }
        result
    }

    fn search_snapshot(
        &self,
        connection: &Connection,
        request: VaultSearchRequest,
    ) -> Result<VaultSearchResult, VaultSearchError> {
        validate_request(&request)?;
        let limit = request.limit;
        let depth = request.depth;
        let query = request
            .query
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToString::to_string);
        let needle = query.as_deref().map(SearchNeedle::new);
        let mut root_path = request
            .path
            .as_deref()
            .map(normalize_path_input)
            .transpose()?;
        let insensitive = indexed_case_policy(connection)? == CasePolicy::Insensitive;
        if insensitive && let Some(path) = &root_path {
            let key = tao_sdk_vault::path_match_key(path, CasePolicy::Insensitive);
            if let Some(file) = FilesRepository::get_by_match_key(connection, &key)
                .map_err(|source| VaultSearchError::Files { source })?
            {
                root_path = Some(file.normalized_path);
            }
        }
        let scope = request
            .scope
            .as_deref()
            .map(normalize_scope)
            .transpose()?
            .map(|scope| {
                if insensitive {
                    tao_sdk_vault::path_match_key(&scope, CasePolicy::Insensitive)
                } else {
                    scope
                }
            });
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
                insensitive,
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

        let content_coverage =
            search_content_coverage(connection, scope.as_deref(), insensitive, &extensions)?;
        let mut result = VaultSearchResult {
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
            content_truncated: false,
            content_coverage,
        };
        bound_result_content(&mut result);
        Ok(result)
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
                score: 0,
                kinds: HashSet::new(),
                reasons: HashSet::new(),
            });
        entry.score = entry.score.max(score);
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

#[derive(Debug, Clone)]
struct PendingSegmentMatch {
    segment: SearchSegmentCandidate,
    matched_in: Vec<String>,
    score: i64,
}

fn validate_request(request: &VaultSearchRequest) -> Result<(), VaultSearchError> {
    if request.limit == 0 || request.limit > 100 || request.depth > 4 {
        return Err(VaultSearchError::InvalidRequest(
            "search limit must be 1..100 and depth must be 0..4".to_string(),
        ));
    }
    if request
        .query
        .as_ref()
        .is_some_and(|query| query.len() > 4096)
        || request
            .extensions
            .iter()
            .flat_map(|entry| entry.split(','))
            .count()
            > 32
        || request.extensions.iter().any(|entry| entry.len() > 256)
    {
        return Err(VaultSearchError::InvalidRequest(
            "search query exceeds the 4096-byte or 32-extension bound".to_string(),
        ));
    }
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
    _vault_root: &Path,
    needle: &SearchNeedle,
    kind: SearchKind,
    scope: Option<&str>,
    scope_case_insensitive: bool,
    extensions: &HashSet<String>,
    limit: u32,
    include_content: bool,
    include_pii: bool,
    candidates: &mut CandidateSet,
) -> Result<IndexedSearchResult, VaultSearchError> {
    let rank_needle = needle.clone();
    connection
        .create_scalar_function(
            "tao_search_rank",
            6,
            rusqlite::functions::FunctionFlags::SQLITE_UTF8
                | rusqlite::functions::FunctionFlags::SQLITE_DETERMINISTIC,
            move |context| {
                let path = context.get::<String>(0)?;
                let label = context.get::<String>(1)?;
                let surface = context.get::<String>(2)?;
                let field = context.get::<String>(3)?;
                let weight = context.get::<i64>(4)?;
                let rank = context.get::<i64>(5)?;
                Ok(candidate_score(
                    &path,
                    &label,
                    &surface,
                    &field,
                    weight,
                    rank,
                    &rank_needle,
                ))
            },
        )
        .map_err(|source| VaultSearchError::Sql {
            operation: "register_search_ranker",
            source,
        })?;
    let surfaces = surfaces_for_kind(kind);
    let extension_filters = sorted_extensions(extensions);
    let query_limit = indexed_candidate_window(limit);
    let fts_query = tao_sdk_search::parser::build_fts_query(&needle.normalized);
    let segment_query = SearchSegmentQuery {
        fts_query,
        surfaces: surfaces.clone(),
        scope: scope.map(ToString::to_string),
        scope_case_insensitive,
        extensions: extension_filters.clone(),
        limit: query_limit,
    };

    let alias_matches = SearchAliasRepository::query(
        connection,
        &needle.normalized,
        &needle.compact,
        &surfaces,
        scope,
        scope_case_insensitive,
        &extension_filters,
        query_limit,
    )
    .map_err(|source| VaultSearchError::SearchAliases { source })?;

    let surface_windows = if surfaces.is_empty() {
        vec!["docs", "files", "properties", "tasks", "graph", "bases"]
    } else {
        surfaces.iter().map(String::as_str).collect()
    };
    let mut segment_candidates = Vec::new();
    let mut seen_segments = HashSet::new();
    for surface in surface_windows {
        let window = SearchSegmentQuery {
            surfaces: vec![surface.to_string()],
            limit: 100,
            ..segment_query.clone()
        };
        for segment in SearchSegmentRepository::query_scored_candidates(connection, &window, false)
            .map_err(|source| VaultSearchError::SearchSegments { source })?
            .into_iter()
            .chain(
                SearchSegmentRepository::query_scored_candidates(connection, &window, true)
                    .map_err(|source| VaultSearchError::SearchSegments { source })?,
            )
        {
            if seen_segments.insert(segment.segment_id.clone()) {
                segment_candidates.push(segment);
            }
        }
    }

    let mut total = SearchSegmentRepository::count_distinct_paths(connection, &segment_query)
        .map_err(|source| VaultSearchError::SearchSegments { source })?;
    let alias_paths = SearchAliasRepository::distinct_paths(
        connection,
        &needle.normalized,
        &needle.compact,
        &surfaces,
        scope,
        scope_case_insensitive,
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
        if !path_allowed(
            &alias.normalized_path,
            scope,
            scope_case_insensitive,
            extensions,
        ) {
            continue;
        }
        if let Some(surface_kind) = kind_for_surface(&alias.surface) {
            candidates.add(
                &alias.normalized_path,
                surface_kind,
                (10_000 + alias.weight) * SEARCH_SCORE_SCALE,
                format!("alias:{}", alias.source),
            );
        }
    }

    let mut pending_segments = HashMap::<String, PendingSegmentMatch>::new();

    for segment in segment_candidates {
        if !path_allowed(
            &segment.normalized_path,
            scope,
            scope_case_insensitive,
            extensions,
        ) {
            continue;
        }
        let Some(surface_kind) = kind_for_surface(&segment.surface) else {
            continue;
        };
        let matched_in = matched_in_for_segment_candidate(&segment, needle);
        let score = indexed_segment_candidate_score(&segment, needle);
        candidates.add(
            &segment.normalized_path,
            surface_kind,
            score,
            format!("{}:{}", segment.surface, matched_in.join(",")),
        );
        let Some(output_key) = segment_candidate_output_key(&segment) else {
            continue;
        };
        let dedupe_key = format!("{}\0{}", segment.surface, output_key);
        let row = PendingSegmentMatch {
            segment,
            matched_in,
            score,
        };
        match pending_segments.get_mut(&dedupe_key) {
            Some(existing) if row.score > existing.score => {
                *existing = row;
            }
            Some(_) => {}
            None => {
                pending_segments.insert(dedupe_key, row);
            }
        }
    }

    let mut pending_segments = pending_segments.into_values().collect::<Vec<_>>();
    pending_segments.sort_by(compare_pending_segment);
    let mut selected_counts = HashMap::<&'static str, usize>::new();
    let mut selected_segments = Vec::new();
    let limit_usize = limit as usize;
    for pending in pending_segments {
        let Some(bucket) = segment_candidate_output_bucket(&pending.segment) else {
            continue;
        };
        let count = selected_counts.entry(bucket).or_insert(0);
        if *count >= limit_usize {
            continue;
        }
        *count += 1;
        selected_segments.push(pending);
    }

    let selected_ids = selected_segments
        .iter()
        .map(|pending| pending.segment.segment_id.clone())
        .collect::<Vec<_>>();
    let mut pending_by_segment_id = selected_segments
        .into_iter()
        .map(|pending| (pending.segment.segment_id.clone(), pending))
        .collect::<HashMap<_, _>>();
    let segment_matches =
        SearchSegmentRepository::hydrate_by_segment_ids(connection, &selected_ids)
            .map_err(|source| VaultSearchError::SearchSegments { source })?;

    let mut docs = HashMap::<String, SearchDocMatch>::new();
    let mut files = HashMap::<String, PendingFileMatch>::new();
    let mut properties = HashMap::<String, SearchPropertyMatch>::new();
    let mut tasks = HashMap::<String, SearchTaskMatch>::new();
    let mut graph = HashMap::<String, SearchGraphMatch>::new();
    let mut base_rows = HashMap::<String, SearchBaseRowMatch>::new();

    for segment in segment_matches {
        let Some(pending) = pending_by_segment_id.remove(&segment.segment_id) else {
            continue;
        };
        let payload = segment_payload(&segment)?;
        let matched_in = pending.matched_in;
        let score = pending.score;
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
                        if let Some(text) = payload.get("excerpt_text").and_then(JsonValue::as_str)
                        {
                            excerpt_for(text, needle).or_else(|| Some(bound_excerpt(text)))
                        } else {
                            excerpt_for_document(connection, &segment.file_id, needle, include_pii)?
                        }
                    } else {
                        None
                    },
                    locator: payload.get("locator").cloned(),
                    revision: payload_string(&payload, "revision"),
                    coverage: payload_string(&payload, "coverage"),
                    stale: payload_bool(&payload, "stale").unwrap_or(false),
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
                let mut row = SearchGraphMatch {
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
                redact_graph_metadata(&mut row, include_pii);
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
                    values: redact_base_values(values, include_pii),
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
    properties
        .sort_by(|a, b| compare_score_path(a, b).then_with(|| a.property_id.cmp(&b.property_id)));
    tasks.sort_by(|a, b| compare_score_path(a, b).then_with(|| a.task_id.cmp(&b.task_id)));
    graph.sort_by(compare_score_source);
    base_rows.sort_by(|a, b| {
        compare_score_path(a, b)
            .then_with(|| a.base_path.cmp(&b.base_path))
            .then_with(|| a.view_name.cmp(&b.view_name))
            .then_with(|| {
                serde_json::to_string(&a.values)
                    .unwrap_or_default()
                    .cmp(&serde_json::to_string(&b.values).unwrap_or_default())
            })
    });

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
    include_pii: bool,
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
                values: redact_base_values(
                    payload
                        .get("values")
                        .and_then(JsonValue::as_object)
                        .cloned()
                        .unwrap_or_default(),
                    include_pii,
                ),
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
        connection,
        &file,
        request.include_content,
        request.include_pii,
    )?;
    let properties = root_properties(connection, &file.file_id, request.include_pii)?;
    let (_, outgoing_edges) = BacklinkGraphService
        .links_page(
            connection,
            &selected_path,
            crate::GraphLinkDirection::Outgoing,
            limit,
            0,
        )
        .map_err(|source| VaultSearchError::Graph { source })?;
    let (_, incoming_edges) = BacklinkGraphService
        .links_page(
            connection,
            &selected_path,
            crate::GraphLinkDirection::Incoming,
            limit,
            0,
        )
        .map_err(|source| VaultSearchError::Graph { source })?;
    let outgoing = outgoing_edges
        .into_iter()
        .map(|edge| {
            let mut row = graph_match_from_edge(edge, 0);
            redact_graph_metadata(&mut row, request.include_pii);
            row
        })
        .collect::<Vec<_>>();
    let incoming = incoming_edges
        .into_iter()
        .map(|edge| {
            let mut row = graph_match_from_edge(edge, 0);
            redact_graph_metadata(&mut row, request.include_pii);
            row
        })
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
        .map(|mut step| {
            if !request.include_pii
                && LinksRepository::get_by_id(connection, &step.link_id)
                    .map_err(|source| VaultSearchError::Links { source })?
                    .is_some_and(|link| link.source_field.starts_with("frontmatter:"))
            {
                step.raw_target = "redacted".to_string();
            }
            Ok(SearchWalkStep {
                depth: step.depth,
                direction: match step.direction {
                    GraphWalkDirection::Outgoing => "outgoing".to_string(),
                    GraphWalkDirection::Incoming => "incoming".to_string(),
                },
                edge_type: match step.edge_type {
                    GraphWalkEdgeType::Wikilink => "wikilink".to_string(),
                    GraphWalkEdgeType::Markdown => "markdown".to_string(),
                    GraphWalkEdgeType::Embed => "embed".to_string(),
                    GraphWalkEdgeType::FolderParent => "folder_parent".to_string(),
                    GraphWalkEdgeType::FolderSibling => "folder_sibling".to_string(),
                },
                source_path: step.source_path,
                target_path: step.target_path,
                raw_target: step.raw_target,
                resolved: step.resolved,
            })
        })
        .collect::<Result<Vec<_>, VaultSearchError>>()?;

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
        context_base_rows =
            base_rows_for_path(connection, &selected_path, limit, request.include_pii)?;
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
    connection: &Connection,
    file: &tao_sdk_storage::FileRecord,
    include_content: bool,
    include_pii: bool,
) -> Result<SearchRoot, VaultSearchError> {
    let document =
        DocumentsRepository::get_by_file_id(connection, &file.file_id).map_err(|source| {
            VaultSearchError::Sql {
                operation: "read_root_revision",
                source,
            }
        })?;
    let front_matter = if include_pii {
        document
            .as_ref()
            .and_then(|doc| doc.raw_text.strip_suffix(&doc.body_text))
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(ToString::to_string)
    } else {
        document
            .as_ref()
            .filter(|doc| doc.raw_text != doc.body_text)
            .map(|_| "redacted".to_string())
    };
    let extracted = ContentRepository::get(connection, &file.file_id).map_err(|source| {
        VaultSearchError::Sql {
            operation: "read_root_content",
            source,
        }
    })?;
    let body_excerpt = if include_content {
        match &document {
            Some(document) => Some(bound_excerpt(&document.body_text)),
            None => ContentRepository::segments(connection, &file.file_id, 0, 1)
                .map_err(|source| VaultSearchError::Sql {
                    operation: "read_root_content_excerpt",
                    source,
                })?
                .first()
                .map(|segment| bound_excerpt(&segment.text)),
        }
    } else {
        None
    };
    let revision = document
        .as_ref()
        .map(|document| document.source_hash.clone())
        .or_else(|| {
            extracted
                .as_ref()
                .and_then(|content| content.served_revision.clone())
        });
    let coverage = document
        .as_ref()
        .map(|_| "complete".to_string())
        .or_else(|| extracted.as_ref().map(|content| content.coverage.clone()));
    let source_diagnostic: bool = connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM file_diagnostics WHERE path=?1)",
            [&file.normalized_path],
            |row| row.get(0),
        )
        .map_err(|source| VaultSearchError::Sql {
            operation: "read_root_diagnostics",
            source,
        })?;
    let stale = source_diagnostic
        || extracted.as_ref().is_some_and(|content| {
            content.served_revision.as_deref() != Some(&content.desired_revision)
                || content.served_extractor_identity.as_deref() != Some(&content.extractor_identity)
        });
    Ok(SearchRoot {
        file_id: file.file_id.clone(),
        path: file.normalized_path.clone(),
        title: document
            .map(|doc| doc.title)
            .unwrap_or_else(|| note_title_from_path(&file.normalized_path)),
        is_markdown: file.is_markdown,
        extension: extension_for_path(&file.normalized_path),
        size: file.size_bytes,
        modified_unix_ms: file.modified_unix_ms,
        indexed_at: file.indexed_at.clone(),
        front_matter,
        body_excerpt,
        revision,
        coverage,
        stale,
    })
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
    if unique_paths.len() > tao_sdk_storage::SQL_PARAMETER_CHUNK {
        let mut rows = HashMap::new();
        for chunk in unique_paths.chunks(tao_sdk_storage::SQL_PARAMETER_CHUNK) {
            rows.extend(files_by_paths(connection, chunk)?);
        }
        return Ok(rows);
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
    if unique_paths.len() > tao_sdk_storage::SQL_PARAMETER_CHUNK {
        let mut rows = HashMap::new();
        for chunk in unique_paths.chunks(tao_sdk_storage::SQL_PARAMETER_CHUNK) {
            rows.extend(link_counts_for_paths(connection, chunk)?);
        }
        return Ok(rows);
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

fn indexed_candidate_window(_limit: u32) -> u32 {
    1_000
}

fn segment_payload(segment: &SearchSegmentMatch) -> Result<JsonValue, VaultSearchError> {
    let value: JsonValue = serde_json::from_str(&segment.payload_json).map_err(|source| {
        VaultSearchError::Payload {
            segment_id: segment.segment_id.clone(),
            source,
        }
    })?;
    let required: &[&str] = match segment.surface.as_str() {
        "docs" => &["file_id", "path", "title", "indexed_at"],
        "files" => &["file_id", "path", "extension", "indexed_at"],
        "properties" => &[
            "property_id",
            "file_id",
            "path",
            "key",
            "value_type",
            "value_json",
            "updated_at",
        ],
        "tasks" => &["task_id", "file_id", "path", "state", "text", "updated_at"],
        "graph" => &[
            "link_id",
            "source_file_id",
            "source_path",
            "raw_target",
            "source_field",
        ],
        "bases" if segment.field == "base_row" => {
            &["base_id", "base_path", "view_name", "file_id", "path"]
        }
        "bases" => &["base_id", "base_path", "file_id", "updated_at"],
        _ => &[],
    };
    let invalid = value.get("schema_version").and_then(JsonValue::as_u64) != Some(1)
        || required
            .iter()
            .any(|key| !value.get(*key).is_some_and(JsonValue::is_string))
        || (segment.surface == "tasks" && !value.get("line").is_some_and(JsonValue::is_i64))
        || (segment.surface == "files"
            && (!value.get("size").is_some_and(JsonValue::is_u64)
                || !value.get("is_markdown").is_some_and(JsonValue::is_boolean)))
        || (segment.surface == "graph"
            && !value.get("resolved").is_some_and(JsonValue::is_boolean))
        || (segment.field == "base_row" && !value.get("values").is_some_and(JsonValue::is_object));
    if invalid {
        return Err(VaultSearchError::InvalidPayload {
            segment_id: segment.segment_id.clone(),
        });
    }
    Ok(value)
}

fn indexed_segment_candidate_score(segment: &SearchSegmentCandidate, needle: &SearchNeedle) -> i64 {
    candidate_score(
        &segment.normalized_path,
        &segment.label,
        &segment.surface,
        &segment.field,
        segment.weight,
        segment.rank_score,
        needle,
    )
}

#[allow(clippy::too_many_arguments)]
fn candidate_score(
    path: &str,
    label: &str,
    surface: &str,
    field: &str,
    weight: i64,
    rank: i64,
    needle: &SearchNeedle,
) -> i64 {
    let mut score = weight + field_score(field) + text_match_score(path, needle).unwrap_or(0);
    if matches!(surface, "docs" | "files") || (surface == "bases" && field == "base") {
        score += text_match_score(label, needle).unwrap_or(0);
    }
    score * SEARCH_SCORE_SCALE + lexical_score(rank)
}

// Fixed-point scores retain lexical ordering without allowing corpus-dependent
// BM25 magnitudes to overwhelm the smallest path/title evidence tier (30).
// The monotone transform avoids the old hard cap, which made all ordinary
// matches tie and discarded useful length-normalized BM25 evidence.
const SEARCH_SCORE_SCALE: i64 = 1_000_000;

fn lexical_score(rank: i64) -> i64 {
    let rank = i128::from(rank.max(0));
    let scale = i128::from(SEARCH_SCORE_SCALE);
    ((29 * scale * rank) / (i128::from(tao_sdk_storage::SEARCH_RANK_SCALE) + rank)) as i64
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

fn matched_in_for_segment_candidate(
    segment: &SearchSegmentCandidate,
    needle: &SearchNeedle,
) -> Vec<String> {
    let mut matched = Vec::new();
    if text_match_score(&segment.normalized_path, needle).is_some() {
        matched.push("path".to_string());
    }
    if segment_label_is_title(segment) && text_match_score(&segment.label, needle).is_some() {
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

fn segment_label_is_title(segment: &SearchSegmentCandidate) -> bool {
    matches!(segment.surface.as_str(), "docs" | "files")
        || (segment.surface == "bases" && segment.field == "base")
}

fn segment_candidate_output_key(segment: &SearchSegmentCandidate) -> Option<String> {
    match segment.surface.as_str() {
        "docs" | "files" => Some(segment.normalized_path.clone()),
        "properties" | "tasks" | "graph" => segment
            .record_id
            .clone()
            .or_else(|| Some(segment.segment_id.clone())),
        "bases" if segment.field == "base_row" => segment
            .record_id
            .clone()
            .or_else(|| Some(segment.segment_id.clone())),
        "bases" => None,
        _ => None,
    }
}

fn segment_candidate_output_bucket(segment: &SearchSegmentCandidate) -> Option<&'static str> {
    match segment.surface.as_str() {
        "docs" => Some("docs"),
        "files" => Some("files"),
        "properties" => Some("properties"),
        "tasks" => Some("tasks"),
        "graph" => Some("graph"),
        "bases" if segment.field == "base_row" => Some("base_rows"),
        "bases" => None,
        _ => None,
    }
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
        file_group: crate::content::classify_content(&file.path).1,
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

fn excerpt_for_document(
    connection: &Connection,
    file_id: &str,
    needle: &SearchNeedle,
    include_pii: bool,
) -> Result<Option<String>, VaultSearchError> {
    let document = DocumentsRepository::get_by_file_id(connection, file_id).map_err(|source| {
        VaultSearchError::Sql {
            operation: "read_excerpt_revision",
            source,
        }
    })?;
    Ok(document.and_then(|doc| {
        excerpt_for(
            if include_pii {
                &doc.raw_text
            } else {
                &doc.body_text
            },
            needle,
        )
    }))
}

fn redact_graph_metadata(row: &mut SearchGraphMatch, include_pii: bool) {
    if !include_pii && row.source_field.starts_with("frontmatter:") {
        row.raw_target = "redacted".to_string();
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
        file_group: crate::content::classify_content(&file.normalized_path).1,
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
    let normalized = raw.trim().replace('\\', "/");
    tao_sdk_vault::validate_relative_vault_path(&normalized).map_err(|error| {
        VaultSearchError::InvalidRequest(format!("invalid vault-relative path '{raw}': {error}"))
    })?;
    tao_sdk_vault::normalize_relative_path(Path::new(&normalized))
        .map_err(|error| VaultSearchError::InvalidRequest(error.to_string()))
}

fn normalize_scope(raw: &str) -> Result<String, VaultSearchError> {
    if raw.trim() == "." || raw.trim().is_empty() {
        return Ok(String::new());
    }
    normalize_path_input(raw.trim_end_matches('/'))
}

fn normalize_extensions(raw: &[String]) -> HashSet<String> {
    raw.iter()
        .flat_map(|entry| entry.split(','))
        .map(str::trim)
        .map(|entry| entry.trim_start_matches('.').to_lowercase())
        .filter(|entry| !entry.is_empty())
        .collect()
}

fn indexed_case_policy(connection: &Connection) -> Result<CasePolicy, VaultSearchError> {
    let record = tao_sdk_storage::IndexStateRepository::get_by_key(connection, "index_case_policy")
        .map_err(|source| {
            VaultSearchError::InvalidRequest(format!("read case policy: {source}"))
        })?;
    Ok(
        if record.is_some_and(|row| row.value_json.trim_matches('"') == "insensitive") {
            CasePolicy::Insensitive
        } else {
            CasePolicy::Sensitive
        },
    )
}

fn path_allowed(
    path: &str,
    scope: Option<&str>,
    insensitive: bool,
    extensions: &HashSet<String>,
) -> bool {
    let matched_path = if insensitive {
        tao_sdk_vault::path_match_key(path, CasePolicy::Insensitive)
    } else {
        path.to_string()
    };
    if let Some(scope) = scope
        && !scope.is_empty()
        && matched_path != scope
        && !matched_path.starts_with(&format!("{scope}/"))
    {
        return false;
    }
    extensions.is_empty() || extensions.contains(&extension_for_path(path))
}

fn extension_for_path(path: &str) -> String {
    tao_sdk_vault::normalized_extension(Path::new(path)).unwrap_or_default()
}

fn normalize_text(value: &str) -> String {
    tao_sdk_search::parser::normalize_query_text(value)
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

fn redact_base_values(
    values: JsonMap<String, JsonValue>,
    include_pii: bool,
) -> JsonMap<String, JsonValue> {
    if include_pii {
        values
    } else {
        values
            .into_iter()
            .map(|(key, _)| (key, JsonValue::String("redacted".to_string())))
            .collect()
    }
}

fn pii_value(raw: &str, include_pii: bool) -> JsonValue {
    if !include_pii {
        return JsonValue::String("redacted".to_string());
    }
    serde_json::from_str(raw).unwrap_or_else(|_| JsonValue::String(raw.to_string()))
}

fn excerpt_for(content: &str, needle: &SearchNeedle) -> Option<String> {
    let mut lower = String::new();
    let mut offsets = Vec::new();
    for (offset, ch) in content.char_indices() {
        for folded in ch.to_lowercase() {
            for _ in 0..folded.len_utf8() {
                offsets.push(offset);
            }
            lower.push(folded);
        }
    }
    let index = lower
        .find(&needle.raw.to_lowercase())
        .or_else(|| needle.tokens.iter().find_map(|token| lower.find(token)))?;
    let original = *offsets.get(index)?;
    let start = previous_char_boundary(content, original.saturating_sub(120));
    let end = next_char_boundary(content, (original + 240).min(content.len()));
    Some(content[start..end].replace('\n', " "))
}

fn bound_result_content(result: &mut VaultSearchResult) {
    const MAX_CONTENT_BYTES: usize = 128 * 1024;
    fn text(value: &mut String, budget: &mut usize, changed: &mut bool) {
        let max = (*budget).min(4096);
        if value.len() > max {
            let end = previous_char_boundary(value, max);
            value.truncate(end);
            *changed = true;
        }
        *budget = budget.saturating_sub(value.len());
    }
    fn value(v: &mut JsonValue, budget: &mut usize, changed: &mut bool, depth: usize) {
        if depth >= 8 {
            *v = JsonValue::String("truncated".to_string());
            *changed = true;
            return;
        }
        match v {
            JsonValue::String(s) => text(s, budget, changed),
            JsonValue::Array(rows) => {
                if rows.len() > 64 {
                    rows.truncate(64);
                    *changed = true;
                }
                for row in rows {
                    value(row, budget, changed, depth + 1);
                }
            }
            JsonValue::Object(rows) => {
                let keys = rows.keys().skip(64).cloned().collect::<Vec<_>>();
                for key in keys {
                    rows.remove(&key);
                    *changed = true;
                }
                for row in rows.values_mut() {
                    value(row, budget, changed, depth + 1);
                }
            }
            _ => {}
        }
    }
    let mut budget = MAX_CONTENT_BYTES;
    let changed = &mut result.content_truncated;
    if result.context.properties.len() > result.limit as usize {
        result.context.properties.truncate(result.limit as usize);
        *changed = true;
    }
    for doc in &mut result.docs {
        if let Some(excerpt) = &mut doc.excerpt {
            text(excerpt, &mut budget, changed);
        }
    }
    for task in &mut result.tasks {
        text(&mut task.text, &mut budget, changed);
    }
    for property in result
        .properties
        .iter_mut()
        .chain(&mut result.context.properties)
    {
        value(&mut property.value, &mut budget, changed, 0);
    }
    for graph in result
        .graph
        .iter_mut()
        .chain(&mut result.context.links.outgoing)
        .chain(&mut result.context.links.incoming)
    {
        text(&mut graph.raw_target, &mut budget, changed);
    }
    for step in &mut result.context.walk {
        text(&mut step.raw_target, &mut budget, changed);
    }
    for row in &mut result.context.base_rows {
        let keys = row.values.keys().skip(64).cloned().collect::<Vec<_>>();
        for key in keys {
            row.values.remove(&key);
            *changed = true;
        }
        for v in row.values.values_mut() {
            value(v, &mut budget, changed, 0);
        }
    }
    if let Some(root) = &mut result.context.root {
        if let Some(front) = &mut root.front_matter {
            text(front, &mut budget, changed);
        }
        if let Some(body) = &mut root.body_excerpt {
            text(body, &mut budget, changed);
        }
    }
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
            .then_with(|| left.path.cmp(&right.path))
    });
}

fn compare_score_path<T: SearchPathScore>(left: &T, right: &T) -> Ordering {
    right
        .score()
        .cmp(&left.score())
        .then_with(|| left.path().cmp(right.path()))
}

fn compare_pending_segment(left: &PendingSegmentMatch, right: &PendingSegmentMatch) -> Ordering {
    right
        .score
        .cmp(&left.score)
        .then_with(|| {
            left.segment
                .normalized_path
                .cmp(&right.segment.normalized_path)
        })
        .then_with(|| left.segment.segment_id.cmp(&right.segment.segment_id))
}

fn compare_score_source(left: &SearchGraphMatch, right: &SearchGraphMatch) -> Ordering {
    right
        .score
        .cmp(&left.score)
        .then_with(|| left.source_path.cmp(&right.source_path))
        .then_with(|| left.raw_target.cmp(&right.raw_target))
        .then_with(|| left.link_id.cmp(&right.link_id))
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
    for window in value.as_bytes().windows(10) {
        if !matches!(window[4], b'-' | b'_')
            || !window[9].is_ascii_digit()
            || window[7] != window[4]
            || !window[..4]
                .iter()
                .chain(&window[5..7])
                .chain(&window[8..])
                .all(u8::is_ascii_digit)
        {
            continue;
        }
        let year = u32::from(window[0] - b'0') * 1000
            + u32::from(window[1] - b'0') * 100
            + u32::from(window[2] - b'0') * 10
            + u32::from(window[3] - b'0');
        let month = (window[5] - b'0') * 10 + window[6] - b'0';
        let day = (window[8] - b'0') * 10 + window[9] - b'0';
        let leap =
            year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400));
        let days = match month {
            1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
            4 | 6 | 9 | 11 => 30,
            2 if leap => 29,
            2 => 28,
            _ => 0,
        };
        if year != 0 && day > 0 && day <= days {
            return Some(format!("{year:04}-{month:02}-{day:02}"));
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
    /// A stored payload does not satisfy its typed surface contract.
    #[error(
        "search segment '{segment_id}' has invalid typed fields; rebuild the derived search index"
    )]
    InvalidPayload {
        /// Invalid derived record identifier.
        segment_id: String,
    },
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

#[cfg(test)]
#[path = "search/tests.rs"]
mod tests;
