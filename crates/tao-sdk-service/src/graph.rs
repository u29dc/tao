//! Backlink graph traversal and analysis service.

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock};

use rusqlite::{Connection, params};
use tao_sdk_storage::{
    FilesRepository, IndexGenerationRepository, LinkEvidenceRepository, LinksRepository,
};
use thiserror::Error;

/// One link graph edge enriched with source/target path metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinkGraphEdge {
    /// Stable link row identifier.
    pub link_id: String,
    /// Source file id.
    pub source_file_id: String,
    /// Source normalized path.
    pub source_path: String,
    /// Raw link target payload.
    pub raw_target: String,
    /// Resolved target file id when available.
    pub resolved_file_id: Option<String>,
    /// Resolved target normalized path when available.
    pub resolved_path: Option<String>,
    /// Optional heading fragment slug.
    pub heading_slug: Option<String>,
    /// Optional block fragment id.
    pub block_id: Option<String>,
    /// Unresolved marker.
    pub is_unresolved: bool,
    /// Stable unresolved reason code.
    pub unresolved_reason: Option<String>,
    /// Link provenance source field.
    pub source_field: String,
    /// Optional original occurrence and independent fragment-resolution evidence.
    pub evidence: Option<serde_json::Value>,
}

/// One graph node row with resolved in/out degree counters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphNodeDegreeRow {
    /// Stable file id.
    pub file_id: String,
    /// Normalized path.
    pub path: String,
    /// Resolved incoming count.
    pub incoming_resolved: u64,
    /// Resolved outgoing count.
    pub outgoing_resolved: u64,
}

/// One scoped inbound-link row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphScopedInboundRow {
    /// Stable file id.
    pub file_id: String,
    /// Normalized file path.
    pub path: String,
    /// Whether row path is markdown.
    pub is_markdown: bool,
    /// Resolved inbound edge count.
    pub inbound_resolved: u64,
}

/// Scoped inbound-link summary counters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphScopedInboundSummary {
    /// Total matched files.
    pub total_files: u64,
    /// Files with at least one inbound edge.
    pub linked_files: u64,
    /// Files with zero inbound edges.
    pub unlinked_files: u64,
}

/// One strict floating-file row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphFloatingRow {
    /// Stable file id.
    pub file_id: String,
    /// Normalized file path.
    pub path: String,
    /// Whether row path is markdown.
    pub is_markdown: bool,
    /// Resolved inbound edge count.
    pub incoming_resolved: u64,
    /// Resolved outgoing edge count.
    pub outgoing_resolved: u64,
}

/// Strict floating-file summary counters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphFloatingSummary {
    /// Total strict floating files.
    pub total_files: u64,
    /// Total strict floating markdown files.
    pub markdown_files: u64,
    /// Total strict floating non-markdown files.
    pub non_markdown_files: u64,
}

/// Input payload for scoped inbound-link audits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphScopedInboundRequest {
    /// Vault-relative scope prefix.
    pub scope_prefix: String,
    /// Include markdown files in result set.
    pub include_markdown: bool,
    /// Include non-markdown files in result set.
    pub include_non_markdown: bool,
    /// Optional excluded scope prefixes.
    pub exclude_prefixes: Vec<String>,
    /// Page size.
    pub limit: u32,
    /// Page offset.
    pub offset: u32,
}

/// One connected component summary row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphComponentRow {
    /// Number of included file nodes in the component.
    pub size: u64,
    /// Member paths (full list or bounded sample, depending on request).
    pub paths: Vec<String>,
    /// Whether `paths` is truncated compared to full membership.
    pub truncated: bool,
}

/// Connected component traversal mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphComponentMode {
    /// Weakly connected components over undirected projection.
    Weak,
    /// Strongly connected components over directed graph.
    Strong,
}

/// Graph walk traversal direction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphWalkDirection {
    /// Edge traversed from source to target.
    Outgoing,
    /// Edge traversed from target to source.
    Incoming,
}

/// One graph walk step row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphWalkStep {
    /// Traversal depth (1-based from root).
    pub depth: u32,
    /// Traversal direction.
    pub direction: GraphWalkDirection,
    /// Stable link identifier.
    pub link_id: String,
    /// Source path.
    pub source_path: String,
    /// Target path when resolved.
    pub target_path: Option<String>,
    /// Raw target token.
    pub raw_target: String,
    /// Whether the edge is resolved.
    pub resolved: bool,
    /// Traversed edge type.
    pub edge_type: GraphWalkEdgeType,
}

/// Graph walk edge classification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphWalkEdgeType {
    /// Wikilink edge from indexed markdown links.
    Wikilink,
    /// Explicit Markdown destination.
    Markdown,
    /// Image or embedded asset reference.
    Embed,
    /// Folder parent overlay edge.
    FolderParent,
    /// Folder sibling overlay edge.
    FolderSibling,
}

/// Input request for graph walk traversal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphWalkRequest {
    /// Root note path for traversal.
    pub path: String,
    /// Maximum traversal depth.
    pub depth: u32,
    /// Maximum number of step rows returned.
    pub limit: u32,
    /// Include unresolved outgoing edges.
    pub include_unresolved: bool,
    /// Include folder relationship overlay edges.
    pub include_folders: bool,
}

/// Input request for bounded shortest-path traversal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphPathRequest {
    /// Source note path.
    pub from_path: String,
    /// Target note path.
    pub to_path: String,
    /// Maximum traversal depth.
    pub max_depth: u32,
    /// Maximum discovered nodes before aborting.
    pub max_nodes: u32,
}

/// Result from bounded shortest-path traversal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphPathResult {
    /// Whether a path was found.
    pub found: bool,
    /// Number of discovered nodes.
    pub explored_nodes: u32,
    /// Ordered path from source to target when found.
    pub path: Vec<String>,
    /// Whether the result proves an answer without exhausting a work bound.
    pub complete: bool,
    /// Limit responsible for an incomplete result, if any.
    pub truncation_reason: Option<String>,
    /// Number of adjacency rows examined within the edge budget.
    pub examined_edges: u64,
}

/// Link graph query service for outgoing, backlink, and unresolved edges.
#[derive(Debug, Default, Clone, Copy)]
pub struct BacklinkGraphService;

impl BacklinkGraphService {
    /// List outgoing edges for one source note path.
    pub fn outgoing_for_path(
        &self,
        connection: &Connection,
        source_path: &str,
    ) -> Result<Vec<LinkGraphEdge>, LinkGraphServiceError> {
        let Some(source_file) = FilesRepository::get_by_normalized_path(connection, source_path)
            .map_err(|source| LinkGraphServiceError::FilesRepository { source })?
        else {
            return Ok(Vec::new());
        };

        let rows = LinksRepository::list_outgoing_with_paths(connection, &source_file.file_id)
            .map_err(|source| LinkGraphServiceError::LinksRepository { source })?;
        enrich_edges(connection, map_link_edges(rows))
    }

    /// List backlinks for one target note path.
    pub fn backlinks_for_path(
        &self,
        connection: &Connection,
        target_path: &str,
    ) -> Result<Vec<LinkGraphEdge>, LinkGraphServiceError> {
        let Some(target_file) = FilesRepository::get_by_normalized_path(connection, target_path)
            .map_err(|source| LinkGraphServiceError::FilesRepository { source })?
        else {
            return Ok(Vec::new());
        };

        let rows = LinksRepository::list_backlinks_with_paths(connection, &target_file.file_id)
            .map_err(|source| LinkGraphServiceError::LinksRepository { source })?;
        enrich_edges(connection, map_link_edges(rows))
    }

    /// List unresolved edges across vault.
    pub fn unresolved_links(
        &self,
        connection: &Connection,
    ) -> Result<Vec<LinkGraphEdge>, LinkGraphServiceError> {
        self.unresolved_links_page(connection, u32::MAX, 0)
            .map(|(_, rows)| rows)
    }

    /// List missing-document, invalid-fragment and pending-fragment occurrences.
    pub fn unresolved_links_page(
        &self,
        connection: &Connection,
        limit: u32,
        offset: u32,
    ) -> Result<(u64, Vec<LinkGraphEdge>), LinkGraphServiceError> {
        // A resolved document may still have an invalid or pending fragment. Use
        // the same predicate for the exact count and paged occurrence selection.
        let predicate = "l.is_unresolved=1 OR e.fragment_status IN ('bad_anchor','bad_block','bad_page','pending')";
        let total = connection.query_row(
            &format!("SELECT COUNT(*) FROM links l LEFT JOIN link_evidence e ON e.link_id=l.link_id WHERE {predicate}"),
            [], |row| row.get::<_, u64>(0),
        ).map_err(graph_sql)?;
        let sql = format!(
            "{} LEFT JOIN link_evidence e ON e.link_id=l.link_id WHERE {predicate} ORDER BY l.link_id LIMIT ?1 OFFSET ?2",
            graph_edge_select()
        );
        let mut statement = connection.prepare(&sql).map_err(graph_sql)?;
        let rows = statement
            .query_map(params![limit, offset], graph_edge_row)
            .map_err(graph_sql)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(graph_sql)?;
        Ok((total, enrich_edges(connection, rows)?))
    }

    /// List one deadends diagnostics window in deterministic path order.
    pub fn deadends_page(
        &self,
        connection: &Connection,
        limit: u32,
        offset: u32,
    ) -> Result<(u64, Vec<GraphNodeDegreeRow>), LinkGraphServiceError> {
        let total = LinksRepository::count_deadends(connection)
            .map_err(|source| LinkGraphServiceError::LinksRepository { source })?;
        let rows = LinksRepository::list_deadends_window(connection, limit, offset)
            .map_err(|source| LinkGraphServiceError::LinksRepository { source })?;
        Ok((total, map_graph_node_degrees(rows)))
    }

    /// List one orphans diagnostics window in deterministic path order.
    pub fn orphans_page(
        &self,
        connection: &Connection,
        limit: u32,
        offset: u32,
    ) -> Result<(u64, Vec<GraphNodeDegreeRow>), LinkGraphServiceError> {
        let total = LinksRepository::count_orphans(connection)
            .map_err(|source| LinkGraphServiceError::LinksRepository { source })?;
        let rows = LinksRepository::list_orphans_window(connection, limit, offset)
            .map_err(|source| LinkGraphServiceError::LinksRepository { source })?;
        Ok((total, map_graph_node_degrees(rows)))
    }

    /// Return one scoped inbound-link audit window plus summary counters.
    pub fn scoped_inbound_page(
        &self,
        connection: &Connection,
        request: &GraphScopedInboundRequest,
    ) -> Result<(GraphScopedInboundSummary, Vec<GraphScopedInboundRow>), LinkGraphServiceError>
    {
        let summary = LinksRepository::summarize_scoped_inbound(
            connection,
            &request.scope_prefix,
            request.include_markdown,
            request.include_non_markdown,
            &request.exclude_prefixes,
        )
        .map_err(|source| LinkGraphServiceError::LinksRepository { source })?;
        let rows = LinksRepository::list_scoped_inbound_window(
            connection,
            &request.scope_prefix,
            request.include_markdown,
            request.include_non_markdown,
            &request.exclude_prefixes,
            request.limit,
            request.offset,
        )
        .map_err(|source| LinkGraphServiceError::LinksRepository { source })?;

        let items = rows
            .into_iter()
            .map(|row| GraphScopedInboundRow {
                file_id: row.file_id,
                path: row.path,
                is_markdown: row.is_markdown,
                inbound_resolved: row.inbound_resolved,
            })
            .collect::<Vec<_>>();
        Ok((
            GraphScopedInboundSummary {
                total_files: summary.total_files,
                linked_files: summary.linked_files,
                unlinked_files: summary.unlinked_files,
            },
            items,
        ))
    }

    /// Return one strict floating-file window plus summary counters.
    pub fn floating_page(
        &self,
        connection: &Connection,
        limit: u32,
        offset: u32,
    ) -> Result<(GraphFloatingSummary, Vec<GraphFloatingRow>), LinkGraphServiceError> {
        let summary = LinksRepository::summarize_floating_default(connection)
            .map_err(|source| LinkGraphServiceError::LinksRepository { source })?;
        let rows = LinksRepository::list_floating_default_window(connection, limit, offset)
            .map_err(|source| LinkGraphServiceError::LinksRepository { source })?;
        let items = rows
            .into_iter()
            .map(|row| GraphFloatingRow {
                file_id: row.file_id,
                path: row.path,
                is_markdown: row.is_markdown,
                incoming_resolved: row.incoming_resolved,
                outgoing_resolved: row.outgoing_resolved,
            })
            .collect::<Vec<_>>();
        Ok((
            GraphFloatingSummary {
                total_files: summary.total_files,
                markdown_files: summary.markdown_files,
                non_markdown_files: summary.non_markdown_files,
            },
            items,
        ))
    }

    /// Build connected components over resolved graph edges and return one deterministic page.
    pub fn components_page(
        &self,
        connection: &Connection,
        mode: GraphComponentMode,
        limit: u32,
        offset: u32,
        include_members: bool,
        sample_size: usize,
    ) -> Result<(u64, Vec<GraphComponentRow>), LinkGraphServiceError> {
        let cache_allowed =
            connection.is_autocommit() || connection.is_readonly("main").unwrap_or(false);
        connection
            .execute_batch("SAVEPOINT tao_graph_components")
            .map_err(graph_sql)?;
        let result = (|| {
            let snapshot = component_snapshot(connection, mode, cache_allowed)?;
            let total = snapshot.len() as u64;
            let items = snapshot
                .iter()
                .skip(offset as usize)
                .take(limit as usize)
                .map(|row| {
                    let length = if include_members {
                        row.paths.len()
                    } else {
                        sample_size.min(row.paths.len())
                    };
                    GraphComponentRow {
                        size: row.size,
                        paths: row.paths[..length].to_vec(),
                        truncated: length < row.paths.len(),
                    }
                })
                .collect();
            Ok((total, items))
        })();
        let release = connection
            .execute_batch("RELEASE tao_graph_components")
            .map_err(graph_sql);
        match result {
            Err(error) => Err(error),
            Ok(value) => {
                release?;
                Ok(value)
            }
        }
    }

    /// Return one bounded occurrence window. Self-links are one occurrence in all mode.
    pub fn links_page(
        &self,
        connection: &Connection,
        path: &str,
        direction: GraphLinkDirection,
        limit: u32,
        offset: u32,
    ) -> Result<(u64, Vec<LinkGraphEdge>), LinkGraphServiceError> {
        let Some(file) = FilesRepository::get_by_normalized_path(connection, path)
            .map_err(|source| LinkGraphServiceError::FilesRepository { source })?
        else {
            return Ok((0, Vec::new()));
        };
        let predicate = match direction {
            GraphLinkDirection::All => "(l.source_file_id = ?1 OR l.resolved_file_id = ?1)",
            GraphLinkDirection::Outgoing => "l.source_file_id = ?1",
            GraphLinkDirection::Incoming => "l.resolved_file_id = ?1",
        };
        let total: u64 = connection
            .query_row(
                &format!("SELECT COUNT(*) FROM links l WHERE {predicate}"),
                [&file.file_id],
                |row| row.get(0),
            )
            .map_err(graph_sql)?;
        let query = format!(
            "{} WHERE {predicate} ORDER BY l.link_id LIMIT ?2 OFFSET ?3",
            graph_edge_select()
        );
        let mut statement = connection.prepare(&query).map_err(graph_sql)?;
        let rows = statement
            .query_map(params![file.file_id, limit, offset], graph_edge_row)
            .map_err(graph_sql)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(graph_sql)?;
        Ok((total, enrich_edges(connection, rows)?))
    }

    /// Find an undirected shortest path, bounded while retrieving adjacency rows.
    pub fn shortest_path(
        &self,
        connection: &Connection,
        request: &GraphPathRequest,
    ) -> Result<GraphPathResult, LinkGraphServiceError> {
        if request.max_nodes == 0 {
            return Err(LinkGraphServiceError::TraversalLimit { max_nodes: 0 });
        }
        let from = FilesRepository::get_by_normalized_path(connection, &request.from_path)
            .map_err(|source| LinkGraphServiceError::FilesRepository { source })?;
        let to = FilesRepository::get_by_normalized_path(connection, &request.to_path)
            .map_err(|source| LinkGraphServiceError::FilesRepository { source })?;
        let (Some(from), Some(to)) = (from, to) else {
            return Ok(path_outcome(Vec::new(), 0, 0, None));
        };
        if from.file_id == to.file_id {
            return Ok(path_outcome(vec![from.normalized_path], 1, 0, None));
        }
        let max_edges = u64::from(request.max_nodes)
            .saturating_mul(16)
            .clamp(64, 1_000_000);
        let mut examined = 0u64;
        let mut paths = HashMap::from([
            (from.file_id.clone(), from.normalized_path),
            (to.file_id.clone(), to.normalized_path),
        ]);
        let mut parents = HashMap::<String, String>::new();
        let mut seen = HashSet::from([from.file_id.clone()]);
        let mut frontier = VecDeque::from([(from.file_id.clone(), 0u32)]);
        let mut depth_limited = false;
        while let Some((current, depth)) = frontier.pop_front() {
            if depth >= request.max_depth {
                depth_limited = true;
                continue;
            }
            // The exact target lookup avoids spending a tiny node budget on unrelated
            // hub neighbors before recognizing a known one-hop shortest path.
            let direct: bool = connection.query_row("SELECT EXISTS(SELECT 1 FROM links WHERE source_file_id=?1 AND resolved_file_id=?2 AND is_unresolved=0 UNION ALL SELECT 1 FROM links WHERE source_file_id=?2 AND resolved_file_id=?1 AND is_unresolved=0)", params![current, to.file_id], |row| row.get(0)).map_err(graph_sql)?;
            if direct {
                if seen.len() >= request.max_nodes as usize {
                    return Ok(path_outcome(
                        Vec::new(),
                        seen.len(),
                        examined,
                        Some("max_nodes"),
                    ));
                }
                parents.insert(to.file_id.clone(), current);
                seen.insert(to.file_id.clone());
                return Ok(path_outcome(
                    reconstruct_path(&from.file_id, &to.file_id, &parents, &paths),
                    seen.len(),
                    examined,
                    None,
                ));
            }
            for direction in [GraphLinkDirection::Outgoing, GraphLinkDirection::Incoming] {
                let remaining = max_edges.saturating_sub(examined);
                if remaining == 0 {
                    return Ok(path_outcome(
                        Vec::new(),
                        seen.len(),
                        examined,
                        Some("max_edges"),
                    ));
                }
                let edges =
                    adjacency_window(connection, &current, direction, false, remaining + 1)?;
                for edge in edges {
                    if examined >= max_edges {
                        return Ok(path_outcome(
                            Vec::new(),
                            seen.len(),
                            examined,
                            Some("max_edges"),
                        ));
                    }
                    examined += 1;
                    let (next, next_path) = match direction {
                        GraphLinkDirection::Outgoing => {
                            let (Some(id), Some(path)) =
                                (edge.resolved_file_id, edge.resolved_path)
                            else {
                                continue;
                            };
                            (id, path)
                        }
                        _ => (edge.source_file_id, edge.source_path),
                    };
                    if seen.contains(&next) {
                        continue;
                    }
                    if seen.len() >= request.max_nodes as usize {
                        return Ok(path_outcome(
                            Vec::new(),
                            seen.len(),
                            examined,
                            Some("max_nodes"),
                        ));
                    }
                    paths.insert(next.clone(), next_path);
                    seen.insert(next.clone());
                    parents.insert(next.clone(), current.clone());
                    if next == to.file_id {
                        return Ok(path_outcome(
                            reconstruct_path(&from.file_id, &to.file_id, &parents, &paths),
                            seen.len(),
                            examined,
                            None,
                        ));
                    }
                    frontier.push_back((next, depth + 1));
                }
            }
        }
        Ok(path_outcome(
            Vec::new(),
            seen.len(),
            examined,
            depth_limited.then_some("max_depth"),
        ))
    }

    /// Compatibility adapter; use `walk_bounded` when completeness evidence matters.
    pub fn walk(
        &self,
        connection: &Connection,
        request: &GraphWalkRequest,
    ) -> Result<Vec<GraphWalkStep>, LinkGraphServiceError> {
        Ok(self.walk_bounded(connection, request)?.items)
    }

    /// Walk edge occurrences with bounded adjacency retrieval and explicit completeness.
    pub fn walk_bounded(
        &self,
        connection: &Connection,
        request: &GraphWalkRequest,
    ) -> Result<GraphWalkResult, LinkGraphServiceError> {
        let mut result = GraphWalkResult {
            items: Vec::new(),
            complete: true,
            truncation_reason: None,
            examined_edges: 0,
            discovered_nodes: 0,
        };
        let Some(start) = FilesRepository::get_by_normalized_path(connection, &request.path)
            .map_err(|source| LinkGraphServiceError::FilesRepository { source })?
        else {
            return Ok(result);
        };
        result.discovered_nodes = 1;
        if request.depth == 0 {
            result.complete = false;
            result.truncation_reason = Some("max_depth".into());
            return Ok(result);
        }
        if request.limit == 0 {
            result.complete = false;
            result.truncation_reason = Some("limit".into());
            return Ok(result);
        }
        let max_edges = u64::from(request.limit)
            .saturating_mul(16)
            .clamp(64, 1_000_000);
        let mut seen_nodes = HashSet::from([start.file_id.clone()]);
        let mut seen_edges = HashSet::<String>::new();
        let mut frontier = VecDeque::from([(start.file_id, start.normalized_path, 0u32)]);
        let mut depth_limited = false;
        while let Some((current, current_path, depth)) = frontier.pop_front() {
            if depth >= request.depth {
                depth_limited = true;
                continue;
            }
            for direction in [GraphLinkDirection::Outgoing, GraphLinkDirection::Incoming] {
                let remaining_work = max_edges.saturating_sub(result.examined_edges);
                if remaining_work == 0 {
                    return Ok(result.truncate("max_edges"));
                }
                let remaining_items = (request.limit as usize).saturating_sub(result.items.len());
                // Already emitted edges can appear from the opposite end; bound both
                // examined work and allocation, without pretending the output cap is work.
                let fetch_limit =
                    remaining_work.min((remaining_items + seen_edges.len() + 1) as u64);
                let edges = adjacency_window(
                    connection,
                    &current,
                    direction,
                    request.include_unresolved,
                    fetch_limit + 1,
                )?;
                for edge in edges {
                    if result.examined_edges >= max_edges {
                        return Ok(result.truncate("max_edges"));
                    }
                    result.examined_edges += 1;
                    if !seen_edges.insert(edge.link_id.clone()) {
                        continue;
                    }
                    if result.items.len() >= request.limit as usize {
                        return Ok(result.truncate("limit"));
                    }
                    let next = match direction {
                        GraphLinkDirection::Outgoing => edge
                            .resolved_file_id
                            .clone()
                            .zip(edge.resolved_path.clone()),
                        _ => Some((edge.source_file_id.clone(), edge.source_path.clone())),
                    };
                    let edge_type = if edge.source_field.ends_with(":embed") {
                        GraphWalkEdgeType::Embed
                    } else if edge.source_field.ends_with(":markdown") {
                        GraphWalkEdgeType::Markdown
                    } else {
                        GraphWalkEdgeType::Wikilink
                    };
                    result.items.push(GraphWalkStep {
                        depth: depth + 1,
                        direction: if direction == GraphLinkDirection::Outgoing {
                            GraphWalkDirection::Outgoing
                        } else {
                            GraphWalkDirection::Incoming
                        },
                        link_id: edge.link_id,
                        source_path: edge.source_path,
                        target_path: edge.resolved_path,
                        raw_target: edge.raw_target,
                        resolved: !edge.is_unresolved,
                        edge_type,
                    });
                    if let Some((id, path)) = next
                        && seen_nodes.insert(id.clone())
                    {
                        result.discovered_nodes = seen_nodes.len() as u64;
                        frontier.push_back((id, path, depth + 1));
                    }
                }
            }
            if request.include_folders {
                // Folder overlay is a derived relation over all included files. The
                // SQL window avoids loading a whole vault/folder map for a short walk.
                for (id, path, edge_type) in folder_window(
                    connection,
                    &current,
                    &current_path,
                    u64::from(request.limit).saturating_add(1),
                )? {
                    if result.examined_edges >= max_edges {
                        return Ok(result.truncate("max_edges"));
                    }
                    result.examined_edges += 1;
                    let link_id = format!(
                        "folder:{current}:{id}:{}",
                        graph_walk_edge_type_label(&edge_type)
                    );
                    if !seen_edges.insert(link_id.clone()) {
                        continue;
                    }
                    if result.items.len() >= request.limit as usize {
                        return Ok(result.truncate("limit"));
                    }
                    result.items.push(GraphWalkStep {
                        depth: depth + 1,
                        direction: GraphWalkDirection::Outgoing,
                        link_id,
                        source_path: current_path.clone(),
                        target_path: Some(path.clone()),
                        raw_target: path.clone(),
                        resolved: true,
                        edge_type,
                    });
                    if seen_nodes.insert(id.clone()) {
                        result.discovered_nodes = seen_nodes.len() as u64;
                        frontier.push_back((id, path, depth + 1));
                    }
                }
            }
            result.discovered_nodes = seen_nodes.len() as u64;
        }
        result.discovered_nodes = seen_nodes.len() as u64;
        if depth_limited {
            result = result.truncate("max_depth");
        }
        Ok(result)
    }
}

fn build_component_rows(
    components_by_ids: Vec<Vec<String>>,
    paths_by_id: &HashMap<String, String>,
    include_members: bool,
    sample_size: usize,
) -> Vec<GraphComponentRow> {
    let mut components = Vec::<GraphComponentRow>::with_capacity(components_by_ids.len());
    for members in components_by_ids {
        let mut paths = members
            .iter()
            .filter_map(|file_id| paths_by_id.get(file_id).cloned())
            .collect::<Vec<_>>();
        paths.sort();
        let full_len = paths.len();
        if !include_members && paths.len() > sample_size {
            paths.truncate(sample_size);
        }
        components.push(GraphComponentRow {
            size: u64::try_from(members.len()).unwrap_or(u64::MAX),
            truncated: !include_members && full_len > paths.len(),
            paths,
        });
    }
    components
}

fn weak_components(
    ids: &[String],
    pairs: &[tao_sdk_storage::ResolvedLinkPair],
) -> Vec<Vec<String>> {
    let mut adjacency = HashMap::<String, Vec<String>>::new();
    for pair in pairs {
        adjacency
            .entry(pair.source_file_id.clone())
            .or_default()
            .push(pair.target_file_id.clone());
        adjacency
            .entry(pair.target_file_id.clone())
            .or_default()
            .push(pair.source_file_id.clone());
    }
    for neighbors in adjacency.values_mut() {
        neighbors.sort();
        neighbors.dedup();
    }

    let mut visited = HashSet::<String>::new();
    let mut components = Vec::<Vec<String>>::new();
    for root in ids {
        if !visited.insert(root.clone()) {
            continue;
        }
        let mut queue = VecDeque::from([root.clone()]);
        let mut members = Vec::<String>::new();
        while let Some(current) = queue.pop_front() {
            members.push(current.clone());
            if let Some(neighbors) = adjacency.get(&current) {
                for next in neighbors {
                    if visited.insert(next.clone()) {
                        queue.push_back(next.clone());
                    }
                }
            }
        }
        members.sort();
        components.push(members);
    }
    components
}

fn strong_components(
    ids: &[String],
    pairs: &[tao_sdk_storage::ResolvedLinkPair],
) -> Vec<Vec<String>> {
    let mut forward = HashMap::<String, Vec<String>>::new();
    let mut reverse = HashMap::<String, Vec<String>>::new();
    for pair in pairs {
        forward
            .entry(pair.source_file_id.clone())
            .or_default()
            .push(pair.target_file_id.clone());
        reverse
            .entry(pair.target_file_id.clone())
            .or_default()
            .push(pair.source_file_id.clone());
    }
    for neighbors in forward.values_mut() {
        neighbors.sort();
        neighbors.dedup();
    }
    for neighbors in reverse.values_mut() {
        neighbors.sort();
        neighbors.dedup();
    }

    let mut visited = HashSet::<String>::new();
    let mut finish_order = Vec::<String>::new();
    for root in ids {
        if visited.contains(root) {
            continue;
        }
        let mut stack = Vec::<(String, bool)>::from([(root.clone(), false)]);
        while let Some((node, expanded)) = stack.pop() {
            if expanded {
                finish_order.push(node);
                continue;
            }
            if !visited.insert(node.clone()) {
                continue;
            }
            stack.push((node.clone(), true));
            if let Some(neighbors) = forward.get(&node) {
                for next in neighbors.iter().rev() {
                    if !visited.contains(next) {
                        stack.push((next.clone(), false));
                    }
                }
            }
        }
    }

    let mut assigned = HashSet::<String>::new();
    let mut components = Vec::<Vec<String>>::new();
    while let Some(root) = finish_order.pop() {
        if !assigned.insert(root.clone()) {
            continue;
        }
        let mut stack = Vec::<String>::from([root]);
        let mut members = Vec::<String>::new();
        while let Some(node) = stack.pop() {
            members.push(node.clone());
            if let Some(neighbors) = reverse.get(&node) {
                for next in neighbors {
                    if assigned.insert(next.clone()) {
                        stack.push(next.clone());
                    }
                }
            }
        }
        members.sort();
        components.push(members);
    }
    components
}

fn note_folder(path: &str) -> &str {
    Path::new(path)
        .parent()
        .and_then(Path::to_str)
        .unwrap_or_default()
}

fn parent_folder(folder: &str) -> Option<&str> {
    if folder.is_empty() {
        return None;
    }
    Path::new(folder)
        .parent()
        .and_then(Path::to_str)
        .or(Some(""))
}

fn graph_walk_edge_type_label(edge_type: &GraphWalkEdgeType) -> &'static str {
    match edge_type {
        GraphWalkEdgeType::Wikilink => "wikilink",
        GraphWalkEdgeType::Markdown => "markdown",
        GraphWalkEdgeType::Embed => "embed",
        GraphWalkEdgeType::FolderParent => "folder-parent",
        GraphWalkEdgeType::FolderSibling => "folder-sibling",
    }
}

fn map_link_edges(rows: Vec<tao_sdk_storage::LinkWithPaths>) -> Vec<LinkGraphEdge> {
    rows.into_iter()
        .map(|row| LinkGraphEdge {
            link_id: row.link_id,
            source_file_id: row.source_file_id,
            source_path: row.source_path,
            raw_target: row.raw_target,
            resolved_file_id: row.resolved_file_id,
            resolved_path: row.resolved_path,
            heading_slug: row.heading_slug,
            block_id: row.block_id,
            is_unresolved: row.is_unresolved,
            unresolved_reason: row.unresolved_reason,
            source_field: row.source_field,
            evidence: None,
        })
        .collect()
}

fn map_graph_node_degrees(rows: Vec<tao_sdk_storage::GraphNodeDegree>) -> Vec<GraphNodeDegreeRow> {
    rows.into_iter()
        .map(|row| GraphNodeDegreeRow {
            file_id: row.file_id,
            path: row.path,
            incoming_resolved: row.incoming_resolved,
            outgoing_resolved: row.outgoing_resolved,
        })
        .collect()
}

/// Link graph query failures.
#[derive(Debug, Error)]
pub enum LinkGraphServiceError {
    /// File lookup by normalized path failed.
    #[error("failed to query file metadata for link graph: {source}")]
    FilesRepository {
        /// Files repository error.
        #[source]
        source: tao_sdk_storage::FilesRepositoryError,
    },
    /// Link graph query failed.
    #[error("failed to query link graph rows: {source}")]
    LinksRepository {
        /// Links repository error.
        #[source]
        source: tao_sdk_storage::LinksRepositoryError,
    },
    /// Bounded graph SQL or evidence decoding failed.
    #[error("graph query failed: {source}")]
    Sql {
        /// SQLite error.
        #[source]
        source: rusqlite::Error,
    },
    /// The graph exceeds a supported preparation budget.
    #[error("graph {resource} exceeds the supported work limit of {limit}")]
    ResourceLimit {
        /// Resource being bounded.
        resource: &'static str,
        /// Maximum supported amount.
        limit: u64,
    },
    /// Traversal exceeded caller-provided bounds.
    #[error("graph traversal aborted after exploring {max_nodes} nodes; increase --max-nodes")]
    TraversalLimit {
        /// Maximum allowed discovered nodes.
        max_nodes: u32,
    },
}

/// Direction selector for occurrence windows and bounded adjacency retrieval.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphLinkDirection {
    /// All incoming/outgoing occurrences, with self-links returned once.
    All,
    /// Source occurrences, including unresolved targets.
    Outgoing,
    /// Incoming references to a known file.
    Incoming,
}

/// A bounded walk describes returned evidence without inventing an exact global total.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphWalkResult {
    /// Unique edge occurrences in traversal order.
    pub items: Vec<GraphWalkStep>,
    /// Whether the traversal exhausted the reachable selected graph.
    pub complete: bool,
    /// Output/work/depth bound preventing a complete traversal.
    pub truncation_reason: Option<String>,
    /// Adjacency rows actually examined.
    pub examined_edges: u64,
    /// Discovered file nodes.
    pub discovered_nodes: u64,
}
impl GraphWalkResult {
    fn truncate(mut self, reason: &str) -> Self {
        self.complete = false;
        self.truncation_reason = Some(reason.to_string());
        self
    }
}

fn graph_sql(source: rusqlite::Error) -> LinkGraphServiceError {
    LinkGraphServiceError::Sql { source }
}
fn graph_edge_select() -> &'static str {
    "SELECT l.link_id,l.source_file_id,sf.normalized_path source_path,l.raw_target,l.resolved_file_id,tf.normalized_path resolved_path,l.heading_slug,l.block_id,l.is_unresolved,l.unresolved_reason,l.source_field FROM links l JOIN files sf ON sf.file_id=l.source_file_id LEFT JOIN files tf ON tf.file_id=l.resolved_file_id"
}
fn graph_edge_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<LinkGraphEdge> {
    Ok(LinkGraphEdge {
        link_id: row.get("link_id")?,
        source_file_id: row.get("source_file_id")?,
        source_path: row.get("source_path")?,
        raw_target: row.get("raw_target")?,
        resolved_file_id: row.get("resolved_file_id")?,
        resolved_path: row.get("resolved_path")?,
        heading_slug: row.get("heading_slug")?,
        block_id: row.get("block_id")?,
        is_unresolved: row.get("is_unresolved")?,
        unresolved_reason: row.get("unresolved_reason")?,
        source_field: row.get("source_field")?,
        evidence: None,
    })
}
fn enrich_edges(
    connection: &Connection,
    mut edges: Vec<LinkGraphEdge>,
) -> Result<Vec<LinkGraphEdge>, LinkGraphServiceError> {
    for edge in &mut edges {
        if let Some(evidence) =
            LinkEvidenceRepository::get_by_link_id(connection, &edge.link_id).map_err(graph_sql)?
        {
            edge.evidence = Some(
                serde_json::json!({"source_span_available":evidence.line > 0,"source_start":(evidence.line > 0).then_some(evidence.source_start),"source_end":(evidence.line > 0).then_some(evidence.source_end),"line":(evidence.line > 0).then_some(evidence.line),"end_line":(evidence.line > 0).then_some(evidence.end_line),"raw_expression":evidence.raw_expression,"syntax":evidence.syntax,"fragment":serde_json::from_str::<serde_json::Value>(&evidence.fragment_json).unwrap_or(serde_json::Value::Null),"fragment_status":evidence.fragment_status,"resolution_rule":evidence.resolution_rule,"candidates":serde_json::from_str::<serde_json::Value>(&evidence.candidates_json).unwrap_or(serde_json::Value::Null)}),
            );
        }
    }
    Ok(edges)
}
fn adjacency_window(
    connection: &Connection,
    id: &str,
    direction: GraphLinkDirection,
    include_unresolved: bool,
    limit: u64,
) -> Result<Vec<LinkGraphEdge>, LinkGraphServiceError> {
    let predicate = if direction == GraphLinkDirection::Outgoing {
        "l.source_file_id=?1"
    } else {
        "l.resolved_file_id=?1"
    };
    let resolved = if include_unresolved {
        ""
    } else {
        " AND l.is_unresolved=0"
    };
    let query = format!(
        "{} WHERE {predicate}{resolved} ORDER BY l.link_id LIMIT ?2",
        graph_edge_select()
    );
    let mut statement = connection.prepare_cached(&query).map_err(graph_sql)?;
    statement
        .query_map(params![id, limit], graph_edge_row)
        .map_err(graph_sql)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(graph_sql)
}
fn folder_window(
    connection: &Connection,
    id: &str,
    path: &str,
    limit: u64,
) -> Result<Vec<(String, String, GraphWalkEdgeType)>, LinkGraphServiceError> {
    let folder = note_folder(path);
    let parent = parent_folder(folder);
    // instr/substr compare literal directory prefixes: '_' and '%' remain filenames.
    let mut statement = connection.prepare_cached("SELECT file_id,normalized_path FROM files WHERE file_id<>?1 AND ((substr(normalized_path,1,length(?2))=?2 AND instr(substr(normalized_path,length(?2)+1),'/')=0) OR (?3 IS NOT NULL AND substr(normalized_path,1,length(?3))=?3 AND instr(substr(normalized_path,length(?3)+1),'/')=0)) ORDER BY normalized_path LIMIT ?4").map_err(graph_sql)?;
    let prefix = if folder.is_empty() {
        String::new()
    } else {
        format!("{folder}/")
    };
    let parent_prefix = parent.map(|parent| {
        if parent.is_empty() {
            String::new()
        } else {
            format!("{parent}/")
        }
    });
    statement
        .query_map(params![id, prefix, parent_prefix, limit], |row| {
            let target: String = row.get(1)?;
            let kind = if note_folder(&target) == folder {
                GraphWalkEdgeType::FolderSibling
            } else {
                GraphWalkEdgeType::FolderParent
            };
            Ok((row.get(0)?, target, kind))
        })
        .map_err(graph_sql)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(graph_sql)
}
fn path_outcome(
    path: Vec<String>,
    nodes: usize,
    examined_edges: u64,
    reason: Option<&str>,
) -> GraphPathResult {
    GraphPathResult {
        found: !path.is_empty(),
        explored_nodes: u32::try_from(nodes).unwrap_or(u32::MAX),
        path,
        complete: reason.is_none(),
        truncation_reason: reason.map(str::to_string),
        examined_edges,
    }
}
fn reconstruct_path(
    from: &str,
    to: &str,
    parents: &HashMap<String, String>,
    paths: &HashMap<String, String>,
) -> Vec<String> {
    let mut ids = vec![to];
    let mut current = to;
    while current != from {
        let Some(parent) = parents.get(current) else {
            return Vec::new();
        };
        ids.push(parent);
        current = parent;
    }
    ids.reverse();
    ids.into_iter()
        .filter_map(|id| paths.get(id).cloned())
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ComponentCacheKey {
    database: String,
    file_identity: String,
    generation: i64,
    strong: bool,
}
type ComponentCacheEntry = (ComponentCacheKey, Arc<Vec<GraphComponentRow>>);
static COMPONENT_CACHE: OnceLock<Mutex<VecDeque<ComponentCacheEntry>>> = OnceLock::new();

fn component_snapshot(
    connection: &Connection,
    mode: GraphComponentMode,
    cache_allowed: bool,
) -> Result<Arc<Vec<GraphComponentRow>>, LinkGraphServiceError> {
    const MAX_COMPONENT_FILES: u64 = 100_000;
    const MAX_COMPONENT_EDGES: u64 = 1_000_000;
    let generation = IndexGenerationRepository::get(connection).map_err(graph_sql)?;
    if generation.files_total > MAX_COMPONENT_FILES {
        return Err(LinkGraphServiceError::ResourceLimit {
            resource: "nodes",
            limit: MAX_COMPONENT_FILES,
        });
    }
    let cache_key = connection
        .path()
        .filter(|_| cache_allowed)
        .filter(|path| !path.is_empty())
        .and_then(|path| {
            let metadata = std::fs::metadata(path).ok()?;
            #[cfg(unix)]
            let file_identity = {
                use std::os::unix::fs::MetadataExt;
                format!("{}:{}", metadata.dev(), metadata.ino())
            };
            #[cfg(not(unix))]
            let file_identity = format!("{:?}", metadata.created().ok());
            Some(ComponentCacheKey {
                database: path.to_string(),
                file_identity,
                generation: generation.canonical_generation,
                strong: matches!(mode, GraphComponentMode::Strong),
            })
        });
    let cache = COMPONENT_CACHE.get_or_init(|| Mutex::new(VecDeque::new()));
    if let Some(key) = &cache_key
        && let Ok(mut entries) = cache.lock()
        && let Some(position) = entries.iter().position(|(old, _)| old == key)
    {
        let entry = entries.remove(position).expect("position came from cache");
        let snapshot = Arc::clone(&entry.1);
        entries.push_back(entry);
        return Ok(snapshot);
    }
    let edge_count: u64 = connection
        .query_row(
            "SELECT COUNT(*) FROM links WHERE resolved_file_id IS NOT NULL AND is_unresolved=0",
            [],
            |row| row.get(0),
        )
        .map_err(graph_sql)?;
    if edge_count > MAX_COMPONENT_EDGES {
        return Err(LinkGraphServiceError::ResourceLimit {
            resource: "edges",
            limit: MAX_COMPONENT_EDGES,
        });
    }
    let mut statement = connection
        .prepare("SELECT file_id,normalized_path FROM files ORDER BY file_id")
        .map_err(graph_sql)?;
    let files = statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(graph_sql)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(graph_sql)?;
    let paths = files.into_iter().collect::<HashMap<_, _>>();
    let mut ids = paths.keys().cloned().collect::<Vec<_>>();
    ids.sort();
    let pairs = LinksRepository::list_resolved_pairs(connection)
        .map_err(|source| LinkGraphServiceError::LinksRepository { source })?;
    let members = match mode {
        GraphComponentMode::Weak => weak_components(&ids, &pairs),
        GraphComponentMode::Strong => strong_components(&ids, &pairs),
    };
    let mut rows = build_component_rows(members, &paths, true, usize::MAX);
    rows.sort_by(|left, right| {
        right
            .size
            .cmp(&left.size)
            .then_with(|| left.paths.first().cmp(&right.paths.first()))
    });
    let snapshot = Arc::new(rows);
    // Four bounded generation snapshots at most; large snapshots remain request-local.
    let bytes = snapshot
        .iter()
        .flat_map(|row| &row.paths)
        .map(String::len)
        .sum::<usize>();
    if bytes <= 8 * 1024 * 1024
        && let Some(key) = cache_key
        && let Ok(mut entries) = cache.lock()
    {
        entries.retain(|(old, _)| old.database != key.database || old.strong != key.strong);
        entries.push_back((key, Arc::clone(&snapshot)));
        while entries.len() > 4 {
            entries.pop_front();
        }
    }
    Ok(snapshot)
}

/// Revalidate incoming physical PDF-page references inside the caller's publication
/// transaction. Unknown page counts remain pending without clearing file resolution.
pub fn revalidate_pdf_page_links(
    connection: &Connection,
    file_id: &str,
    page_count: Option<u32>,
) -> rusqlite::Result<usize> {
    let status = "CASE WHEN json_extract(fragment_json,'$.kind')='invalid_page' THEN 'bad_page' WHEN ?2 IS NULL THEN 'pending' WHEN json_extract(fragment_json,'$.value') BETWEEN 1 AND ?2 THEN 'resolved' ELSE 'bad_page' END";
    connection.execute(
        &format!("UPDATE link_evidence SET fragment_status={status} WHERE link_id IN (SELECT link_id FROM links WHERE resolved_file_id=?1) AND json_extract(fragment_json,'$.kind') IN ('page','invalid_page') AND fragment_status IS NOT ({status})"),
        params![file_id,page_count],
    )
}

#[cfg(test)]
mod correctness_tests {
    use super::*;
    use tao_sdk_storage::{FileRecordInput, LinkEvidenceInput, LinkRecordInput, run_migrations};

    fn database() -> Connection {
        let mut connection = Connection::open_in_memory().unwrap();
        run_migrations(&mut connection).unwrap();
        connection
    }
    fn file(connection: &Connection, path: &str) {
        FilesRepository::insert(
            connection,
            &FileRecordInput {
                file_id: path.into(),
                normalized_path: path.into(),
                match_key: path.into(),
                absolute_path: format!("/fixture/{path}"),
                size_bytes: 1,
                modified_unix_ms: 1,
                hash_blake3: path.into(),
                is_markdown: path.ends_with(".md"),
            },
        )
        .unwrap();
    }
    fn edge(connection: &Connection, id: &str, source: &str, target: Option<&str>, field: &str) {
        LinksRepository::insert(
            connection,
            &LinkRecordInput {
                link_id: id.into(),
                source_file_id: source.into(),
                raw_target: target.unwrap_or("missing.md").into(),
                resolved_file_id: target.map(str::to_string),
                heading_slug: None,
                block_id: None,
                is_unresolved: target.is_none(),
                unresolved_reason: target.is_none().then(|| "missing-note".into()),
                source_field: field.into(),
            },
        )
        .unwrap();
    }

    #[test]
    fn all_file_components_include_attachment_members_and_consistent_sizes() {
        let c = database();
        for path in ["a.md", "b.md", "image.png"] {
            file(&c, path);
        }
        edge(&c, "a-image", "a.md", Some("image.png"), "body:embed");
        edge(&c, "b-image", "b.md", Some("image.png"), "body:embed");
        let (total, rows) = BacklinkGraphService
            .components_page(&c, GraphComponentMode::Weak, 10, 0, true, 10)
            .unwrap();
        assert_eq!(total, 1);
        assert_eq!(rows[0].size, 3);
        assert_eq!(rows[0].paths, ["a.md", "b.md", "image.png"]);
        assert!(!rows[0].truncated);
        let (_, strong) = BacklinkGraphService
            .components_page(&c, GraphComponentMode::Strong, 10, 0, true, 10)
            .unwrap();
        assert_eq!(strong.len(), 3);
        assert!(strong.iter().all(|row| row.paths.len() as u64 == row.size));
    }

    #[test]
    fn one_hop_target_survives_a_tiny_budget_and_unrelated_neighbors() {
        let c = database();
        for path in ["a.md", "b.md", "c.md", "z.md"] {
            file(&c, path);
        }
        edge(&c, "1", "a.md", Some("b.md"), "body");
        edge(&c, "2", "a.md", Some("c.md"), "body");
        edge(&c, "3", "a.md", Some("z.md"), "body");
        let result = BacklinkGraphService
            .shortest_path(
                &c,
                &GraphPathRequest {
                    from_path: "a.md".into(),
                    to_path: "z.md".into(),
                    max_depth: 1,
                    max_nodes: 2,
                },
            )
            .unwrap();
        assert!(result.found && result.complete);
        assert_eq!(result.path, ["a.md", "z.md"]);
        assert_eq!(result.explored_nodes, 2);
        let limited = BacklinkGraphService
            .shortest_path(
                &c,
                &GraphPathRequest {
                    from_path: "a.md".into(),
                    to_path: "z.md".into(),
                    max_depth: 0,
                    max_nodes: 2,
                },
            )
            .unwrap();
        assert!(!limited.complete);
        assert_eq!(limited.truncation_reason.as_deref(), Some("max_depth"));
    }

    #[test]
    fn occurrence_windows_include_unresolved_duplicates_and_provenance() {
        let c = database();
        file(&c, "a.md");
        file(&c, "b.md");
        edge(&c, "1", "a.md", Some("b.md"), "body");
        edge(&c, "2", "a.md", Some("b.md"), "body");
        edge(&c, "3", "a.md", None, "body:markdown");
        LinkEvidenceRepository::upsert(
            &c,
            &LinkEvidenceInput {
                link_id: "2".into(),
                source_start: 10,
                source_end: 15,
                line: 2,
                end_line: 2,
                raw_expression: "[[b]]".into(),
                syntax: "wiki".into(),
                fragment_json: "null".into(),
                fragment_status: "not_requested".into(),
                resolution_rule: "wiki_discovery".into(),
                candidates_json: "[\"b.md\"]".into(),
            },
        )
        .unwrap();
        let (total, rows) = BacklinkGraphService
            .links_page(&c, "a.md", GraphLinkDirection::All, 1, 1)
            .unwrap();
        assert_eq!(total, 3);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].link_id, "2");
        assert_eq!(rows[0].evidence.as_ref().unwrap()["line"], 2);
        let (_, last) = BacklinkGraphService
            .links_page(&c, "a.md", GraphLinkDirection::Outgoing, 1, 2)
            .unwrap();
        assert!(last[0].is_unresolved);
    }

    #[test]
    fn component_cache_invalidates_across_committed_topology_generations() {
        let fixture_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target");
        let temp = tempfile::tempdir_in(fixture_root).unwrap();
        let database_path = temp.path().join("graph.sqlite");
        let mut c = Connection::open(&database_path).unwrap();
        run_migrations(&mut c).unwrap();
        file(&c, "a.md");
        file(&c, "b.md");
        edge(&c, "1", "a.md", Some("b.md"), "body");
        assert_eq!(
            BacklinkGraphService
                .components_page(&c, GraphComponentMode::Weak, 10, 0, true, 10)
                .unwrap()
                .0,
            1
        );
        c.execute("DELETE FROM links", []).unwrap();
        let second = Connection::open(&database_path).unwrap();
        assert_eq!(
            BacklinkGraphService
                .components_page(&second, GraphComponentMode::Weak, 10, 0, true, 10)
                .unwrap()
                .0,
            2
        );
    }

    #[test]
    fn component_cache_never_publishes_rolled_back_write_snapshots() {
        let fixture_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target");
        let temp = tempfile::tempdir_in(fixture_root).unwrap();
        let mut c = Connection::open(temp.path().join("graph.sqlite")).unwrap();
        run_migrations(&mut c).unwrap();
        for name in ["a.md", "b.md", "c.md"] {
            file(&c, name);
        }
        c.execute_batch("BEGIN").unwrap();
        edge(&c, "temporary", "a.md", Some("b.md"), "body");
        let (_, inside) = BacklinkGraphService
            .components_page(&c, GraphComponentMode::Weak, 10, 0, true, 10)
            .unwrap();
        assert_eq!(inside[0].paths, ["a.md", "b.md"]);
        c.execute_batch("ROLLBACK").unwrap();
        edge(&c, "committed", "a.md", Some("c.md"), "body");
        let (_, committed) = BacklinkGraphService
            .components_page(&c, GraphComponentMode::Weak, 10, 0, true, 10)
            .unwrap();
        assert_eq!(committed[0].paths, ["a.md", "c.md"]);
    }

    #[test]
    fn walk_bounds_work_and_reports_real_link_kinds() {
        let c = database();
        file(&c, "source.md");
        for index in 0..200 {
            let path = format!("target-{index:03}.pdf");
            file(&c, &path);
            edge(
                &c,
                &format!("edge-{index:03}"),
                "source.md",
                Some(&path),
                "body:embed",
            );
        }
        let result = BacklinkGraphService
            .walk_bounded(
                &c,
                &GraphWalkRequest {
                    path: "source.md".into(),
                    depth: 8,
                    limit: 3,
                    include_unresolved: false,
                    include_folders: false,
                },
            )
            .unwrap();
        assert_eq!(result.items.len(), 3);
        assert!(!result.complete);
        assert_eq!(result.truncation_reason.as_deref(), Some("limit"));
        assert!(result.examined_edges <= 4);
        assert!(
            result
                .items
                .iter()
                .all(|item| item.edge_type == GraphWalkEdgeType::Embed)
        );
    }

    #[test]
    fn walk_emits_each_occurrence_once_across_both_endpoints() {
        let c = database();
        file(&c, "a.md");
        file(&c, "b.md");
        edge(&c, "1", "a.md", Some("b.md"), "body:markdown");
        let result = BacklinkGraphService
            .walk_bounded(
                &c,
                &GraphWalkRequest {
                    path: "a.md".into(),
                    depth: 8,
                    limit: 10,
                    include_unresolved: false,
                    include_folders: false,
                },
            )
            .unwrap();
        assert!(result.complete);
        assert_eq!(result.items.len(), 1);
        assert_eq!(result.items[0].edge_type, GraphWalkEdgeType::Markdown);
    }
}

#[cfg(test)]
mod page_tests {
    use super::*;
    #[test]
    fn page_status_follows_physical_count_without_removing_document_edges() {
        let mut c = Connection::open_in_memory().unwrap();
        tao_sdk_storage::run_migrations(&mut c).unwrap();
        for (id, markdown) in [("note.md", true), ("paper.pdf", false)] {
            FilesRepository::insert(
                &c,
                &tao_sdk_storage::FileRecordInput {
                    file_id: id.into(),
                    normalized_path: id.into(),
                    match_key: id.into(),
                    absolute_path: format!("/fixture/{id}"),
                    size_bytes: 1,
                    modified_unix_ms: 1,
                    hash_blake3: id.into(),
                    is_markdown: markdown,
                },
            )
            .unwrap();
        }
        LinksRepository::insert(
            &c,
            &tao_sdk_storage::LinkRecordInput {
                link_id: "page".into(),
                source_file_id: "note.md".into(),
                raw_target: "paper.pdf".into(),
                resolved_file_id: Some("paper.pdf".into()),
                heading_slug: None,
                block_id: None,
                is_unresolved: false,
                unresolved_reason: None,
                source_field: "body:markdown".into(),
            },
        )
        .unwrap();
        LinkEvidenceRepository::upsert(
            &c,
            &tao_sdk_storage::LinkEvidenceInput {
                link_id: "page".into(),
                source_start: 0,
                source_end: 20,
                line: 1,
                end_line: 1,
                raw_expression: "[x](paper.pdf#page=2)".into(),
                syntax: "markdown".into(),
                fragment_json: r#"{"kind":"page","value":2}"#.into(),
                fragment_status: "pending".into(),
                resolution_rule: "source_relative".into(),
                candidates_json: "[]".into(),
            },
        )
        .unwrap();
        for (count, status) in [
            (Some(2), "resolved"),
            (Some(1), "bad_page"),
            (None, "pending"),
        ] {
            assert_eq!(
                revalidate_pdf_page_links(&c, "paper.pdf", count).unwrap(),
                1
            );
            assert_eq!(
                revalidate_pdf_page_links(&c, "paper.pdf", count).unwrap(),
                0
            );
            let (audit_total, audit_rows) = BacklinkGraphService
                .unresolved_links_page(&c, 10, 0)
                .unwrap();
            assert_eq!(audit_total, u64::from(status != "resolved"));
            if let Some(issue) = audit_rows.first() {
                assert!(!issue.is_unresolved);
                assert_eq!(issue.resolved_path.as_deref(), Some("paper.pdf"));
                assert_eq!(issue.evidence.as_ref().unwrap()["fragment_status"], status);
            }
            assert_eq!(
                LinkEvidenceRepository::get_by_link_id(&c, "page")
                    .unwrap()
                    .unwrap()
                    .fragment_status,
                status
            );
        }
        assert_eq!(
            BacklinkGraphService
                .links_page(&c, "paper.pdf", GraphLinkDirection::Incoming, 10, 0)
                .unwrap()
                .0,
            1
        );
    }
}
