//! Backlink graph traversal and analysis service.

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::Path;

use rusqlite::Connection;
use tao_sdk_storage::{FilesRepository, LinksRepository};
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
    /// Number of markdown nodes in the component.
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
        Ok(map_link_edges(rows))
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
        Ok(map_link_edges(rows))
    }

    /// List unresolved edges across vault.
    pub fn unresolved_links(
        &self,
        connection: &Connection,
    ) -> Result<Vec<LinkGraphEdge>, LinkGraphServiceError> {
        let rows = LinksRepository::list_unresolved_with_paths(connection)
            .map_err(|source| LinkGraphServiceError::LinksRepository { source })?;
        Ok(map_link_edges(rows))
    }

    /// List one unresolved edges window across vault.
    pub fn unresolved_links_page(
        &self,
        connection: &Connection,
        limit: u32,
        offset: u32,
    ) -> Result<(u64, Vec<LinkGraphEdge>), LinkGraphServiceError> {
        let total = LinksRepository::count_unresolved(connection)
            .map_err(|source| LinkGraphServiceError::LinksRepository { source })?;
        let rows = LinksRepository::list_unresolved_with_paths_window(connection, limit, offset)
            .map_err(|source| LinkGraphServiceError::LinksRepository { source })?;
        Ok((total, map_link_edges(rows)))
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
        let markdown_files = FilesRepository::list_all(connection)
            .map_err(|source| LinkGraphServiceError::FilesRepository { source })?
            .into_iter()
            .filter(|file| file.is_markdown)
            .map(|file| (file.file_id, file.normalized_path))
            .collect::<Vec<_>>();
        let mut paths_by_id = HashMap::with_capacity(markdown_files.len());
        let mut ids = Vec::with_capacity(markdown_files.len());
        for (file_id, path) in markdown_files {
            ids.push(file_id.clone());
            paths_by_id.insert(file_id, path);
        }
        ids.sort();

        let pairs = LinksRepository::list_resolved_pairs(connection)
            .map_err(|source| LinkGraphServiceError::LinksRepository { source })?;
        let components_by_ids = match mode {
            GraphComponentMode::Weak => weak_components(&ids, &pairs),
            GraphComponentMode::Strong => strong_components(&ids, &pairs),
        };
        let mut components = build_component_rows(
            components_by_ids,
            &paths_by_id,
            include_members,
            sample_size,
        );

        components.sort_by(|left, right| {
            right
                .size
                .cmp(&left.size)
                .then_with(|| left.paths.first().cmp(&right.paths.first()))
        });
        let total = u64::try_from(components.len()).unwrap_or(u64::MAX);
        let items = components
            .into_iter()
            .skip(offset as usize)
            .take(limit as usize)
            .collect::<Vec<_>>();
        Ok((total, items))
    }

    /// Walk graph neighbors from one root path using frontier SQL lookups.
    pub fn walk(
        &self,
        connection: &Connection,
        request: &GraphWalkRequest,
    ) -> Result<Vec<GraphWalkStep>, LinkGraphServiceError> {
        if request.depth == 0 || request.limit == 0 {
            return Ok(Vec::new());
        }

        let Some(start_file) =
            FilesRepository::get_by_normalized_path(connection, &request.path)
                .map_err(|source| LinkGraphServiceError::FilesRepository { source })?
        else {
            return Ok(Vec::new());
        };
        let path_by_id = FilesRepository::list_all(connection)
            .map_err(|source| LinkGraphServiceError::FilesRepository { source })?
            .into_iter()
            .filter(|row| row.is_markdown)
            .map(|row| (row.file_id, row.normalized_path))
            .collect::<HashMap<_, _>>();
        let mut folder_members = HashMap::<String, Vec<String>>::new();
        if request.include_folders {
            for (file_id, path) in &path_by_id {
                folder_members
                    .entry(note_folder(path).to_string())
                    .or_default()
                    .push(file_id.clone());
            }
            for members in folder_members.values_mut() {
                members.sort();
                members.dedup();
            }
        }

        let mut steps = Vec::<GraphWalkStep>::new();
        let mut frontier = vec![start_file.file_id];
        let mut visited_depth = HashMap::<String, u32>::new();
        visited_depth.insert(frontier[0].clone(), 0);
        let hard_limit = request.limit as usize;

        for depth in 0..request.depth {
            if frontier.is_empty() || steps.len() >= hard_limit {
                break;
            }

            let outgoing = LinksRepository::list_outgoing_for_sources_with_paths(
                connection,
                &frontier,
                request.include_unresolved,
            )
            .map_err(|source| LinkGraphServiceError::LinksRepository { source })?;
            let incoming =
                LinksRepository::list_incoming_for_targets_with_paths(connection, &frontier)
                    .map_err(|source| LinkGraphServiceError::LinksRepository { source })?;

            let mut next_frontier = Vec::<String>::new();
            let next_depth = depth + 1;

            for edge in outgoing {
                if steps.len() >= hard_limit {
                    break;
                }
                let resolved = !edge.is_unresolved && edge.resolved_file_id.is_some();
                steps.push(GraphWalkStep {
                    depth: next_depth,
                    direction: GraphWalkDirection::Outgoing,
                    link_id: edge.link_id,
                    source_path: edge.source_path,
                    target_path: edge.resolved_path,
                    raw_target: edge.raw_target,
                    resolved,
                    edge_type: GraphWalkEdgeType::Wikilink,
                });

                if let Some(target_id) = edge.resolved_file_id {
                    let should_visit = visited_depth
                        .get(&target_id)
                        .map(|seen_depth| next_depth < *seen_depth)
                        .unwrap_or(true);
                    if should_visit {
                        visited_depth.insert(target_id.clone(), next_depth);
                        next_frontier.push(target_id);
                    }
                }
            }

            for edge in incoming {
                if steps.len() >= hard_limit {
                    break;
                }
                steps.push(GraphWalkStep {
                    depth: next_depth,
                    direction: GraphWalkDirection::Incoming,
                    link_id: edge.link_id,
                    source_path: edge.source_path,
                    target_path: edge.resolved_path,
                    raw_target: edge.raw_target,
                    resolved: true,
                    edge_type: GraphWalkEdgeType::Wikilink,
                });
                let source_id = edge.source_file_id;
                let should_visit = visited_depth
                    .get(&source_id)
                    .map(|seen_depth| next_depth < *seen_depth)
                    .unwrap_or(true);
                if should_visit {
                    visited_depth.insert(source_id.clone(), next_depth);
                    next_frontier.push(source_id);
                }
            }
            if request.include_folders {
                for source_id in &frontier {
                    if steps.len() >= hard_limit {
                        break;
                    }
                    let Some(source_path) = path_by_id.get(source_id) else {
                        continue;
                    };
                    let source_folder = note_folder(source_path).to_string();
                    let mut folder_targets = Vec::<(String, GraphWalkEdgeType)>::new();

                    if let Some(parent_folder) = parent_folder(&source_folder)
                        && let Some(parent_members) = folder_members.get(parent_folder)
                    {
                        for target_id in parent_members {
                            if target_id != source_id {
                                folder_targets
                                    .push((target_id.clone(), GraphWalkEdgeType::FolderParent));
                            }
                        }
                    }
                    if let Some(sibling_members) = folder_members.get(&source_folder) {
                        for target_id in sibling_members {
                            if target_id != source_id {
                                folder_targets
                                    .push((target_id.clone(), GraphWalkEdgeType::FolderSibling));
                            }
                        }
                    }

                    folder_targets.sort_by(|left, right| left.0.cmp(&right.0));
                    folder_targets.dedup();

                    for (target_id, edge_type) in folder_targets {
                        if steps.len() >= hard_limit {
                            break;
                        }
                        let Some(target_path) = path_by_id.get(&target_id).cloned() else {
                            continue;
                        };
                        let link_id = format!(
                            "folder:{source_id}:{target_id}:{}",
                            graph_walk_edge_type_label(&edge_type)
                        );
                        steps.push(GraphWalkStep {
                            depth: next_depth,
                            direction: GraphWalkDirection::Outgoing,
                            link_id,
                            source_path: source_path.clone(),
                            target_path: Some(target_path.clone()),
                            raw_target: target_path,
                            resolved: true,
                            edge_type,
                        });
                        let should_visit = visited_depth
                            .get(&target_id)
                            .map(|seen_depth| next_depth < *seen_depth)
                            .unwrap_or(true);
                        if should_visit {
                            visited_depth.insert(target_id.clone(), next_depth);
                            next_frontier.push(target_id);
                        }
                    }
                }
            }

            next_frontier.sort();
            next_frontier.dedup();
            frontier = next_frontier;
        }

        Ok(steps)
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
}
