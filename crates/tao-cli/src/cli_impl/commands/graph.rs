use super::super::*;

pub(crate) fn handle(command: GraphCommands, runtime: &mut RuntimeMode) -> Result<CommandResult> {
    match command {
        GraphCommands::Outgoing(args) => {
            let resolved = args.resolve()?;
            let path = normalize_relative_note_path_arg(&args.path, "--path")?;
            let panels = with_kernel(runtime, &resolved, |kernel| {
                expect_bridge_value(kernel.note_links(&path), "graph.outgoing")
            })?;
            let items = panels
                .outgoing
                .into_iter()
                .map(|link| {
                    serde_json::json!({
                        "source_path": link.source_path,
                        "target_path": link.target_path,
                        "heading": link.heading,
                        "block_id": link.block_id,
                        "display_text": link.display_text,
                        "kind": link.kind,
                        "resolved": link.resolved,
                    })
                })
                .collect::<Vec<_>>();
            Ok(CommandResult {
                command: "graph.outgoing".to_string(),
                summary: "graph outgoing completed".to_string(),
                args: serde_json::json!({
                    "path": path,
                    "total": items.len(),
                    "items": items,
                }),
            })
        }
        GraphCommands::Backlinks(args) => {
            let resolved = args.resolve()?;
            let path = normalize_relative_note_path_arg(&args.path, "--path")?;
            let panels = with_kernel(runtime, &resolved, |kernel| {
                expect_bridge_value(kernel.note_links(&path), "graph.backlinks")
            })?;
            let items = panels
                .backlinks
                .into_iter()
                .map(|link| {
                    serde_json::json!({
                        "source_path": link.source_path,
                        "target_path": link.target_path,
                        "heading": link.heading,
                        "block_id": link.block_id,
                        "display_text": link.display_text,
                        "kind": link.kind,
                        "resolved": link.resolved,
                    })
                })
                .collect::<Vec<_>>();
            Ok(CommandResult {
                command: "graph.backlinks".to_string(),
                summary: "graph backlinks completed".to_string(),
                args: serde_json::json!({
                    "path": path,
                    "total": items.len(),
                    "items": items,
                }),
            })
        }
        GraphCommands::InboundScope(args) => {
            if !args.include_markdown && !args.include_non_md {
                return Err(anyhow!(
                    "graph inbound-scope requires at least one file-kind selector: --include-markdown and/or --include-non-md"
                ));
            }

            let resolved = args.resolve()?;
            let mut scope = args.scope.trim().trim_matches('/').replace('\\', "/");
            if scope == "." {
                scope.clear();
            }
            if !scope.is_empty() {
                validate_relative_vault_path(&scope)
                    .map_err(|source| anyhow!("invalid --scope '{}': {source}", args.scope))?;
            }

            let mut exclude_prefixes = Vec::<String>::new();
            for prefix in &args.exclude_prefix {
                let mut normalized = prefix.trim().trim_matches('/').replace('\\', "/");
                if normalized == "." {
                    normalized.clear();
                }
                if normalized.is_empty() {
                    continue;
                }
                validate_relative_vault_path(&normalized)
                    .map_err(|source| anyhow!("invalid --exclude-prefix '{}': {source}", prefix))?;
                exclude_prefixes.push(normalized);
            }
            exclude_prefixes.sort();
            exclude_prefixes.dedup();

            let (summary, rows) = with_connection(runtime, &resolved, |connection| {
                Ok(BacklinkGraphService.scoped_inbound_page(
                    connection,
                    &GraphScopedInboundRequest {
                        scope_prefix: scope.clone(),
                        include_markdown: args.include_markdown,
                        include_non_markdown: args.include_non_md,
                        exclude_prefixes: exclude_prefixes.clone(),
                        limit: args.limit,
                        offset: args.offset,
                    },
                )?)
            })
            .map_err(|source| anyhow!("graph inbound-scope failed: {source}"))?;
            let items = rows
                .into_iter()
                .map(|row| {
                    serde_json::json!({
                        "file_id": row.file_id,
                        "path": row.path,
                        "is_markdown": row.is_markdown,
                        "inbound_resolved": row.inbound_resolved,
                        "linked": row.inbound_resolved > 0,
                    })
                })
                .collect::<Vec<_>>();
            Ok(CommandResult {
                command: "graph.inbound-scope".to_string(),
                summary: "graph inbound-scope completed".to_string(),
                args: serde_json::json!({
                    "scope": scope,
                    "include_markdown": args.include_markdown,
                    "include_non_md": args.include_non_md,
                    "exclude_prefixes": exclude_prefixes,
                    "total_files": summary.total_files,
                    "linked_files": summary.linked_files,
                    "unlinked_files": summary.unlinked_files,
                    "total": summary.total_files,
                    "limit": args.limit,
                    "offset": args.offset,
                    "items": items,
                }),
            })
        }
        GraphCommands::Unresolved(args) => {
            let resolved = args.resolve()?;
            let (total, rows) = with_connection(runtime, &resolved, |connection| {
                Ok(BacklinkGraphService.unresolved_links_page(
                    connection,
                    args.limit,
                    args.offset,
                )?)
            })
            .map_err(|source| anyhow!("query unresolved links failed: {source}"))?;
            let items = rows.into_iter().map(link_edge_to_json).collect::<Vec<_>>();
            Ok(CommandResult {
                command: "graph.unresolved".to_string(),
                summary: "graph unresolved completed".to_string(),
                args: serde_json::json!({
                    "total": total,
                    "limit": args.limit,
                    "offset": args.offset,
                    "items": items,
                }),
            })
        }
        GraphCommands::Deadends(args) => {
            let resolved = args.resolve()?;
            let (total, rows) = with_connection(runtime, &resolved, |connection| {
                Ok(BacklinkGraphService.deadends_page(connection, args.limit, args.offset)?)
            })
            .map_err(|source| anyhow!("query deadends failed: {source}"))?;
            let items = rows
                .into_iter()
                .map(|row| {
                    serde_json::json!({
                        "file_id": row.file_id,
                        "path": row.path,
                        "incoming_resolved": row.incoming_resolved,
                        "outgoing_resolved": row.outgoing_resolved,
                    })
                })
                .collect::<Vec<_>>();
            Ok(CommandResult {
                command: "graph.deadends".to_string(),
                summary: "graph deadends completed".to_string(),
                args: serde_json::json!({
                    "total": total,
                    "limit": args.limit,
                    "offset": args.offset,
                    "items": items,
                }),
            })
        }
        GraphCommands::Orphans(args) => {
            let resolved = args.resolve()?;
            let (total, rows) = with_connection(runtime, &resolved, |connection| {
                Ok(BacklinkGraphService.orphans_page(connection, args.limit, args.offset)?)
            })
            .map_err(|source| anyhow!("query orphans failed: {source}"))?;
            let items = rows
                .into_iter()
                .map(|row| {
                    serde_json::json!({
                        "file_id": row.file_id,
                        "path": row.path,
                        "incoming_resolved": row.incoming_resolved,
                        "outgoing_resolved": row.outgoing_resolved,
                    })
                })
                .collect::<Vec<_>>();
            Ok(CommandResult {
                command: "graph.orphans".to_string(),
                summary: "graph orphans completed".to_string(),
                args: serde_json::json!({
                    "total": total,
                    "limit": args.limit,
                    "offset": args.offset,
                    "items": items,
                }),
            })
        }
        GraphCommands::Floating(args) => {
            let resolved = args.resolve()?;
            let (summary, rows) = with_connection(runtime, &resolved, |connection| {
                Ok(BacklinkGraphService.floating_page(connection, args.limit, args.offset)?)
            })
            .map_err(|source| anyhow!("query floating files failed: {source}"))?;
            let items = rows
                .into_iter()
                .map(|row| {
                    serde_json::json!({
                        "path": row.path,
                        "reason": "no_incoming_no_outgoing",
                        "is_markdown": row.is_markdown,
                    })
                })
                .collect::<Vec<_>>();
            Ok(CommandResult {
                command: "graph.floating".to_string(),
                summary: "graph floating completed".to_string(),
                args: serde_json::json!({
                    "total_floating": summary.total_files,
                    "notes_count": summary.markdown_files,
                    "attachments_count": summary.non_markdown_files,
                    "total": summary.total_files,
                    "limit": args.limit,
                    "offset": args.offset,
                    "items": items,
                }),
            })
        }
        GraphCommands::Components(args) => {
            let resolved = args.resolve()?;
            let mode = GraphComponentModeArg::parse(args.mode.trim())?;
            let (total, rows) = with_connection(runtime, &resolved, |connection| {
                Ok(BacklinkGraphService.components_page(
                    connection,
                    mode.as_service_mode(),
                    args.limit,
                    args.offset,
                    args.include_members,
                    args.sample_size as usize,
                )?)
            })
            .map_err(|source| anyhow!("query graph components failed: {source}"))?;
            let items = rows
                .into_iter()
                .map(|row| {
                    serde_json::json!({
                        "size": row.size,
                        "paths": row.paths,
                        "truncated": row.truncated,
                    })
                })
                .collect::<Vec<_>>();
            Ok(CommandResult {
                command: "graph.components".to_string(),
                summary: "graph components completed".to_string(),
                args: serde_json::json!({
                    "mode": mode.as_str(),
                    "total": total,
                    "limit": args.limit,
                    "offset": args.offset,
                    "include_members": args.include_members,
                    "sample_size": args.sample_size,
                    "items": items,
                }),
            })
        }
        GraphCommands::Neighbors(args) => {
            let resolved = args.resolve()?;
            let path = normalize_relative_note_path_arg(&args.path, "--path")?;
            let direction = GraphNeighborDirection::parse(args.direction.trim())?;
            let (total, items) = with_connection(runtime, &resolved, |connection| {
                let mut rows = Vec::<serde_json::Value>::new();

                if matches!(
                    direction,
                    GraphNeighborDirection::All | GraphNeighborDirection::Outgoing
                ) {
                    let outgoing = BacklinkGraphService.outgoing_for_path(connection, &path)?;
                    for edge in outgoing {
                        let Some(target_path) = edge.resolved_path.clone() else {
                            continue;
                        };
                        rows.push(serde_json::json!({
                            "path": target_path,
                            "direction": "outgoing",
                            "link_id": edge.link_id,
                            "source_path": edge.source_path,
                            "raw_target": edge.raw_target,
                        }));
                    }
                }

                if matches!(
                    direction,
                    GraphNeighborDirection::All | GraphNeighborDirection::Incoming
                ) {
                    let incoming = BacklinkGraphService.backlinks_for_path(connection, &path)?;
                    for edge in incoming {
                        rows.push(serde_json::json!({
                            "path": edge.source_path,
                            "direction": "incoming",
                            "link_id": edge.link_id,
                            "source_path": edge.source_path,
                            "raw_target": edge.raw_target,
                        }));
                    }
                }

                rows.sort_by(|left, right| {
                    let left_path = left
                        .get("path")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or_default();
                    let right_path = right
                        .get("path")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or_default();
                    let left_direction = left
                        .get("direction")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or_default();
                    let right_direction = right
                        .get("direction")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or_default();
                    left_path
                        .cmp(right_path)
                        .then_with(|| left_direction.cmp(right_direction))
                });
                rows.dedup_by(|left, right| {
                    left.get("path") == right.get("path")
                        && left.get("direction") == right.get("direction")
                });

                let total = u64::try_from(rows.len()).unwrap_or(u64::MAX);
                let items = paginate_json_items(rows, args.limit, args.offset);
                Ok((total, items))
            })
            .map_err(|source| anyhow!("graph neighbors failed: {source}"))?;
            Ok(CommandResult {
                command: "graph.neighbors".to_string(),
                summary: "graph neighbors completed".to_string(),
                args: serde_json::json!({
                    "path": path,
                    "direction": args.direction,
                    "total": total,
                    "limit": args.limit,
                    "offset": args.offset,
                    "items": items,
                }),
            })
        }
        GraphCommands::Path(args) => {
            if args.max_nodes == 0 {
                return Err(anyhow!("--max-nodes must be greater than zero"));
            }
            let resolved = args.resolve()?;
            let from = normalize_relative_note_path_arg(&args.from, "--from")?;
            let to = normalize_relative_note_path_arg(&args.to, "--to")?;
            let (found, explored_nodes, path) = with_connection(runtime, &resolved, |connection| {
                let Some(from_file) = FilesRepository::get_by_normalized_path(connection, &from)? else {
                    return Ok((false, 0_u32, Vec::<String>::new()));
                };
                let Some(to_file) = FilesRepository::get_by_normalized_path(connection, &to)? else {
                    return Ok((false, 0_u32, Vec::<String>::new()));
                };

                if from_file.file_id == to_file.file_id {
                    return Ok((true, 1_u32, vec![from_file.normalized_path]));
                }

                let file_rows = FilesRepository::list_all(connection)?;
                let mut path_by_file_id = HashMap::<String, String>::new();
                for row in file_rows {
                    if row.is_markdown {
                        path_by_file_id.insert(row.file_id, row.normalized_path);
                    }
                }

                let pairs = LinksRepository::list_resolved_pairs(connection)?;
                let mut adjacency = HashMap::<String, Vec<String>>::new();
                for pair in pairs {
                    adjacency
                        .entry(pair.source_file_id.clone())
                        .or_default()
                        .push(pair.target_file_id.clone());
                    adjacency
                        .entry(pair.target_file_id)
                        .or_default()
                        .push(pair.source_file_id);
                }
                for neighbors in adjacency.values_mut() {
                    neighbors.sort();
                    neighbors.dedup();
                }

                let from_id = from_file.file_id;
                let to_id = to_file.file_id;
                let mut queue = VecDeque::<String>::from([from_id.clone()]);
                let mut depth_by_id = HashMap::<String, u32>::new();
                let mut parent_by_id = HashMap::<String, String>::new();
                depth_by_id.insert(from_id.clone(), 0);
                let mut explored_nodes: u32 = 1;

                while let Some(current) = queue.pop_front() {
                    if current == to_id {
                        break;
                    }
                    let current_depth = *depth_by_id.get(&current).unwrap_or(&0);
                    if current_depth >= args.max_depth {
                        continue;
                    }
                    if let Some(neighbors) = adjacency.get(&current) {
                        for next in neighbors {
                            if depth_by_id.contains_key(next) {
                                continue;
                            }
                            explored_nodes = explored_nodes.saturating_add(1);
                            if explored_nodes > args.max_nodes {
                                return Err(anyhow!(
                                    "graph path aborted after exploring {} nodes; increase --max-nodes",
                                    args.max_nodes
                                ));
                            }
                            depth_by_id.insert(next.clone(), current_depth + 1);
                            parent_by_id.insert(next.clone(), current.clone());
                            queue.push_back(next.clone());
                        }
                    }
                }

                if !depth_by_id.contains_key(&to_id) {
                    return Ok((false, explored_nodes, Vec::<String>::new()));
                }

                let mut path_ids = vec![to_id.clone()];
                let mut cursor = to_id;
                while let Some(parent) = parent_by_id.get(&cursor).cloned() {
                    path_ids.push(parent.clone());
                    if parent == from_id {
                        break;
                    }
                    cursor = parent;
                }
                path_ids.reverse();
                let path = path_ids
                    .into_iter()
                    .filter_map(|file_id| path_by_file_id.get(&file_id).cloned())
                    .collect::<Vec<_>>();
                Ok((true, explored_nodes, path))
            })
            .map_err(|source| anyhow!("graph path failed: {source}"))?;
            let edge_count = path.len().saturating_sub(1);
            Ok(CommandResult {
                command: "graph.path".to_string(),
                summary: "graph path completed".to_string(),
                args: serde_json::json!({
                    "from": from,
                    "to": to,
                    "found": found,
                    "max_depth": args.max_depth,
                    "max_nodes": args.max_nodes,
                    "explored_nodes": explored_nodes,
                    "edge_count": edge_count,
                    "path": path,
                }),
            })
        }
        GraphCommands::Walk(args) => {
            let resolved = args.resolve()?;
            let path = normalize_relative_note_path_arg(&args.path, "--path")?;
            let traversed = with_connection(runtime, &resolved, |connection| {
                Ok(BacklinkGraphService.walk(
                    connection,
                    &GraphWalkRequest {
                        path: path.clone(),
                        depth: args.depth,
                        limit: args.limit,
                        include_unresolved: args.include_unresolved,
                        include_folders: args.include_folders,
                    },
                )?)
            })
            .map_err(|source| anyhow!("graph walk failed: {source}"))?;
            let items = traversed
                .into_iter()
                .map(|step| {
                    let direction = match step.direction {
                        GraphWalkDirection::Outgoing => "outgoing",
                        GraphWalkDirection::Incoming => "incoming",
                    };
                    let edge_type = match step.edge_type {
                        tao_sdk_service::GraphWalkEdgeType::Wikilink => "wikilink",
                        tao_sdk_service::GraphWalkEdgeType::FolderParent => "folder-parent",
                        tao_sdk_service::GraphWalkEdgeType::FolderSibling => "folder-sibling",
                    };
                    serde_json::json!({
                        "depth": step.depth,
                        "direction": direction,
                        "edge_type": edge_type,
                        "link_id": step.link_id,
                        "source_path": step.source_path,
                        "target_path": step.target_path,
                        "raw_target": step.raw_target,
                        "resolved": step.resolved,
                    })
                })
                .collect::<Vec<_>>();
            Ok(CommandResult {
                command: "graph.walk".to_string(),
                summary: "graph walk completed".to_string(),
                args: serde_json::json!({
                    "path": path,
                    "depth": args.depth,
                    "include_folders": args.include_folders,
                    "total": items.len(),
                    "items": items,
                }),
            })
        }
    }
}

pub(in crate::cli_impl) fn dispatch(
    command: GraphCommands,
    runtime: &mut RuntimeMode,
) -> Result<CommandResult> {
    handle(command, runtime)
}
