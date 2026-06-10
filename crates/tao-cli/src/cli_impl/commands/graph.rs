use super::super::*;

pub(crate) fn handle(command: GraphCommands, runtime: &mut RuntimeMode) -> Result<CommandResult> {
    match command {
        GraphCommands::Links(args) => {
            let result = handle(
                GraphCommands::Neighbors(GraphNeighborsArgs {
                    vault_root: args.vault_root,
                    db_path: args.db_path,
                    path: args.path,
                    direction: args.direction,
                    limit: args.limit,
                    offset: args.offset,
                }),
                runtime,
            )?;
            Ok(retag_graph_result(result, "graph.links", None))
        }
        GraphCommands::Audit(args) => {
            let kind = args.kind.trim().to_ascii_lowercase();
            let command = match kind.as_str() {
                "unresolved" => GraphCommands::Unresolved(GraphWindowArgs {
                    vault_root: args.vault_root,
                    db_path: args.db_path,
                    limit: args.limit,
                    offset: args.offset,
                }),
                "deadends" => GraphCommands::Deadends(GraphWindowArgs {
                    vault_root: args.vault_root,
                    db_path: args.db_path,
                    limit: args.limit,
                    offset: args.offset,
                }),
                "orphans" => GraphCommands::Orphans(GraphWindowArgs {
                    vault_root: args.vault_root,
                    db_path: args.db_path,
                    limit: args.limit,
                    offset: args.offset,
                }),
                "floating" => GraphCommands::Floating(GraphWindowArgs {
                    vault_root: args.vault_root,
                    db_path: args.db_path,
                    limit: args.limit,
                    offset: args.offset,
                }),
                "components" => GraphCommands::Components(GraphComponentsArgs {
                    vault_root: args.vault_root,
                    db_path: args.db_path,
                    limit: args.limit,
                    offset: args.offset,
                    include_members: args.include_members,
                    sample_size: args.sample_size,
                    mode: args.mode,
                }),
                "inbound-scope" => GraphCommands::InboundScope(GraphInboundScopeArgs {
                    vault_root: args.vault_root,
                    db_path: args.db_path,
                    scope: args.scope.ok_or_else(|| {
                        anyhow!("graph audit --kind inbound-scope requires --scope")
                    })?,
                    include_markdown: args.include_markdown,
                    include_non_md: args.include_non_md,
                    exclude_prefix: args.exclude_prefix,
                    limit: args.limit,
                    offset: args.offset,
                }),
                _ => {
                    return Err(anyhow!(
                        "unsupported --kind '{}'; expected one of: unresolved|deadends|orphans|floating|components|inbound-scope",
                        args.kind
                    ));
                }
            };
            let result = handle(command, runtime)?;
            Ok(retag_graph_result(result, "graph.audit", Some(kind)))
        }
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
            let path_result = with_connection(runtime, &resolved, |connection| {
                Ok(BacklinkGraphService.shortest_path(
                    connection,
                    &GraphPathRequest {
                        from_path: from.clone(),
                        to_path: to.clone(),
                        max_depth: args.max_depth,
                        max_nodes: args.max_nodes,
                    },
                )?)
            })
            .map_err(|source| anyhow!("graph path failed: {source}"))?;
            let edge_count = path_result.path.len().saturating_sub(1);
            Ok(CommandResult {
                command: "graph.path".to_string(),
                summary: "graph path completed".to_string(),
                args: serde_json::json!({
                    "from": from,
                    "to": to,
                    "found": path_result.found,
                    "max_depth": args.max_depth,
                    "max_nodes": args.max_nodes,
                    "explored_nodes": path_result.explored_nodes,
                    "edge_count": edge_count,
                    "path": path_result.path,
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

fn retag_graph_result(
    mut result: CommandResult,
    command: &str,
    kind: Option<String>,
) -> CommandResult {
    result.command = command.to_string();
    result.summary = format!("{command} completed");
    if let Some(kind) = kind
        && let Some(object) = result.args.as_object_mut()
    {
        object.insert("kind".to_string(), JsonValue::String(kind));
    }
    result
}

pub(in crate::cli_impl) fn dispatch(
    command: GraphCommands,
    runtime: &mut RuntimeMode,
) -> Result<CommandResult> {
    handle(command, runtime)
}
