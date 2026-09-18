use super::super::*;

pub(crate) fn handle(command: GraphCommands, runtime: &mut RuntimeMode) -> Result<CommandResult> {
    match command {
        GraphCommands::Audit(args) => handle_audit(args, runtime),
        GraphCommands::Links(args) => {
            let resolved = args.resolve()?;
            let path = normalize_relative_note_path_arg(&args.path, "--path")?;
            let direction = GraphNeighborDirection::parse(args.direction.trim())?;
            let service_direction = match direction {
                GraphNeighborDirection::All => tao_sdk_service::GraphLinkDirection::All,
                GraphNeighborDirection::Outgoing => tao_sdk_service::GraphLinkDirection::Outgoing,
                GraphNeighborDirection::Incoming => tao_sdk_service::GraphLinkDirection::Incoming,
            };
            let (total, rows) = with_connection(runtime, &resolved, |connection| {
                Ok(BacklinkGraphService.links_page(
                    connection,
                    &path,
                    service_direction,
                    args.limit,
                    args.offset,
                )?)
            })
            .map_err(|source| anyhow!("graph occurrences failed: {source}"))?;
            let items = rows
                .into_iter()
                .map(|edge| {
                    let outgoing = service_direction
                        != tao_sdk_service::GraphLinkDirection::Incoming
                        && edge.source_path == path;
                    let neighbor = if outgoing {
                        edge.resolved_path.clone()
                    } else {
                        Some(edge.source_path.clone())
                    };
                    let mut value = link_edge_to_json(edge);
                    value["direction"] =
                        serde_json::json!(if outgoing { "outgoing" } else { "incoming" });
                    value["path"] = serde_json::json!(neighbor);
                    value
                })
                .collect::<Vec<_>>();
            Ok(CommandResult {
                command: "graph.links".to_string(),
                summary: "graph links completed".to_string(),
                args: serde_json::json!({
                    "path": path,
                    "direction": args.direction,
                    "representation": "edge_occurrences",
                    "complete": u64::from(args.offset).saturating_add(items.len() as u64) >= total,
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
                    "examined_edges": path_result.examined_edges,
                    "complete": path_result.complete,
                    "truncation_reason": path_result.truncation_reason,
                    "edge_count": edge_count,
                    "path": path_result.path,
                }),
            })
        }
        GraphCommands::Walk(args) => {
            let resolved = args.resolve()?;
            let path = normalize_relative_note_path_arg(&args.path, "--path")?;
            let traversed = with_connection(runtime, &resolved, |connection| {
                Ok(BacklinkGraphService.walk_bounded(
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
            let complete = traversed.complete;
            let truncation_reason = traversed.truncation_reason;
            let examined_edges = traversed.examined_edges;
            let discovered_nodes = traversed.discovered_nodes;
            let items = traversed
                .items
                .into_iter()
                .map(|step| {
                    let direction = match step.direction {
                        GraphWalkDirection::Outgoing => "outgoing",
                        GraphWalkDirection::Incoming => "incoming",
                    };
                    let edge_type = match step.edge_type {
                        tao_sdk_service::GraphWalkEdgeType::Wikilink => "wikilink",
                        tao_sdk_service::GraphWalkEdgeType::Markdown => "markdown",
                        tao_sdk_service::GraphWalkEdgeType::Embed => "embed",
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
                    "returned": items.len(),
                    "total": if complete { Some(items.len()) } else { None },
                    "limit": args.limit,
                    "complete": complete,
                    "truncation_reason": truncation_reason,
                    "examined_edges": examined_edges,
                    "discovered_nodes": discovered_nodes,
                    "items": items,
                }),
            })
        }
    }
}

fn handle_audit(args: GraphAuditArgs, runtime: &mut RuntimeMode) -> Result<CommandResult> {
    let kind = args.kind.trim().to_ascii_lowercase();
    if kind != "inbound-scope"
        && (args.scope.is_some()
            || args.include_markdown
            || args.include_non_md
            || !args.exclude_prefix.is_empty())
    {
        return Err(anyhow!(
            "--scope, --include-markdown, --include-non-md and --exclude-prefix require --kind inbound-scope"
        ));
    }
    if kind != "components"
        && (args.include_members || args.sample_size != 64 || args.mode != "weak")
    {
        return Err(anyhow!(
            "--include-members, --sample-size and --mode require --kind components"
        ));
    }
    let result: Result<CommandResult> = match kind.as_str() {
        "inbound-scope" => {
            if !args.include_markdown && !args.include_non_md {
                return Err(anyhow!(
                    "graph inbound-scope requires at least one file-kind selector: --include-markdown and/or --include-non-md"
                ));
            }

            let resolved = args.resolve()?;
            let scope_arg = args
                .scope
                .as_deref()
                .ok_or_else(|| anyhow!("graph audit --kind inbound-scope requires --scope"))?;
            let mut scope = scope_arg.trim().trim_matches('/').replace('\\', "/");
            if scope == "." {
                scope.clear();
            }
            if !scope.is_empty() {
                validate_relative_vault_path(&scope)
                    .map_err(|source| anyhow!("invalid --scope '{}': {source}", scope_arg))?;
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
                command: "graph.audit".to_string(),
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
        "unresolved" => {
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
                command: "graph.audit".to_string(),
                summary: "graph unresolved completed".to_string(),
                args: serde_json::json!({
                    "total": total,
                    "limit": args.limit,
                    "offset": args.offset,
                    "items": items,
                }),
            })
        }
        "deadends" => {
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
                command: "graph.audit".to_string(),
                summary: "graph deadends completed".to_string(),
                args: serde_json::json!({
                    "total": total,
                    "limit": args.limit,
                    "offset": args.offset,
                    "items": items,
                }),
            })
        }
        "orphans" => {
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
                command: "graph.audit".to_string(),
                summary: "graph orphans completed".to_string(),
                args: serde_json::json!({
                    "total": total,
                    "limit": args.limit,
                    "offset": args.offset,
                    "items": items,
                }),
            })
        }
        "floating" => {
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
                command: "graph.audit".to_string(),
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
        "components" => {
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
                command: "graph.audit".to_string(),
                summary: "graph components completed".to_string(),
                args: serde_json::json!({
                    "mode": mode.as_str(),
                    "domain": "all_files",
                    "complete": true,
                    "total": total,
                    "limit": args.limit,
                    "offset": args.offset,
                    "include_members": args.include_members,
                    "sample_size": args.sample_size,
                    "items": items,
                }),
            })
        }
        _ => {
            return Err(anyhow!(
                "unsupported --kind '{}'; expected unresolved|deadends|orphans|floating|components|inbound-scope",
                args.kind
            ));
        }
    };
    let mut result = result?;
    result.args["kind"] = serde_json::json!(kind);
    Ok(result)
}

pub(in crate::cli_impl) fn dispatch(
    command: GraphCommands,
    runtime: &mut RuntimeMode,
) -> Result<CommandResult> {
    handle(command, runtime)
}
