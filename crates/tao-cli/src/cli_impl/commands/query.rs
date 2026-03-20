use super::super::*;

pub(crate) fn handle(args: QueryArgs, runtime: &mut RuntimeMode) -> Result<CommandResult> {
    let from = args.from.trim();
    let limit = args.limit.max(1);
    let where_expr = parse_where_expression_opt(args.where_clause.as_deref())
        .map_err(|source| anyhow!("parse --where failed: {source}"))?;
    let sort_keys = parse_sort_keys(args.sort.as_deref())
        .map_err(|source| anyhow!("parse --sort failed: {source}"))?;
    if from.eq_ignore_ascii_case("docs") {
        let columns = parse_query_docs_columns(args.select.as_deref())?;
        let projection = query_docs_projection(&columns);
        let logical_plan = LogicalPlanBuilder
            .build(LogicalQueryPlanRequest {
                from: from.to_string(),
                query: args.query.clone(),
                where_expr: where_expr.clone(),
                sort_keys: sort_keys.clone(),
                projection: columns
                    .iter()
                    .map(|column| column.key().to_string())
                    .collect(),
                limit: u64::from(limit),
                offset: u64::from(args.offset),
                execute: !args.explain || args.execute,
            })
            .map_err(|source| anyhow!("build logical query plan failed: {source}"))?;
        let physical_plan = PhysicalPlanOptimizer.optimize(
            PhysicalPlanBuilder
                .build(&logical_plan)
                .map_err(|source| anyhow!("build physical query plan failed: {source}"))?,
        );
        if args.explain && !args.execute {
            return Ok(CommandResult {
                command: "query.run".to_string(),
                summary: "query explain completed".to_string(),
                args: serde_json::json!({
                    "from": "docs",
                    "logical_plan": {
                        "scope": logical_plan.scope.label(),
                        "query": logical_plan.query,
                        "has_where": logical_plan.where_expr.is_some(),
                        "sort_keys": logical_plan.sort_keys.iter().map(|sort| {
                            serde_json::json!({
                                "field": sort.field,
                                "direction": match sort.direction {
                                    tao_sdk_search::SortDirection::Asc => "asc",
                                    tao_sdk_search::SortDirection::Desc => "desc",
                                },
                                "null_order": match sort.null_order {
                                    tao_sdk_search::NullOrder::First => "first",
                                    tao_sdk_search::NullOrder::Last => "last",
                                },
                            })
                        }).collect::<Vec<_>>(),
                        "projection": logical_plan.projection,
                        "limit": logical_plan.limit,
                        "offset": logical_plan.offset,
                        "execute": logical_plan.execute,
                    },
                    "physical_plan": {
                        "adapter": physical_plan.adapter.label(),
                        "stages": physical_plan.filter_stages,
                        "limit": physical_plan.limit,
                        "offset": physical_plan.offset,
                        "execute": physical_plan.execute,
                    }
                }),
            });
        }
        let resolved = args.resolve()?;
        let apply_post_filters = where_expr.is_some() || !sort_keys.is_empty();
        let query = args.query.clone().unwrap_or_default();

        let (total, rows) = if apply_post_filters {
            if sort_keys.is_empty() {
                collect_docs_rows_for_where_only(
                    runtime,
                    &resolved,
                    &query,
                    &columns,
                    where_expr
                        .as_ref()
                        .expect("apply_post_filters implies where expr when no sort keys"),
                    limit,
                    args.offset,
                )?
            } else {
                let mut accumulator =
                    QueryPostFilterAccumulator::new(args.offset, limit, &sort_keys);
                with_connection(runtime, &resolved, |connection| {
                    let mut query_offset = 0_u64;

                    loop {
                        let page = SearchQueryService.query_projected(
                            Path::new(&resolved.vault_root),
                            connection,
                            SearchQueryRequest {
                                query: query.clone(),
                                limit: QUERY_DOCS_POST_FILTER_PAGE_LIMIT,
                                offset: query_offset,
                            },
                            SearchQueryProjection::default(),
                        )?;
                        let batch_count = u64::try_from(page.items.len()).unwrap_or(u64::MAX);
                        if batch_count == 0 {
                            break;
                        }

                        let batch_rows = page
                            .items
                            .into_iter()
                            .map(query_docs_row)
                            .collect::<Vec<_>>();
                        let filtered = apply_post_filter_batch(batch_rows, where_expr.as_ref())?;
                        accumulator.push_batch(filtered);

                        query_offset = query_offset.saturating_add(batch_count);
                        if query_offset >= page.total {
                            break;
                        }
                    }

                    Ok::<(), anyhow::Error>(())
                })
                .map_err(|source| anyhow!("query docs failed: {source}"))?;
                accumulator.finish_query_docs(&columns)
            }
        } else {
            let page = with_connection(runtime, &resolved, |connection| {
                Ok(SearchQueryService.query_projected(
                    Path::new(&resolved.vault_root),
                    connection,
                    SearchQueryRequest {
                        query,
                        limit: u64::from(limit),
                        offset: u64::from(args.offset),
                    },
                    projection,
                )?)
            })
            .map_err(|source| anyhow!("query docs failed: {source}"))?;
            let rows = page
                .items
                .into_iter()
                .filter_map(|item| match project_query_docs_row(item, &columns) {
                    JsonValue::Object(map) => Some(JsonValue::Object(map)),
                    _ => None,
                })
                .collect::<Vec<_>>();
            (page.total, rows)
        };
        let selected_columns = columns
            .iter()
            .map(|column| column.key())
            .collect::<Vec<_>>();
        let mut args_payload = serde_json::json!({
            "from": "docs",
            "columns": selected_columns,
            "rows": rows,
            "total": total,
            "limit": limit,
            "offset": args.offset,
        });
        if args.explain {
            args_payload["explain"] = serde_json::json!({
                "adapter": physical_plan.adapter.label(),
                "stages": physical_plan.filter_stages,
            });
        }
        return Ok(CommandResult {
            command: "query.run".to_string(),
            summary: "query run completed".to_string(),
            args: args_payload,
        });
    }

    if let Some(base_id_or_path) = from.strip_prefix("base:") {
        let view_name = args
            .view_name
            .clone()
            .ok_or_else(|| anyhow!("query base scope requires --view-name"))?;
        let logical_plan = LogicalPlanBuilder
            .build(LogicalQueryPlanRequest {
                from: from.to_string(),
                query: args.query.clone(),
                where_expr: where_expr.clone(),
                sort_keys: sort_keys.clone(),
                projection: Vec::new(),
                limit: u64::from(limit),
                offset: u64::from(args.offset),
                execute: !args.explain || args.execute,
            })
            .map_err(|source| anyhow!("build logical query plan failed: {source}"))?;
        let physical_plan = PhysicalPlanOptimizer.optimize(
            PhysicalPlanBuilder
                .build(&logical_plan)
                .map_err(|source| anyhow!("build physical query plan failed: {source}"))?,
        );
        if args.explain && !args.execute {
            return Ok(CommandResult {
                command: "query.run".to_string(),
                summary: "query explain completed".to_string(),
                args: serde_json::json!({
                    "from": from,
                    "logical_plan": {
                        "scope": logical_plan.scope.label(),
                        "has_where": logical_plan.where_expr.is_some(),
                        "sort_keys": logical_plan.sort_keys.iter().map(|sort| {
                            serde_json::json!({
                                "field": sort.field,
                                "direction": match sort.direction {
                                    tao_sdk_search::SortDirection::Asc => "asc",
                                    tao_sdk_search::SortDirection::Desc => "desc",
                                },
                                "null_order": match sort.null_order {
                                    tao_sdk_search::NullOrder::First => "first",
                                    tao_sdk_search::NullOrder::Last => "last",
                                },
                            })
                        }).collect::<Vec<_>>(),
                        "limit": logical_plan.limit,
                        "offset": logical_plan.offset,
                        "execute": logical_plan.execute,
                    },
                    "physical_plan": {
                        "adapter": physical_plan.adapter.label(),
                        "stages": physical_plan.filter_stages,
                        "limit": physical_plan.limit,
                        "offset": physical_plan.offset,
                        "execute": physical_plan.execute,
                    }
                }),
            });
        }

        let fast_page_size = args.offset.saturating_add(limit);
        let (base_id, file_path, view_name, total, rows) = if where_expr.is_none()
            && sort_keys.is_empty()
        {
            let result = handle_base(
                BaseCommands::View(BaseViewArgs {
                    vault_root: args.vault_root.clone(),
                    db_path: args.db_path.clone(),
                    path_or_id: base_id_or_path.to_string(),
                    view_name: view_name.clone(),
                    page: 1,
                    page_size: fast_page_size.max(1),
                }),
                runtime,
            )?;
            let base_id = result
                .args
                .get("base_id")
                .cloned()
                .unwrap_or_else(|| JsonValue::String(base_id_or_path.to_string()));
            let file_path = result
                .args
                .get("file_path")
                .cloned()
                .unwrap_or(JsonValue::Null);
            let view_name = result
                .args
                .get("view_name")
                .cloned()
                .unwrap_or_else(|| JsonValue::String(view_name.clone()));
            let total = result
                .args
                .get("total")
                .and_then(JsonValue::as_u64)
                .unwrap_or(0);
            let rows = result
                .args
                .get("rows")
                .and_then(JsonValue::as_array)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .skip(args.offset as usize)
                .take(limit as usize)
                .collect::<Vec<_>>();
            (base_id, file_path, view_name, total, rows)
        } else {
            const QUERY_BASE_PAGE_SIZE: u32 = 512;
            let mut accumulator = QueryPostFilterAccumulator::new(args.offset, limit, &sort_keys);
            let mut page = 1_u32;

            loop {
                let result = handle_base(
                    BaseCommands::View(BaseViewArgs {
                        vault_root: args.vault_root.clone(),
                        db_path: args.db_path.clone(),
                        path_or_id: base_id_or_path.to_string(),
                        view_name: view_name.clone(),
                        page,
                        page_size: QUERY_BASE_PAGE_SIZE,
                    }),
                    runtime,
                )?;
                let batch_rows = result
                    .args
                    .get("rows")
                    .and_then(JsonValue::as_array)
                    .cloned()
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(|row| row.as_object().cloned())
                    .map(flatten_base_query_row)
                    .collect::<Vec<_>>();
                let filtered = apply_post_filter_batch(batch_rows, where_expr.as_ref())?;
                accumulator.push_batch(filtered);

                let has_more = result
                    .args
                    .get("has_more")
                    .and_then(JsonValue::as_bool)
                    .unwrap_or(false);
                if !has_more {
                    let (total, rows) = accumulator.finish();
                    let rows = rows
                        .into_iter()
                        .filter_map(|row| row.as_object().cloned())
                        .map(|mut row| {
                            let file_id = row.remove("file_id").unwrap_or(JsonValue::Null);
                            let file_path = row.remove("path").unwrap_or(JsonValue::Null);
                            serde_json::json!({
                                "file_id": file_id,
                                "file_path": file_path,
                                "values": row,
                            })
                        })
                        .collect::<Vec<_>>();
                    break (
                        result
                            .args
                            .get("base_id")
                            .cloned()
                            .unwrap_or_else(|| JsonValue::String(base_id_or_path.to_string())),
                        result
                            .args
                            .get("file_path")
                            .cloned()
                            .unwrap_or(JsonValue::Null),
                        result
                            .args
                            .get("view_name")
                            .cloned()
                            .unwrap_or_else(|| JsonValue::String(view_name.clone())),
                        total,
                        rows,
                    );
                }
                page = page.saturating_add(1);
            }
        };

        let mut args_payload = serde_json::json!({
            "from": from,
            "base_id": base_id,
            "file_path": file_path,
            "view_name": view_name,
            "rows": rows,
            "total": total,
            "limit": limit,
            "offset": args.offset,
        });
        if args.explain {
            args_payload["explain"] = serde_json::json!({
                "adapter": physical_plan.adapter.label(),
                "stages": physical_plan.filter_stages,
            });
        }
        return Ok(CommandResult {
            command: "query.run".to_string(),
            summary: "query run completed".to_string(),
            args: args_payload,
        });
    }

    if from.eq_ignore_ascii_case("graph") {
        let graph_result = if let Some(path) = &args.path {
            let normalized_path = normalize_relative_note_path_arg(path, "--path")?;
            let resolved = args.resolve()?;
            let panels = with_kernel(runtime, &resolved, |kernel| {
                expect_bridge_value(kernel.note_links(&normalized_path), "query.graph")
            })?;
            let outgoing = panels
                .outgoing
                .iter()
                .map(|link| {
                    serde_json::json!({
                        "direction": "outgoing",
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
            let backlinks = panels
                .backlinks
                .iter()
                .map(|link| {
                    serde_json::json!({
                        "direction": "backlinks",
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
            let mut items = outgoing.clone();
            items.extend(backlinks.clone());
            CommandResult {
                command: "graph.links".to_string(),
                summary: "graph links completed".to_string(),
                args: serde_json::json!({
                    "path": normalized_path,
                    "outgoing_total": outgoing.len(),
                    "backlinks_total": backlinks.len(),
                    "total": outgoing.len() + backlinks.len(),
                    "outgoing": outgoing,
                    "backlinks": backlinks,
                    "items": items,
                }),
            }
        } else {
            handle_graph(
                GraphCommands::Unresolved(GraphWindowArgs {
                    vault_root: args.vault_root.clone(),
                    db_path: args.db_path.clone(),
                    limit: args.limit,
                    offset: args.offset,
                }),
                runtime,
            )?
        };
        return Ok(retag_result(
            graph_result,
            "query.run",
            "query run completed",
        ));
    }

    if from.eq_ignore_ascii_case("task") {
        let task_result = handle_task(
            TaskCommands::List(TaskListArgs {
                vault_root: args.vault_root.clone(),
                db_path: args.db_path.clone(),
                state: None,
                query: args.query.clone(),
                limit: args.limit,
                offset: args.offset,
            }),
            false,
            runtime,
        )?;
        return Ok(retag_result(
            task_result,
            "query.run",
            "query run completed",
        ));
    }

    if from.eq_ignore_ascii_case("meta:tags") {
        let result = handle_meta(
            MetaCommands::Tags(GraphWindowArgs {
                vault_root: args.vault_root.clone(),
                db_path: args.db_path.clone(),
                limit: args.limit,
                offset: args.offset,
            }),
            runtime,
        )?;
        return Ok(retag_result(result, "query.run", "query run completed"));
    }

    if from.eq_ignore_ascii_case("meta:aliases") {
        let result = handle_meta(
            MetaCommands::Aliases(GraphWindowArgs {
                vault_root: args.vault_root.clone(),
                db_path: args.db_path.clone(),
                limit: args.limit,
                offset: args.offset,
            }),
            runtime,
        )?;
        return Ok(retag_result(result, "query.run", "query run completed"));
    }

    if from.eq_ignore_ascii_case("meta:properties") {
        let result = handle_meta(
            MetaCommands::Properties(GraphWindowArgs {
                vault_root: args.vault_root,
                db_path: args.db_path,
                limit: args.limit,
                offset: args.offset,
            }),
            runtime,
        )?;
        return Ok(retag_result(result, "query.run", "query run completed"));
    }

    Err(anyhow!(
        "unsupported query scope '{}'; supported scopes: docs, graph, task, meta:tags, meta:aliases, meta:properties, base:<id-or-path>",
        from
    ))
}

pub(in crate::cli_impl) fn dispatch(
    args: QueryArgs,
    runtime: &mut RuntimeMode,
) -> Result<CommandResult> {
    handle(args, runtime)
}
