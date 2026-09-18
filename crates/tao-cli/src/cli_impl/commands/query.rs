use super::super::*;

/// Validate scope capabilities before opening or refreshing runtime state.
pub(crate) fn validate_capabilities(args: &QueryArgs) -> Result<()> {
    let from = args.from.trim();
    let docs = from.eq_ignore_ascii_case("docs");
    let base = from.starts_with("base:");
    let graph = from.eq_ignore_ascii_case("graph");
    let task = from.eq_ignore_ascii_case("task");
    let meta = ["meta:tags", "meta:aliases", "meta:properties"]
        .iter()
        .any(|scope| from.eq_ignore_ascii_case(scope));
    if !(docs || base || graph || task || meta) {
        return Err(CliContractError::invalid_argument(format!(
            "unsupported query scope '{from}'"
        ))
        .into());
    }
    if args.limit == 0 || args.limit > 1000 {
        return Err(
            CliContractError::invalid_argument("query --limit must be between 1 and 1000").into(),
        );
    }
    let reject = |flag: &str| -> anyhow::Error {
        CliContractError::invalid_argument(format!("{flag} is not supported for query scope '{from}'; run tao tools query.run for scope capabilities")).into()
    };
    if !docs && args.select.is_some() {
        return Err(reject("--select"));
    }
    if !docs && !base && args.where_clause.is_some() {
        return Err(reject("--where"));
    }
    if !docs && !base && args.sort.is_some() {
        return Err(reject("--sort"));
    }
    if !graph && args.path.is_some() {
        return Err(reject("--path"));
    }
    if !base && args.view_name.is_some() {
        return Err(reject("--view-name"));
    }
    if base
        && (from == "base:"
            || args
                .view_name
                .as_deref()
                .is_none_or(|v| v.trim().is_empty()))
    {
        return Err(CliContractError::invalid_argument(
            "query base scope requires a base identifier and --view-name",
        )
        .into());
    }
    if !docs && !base && !task && args.query.is_some() {
        return Err(reject("--query"));
    }
    if args.execute && !args.explain {
        return Err(CliContractError::invalid_argument("--execute requires --explain").into());
    }
    Ok(())
}

pub(crate) fn handle(args: QueryArgs, runtime: &mut RuntimeMode) -> Result<CommandResult> {
    validate_capabilities(&args)?;
    let from = args.from.trim();
    let limit = args.limit;
    let where_expr =
        parse_where_expression_opt(args.where_clause.as_deref()).map_err(|source| {
            CliContractError::query_parse_error(format!("parse --where failed: {source}"))
        })?;
    let sort_keys = parse_sort_keys(args.sort.as_deref()).map_err(|source| {
        CliContractError::query_parse_error(format!("parse --sort failed: {source}"))
    })?;
    if args.explain
        && !args.execute
        && !from.eq_ignore_ascii_case("docs")
        && !from.starts_with("base:")
    {
        return Ok(CommandResult {
            command: "query.run".into(),
            summary: "query explain completed".into(),
            args: {
                let mut plan = generic_window_plan(&args, false);
                plan["from"] = serde_json::json!(from);
                plan
            },
        });
    }
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
            .map_err(|source| query_planning_error("build logical query plan failed", source))?;
        let physical_plan =
            PhysicalPlanOptimizer.optimize(PhysicalPlanBuilder.build(&logical_plan).map_err(
                |source| query_planning_error("build physical query plan failed", source),
            )?);
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
                    if query.trim().is_empty() {
                        let mut offset = 0;
                        loop {
                            let records =
                                SearchSegmentRepository::list_docs_page(connection, 512, offset)?;
                            let count = records.len();
                            let batch_rows = records
                                .into_iter()
                                .map(query_docs_row_from_segment)
                                .collect();
                            accumulator.push_batch(apply_post_filter_batch(
                                batch_rows,
                                where_expr.as_ref(),
                            )?)?;
                            offset += count as u64;
                            if count < 512 {
                                break;
                            }
                        }
                        return Ok::<(), anyhow::Error>(());
                    }

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
                        accumulator.push_batch(filtered)?;

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
                "limit": args.limit,
                "offset": args.offset,
                "execute": true,
            });
        }
        return Ok(CommandResult {
            command: "query.run".to_string(),
            summary: "query run completed".to_string(),
            args: args_payload,
        });
    }

    if let Some(base_id_or_path) = from.strip_prefix("base:") {
        let view_name = args.view_name.clone().ok_or_else(|| {
            CliContractError::invalid_argument("query base scope requires --view-name")
        })?;
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
            .map_err(|source| query_planning_error("build logical query plan failed", source))?;
        let physical_plan =
            PhysicalPlanOptimizer.optimize(PhysicalPlanBuilder.build(&logical_plan).map_err(
                |source| query_planning_error("build physical query plan failed", source),
            )?);
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

        let query_filter = args
            .query
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToString::to_string);
        let requires_post_filter =
            where_expr.is_some() || !sort_keys.is_empty() || query_filter.is_some();
        let resolved = args.resolve()?;
        let (base, plan, page) = with_connection(runtime, &resolved, |connection| {
            let base = BasesRepository::list_with_paths(connection)?
                .into_iter()
                .find(|base| base.base_id == base_id_or_path || base.file_path == base_id_or_path)
                .ok_or_else(|| anyhow!("base id/path not found: {base_id_or_path}"))?;
            let document = decode_base_document(&base.config_json)
                .with_context(|| format!("decode base document '{}'", base.file_path))?;
            let registry = BaseViewRegistry::from_document(&document)
                .map_err(|source| anyhow!("decode base view registry failed: {source}"))?;
            let mut plan = BaseTableQueryPlanner.compile(
                &registry,
                &TableQueryPlanRequest {
                    view_name: view_name.clone(),
                    page: 1,
                    page_size: limit,
                },
            )?;
            plan.offset = args.offset as usize;
            let options = BaseTableExecutionOptions {
                include_summaries: false,
                coercion_mode: tao_sdk_bases::BaseCoercionMode::Permissive,
                case_policy: resolved.case_policy,
            };
            let page = if requires_post_filter {
                BaseTableExecutorService.execute_all_with_options(connection, &plan, options)?
            } else {
                BaseTableExecutorService.execute_with_options(connection, &plan, options)?
            };
            Ok((base, plan, page))
        })?;
        let rows = page
            .rows
            .into_iter()
            .map(|row| {
                serde_json::json!({
                    "file_id": row.file_id,
                    "file_path": row.file_path,
                    "values": row.values,
                })
            })
            .collect::<Vec<_>>();
        let (total, rows) = if requires_post_filter {
            let rows = rows
                .into_iter()
                .filter_map(|row| row.as_object().cloned())
                .map(flatten_base_query_row)
                .collect();
            let mut filtered = apply_post_filter_batch(rows, where_expr.as_ref())?;
            if let Some(query_filter) = query_filter.as_deref() {
                filtered.retain(|row| row_matches_text_query(row, query_filter));
            }
            let mut accumulator = QueryPostFilterAccumulator::new(args.offset, limit, &sort_keys);
            accumulator.push_batch(filtered)?;
            let (total, rows) = accumulator.finish();
            let rows = rows
                .into_iter()
                .filter_map(|row| row.as_object().cloned())
                .map(|mut row| {
                    let file_id = row.remove("file_id").unwrap_or(JsonValue::Null);
                    let file_path = row.remove("path").unwrap_or(JsonValue::Null);
                    serde_json::json!({"file_id":file_id,"file_path":file_path,"values":row})
                })
                .collect();
            (total, rows)
        } else {
            (page.total, rows)
        };
        let base_id = base.base_id;
        let file_path = base.file_path;
        let view_name = plan.view_name;

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
                "limit": args.limit,
                "offset": args.offset,
                "execute": true,
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
            let (total, outgoing_total, backlinks_total, edges) =
                with_connection(runtime, &resolved, |connection| {
                    use tao_sdk_service::{BacklinkGraphService, GraphLinkDirection};
                    let (total, edges) = BacklinkGraphService.links_page(
                        connection,
                        &normalized_path,
                        GraphLinkDirection::All,
                        args.limit,
                        args.offset,
                    )?;
                    let (outgoing_total, _) = BacklinkGraphService.links_page(
                        connection,
                        &normalized_path,
                        GraphLinkDirection::Outgoing,
                        0,
                        0,
                    )?;
                    let (backlinks_total, _) = BacklinkGraphService.links_page(
                        connection,
                        &normalized_path,
                        GraphLinkDirection::Incoming,
                        0,
                        0,
                    )?;
                    Ok((total, outgoing_total, backlinks_total, edges))
                })?;
            let items = edges
                .iter()
                .map(|edge| {
                    let mut value = link_edge_to_json(edge.clone());
                    value["direction"] =
                        serde_json::json!(if edge.source_path == normalized_path {
                            "outgoing"
                        } else {
                            "backlinks"
                        });
                    value
                })
                .collect::<Vec<_>>();
            let outgoing_window = items
                .iter()
                .filter(|item| item["direction"] == "outgoing")
                .cloned()
                .collect::<Vec<_>>();
            let backlinks_window = items
                .iter()
                .filter(|item| item["resolved_path"] == normalized_path)
                .cloned()
                .collect::<Vec<_>>();
            CommandResult {
                command: "graph.links".to_string(),
                summary: "graph links completed".to_string(),
                args: serde_json::json!({
                    "path": normalized_path,
                    "outgoing_total": outgoing_total,
                    "backlinks_total": backlinks_total,
                    "total": total,
                    "limit": args.limit,
                    "offset": args.offset,
                    "outgoing": outgoing_window,
                    "backlinks": backlinks_window,
                    "items": items,
                }),
            }
        } else {
            handle_graph(
                GraphCommands::Audit(GraphAuditArgs {
                    kind: "unresolved".into(),
                    scope: None,
                    include_markdown: false,
                    include_non_md: false,
                    exclude_prefix: Vec::new(),
                    include_members: false,
                    sample_size: 64,
                    mode: "weak".into(),
                    vault_root: args.vault_root.clone(),
                    db_path: args.db_path.clone(),
                    limit: args.limit,
                    offset: args.offset,
                }),
                runtime,
            )?
        };
        return Ok(finish_window_query(graph_result, &args));
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
            runtime,
        )?;
        return Ok(finish_window_query(task_result, &args));
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
        return Ok(finish_window_query(result, &args));
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
        return Ok(finish_window_query(result, &args));
    }

    if from.eq_ignore_ascii_case("meta:properties") {
        let result = handle_meta(
            MetaCommands::Properties(GraphWindowArgs {
                vault_root: args.vault_root.clone(),
                db_path: args.db_path.clone(),
                limit: args.limit,
                offset: args.offset,
            }),
            runtime,
        )?;
        return Ok(finish_window_query(result, &args));
    }

    Err(CliContractError::invalid_argument(format!(
        "unsupported query scope '{}'; supported scopes: docs, graph, task, meta:tags, meta:aliases, meta:properties, base:<id-or-path>",
        from
    ))
    .into())
}

fn finish_window_query(mut result: CommandResult, args: &QueryArgs) -> CommandResult {
    result.args["from"] = serde_json::json!(args.from.trim().to_ascii_lowercase());
    if args.explain {
        result.args["explain"] = generic_window_plan(args, true)["physical_plan"].take();
    }
    retag_result(result, "query.run", "query run completed")
}

fn generic_window_plan(args: &QueryArgs, execute: bool) -> JsonValue {
    let scope = args.from.trim().to_ascii_lowercase();
    let adapter = match scope.as_str() {
        "graph" if args.path.is_some() => "sqlite_link_occurrence_window",
        "graph" => "sqlite_unresolved_link_window",
        "task" => "sqlite_task_window",
        _ => "sqlite_metadata_aggregation",
    };
    serde_json::json!({
        "logical_plan":{"scope":scope,"limit":args.limit,"offset":args.offset,"execute":execute},
        "physical_plan":{"adapter":adapter,"stages":["scope_validation","snapshot_read","count_and_window"],"execute":execute,"limit":args.limit,"offset":args.offset}
    })
}

fn query_planning_error(context: &'static str, source: impl std::fmt::Display) -> anyhow::Error {
    let message = format!("{context}: {source}");
    if message.contains("type mismatch for ordered comparison")
        || message.contains("type mismatch for string comparison")
    {
        CliContractError::query_type_mismatch(message).into()
    } else {
        CliContractError::invalid_argument(message).into()
    }
}

pub(in crate::cli_impl) fn dispatch(
    args: QueryArgs,
    runtime: &mut RuntimeMode,
) -> Result<CommandResult> {
    handle(args, runtime)
}
