use super::super::*;

pub(crate) fn handle(command: BaseCommands, runtime: &mut RuntimeMode) -> Result<CommandResult> {
    match command {
        BaseCommands::List(args) => {
            let resolved = args.resolve()?;
            let bases = with_connection(runtime, &resolved, |connection| {
                Ok(BasesRepository::list_with_paths(connection)?)
            })
            .map_err(|source| anyhow!("query bases failed: {source}"))?;
            let mut items = Vec::with_capacity(bases.len());
            let mut invalid = Vec::new();
            for base in bases {
                match decode_base_document(&base.config_json) {
                    Ok(document) => items.push(serde_json::json!({
                        "base_id": base.base_id,
                        "file_path": base.file_path,
                        "views": document
                            .views
                            .into_iter()
                            .map(|view| view.name)
                            .collect::<Vec<_>>(),
                        "updated_at": base.updated_at,
                    })),
                    Err(_) => invalid.push(serde_json::json!({
                        "base_id": base.base_id,
                        "file_path": base.file_path,
                        "updated_at": base.updated_at,
                        "diagnostics": validate_base_config_json(&base.config_json),
                    })),
                }
            }
            Ok(CommandResult {
                command: "base.list".to_string(),
                summary: "base list completed".to_string(),
                args: serde_json::json!({
                    "total": items.len() + invalid.len(),
                    "valid_total": items.len(),
                    "invalid_total": invalid.len(),
                    "items": items,
                    "invalid": invalid,
                }),
            })
        }
        BaseCommands::View(args) => {
            let resolved = args.resolve()?;
            let base = with_connection(runtime, &resolved, |connection| {
                Ok(BasesRepository::list_with_paths(connection)?)
            })
            .map_err(|source| anyhow!("query bases failed: {source}"))?
            .into_iter()
            .find(|base| base.base_id == args.path_or_id || base.file_path == args.path_or_id)
            .ok_or_else(|| anyhow!("base id/path not found: {}", args.path_or_id))?;
            let document = decode_base_document(&base.config_json)
                .with_context(|| format!("decode base document '{}'", base.file_path))?;
            let registry = BaseViewRegistry::from_document(&document)
                .map_err(|source| anyhow!("decode base view registry failed: {source}"))?;
            let plan = BaseTableQueryPlanner
                .compile(
                    &registry,
                    &TableQueryPlanRequest {
                        view_name: args.view_name.clone(),
                        page: args.page,
                        page_size: args.page_size,
                    },
                )
                .map_err(|source| anyhow!("compile base table query plan failed: {source}"))?;
            let page = with_connection(runtime, &resolved, |connection| {
                Ok(BaseTableExecutorService.execute_with_options(
                    connection,
                    &plan,
                    BaseTableExecutionOptions {
                        include_summaries: false,
                        coercion_mode: tao_sdk_bases::BaseCoercionMode::Permissive,
                    },
                )?)
            })
            .map_err(|source| anyhow!("execute base table query failed: {source}"))?;
            let has_more = (args.page as usize * args.page_size as usize) < page.total as usize;
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
            Ok(CommandResult {
                command: "base.view".to_string(),
                summary: "base view completed".to_string(),
                args: serde_json::json!({
                    "base_id": base.base_id,
                    "file_path": base.file_path,
                    "view_name": plan.view_name,
                    "page": args.page,
                    "page_size": args.page_size,
                    "total": page.total,
                    "has_more": has_more,
                    "columns": plan.columns,
                    "sorts": plan.sorts,
                    "grouping": page.grouping,
                    "relation_diagnostics": page.relation_diagnostics,
                    "execution": page.execution,
                    "rows": rows,
                }),
            })
        }
        BaseCommands::Schema(args) => {
            let resolved = args.resolve()?;
            let base = with_connection(runtime, &resolved, |connection| {
                Ok(BasesRepository::list_with_paths(connection)?)
            })
            .map_err(|source| anyhow!("query bases failed: {source}"))?
            .into_iter()
            .find(|base| base.base_id == args.path_or_id || base.file_path == args.path_or_id)
            .ok_or_else(|| anyhow!("base id/path not found: {}", args.path_or_id))?;
            let document = decode_base_document(&base.config_json)
                .with_context(|| format!("decode base document '{}'", base.file_path))?;
            let views = document
                .views
                .iter()
                .map(|view| {
                    serde_json::json!({
                        "name": view.name,
                        "kind": view.kind.as_str(),
                        "source": view.source,
                        "columns": view.columns.iter().map(|column| {
                            serde_json::json!({
                                "name": column.key,
                                "label": column.label,
                                "hidden": column.hidden,
                                "width": column.width,
                                "filterable": true,
                                "sortable": true,
                            })
                        }).collect::<Vec<_>>(),
                    })
                })
                .collect::<Vec<_>>();
            Ok(CommandResult {
                command: "base.schema".to_string(),
                summary: "base schema completed".to_string(),
                args: serde_json::json!({
                    "base_id": base.base_id,
                    "file_path": base.file_path,
                    "views": views,
                }),
            })
        }
        BaseCommands::Validate(args) => {
            let resolved = args.resolve()?;
            let result = with_connection(runtime, &resolved, |connection| {
                Ok(BaseValidationService.validate(connection, &args.path_or_id)?)
            })
            .map_err(|source| anyhow!("validate base failed: {source}"))?;
            let valid = !result
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.severity == BaseDiagnosticSeverity::Error);
            Ok(CommandResult {
                command: "base.validate".to_string(),
                summary: "base validate completed".to_string(),
                args: serde_json::json!({
                    "base_id": result.base_id,
                    "file_id": result.file_id,
                    "file_path": result.file_path,
                    "valid": valid,
                    "diagnostics": result.diagnostics,
                }),
            })
        }
    }
}

pub(in crate::cli_impl) fn dispatch(
    command: BaseCommands,
    runtime: &mut RuntimeMode,
) -> Result<CommandResult> {
    handle(command, runtime)
}
