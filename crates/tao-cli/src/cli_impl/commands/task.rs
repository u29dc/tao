use super::super::*;

pub(crate) fn handle(
    command: TaskCommands,
    allow_writes: bool,
    runtime: &mut RuntimeMode,
) -> Result<CommandResult> {
    match command {
        TaskCommands::List(args) => {
            let resolved = args.resolve()?;
            let state = args
                .state
                .as_deref()
                .map(str::trim)
                .filter(|state| !state.is_empty());
            let query = args
                .query
                .as_deref()
                .map(str::trim)
                .filter(|query| !query.is_empty());
            let (total, rows) = with_connection(runtime, &resolved, |connection| {
                let total = TasksRepository::count_with_paths(connection, state, query, None)
                    .map_err(|source| anyhow!("count tasks failed: {source}"))?;
                let rows = TasksRepository::list_with_paths(
                    connection,
                    state,
                    query,
                    None,
                    args.limit,
                    args.offset,
                )
                .map_err(|source| anyhow!("list tasks failed: {source}"))?;
                Ok((total, rows))
            })?;
            let items = rows
                .into_iter()
                .map(|row| {
                    let line = usize::try_from(row.line_number).unwrap_or(0);
                    serde_json::to_value(ExtractedTask {
                        path: row.file_path,
                        line,
                        state: row.state,
                        text: row.text,
                    })
                    .unwrap_or(JsonValue::Null)
                })
                .collect::<Vec<_>>();
            Ok(CommandResult {
                command: "task.list".to_string(),
                summary: "task list completed".to_string(),
                args: serde_json::json!({
                    "total": total,
                    "limit": args.limit,
                    "offset": args.offset,
                    "items": items,
                }),
            })
        }
        TaskCommands::SetState(args) => {
            let resolved = args.resolve()?;
            ensure_writes_enabled(allow_writes, resolved.read_only, "task.set-state")?;
            let absolute = resolve_existing_vault_note_path(&resolved, &args.path)
                .map_err(|source| anyhow!("resolve task note path '{}': {source}", args.path))?;
            let markdown = fs::read_to_string(&absolute)
                .with_context(|| format!("read markdown note '{}'", absolute.display()))?;
            let mut lines = markdown.lines().map(str::to_string).collect::<Vec<_>>();
            if args.line == 0 || args.line > lines.len() {
                return Err(anyhow!(
                    "task line is out of range for '{}': {}",
                    args.path,
                    args.line
                ));
            }
            let index = args.line - 1;
            let updated = update_task_line_state(&lines[index], &args.state)?;
            lines[index] = updated;
            let mut rebuilt = lines.join("\n");
            if markdown.ends_with('\n') {
                rebuilt.push('\n');
            }
            fs::write(&absolute, rebuilt)
                .with_context(|| format!("write markdown note '{}'", absolute.display()))?;

            with_connection(runtime, &resolved, |connection| {
                WatchReconcileService::default()
                    .reconcile_once(
                        Path::new(&resolved.vault_root),
                        connection,
                        resolved.case_policy,
                    )
                    .map_err(|source| anyhow!("reconcile after task state update failed: {source}"))
            })?;

            Ok(CommandResult {
                command: "task.set-state".to_string(),
                summary: "task set-state completed".to_string(),
                args: serde_json::json!({
                    "path": args.path,
                    "line": args.line,
                    "state": args.state,
                }),
            })
        }
    }
}

pub(in crate::cli_impl) fn dispatch(
    command: TaskCommands,
    allow_writes: bool,
    runtime: &mut RuntimeMode,
) -> Result<CommandResult> {
    handle(command, allow_writes, runtime)
}
