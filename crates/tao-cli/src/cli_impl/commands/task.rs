use super::super::*;

pub(crate) fn handle(command: TaskCommands, runtime: &mut RuntimeMode) -> Result<CommandResult> {
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
    }
}

pub(in crate::cli_impl) fn dispatch(
    command: TaskCommands,
    runtime: &mut RuntimeMode,
) -> Result<CommandResult> {
    handle(command, runtime)
}
