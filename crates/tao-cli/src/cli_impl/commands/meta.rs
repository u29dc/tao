use super::super::*;

pub(crate) fn handle(command: MetaCommands, runtime: &mut RuntimeMode) -> Result<CommandResult> {
    match command {
        MetaCommands::Properties(args) => {
            let resolved = args.resolve()?;
            let items = with_connection(runtime, &resolved, |connection| {
                let mut statement = connection
                    .prepare(
                        "SELECT key, COUNT(*) AS total FROM properties GROUP BY key ORDER BY key ASC",
                    )
                    .context("prepare properties aggregate query")?;
                let rows = statement
                    .query_map([], |row| {
                        Ok(serde_json::json!({
                            "key": row.get::<_, String>(0)?,
                            "total": row.get::<_, u64>(1)?,
                        }))
                    })
                    .context("query properties aggregate rows")?;
                let mut items = Vec::new();
                for row in rows {
                    items.push(row.context("map properties aggregate row")?);
                }
                Ok(items)
            })?;
            let total = items.len();
            let items = paginate_json_items(items, args.limit, args.offset);
            Ok(CommandResult {
                command: "meta.properties".to_string(),
                summary: "meta properties completed".to_string(),
                args: serde_json::json!({
                    "total": total,
                    "limit": args.limit,
                    "offset": args.offset,
                    "items": items,
                }),
            })
        }
        MetaCommands::Tags(args) => handle_meta_token_aggregate(args, "tags", "meta.tags", runtime),
        MetaCommands::Aliases(args) => {
            handle_meta_token_aggregate(args, "aliases", "meta.aliases", runtime)
        }
        MetaCommands::Tasks(args) => {
            let result = handle_task(TaskCommands::List(args), false, runtime)?;
            Ok(retag_result(result, "meta.tasks", "meta tasks completed"))
        }
    }
}

pub(in crate::cli_impl) fn dispatch(
    command: MetaCommands,
    runtime: &mut RuntimeMode,
) -> Result<CommandResult> {
    handle(command, runtime)
}
