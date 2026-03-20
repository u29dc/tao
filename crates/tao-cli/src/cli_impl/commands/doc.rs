use super::super::*;

pub(crate) fn handle(
    command: DocCommands,
    allow_writes: bool,
    runtime: &mut RuntimeMode,
) -> Result<CommandResult> {
    match command {
        DocCommands::Read(args) => {
            let resolved = args.resolve()?;
            let note = with_kernel(runtime, &resolved, |kernel| {
                expect_bridge_value(kernel.note_get(&args.path), "doc.read")
            })?;
            Ok(CommandResult {
                command: "doc.read".to_string(),
                summary: "doc read completed".to_string(),
                args: serde_json::json!({
                    "path": note.path,
                    "title": note.title,
                    "front_matter": note.front_matter,
                    "body": note.body,
                    "headings_total": note.headings_total,
                }),
            })
        }
        DocCommands::Write(args) => {
            let resolved = args.resolve()?;
            ensure_writes_enabled(allow_writes, resolved.read_only, "doc.write")?;
            let ack = with_kernel(runtime, &resolved, |kernel| {
                expect_bridge_value(
                    kernel.note_put_with_policy(&args.path, &args.content, true),
                    "doc.write",
                )
            })?;
            Ok(CommandResult {
                command: "doc.write".to_string(),
                summary: "doc write completed".to_string(),
                args: serde_json::json!({
                    "path": ack.path,
                    "file_id": ack.file_id,
                    "action": ack.action,
                    "index_synced": ack.index_synced,
                    "event_logged": ack.event_logged,
                    "warnings": ack.warnings,
                }),
            })
        }
        DocCommands::List(args) => {
            let resolved = args.resolve()?;
            let mut after_path: Option<String> = None;
            let mut items = Vec::new();
            loop {
                let page = with_kernel(runtime, &resolved, |kernel| {
                    expect_bridge_value(kernel.notes_list(after_path.as_deref(), 1000), "doc.list")
                })?;
                after_path = page.next_cursor;
                items.extend(page.items.into_iter().map(|item| {
                    serde_json::json!({
                        "file_id": item.file_id,
                        "path": item.path,
                        "title": item.title,
                        "updated_at": item.updated_at,
                    })
                }));
                if after_path.is_none() {
                    break;
                }
            }
            Ok(CommandResult {
                command: "doc.list".to_string(),
                summary: "doc list completed".to_string(),
                args: serde_json::json!({
                    "total": items.len(),
                    "items": items,
                }),
            })
        }
    }
}

pub(in crate::cli_impl) fn dispatch(
    command: DocCommands,
    allow_writes: bool,
    runtime: &mut RuntimeMode,
) -> Result<CommandResult> {
    handle(command, allow_writes, runtime)
}
