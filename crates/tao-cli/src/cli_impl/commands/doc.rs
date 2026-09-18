use super::super::*;

/// Reject invalid windows before routing can open or refresh internal state.
pub(crate) fn validate_args(command: &DocCommands) -> Result<()> {
    let (offset, limit) = match command {
        DocCommands::Read(args) => (args.offset, args.limit as u64),
        DocCommands::List(args) => (args.offset, u64::from(args.limit)),
    };
    if !(1..=1000).contains(&limit) {
        return Err(
            CliContractError::invalid_argument("doc --limit must be between 1 and 1000").into(),
        );
    }
    if i64::try_from(offset).is_err() {
        return Err(CliContractError::invalid_argument(
            "doc --offset must fit a signed 64-bit SQLite integer",
        )
        .into());
    }
    if let DocCommands::Read(args) = command
        && offset > 0
        && args
            .revision
            .as_deref()
            .is_none_or(|revision| revision.trim().is_empty())
    {
        return Err(CliContractError::invalid_argument(
            "doc read --offset greater than zero requires --revision from the first response's continuation_revision",
        )
        .into());
    }
    Ok(())
}

pub(crate) fn handle(command: DocCommands, runtime: &mut RuntimeMode) -> Result<CommandResult> {
    validate_args(&command)?;
    match command {
        DocCommands::Read(args) => {
            let resolved = args.resolve()?;
            let content = with_kernel(runtime, &resolved, |kernel| {
                expect_bridge_value(
                    kernel.content_get(
                        &args.path,
                        args.offset,
                        args.limit,
                        args.revision.as_deref(),
                    ),
                    "doc.read",
                )
            })?;
            Ok(CommandResult {
                command: "doc.read".to_string(),
                summary: "indexed content read completed".to_string(),
                args: serde_json::to_value(content)?,
            })
        }
        DocCommands::List(args) => {
            let resolved = args.resolve()?;
            let listing = with_kernel(runtime, &resolved, |kernel| {
                expect_bridge_value(kernel.documents_page(args.offset, args.limit), "doc.list")
            })?;
            Ok(CommandResult {
                command: "doc.list".to_string(),
                summary: "doc list completed".to_string(),
                args: serde_json::to_value(listing)?,
            })
        }
    }
}

pub(in crate::cli_impl) fn dispatch(
    command: DocCommands,
    runtime: &mut RuntimeMode,
) -> Result<CommandResult> {
    handle(command, runtime)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_doc_windows_fail_before_routing_or_database_creation() {
        let directory =
            tempfile::tempdir_in(Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target"))
                .unwrap();
        let db = directory.path().join("not-created/index.sqlite");
        for arguments in [
            vec!["read", "--path", "note.md", "--limit", "0"],
            vec!["read", "--path", "note.md", "--limit", "1001"],
            vec!["read", "--path", "note.md", "--offset", "1"],
            vec![
                "read",
                "--path",
                "note.md",
                "--offset",
                "9223372036854775808",
            ],
            vec!["list", "--offset", "9223372036854775808"],
        ] {
            let mut raw = vec!["tao", "doc"];
            raw.extend(arguments);
            raw.extend([
                "--vault-root",
                directory.path().to_str().unwrap(),
                "--db-path",
                db.to_str().unwrap(),
            ]);
            let result = run_from_args(raw.into_iter().map(OsString::from).collect());
            assert_eq!(result.exit_kind, ExitKind::Failure);
            let output: JsonValue =
                serde_json::from_str(result.stdout.as_deref().unwrap()).unwrap();
            assert_eq!(output["error"]["code"], "invalid_argument");
            assert!(!db.parent().unwrap().exists());
            assert!(!directory.path().join(".tao").exists());
        }
    }
}
