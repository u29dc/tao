use super::super::*;

pub(crate) fn handle(args: SearchArgs, runtime: &mut RuntimeMode) -> Result<CommandResult> {
    let resolved = args.resolve()?;
    let kind = SearchKind::parse(&args.kind)?;
    let path = args
        .path
        .as_deref()
        .map(str::trim)
        .filter(|path| !path.is_empty())
        .map(ToString::to_string);
    let query = args
        .query
        .as_deref()
        .map(str::trim)
        .filter(|query| !query.is_empty())
        .map(ToString::to_string);
    let include_pii = args.include_pii && !args.no_pii;
    let result = with_connection(runtime, &resolved, |connection| {
        Ok(VaultSearchService.search(
            connection,
            VaultSearchRequest {
                vault_root: PathBuf::from(&resolved.vault_root),
                query,
                path,
                kind,
                scope: args.scope.clone(),
                extensions: args.ext.clone(),
                include_context: args.context,
                depth: args.depth,
                limit: args.limit,
                include_content: args.include_content,
                include_pii,
            },
            resolved.case_policy,
        )?)
    })
    .map_err(|source| anyhow!("search failed: {source}"))?;
    Ok(CommandResult {
        command: "search.run".to_string(),
        summary: "search completed".to_string(),
        args: serde_json::to_value(result).context("serialize search result")?,
    })
}

pub(in crate::cli_impl) fn dispatch(
    args: SearchArgs,
    runtime: &mut RuntimeMode,
) -> Result<CommandResult> {
    handle(args, runtime)
}
