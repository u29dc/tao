use super::super::*;

pub(crate) fn handle(command: VaultCommands, runtime: &mut RuntimeMode) -> Result<CommandResult> {
    match command {
        VaultCommands::Open(args) => {
            let resolved = args.resolve()?;
            let migration_count: i64 = with_connection(runtime, &resolved, |connection| {
                connection
                    .query_row("SELECT COUNT(*) FROM schema_migrations", [], |row| {
                        row.get(0)
                    })
                    .context("query migration count")
            })?;
            Ok(CommandResult {
                command: "vault.open".to_string(),
                summary: "vault open completed".to_string(),
                args: serde_json::json!({
                    "vault_root": resolved.vault_root,
                    "db_path": resolved.db_path,
                    "db_ready": true,
                    "migrations_applied": migration_count,
                }),
            })
        }
        VaultCommands::Preflight(args) => {
            let resolved = args.resolve()?;
            let vault_root = Path::new(&resolved.vault_root);
            if !vault_root.exists() {
                return Err(CliContractError::blocked_prerequisite(format!(
                    "vault root does not exist: {}",
                    resolved.vault_root
                ))
                .into());
            }
            if !vault_root.is_dir() {
                return Err(CliContractError::blocked_prerequisite(format!(
                    "vault root is not a directory: {}",
                    resolved.vault_root
                ))
                .into());
            }

            let database_exists = Path::new(&resolved.db_path).exists();
            let connection = if database_exists {
                Connection::open_with_flags(&resolved.db_path, OpenFlags::SQLITE_OPEN_READ_ONLY)
            } else {
                Connection::open_in_memory()
            }
            .map_err(|source| {
                CliContractError::blocked_prerequisite(format!(
                    "open sqlite database '{}': {source}",
                    resolved.db_path
                ))
            })?;
            let report = preflight_migrations(&connection)
                .map_err(|source| anyhow!("migration preflight failed: {source}"))?;
            Ok(CommandResult {
                command: "vault.preflight".to_string(),
                summary: "vault preflight completed".to_string(),
                args: serde_json::json!({
                    "database_exists": database_exists,
                    "migrations_table_exists": report.migrations_table_exists,
                    "known_migrations": report.known_migrations,
                    "applied_migrations": report.applied_migrations,
                    "pending_migrations": report.pending_migrations,
                }),
            })
        }
        VaultCommands::Reindex(args) => {
            if args.dry_run && args.wait_content_ms != 0 {
                return Err(CliContractError::invalid_argument(
                    "--wait-content-ms cannot be combined with --dry-run",
                )
                .into());
            }
            let resolved = args.resolve()?;
            if args.dry_run {
                let connection = Connection::open_with_flags(
                    &resolved.db_path,
                    OpenFlags::SQLITE_OPEN_READ_ONLY,
                )
                .with_context(|| {
                    format!("open sqlite database '{}' read-only", resolved.db_path)
                })?;
                let refresh = IndexRefreshService
                    .inspect(
                        Path::new(&resolved.vault_root),
                        &connection,
                        resolved.case_policy,
                        ReconciliationScanMode::VerifyContentHashes,
                    )
                    .map_err(|source| anyhow!("inspect index refresh status failed: {source}"))?;
                let totals = query_index_totals(&connection)
                    .map_err(|source| anyhow!("vault reindex total query failed: {source}"))?;
                let mode = if refresh.rebuild_reason.is_some() {
                    "full_rebuild"
                } else {
                    "reconcile"
                };
                return Ok(CommandResult {
                    command: "vault.reindex".to_string(),
                    summary: "vault reindex dry-run completed".to_string(),
                    args: serde_json::json!({
                        "mode": mode,
                        "reason": refresh.rebuild_reason.map(|reason| reason.to_string()),
                        "dry_run": true,
                        "scan_mode": "content_hash",
                        "would_write": refresh.rebuild_reason.is_some() || refresh.drift_paths > 0 || refresh.would_rebuild_search_index,
                        "indexed_files": totals.indexed_files,
                        "markdown_files": totals.markdown_files,
                        "links_total": totals.links_total,
                        "unresolved_links": totals.unresolved_links,
                        "properties_total": totals.properties_total,
                        "bases_total": totals.bases_total,
                        "search_segments_total": totals.search_segments_total,
                        "search_aliases_total": totals.search_aliases_total,
                        "search_index_stale": refresh.search_index_stale,
                        "would_rebuild_search_index": refresh.would_rebuild_search_index
                            || refresh.rebuild_reason.is_some()
                            || refresh.drift_paths > 0,
                        "search_segments_rebuilt": false,
                        "search_corpus_refresh": "none",
                        "drift_paths": refresh.drift_paths,
                        "batches_applied": 0_u64,
                        "upserted_files": 0_u64,
                        "removed_files": 0_u64,
                    }),
                });
            }
            let (
                mode,
                reason,
                drift_paths,
                batches_applied,
                upserted_files,
                removed_files,
                totals,
                search_segments_rebuilt,
                search_corpus_refresh,
                content,
                content_complete,
            ) = with_connection(runtime, &resolved, |connection| {
                let outcome = IndexRefreshService
                    .refresh(
                        Path::new(&resolved.vault_root),
                        connection,
                        resolved.case_policy,
                        IndexRefreshOptions {
                            scan_mode: ReconciliationScanMode::VerifyContentHashes,
                            max_batch_size: 128,
                        },
                    )
                    .map_err(|source| anyhow!("vault reindex failed: {source}"))?;
                let spool_root = tao_sdk_service::content_spool_root(
                    connection,
                    Path::new(&resolved.vault_root),
                );
                let content = tao_sdk_service::ContentIndexService
                    .process_pending_cancellable(
                        connection,
                        Path::new(&resolved.vault_root),
                        &spool_root,
                        std::time::Duration::from_millis(args.wait_content_ms)
                            .min(request_remaining()),
                        resolved.case_policy,
                        &request_cancellation_flag(),
                    )
                    .map_err(|source| anyhow!("content extraction failed: {source}"))?;
                let totals = query_index_totals(connection)
                    .map_err(|source| anyhow!("vault reindex total query failed: {source}"))?;
                let mode = if matches!(outcome.mode, IndexRefreshMode::FullRebuild) {
                    "full_rebuild"
                } else {
                    "reconcile"
                };
                Ok((
                    mode,
                    outcome.reason.map(str::to_string),
                    outcome.drift_paths,
                    outcome.batches_applied,
                    outcome.upserted_files,
                    outcome.removed_files,
                    totals,
                    outcome.search_segments_rebuilt,
                    outcome.search_corpus_refresh.as_str(),
                    content,
                    tao_sdk_storage::ContentRepository::coverage_stats(connection)?
                        .iter()
                        .all(|(coverage, _)| {
                            matches!(coverage.as_str(), "complete" | "unsupported")
                        }),
                ))
            })?;
            Ok(CommandResult {
                command: "vault.reindex".to_string(),
                summary: "vault reindex completed".to_string(),
                args: serde_json::json!({
                    "mode": mode,
                    "reason": reason,
                    "scan_mode": "content_hash",
                    "indexed_files": totals.indexed_files,
                    "markdown_files": totals.markdown_files,
                    "links_total": totals.links_total,
                    "unresolved_links": totals.unresolved_links,
                    "properties_total": totals.properties_total,
                    "bases_total": totals.bases_total,
                    "search_segments_total": totals.search_segments_total,
                    "search_aliases_total": totals.search_aliases_total,
                    "content": content,
                    "index_complete": true,
                    "content_complete": content_complete && content.queued == 0 && content.running == 0 && content.failed == 0,
                    "search_index_stale": false,
                    "would_rebuild_search_index": false,
                    "search_segments_rebuilt": search_segments_rebuilt,
                    "search_corpus_refresh": search_corpus_refresh,
                    "drift_paths": drift_paths,
                    "batches_applied": batches_applied,
                    "upserted_files": upserted_files,
                    "removed_files": removed_files,
                }),
            })
        }
        VaultCommands::Daemon { command } => handle_daemon(command),
        VaultCommands::DaemonServe(args) => {
            let socket = args.resolve_socket()?;
            run_daemon_server(&socket)?;
            Ok(CommandResult {
                command: "vault.daemon.serve".to_string(),
                summary: "vault daemon serve stopped".to_string(),
                args: serde_json::json!({
                    "socket": socket,
                    "stopped": true,
                }),
            })
        }
    }
}

pub(in crate::cli_impl) fn dispatch(
    command: VaultCommands,
    runtime: &mut RuntimeMode,
) -> Result<CommandResult> {
    handle(command, runtime)
}
