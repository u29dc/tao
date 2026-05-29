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
        VaultCommands::Stats(args) => {
            let resolved = args.resolve()?;
            let (snapshot, runtime_state) =
                super::health::load_cli_health_snapshot(&resolved, runtime)
                    .map_err(|source| anyhow!("vault stats failed: {source}"))?;
            Ok(CommandResult {
                command: "vault.stats".to_string(),
                summary: "vault stats completed".to_string(),
                args: serde_json::json!({
                    "vault_root": snapshot.vault_root,
                    "files_total": snapshot.files_total,
                    "markdown_files": snapshot.markdown_files,
                    "db_healthy": snapshot.db_healthy,
                    "db_migrations": snapshot.db_migrations,
                    "index_lag": snapshot.index_lag,
                    "watcher_status": snapshot.watcher_status,
                    "last_index_updated_at": snapshot.last_index_updated_at,
                    "runtime": runtime_state,
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

            let connection = Connection::open(&resolved.db_path).map_err(|source| {
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
                    "migrations_table_exists": report.migrations_table_exists,
                    "known_migrations": report.known_migrations,
                    "applied_migrations": report.applied_migrations,
                    "pending_migrations": report.pending_migrations,
                }),
            })
        }
        VaultCommands::Reindex(args) => {
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
                        "would_write": true,
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
                ))
            })?;
            Ok(CommandResult {
                command: "vault.reindex".to_string(),
                summary: "vault reindex completed".to_string(),
                args: serde_json::json!({
                    "mode": mode,
                    "reason": reason,
                    "indexed_files": totals.indexed_files,
                    "markdown_files": totals.markdown_files,
                    "links_total": totals.links_total,
                    "unresolved_links": totals.unresolved_links,
                    "properties_total": totals.properties_total,
                    "bases_total": totals.bases_total,
                    "search_segments_total": totals.search_segments_total,
                    "search_aliases_total": totals.search_aliases_total,
                    "search_index_stale": false,
                    "would_rebuild_search_index": false,
                    "search_segments_rebuilt": search_segments_rebuilt,
                    "drift_paths": drift_paths,
                    "batches_applied": batches_applied,
                    "upserted_files": upserted_files,
                    "removed_files": removed_files,
                }),
            })
        }
        VaultCommands::Reconcile(args) => {
            let resolved = args.resolve()?;
            let result = with_connection(runtime, &resolved, |connection| {
                WatchReconcileService::default()
                    .reconcile_once(
                        Path::new(&resolved.vault_root),
                        connection,
                        resolved.case_policy,
                    )
                    .map_err(|source| anyhow!("vault reconcile failed: {source}"))
            })?;
            Ok(CommandResult {
                command: "vault.reconcile".to_string(),
                summary: "vault reconcile completed".to_string(),
                args: serde_json::json!({
                    "scanned_files": result.scanned_files,
                    "inserted_paths": result.inserted_paths,
                    "updated_paths": result.updated_paths,
                    "removed_files": result.removed_files,
                    "drift_paths": result.drift_paths,
                    "batches_applied": result.batches_applied,
                    "upserted_files": result.upserted_files,
                    "links_reindexed": result.links_reindexed,
                    "properties_reindexed": result.properties_reindexed,
                    "bases_reindexed": result.bases_reindexed,
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
