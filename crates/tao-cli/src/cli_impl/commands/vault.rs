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
                return Err(anyhow!(
                    "vault root does not exist: {}",
                    resolved.vault_root
                ));
            }
            if !vault_root.is_dir() {
                return Err(anyhow!(
                    "vault root is not a directory: {}",
                    resolved.vault_root
                ));
            }

            let connection = Connection::open(&resolved.db_path)
                .with_context(|| format!("open sqlite database '{}'", resolved.db_path))?;
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
            let (mode, reason, drift_paths, batches_applied, upserted_files, removed_files, totals) =
                with_connection(runtime, &resolved, |connection| {
                    let refresh = query_index_refresh_status(
                        Path::new(&resolved.vault_root),
                        connection,
                        resolved.case_policy,
                    )?;

                    if let Some(reason) = refresh.rebuild_reason {
                        let rebuild = FullIndexService::default()
                            .rebuild(
                                Path::new(&resolved.vault_root),
                                connection,
                                resolved.case_policy,
                            )
                            .map_err(|source| {
                                anyhow!("vault reindex full rebuild failed: {source}")
                            })?;
                        let totals = query_index_totals(connection).map_err(|source| {
                            anyhow!("vault reindex total query failed: {source}")
                        })?;
                        return Ok((
                            "full_rebuild",
                            Some(reason.to_string()),
                            refresh.drift_paths,
                            1_u64,
                            rebuild.indexed_files,
                            0_u64,
                            totals,
                        ));
                    }

                    let reconcile = WatchReconcileService::default()
                        .reconcile_once(
                            Path::new(&resolved.vault_root),
                            connection,
                            resolved.case_policy,
                        )
                        .map_err(|source| anyhow!("vault reindex failed: {source}"))?;
                    let totals = query_index_totals(connection)
                        .map_err(|source| anyhow!("vault reindex total query failed: {source}"))?;
                    Ok((
                        "reconcile",
                        None,
                        reconcile.drift_paths,
                        reconcile.batches_applied,
                        reconcile.upserted_files,
                        reconcile.removed_files,
                        totals,
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
