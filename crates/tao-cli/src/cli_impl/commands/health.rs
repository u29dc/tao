use super::super::*;

pub(crate) fn load_cli_health_snapshot(
    resolved: &ResolvedVaultPathArgs,
    runtime: &mut RuntimeMode,
) -> Result<(tao_sdk_service::HealthSnapshot, CliRuntimeState)> {
    let runtime_state = runtime_state_for_resolved(resolved, runtime);
    let index_lag = with_connection(runtime, resolved, |connection| {
        let refresh = query_index_refresh_status(
            Path::new(&resolved.vault_root),
            connection,
            resolved.case_policy,
        )?;
        Ok(refresh.drift_paths)
    })?;
    let watcher_status = watcher_status_for_runtime_state(&runtime_state);
    let snapshot = with_connection(runtime, resolved, |connection| {
        Ok(HealthSnapshotService.snapshot(
            Path::new(&resolved.vault_root),
            connection,
            index_lag,
            watcher_status.clone(),
        )?)
    })?;
    Ok((snapshot, runtime_state))
}

pub(crate) fn handle(args: VaultPathArgs, runtime: &mut RuntimeMode) -> Result<CommandResult> {
    let resolved = args.resolve().map_err(|source| {
        health_blocked_error(
            source.to_string(),
            "set --vault-root explicitly or configure [vault].root before retrying",
            args.vault_root
                .clone()
                .map(JsonValue::String)
                .unwrap_or(JsonValue::Null),
            args.db_path
                .clone()
                .map(JsonValue::String)
                .unwrap_or(JsonValue::Null),
            "config",
        )
    })?;

    let (snapshot, runtime_state) =
        load_cli_health_snapshot(&resolved, runtime).map_err(|source| {
            health_blocked_error(
                source.to_string(),
                "run `tao vault open --vault-root <path>` and verify the database path is writable",
                JsonValue::String(resolved.vault_root.clone()),
                JsonValue::String(resolved.db_path.clone()),
                "runtime",
            )
        })?;

    let mut status = "ready";
    let mut checks = vec![serde_json::json!({
        "name": "vault",
        "status": "pass",
        "message": format!("vault root resolved to '{}'", snapshot.vault_root),
        "fix": JsonValue::Null,
    })];

    if snapshot.db_healthy {
        checks.push(serde_json::json!({
            "name": "database",
            "status": "pass",
            "message": "sqlite database is healthy",
            "fix": JsonValue::Null,
        }));
    } else {
        status = "degraded";
        checks.push(serde_json::json!({
            "name": "database",
            "status": "degraded",
            "message": "sqlite database reported an unhealthy state",
            "fix": "run `tao vault open` to bootstrap paths and verify sqlite permissions",
        }));
    }

    if snapshot.index_lag == 0 {
        checks.push(serde_json::json!({
            "name": "index",
            "status": "pass",
            "message": "index is up to date",
            "fix": JsonValue::Null,
        }));
    } else {
        status = "degraded";
        checks.push(serde_json::json!({
            "name": "index",
            "status": "degraded",
            "message": format!("index lag is {}", snapshot.index_lag),
            "fix": "run `tao vault reconcile` or `tao vault reindex` to refresh index state",
        }));
    }

    Ok(CommandResult {
        command: "health".to_string(),
        summary: "health completed".to_string(),
        args: serde_json::json!({
            "status": status,
            "vault_root": snapshot.vault_root,
            "db_path": resolved.db_path,
            "checks": checks,
            "stats": {
                "files_total": snapshot.files_total,
                "markdown_files": snapshot.markdown_files,
                "db_healthy": snapshot.db_healthy,
                "db_migrations": snapshot.db_migrations,
                "index_lag": snapshot.index_lag,
                "watcher_status": snapshot.watcher_status,
                "last_index_updated_at": snapshot.last_index_updated_at,
            },
            "runtime": runtime_state,
        }),
    })
}

pub(crate) fn health_blocked_error(
    message: String,
    fix: &str,
    vault_root: JsonValue,
    db_path: JsonValue,
    check_name: &str,
) -> anyhow::Error {
    CliContractError::blocked(
        "blocked_prerequisite",
        message.clone(),
        Some(fix.to_string()),
        Some(serde_json::json!({
            "status": "blocked",
            "vault_root": vault_root,
            "db_path": db_path,
            "checks": [
                {
                    "name": check_name,
                    "status": "blocked",
                    "message": message,
                    "fix": fix,
                }
            ],
            "stats": JsonValue::Null,
        })),
    )
    .into()
}

pub(in crate::cli_impl) fn dispatch(
    args: VaultPathArgs,
    runtime: &mut RuntimeMode,
) -> Result<CommandResult> {
    handle(args, runtime)
}
