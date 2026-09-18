use super::super::*;

pub(crate) fn load_cli_health_snapshot(
    resolved: &ResolvedVaultPathArgs,
    runtime: &mut RuntimeMode,
    deep: bool,
) -> Result<(tao_sdk_service::HealthSnapshot, CliRuntimeState)> {
    let runtime_state = runtime_state_for_resolved(resolved, runtime);
    let index_lag = with_connection(runtime, resolved, |connection| {
        let refresh = if deep {
            IndexRefreshService
                .inspect(
                    Path::new(&resolved.vault_root),
                    connection,
                    resolved.case_policy,
                    ReconciliationScanMode::MetadataOnly,
                )
                .map_err(|source| anyhow!("inspect index refresh status failed: {source}"))?
        } else {
            IndexRefreshService
                .inspect_cached(connection)
                .map_err(|source| anyhow!("inspect cached index refresh status failed: {source}"))?
        };
        Ok(refresh.drift_paths)
    })?;
    let watcher_status = watcher_status_for_runtime_state(&runtime_state);
    let mut snapshot = with_connection(runtime, resolved, |connection| {
        Ok(HealthSnapshotService.snapshot(
            Path::new(&resolved.vault_root),
            connection,
            index_lag,
            watcher_status.clone(),
        )?)
    })?;
    if deep {
        let errors = with_connection(runtime, resolved, |connection| {
            let mut errors = Vec::new();
            let mut statement = connection.prepare("PRAGMA quick_check")?;
            for row in statement.query_map([], |row| row.get::<_, String>(0))? {
                let result = row?;
                if result != "ok" && errors.len() < 100 {
                    errors.push(result);
                }
            }
            let has_fk_violation = connection
                .prepare("PRAGMA foreign_key_check")?
                .query([])?
                .next()?
                .is_some();
            if has_fk_violation {
                errors.push("foreign key consistency check failed".into());
            }
            if let Err(error) = SearchSegmentRepository::check_integrity_read_only(connection) {
                errors.push(format!("full-text index: {error}"));
            }
            Ok(errors)
        })?;
        snapshot.db_healthy = errors.is_empty();
        snapshot.consistency_checks = vec![
            "sqlite_quick_check".into(),
            "foreign_keys".into(),
            "fts_external_content_integrity_snapshot".into(),
        ];
        snapshot.consistency_errors = errors;
    }
    Ok((snapshot, runtime_state))
}

pub(crate) fn handle(args: HealthArgs, runtime: &mut RuntimeMode) -> Result<CommandResult> {
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

    let (snapshot, runtime_state) = load_cli_health_snapshot(&resolved, runtime, args.deep)
        .map_err(|source| {
            health_blocked_error(
                source.to_string(),
                "run `tao vault open --vault-root <path>` and verify the database path is writable",
                JsonValue::String(resolved.vault_root.clone()),
                JsonValue::String(resolved.db_path.clone()),
                "runtime",
            )
        })?;

    let (content, coverage, source_diagnostics) =
        with_connection(runtime, &resolved, |connection| {
            Ok((
                tao_sdk_service::ContentIndexService.status(connection)?,
                tao_sdk_storage::ContentRepository::coverage_stats(connection)?,
                connection.query_row("SELECT COUNT(*) FROM file_diagnostics", [], |row| {
                    row.get::<_, u64>(0)
                })?,
            ))
        })?;
    let incomplete_content = coverage
        .iter()
        .filter(|(state, _)| !matches!(state.as_str(), "complete" | "unsupported"))
        .map(|(_, count)| *count)
        .sum::<u64>();
    let coverage = coverage
        .into_iter()
        .map(|(state, count)| (state, serde_json::json!(count)))
        .collect::<serde_json::Map<_, _>>();
    let capabilities = tao_sdk_service::content_capabilities();
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
            "message": if args.deep { "sqlite quick check and foreign key checks passed" } else { "sqlite database is readable; integrity was not checked" },
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

    if snapshot.index_lag == 0
        && snapshot.derived_current
        && snapshot.last_index_updated_at.is_some()
    {
        checks.push(serde_json::json!({
            "name": "index",
            "status": "pass",
            "message": if args.deep { "no metadata drift detected and derived publication is current" } else { "no recorded index lag; filesystem was not scanned" },
            "fix": JsonValue::Null,
        }));
    } else {
        status = "degraded";
        checks.push(serde_json::json!({
            "name": "index",
            "status": "degraded",
            "message": format!("index lag is {}; derived current: {}; initialized: {}", snapshot.index_lag, snapshot.derived_current, snapshot.last_index_updated_at.is_some()),
            "fix": "run `tao vault reindex` to refresh index state",
        }));
    }

    if !snapshot.schema_compatible {
        status = "blocked";
    }
    checks.push(serde_json::json!({"name":"schema","status":if snapshot.schema_compatible {"pass"} else {"blocked"},"message":if snapshot.schema_compatible {"schema is compatible"} else {"pending migrations"},"fix":if snapshot.schema_compatible {JsonValue::Null} else {JsonValue::String("run tao vault open to apply supported migrations".into())}}));
    if let Some(reason) = &snapshot.watcher_reason {
        if status != "blocked" {
            status = "degraded";
        }
        checks.push(serde_json::json!({"name":"watcher","status":"degraded","message":reason,"fix":"run tao vault reindex after restoring filesystem access"}));
    }

    if content.failed > 0
        || content.queued > 0
        || content.running > 0
        || incomplete_content > 0
        || source_diagnostics > 0
    {
        if status != "blocked" {
            status = "degraded";
        }
        checks.push(serde_json::json!({"name":"content","status":"degraded","message":format!("{} queued, {} running, {} failed extraction jobs; {} files with incomplete content; {} source diagnostics",content.queued,content.running,content.failed,incomplete_content,source_diagnostics),"fix":"inspect content coverage and local PDF/OCR capabilities; run tao vault reindex --wait-content-ms 120000 to process pending work"}));
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
                "schema_compatible": snapshot.schema_compatible,
                "canonical_generation": snapshot.canonical_generation,
                "search_generation": snapshot.search_generation,
                "derived_current": snapshot.derived_current,
                "consistency_checks": snapshot.consistency_checks,
                "consistency_errors": snapshot.consistency_errors,
                "index_lag": snapshot.index_lag,
                "scan_mode": if args.deep { "deep_metadata" } else { "cached" },
                "watcher_status": snapshot.watcher_status,
                "watcher_reason": snapshot.watcher_reason,
                "last_index_updated_at": snapshot.last_index_updated_at,
            },
            "runtime": runtime_state,
            "content": {"jobs":content,"coverage":coverage,"source_diagnostics":source_diagnostics,"capabilities":capabilities},
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
    args: HealthArgs,
    runtime: &mut RuntimeMode,
) -> Result<CommandResult> {
    handle(args, runtime)
}
