use super::*;

#[derive(Debug, Clone)]
pub(crate) struct ResolvedVaultPathArgs {
    pub(crate) vault_root: String,
    pub(crate) data_dir: String,
    pub(crate) db_path: String,
    pub(crate) case_policy: CasePolicy,
    pub(crate) read_only: bool,
}

#[derive(Debug, Default)]
pub(crate) struct RuntimeCache {
    pub(crate) kernels: HashMap<String, BridgeKernel>,
    pub(crate) connections: HashMap<String, Connection>,
    pub(crate) command_results: HashMap<String, CachedCommandResult>,
    pub(crate) change_monitors: HashMap<String, VaultChangeMonitor>,
    pub(crate) last_reconciled_generation: HashMap<String, u64>,
}

#[derive(Debug, Clone)]
pub(crate) struct CachedCommandResult {
    pub(crate) runtime_key: String,
    pub(crate) result: CommandResult,
}

#[derive(Debug)]
pub(crate) enum RuntimeMode {
    OneShot,
    Daemon(Box<RuntimeCache>),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct CliRuntimeState {
    pub(crate) backend: &'static str,
    pub(crate) daemon_running: bool,
    pub(crate) change_monitor_initialized: bool,
    pub(crate) cached_connection: bool,
}

pub(crate) fn runtime_cache_key(args: &ResolvedVaultPathArgs) -> String {
    format!(
        "{}\u{1f}{}\u{1f}{}\u{1f}{:?}\u{1f}{}",
        args.vault_root, args.data_dir, args.db_path, args.case_policy, args.read_only
    )
}

pub(crate) fn with_connection<T>(
    runtime: &mut RuntimeMode,
    args: &ResolvedVaultPathArgs,
    operation: impl FnOnce(&mut Connection) -> Result<T>,
) -> Result<T> {
    match runtime {
        RuntimeMode::OneShot => {
            let mut connection = open_initialized_connection(args)?;
            operation(&mut connection)
        }
        RuntimeMode::Daemon(cache) => {
            let key = runtime_cache_key(args);
            if !cache.connections.contains_key(&key) {
                let connection = open_initialized_connection(args)?;
                cache.connections.insert(key.clone(), connection);
            }
            let connection = cache.connections.get_mut(&key).ok_or_else(|| {
                anyhow!(
                    "runtime cache missing sqlite connection for {}",
                    args.db_path
                )
            })?;
            operation(connection)
        }
    }
}

pub(crate) fn with_kernel<T>(
    runtime: &mut RuntimeMode,
    args: &ResolvedVaultPathArgs,
    operation: impl FnOnce(&mut BridgeKernel) -> Result<T>,
) -> Result<T> {
    match runtime {
        RuntimeMode::OneShot => {
            let mut kernel = open_bridge_kernel(args)?;
            operation(&mut kernel)
        }
        RuntimeMode::Daemon(cache) => {
            let key = runtime_cache_key(args);
            if !cache.kernels.contains_key(&key) {
                let kernel = open_bridge_kernel(args)?;
                cache.kernels.insert(key.clone(), kernel);
            }
            let kernel = cache.kernels.get_mut(&key).ok_or_else(|| {
                anyhow!("runtime cache missing bridge kernel for {}", args.db_path)
            })?;
            operation(kernel)
        }
    }
}

pub(crate) fn resolve_vault_paths(
    vault_root_override: Option<&str>,
    db_path_override: Option<&str>,
) -> Result<ResolvedVaultPathArgs> {
    let config = SdkConfigLoader::load(SdkConfigOverrides {
        vault_root: vault_root_override.map(PathBuf::from),
        db_path: db_path_override.map(PathBuf::from),
        ..SdkConfigOverrides::default()
    })
    .map_err(|source| anyhow!("resolve sdk config failed: {source}"))?;

    Ok(ResolvedVaultPathArgs {
        vault_root: config.vault_root.to_string_lossy().to_string(),
        data_dir: config.data_dir.to_string_lossy().to_string(),
        db_path: config.db_path.to_string_lossy().to_string(),
        case_policy: config.case_policy,
        read_only: config.read_only,
    })
}

pub(crate) fn open_bridge_kernel(args: &ResolvedVaultPathArgs) -> Result<BridgeKernel> {
    ensure_runtime_paths_for_args(args)?;
    BridgeKernel::open_with_case_policy(&args.vault_root, &args.db_path, args.case_policy)
        .map_err(|source| anyhow!("open bridge kernel failed: {source}"))
}

pub(crate) fn expect_bridge_value<T>(envelope: BridgeEnvelope<T>, command: &str) -> Result<T> {
    if envelope.ok {
        return envelope
            .value
            .ok_or_else(|| anyhow!("{command} returned success without payload"));
    }

    match envelope.error {
        Some(error) => {
            let mut message = format!("{command} failed [{}]: {}", error.code, error.message);
            if let Some(hint) = error.hint {
                message.push_str(&format!("; hint: {hint}"));
            }
            Err(anyhow!(message))
        }
        None => Err(anyhow!("{command} failed without an error payload")),
    }
}

pub(crate) fn decode_base_document(config_json: &str) -> Result<BaseDocument> {
    decode_base_config_json(config_json).map_err(|source| anyhow!("{source}"))
}

pub(crate) fn open_initialized_connection(args: &ResolvedVaultPathArgs) -> Result<Connection> {
    let vault_root = Path::new(&args.vault_root);
    if !vault_root.exists() {
        return Err(anyhow!("vault root does not exist: {}", args.vault_root));
    }
    if !vault_root.is_dir() {
        return Err(anyhow!(
            "vault root is not a directory: {}",
            args.vault_root
        ));
    }

    ensure_runtime_paths_for_args(args)?;
    let mut connection = Connection::open(&args.db_path)
        .with_context(|| format!("open sqlite database '{}'", args.db_path))?;
    run_migrations(&mut connection).map_err(|source| anyhow!("run migrations failed: {source}"))?;
    Ok(connection)
}

pub(crate) fn ensure_runtime_paths_for_args(args: &ResolvedVaultPathArgs) -> Result<()> {
    ensure_runtime_paths(&tao_sdk_service::SdkConfig {
        vault_root: PathBuf::from(&args.vault_root),
        data_dir: PathBuf::from(&args.data_dir),
        db_path: PathBuf::from(&args.db_path),
        case_policy: args.case_policy,
        tracing_enabled: true,
        feature_flags: Vec::new(),
        read_only: args.read_only,
    })
    .map_err(|source| anyhow!("prepare runtime paths failed: {source}"))
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct IndexTotals {
    pub(crate) indexed_files: u64,
    pub(crate) markdown_files: u64,
    pub(crate) links_total: u64,
    pub(crate) unresolved_links: u64,
    pub(crate) properties_total: u64,
    pub(crate) bases_total: u64,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct IndexRefreshStatus {
    pub(crate) drift_paths: u64,
    pub(crate) rebuild_reason: Option<&'static str>,
}

pub(crate) fn query_index_totals(connection: &Connection) -> Result<IndexTotals> {
    let (indexed_files, markdown_files): (u64, u64) = connection
        .query_row(
            "SELECT COUNT(*), COALESCE(SUM(CASE WHEN is_markdown = 1 THEN 1 ELSE 0 END), 0) FROM files",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .context("query files totals")?;
    let (links_total, unresolved_links): (u64, u64) = connection
        .query_row(
            "SELECT COUNT(*), COALESCE(SUM(CASE WHEN is_unresolved = 1 THEN 1 ELSE 0 END), 0) FROM links",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .context("query links totals")?;
    let properties_total: u64 = connection
        .query_row("SELECT COUNT(*) FROM properties", [], |row| row.get(0))
        .context("query properties total")?;
    let bases_total: u64 = connection
        .query_row("SELECT COUNT(*) FROM bases", [], |row| row.get(0))
        .context("query bases total")?;

    Ok(IndexTotals {
        indexed_files,
        markdown_files,
        links_total,
        unresolved_links,
        properties_total,
        bases_total,
    })
}

pub(crate) fn query_index_refresh_status(
    vault_root: &Path,
    connection: &Connection,
    case_policy: CasePolicy,
) -> Result<IndexRefreshStatus> {
    let drift = ReconciliationScannerService::default()
        .scan(vault_root, connection, case_policy)
        .map_err(|source| anyhow!("scan index drift failed: {source}"))?;
    let inconsistent_paths = count_inconsistent_file_rows(vault_root, connection, case_policy)?;
    let rebuild_reason = if index_requires_full_rebuild(connection)? {
        Some("link_resolution_version_mismatch")
    } else if inconsistent_paths > 0 {
        Some("file_path_mismatch")
    } else {
        None
    };
    Ok(IndexRefreshStatus {
        drift_paths: if rebuild_reason.is_some() {
            drift.drift_paths.max(inconsistent_paths).max(1)
        } else {
            drift.drift_paths
        },
        rebuild_reason,
    })
}

pub(crate) fn index_requires_full_rebuild(connection: &Connection) -> Result<bool> {
    let Some(record) =
        IndexStateRepository::get_by_key(connection, LINK_RESOLUTION_VERSION_STATE_KEY)
            .map_err(|source| anyhow!("read link resolution version failed: {source}"))?
    else {
        return Ok(true);
    };

    let stored_version = serde_json::from_str::<u32>(&record.value_json).unwrap_or_default();
    Ok(stored_version != CURRENT_LINK_RESOLUTION_VERSION)
}

pub(crate) fn count_inconsistent_file_rows(
    vault_root: &Path,
    connection: &Connection,
    case_policy: CasePolicy,
) -> Result<u64> {
    let canonicalizer = PathCanonicalizationService::new(vault_root, case_policy)
        .map_err(|source| anyhow!("create vault canonicalizer failed: {source}"))?;
    let files = FilesRepository::list_all(connection)
        .map_err(|source| anyhow!("list indexed files failed: {source}"))?;

    let mut mismatches = 0_u64;
    for file in files {
        let absolute = Path::new(&file.absolute_path);
        let Ok(canonical) = canonicalizer.canonicalize(absolute) else {
            mismatches = mismatches.saturating_add(1);
            continue;
        };
        if canonical.normalized != file.normalized_path {
            mismatches = mismatches.saturating_add(1);
        }
    }

    Ok(mismatches)
}
