use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DaemonExecutionPolicy {
    ObservationalFresh,
    CachedReadWithRefresh,
    ExplicitWork,
}

impl DaemonExecutionPolicy {
    pub(crate) fn refreshes_runtime(self) -> bool {
        matches!(self, Self::CachedReadWithRefresh)
    }

    pub(crate) fn uses_result_cache(self) -> bool {
        matches!(self, Self::CachedReadWithRefresh)
    }

    pub(crate) fn clears_runtime_cache_on_success(self) -> bool {
        matches!(self, Self::ExplicitWork)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DaemonExecuteRequest {
    pub(crate) command: Commands,
    pub(crate) allow_writes: bool,
    pub(crate) json: bool,
    pub(crate) json_stream: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum DaemonRequest {
    Execute { payload: Box<DaemonExecuteRequest> },
    Status,
    Shutdown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DaemonStatus {
    pub(crate) uptime_ms: u128,
    pub(crate) cached_connections: usize,
    pub(crate) cached_kernels: usize,
    pub(crate) cached_results: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DaemonResponse {
    pub(crate) ok: bool,
    pub(crate) output: Option<String>,
    pub(crate) error: Option<String>,
    pub(crate) status: Option<DaemonStatus>,
}

pub(crate) fn maybe_forward_to_daemon(cli: &Cli) -> Result<Option<String>> {
    if is_daemon_control_command(&cli.command) || !command_supports_daemon_forwarding(&cli.command)
    {
        return Ok(None);
    }

    #[cfg(not(unix))]
    {
        let _ = cli;
        return Ok(None);
    }

    #[cfg(unix)]
    {
        let Some(socket) = resolve_daemon_socket_for_cli(cli)? else {
            return Ok(None);
        };
        let request = DaemonRequest::Execute {
            payload: Box::new(DaemonExecuteRequest {
                command: cli.command.clone(),
                allow_writes: cli.allow_writes,
                json: cli.json,
                json_stream: cli.json_stream,
            }),
        };
        let mut auto_start_attempted = false;

        loop {
            let response = match daemon_request(&socket, &request) {
                Ok(response) => response,
                Err(source) => {
                    if daemon_socket_is_unavailable(&source) && !auto_start_attempted {
                        ensure_daemon_running(&socket, DEFAULT_DAEMON_STARTUP_TIMEOUT_MS)?;
                        auto_start_attempted = true;
                        continue;
                    }
                    return Err(source);
                }
            };
            if !response.ok {
                let message = response
                    .error
                    .unwrap_or_else(|| "daemon returned unknown failure".to_string());
                return Err(anyhow!(message));
            }
            return response
                .output
                .map(Some)
                .ok_or_else(|| anyhow!("daemon execute response missing output payload"));
        }
    }
}

pub(crate) fn command_supports_daemon_forwarding(command: &Commands) -> bool {
    !matches!(command, Commands::Tools(_))
}

pub(crate) fn resolve_daemon_socket_for_cli(cli: &Cli) -> Result<Option<String>> {
    if let Some(socket) = cli.daemon_socket.as_ref() {
        return Ok(Some(socket.clone()));
    }
    let Some(vault) = (match resolve_command_vault_paths(&cli.command) {
        Ok(vault) => vault,
        Err(_) => return Ok(None),
    }) else {
        return Ok(None);
    };
    Ok(Some(derive_daemon_socket_for_vault(&vault.vault_root)?))
}

pub(crate) fn resolve_command_vault_paths(
    command: &Commands,
) -> Result<Option<ResolvedVaultPathArgs>> {
    let resolved = match command {
        Commands::Tools(_) => return Ok(None),
        Commands::Health(args) => args.resolve()?,
        Commands::Doc { command } => match command {
            DocCommands::Read(args) => args.resolve()?,
            DocCommands::Write(args) => args.resolve()?,
            DocCommands::List(args) => args.resolve()?,
        },
        Commands::Base { command } => match command {
            BaseCommands::List(args) => args.resolve()?,
            BaseCommands::View(args) => args.resolve()?,
            BaseCommands::Schema(args) => args.resolve()?,
            BaseCommands::Validate(args) => args.resolve()?,
        },
        Commands::Graph { command } => match command {
            GraphCommands::Outgoing(args) => args.resolve()?,
            GraphCommands::Backlinks(args) => args.resolve()?,
            GraphCommands::InboundScope(args) => args.resolve()?,
            GraphCommands::Unresolved(args) => args.resolve()?,
            GraphCommands::Deadends(args) => args.resolve()?,
            GraphCommands::Orphans(args) => args.resolve()?,
            GraphCommands::Floating(args) => args.resolve()?,
            GraphCommands::Components(args) => args.resolve()?,
            GraphCommands::Neighbors(args) => args.resolve()?,
            GraphCommands::Path(args) => args.resolve()?,
            GraphCommands::Walk(args) => args.resolve()?,
        },
        Commands::Meta { command } => match command {
            MetaCommands::Properties(args) => args.resolve()?,
            MetaCommands::Tags(args) => args.resolve()?,
            MetaCommands::Aliases(args) => args.resolve()?,
            MetaCommands::Tasks(args) => args.resolve()?,
        },
        Commands::Task { command } => match command {
            TaskCommands::List(args) => args.resolve()?,
            TaskCommands::SetState(args) => args.resolve()?,
        },
        Commands::Query(args) => args.resolve()?,
        Commands::Vault { command } => match command {
            VaultCommands::Open(args) => args.resolve()?,
            VaultCommands::Stats(args) => args.resolve()?,
            VaultCommands::Preflight(args) => args.resolve()?,
            VaultCommands::Reindex(args) => args.resolve()?,
            VaultCommands::Reconcile(args) => args.resolve()?,
            VaultCommands::Daemon { .. } | VaultCommands::DaemonServe(_) => return Ok(None),
        },
    };

    Ok(Some(resolved))
}

pub(crate) fn is_daemon_control_command(command: &Commands) -> bool {
    matches!(
        command,
        Commands::Vault {
            command: VaultCommands::Daemon { .. } | VaultCommands::DaemonServe(_)
        }
    )
}

pub(crate) fn daemon_cache_key(command: &Commands) -> Result<String> {
    serde_json::to_string(command).context("serialize command cache key")
}

pub(crate) fn daemon_execution_policy(command: &Commands) -> DaemonExecutionPolicy {
    match command {
        Commands::Tools(_) => DaemonExecutionPolicy::ExplicitWork,
        Commands::Health(_) => DaemonExecutionPolicy::ObservationalFresh,
        Commands::Doc { command } => match command {
            DocCommands::Read(_) | DocCommands::List(_) => {
                DaemonExecutionPolicy::CachedReadWithRefresh
            }
            DocCommands::Write(_) => DaemonExecutionPolicy::ExplicitWork,
        },
        Commands::Base { .. } => DaemonExecutionPolicy::CachedReadWithRefresh,
        Commands::Graph { .. } => DaemonExecutionPolicy::CachedReadWithRefresh,
        Commands::Meta { .. } => DaemonExecutionPolicy::CachedReadWithRefresh,
        Commands::Task { command } => match command {
            TaskCommands::List(_) => DaemonExecutionPolicy::CachedReadWithRefresh,
            TaskCommands::SetState(_) => DaemonExecutionPolicy::ExplicitWork,
        },
        Commands::Query(_) => DaemonExecutionPolicy::CachedReadWithRefresh,
        Commands::Vault { command } => match command {
            VaultCommands::Stats(_) | VaultCommands::Preflight(_) => {
                DaemonExecutionPolicy::ObservationalFresh
            }
            VaultCommands::Open(_)
            | VaultCommands::Reindex(_)
            | VaultCommands::Reconcile(_)
            | VaultCommands::Daemon { .. }
            | VaultCommands::DaemonServe(_) => DaemonExecutionPolicy::ExplicitWork,
        },
    }
}

pub(crate) fn maybe_refresh_daemon_state(
    command: &Commands,
    runtime: &mut RuntimeMode,
) -> Result<bool> {
    let RuntimeMode::Daemon(_) = runtime else {
        return Ok(false);
    };
    if !daemon_execution_policy(command).refreshes_runtime() {
        return Ok(false);
    }

    let Some(resolved) = resolve_command_vault_paths(command)? else {
        return Ok(false);
    };
    let runtime_key = runtime_cache_key(&resolved);
    let (current_generation, first_observation) = if let RuntimeMode::Daemon(cache) = runtime {
        let first_observation = if !cache.change_monitors.contains_key(&runtime_key) {
            let monitor =
                VaultChangeMonitor::start(Path::new(&resolved.vault_root)).with_context(|| {
                    format!(
                        "start daemon filesystem monitor for vault '{}'",
                        resolved.vault_root
                    )
                })?;
            cache.change_monitors.insert(runtime_key.clone(), monitor);
            true
        } else {
            false
        };
        let generation = cache
            .change_monitors
            .get(&runtime_key)
            .map(VaultChangeMonitor::generation)
            .unwrap_or(0);
        (generation, first_observation)
    } else {
        (0, false)
    };

    if first_observation {
        let refreshed = with_connection(runtime, &resolved, |connection| {
            let refresh = query_index_refresh_status(
                Path::new(&resolved.vault_root),
                connection,
                resolved.case_policy,
            )?;
            if let Some(reason) = refresh.rebuild_reason {
                FullIndexService::default()
                    .rebuild(
                        Path::new(&resolved.vault_root),
                        connection,
                        resolved.case_policy,
                    )
                    .map_err(|source| anyhow!("daemon initial full rebuild failed: {source}"))?;
                return Ok(Some(reason));
            }
            if refresh.drift_paths > 0 {
                WatchReconcileService::default()
                    .reconcile_once(
                        Path::new(&resolved.vault_root),
                        connection,
                        resolved.case_policy,
                    )
                    .map_err(|source| anyhow!("daemon initial reconcile failed: {source}"))?;
                return Ok(Some("drift"));
            }
            Ok(None)
        })?;
        if let RuntimeMode::Daemon(cache) = runtime {
            cache
                .last_reconciled_generation
                .insert(runtime_key.clone(), current_generation);
            if refreshed.is_some() {
                clear_cached_results_for_runtime(cache, &runtime_key);
            }
        }
        return Ok(refreshed.is_some());
    }

    if let RuntimeMode::Daemon(cache) = runtime
        && cache
            .last_reconciled_generation
            .get(&runtime_key)
            .is_some_and(|generation| *generation == current_generation)
    {
        return Ok(false);
    }

    let reconcile = with_connection(runtime, &resolved, |connection| {
        WatchReconcileService::default()
            .reconcile_once(
                Path::new(&resolved.vault_root),
                connection,
                resolved.case_policy,
            )
            .map_err(|source| anyhow!("daemon reconcile failed: {source}"))
    })?;

    if let RuntimeMode::Daemon(cache) = runtime {
        cache
            .last_reconciled_generation
            .insert(runtime_key.clone(), current_generation);
        if reconcile.drift_paths > 0 {
            clear_cached_results_for_runtime(cache, &runtime_key);
        }
    }

    Ok(reconcile.drift_paths > 0)
}

pub(crate) fn runtime_state_for_resolved(
    resolved: &ResolvedVaultPathArgs,
    runtime: &RuntimeMode,
) -> CliRuntimeState {
    match runtime {
        RuntimeMode::OneShot => CliRuntimeState {
            backend: "oneshot",
            daemon_running: false,
            change_monitor_initialized: false,
            cached_connection: false,
        },
        RuntimeMode::Daemon(cache) => {
            let runtime_key = runtime_cache_key(resolved);
            CliRuntimeState {
                backend: "daemon",
                daemon_running: true,
                change_monitor_initialized: cache.change_monitors.contains_key(&runtime_key),
                cached_connection: cache.connections.contains_key(&runtime_key),
            }
        }
    }
}

pub(crate) fn watcher_status_for_runtime_state(runtime_state: &CliRuntimeState) -> WatcherStatus {
    if runtime_state.change_monitor_initialized {
        WatcherStatus::Running
    } else {
        WatcherStatus::Stopped
    }
}

pub(crate) fn update_daemon_command_cache(
    runtime: &mut RuntimeMode,
    policy: DaemonExecutionPolicy,
    runtime_key: Option<&str>,
    cache_key: Option<String>,
    result: &CommandResult,
) {
    let RuntimeMode::Daemon(cache) = runtime else {
        return;
    };

    if policy.uses_result_cache()
        && let Some(key) = cache_key
    {
        let cache_runtime_key = runtime_key.unwrap_or("<global>").to_string();
        cache.command_results.insert(
            key,
            CachedCommandResult {
                runtime_key: cache_runtime_key,
                result: result.clone(),
            },
        );
        return;
    }

    if policy.clears_runtime_cache_on_success() {
        if let Some(runtime_key) = runtime_key {
            clear_cached_results_for_runtime(cache, runtime_key);
        } else {
            cache.command_results.clear();
        }
    }
}

pub(crate) fn clear_cached_results_for_runtime(cache: &mut RuntimeCache, runtime_key: &str) {
    cache
        .command_results
        .retain(|_, entry| entry.runtime_key != runtime_key);
}

pub(crate) fn resolve_daemon_socket(
    socket_override: Option<&str>,
    vault_root_override: Option<&str>,
    db_path_override: Option<&str>,
) -> Result<String> {
    if let Some(socket) = socket_override {
        return Ok(socket.to_string());
    }
    let resolved = resolve_vault_paths(vault_root_override, db_path_override)?;
    derive_daemon_socket_for_vault(&resolved.vault_root)
}

pub(crate) fn derive_daemon_socket_for_vault(vault_root: &str) -> Result<String> {
    let socket_dir = default_daemon_socket_dir()?;
    let hash = blake3::hash(vault_root.as_bytes()).to_hex().to_string();
    let file_name = format!("vault-{}.sock", &hash[..16]);
    Ok(socket_dir.join(file_name).to_string_lossy().to_string())
}

pub(crate) fn default_daemon_socket_dir() -> Result<PathBuf> {
    if let Some(home) = std::env::var_os("HOME") {
        return Ok(PathBuf::from(home).join(DEFAULT_DAEMON_SOCKET_DIR));
    }
    let cwd = std::env::current_dir().context("resolve cwd for daemon socket dir fallback")?;
    Ok(cwd.join(".tao/daemons"))
}

pub(crate) fn ensure_daemon_running(socket: &str, startup_timeout_ms: u64) -> Result<Option<u32>> {
    if daemon_status_probe(socket)?.is_some() {
        return Ok(None);
    }

    let current_exe = std::env::current_exe().context("resolve current executable path")?;
    let child = ProcessCommand::new(current_exe)
        .arg("vault")
        .arg("daemon-serve")
        .arg("--socket")
        .arg(socket)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .with_context(|| format!("spawn background daemon at '{socket}'"))?;
    let pid = child.id();
    wait_for_daemon_startup(socket, startup_timeout_ms)?;
    Ok(Some(pid))
}

pub(crate) fn handle_daemon(command: DaemonCommands) -> Result<CommandResult> {
    match command {
        DaemonCommands::Start(args) => {
            let socket = args.resolve_socket()?;
            if args.foreground {
                run_daemon_server(&socket)?;
                return Ok(CommandResult {
                    command: "vault.daemon.start".to_string(),
                    summary: "vault daemon foreground session stopped".to_string(),
                    args: serde_json::json!({
                        "socket": socket,
                        "foreground": true,
                        "stopped": true,
                    }),
                });
            }

            let pid = ensure_daemon_running(&socket, args.startup_timeout_ms)?;
            let started = pid.is_some();

            Ok(CommandResult {
                command: "vault.daemon.start".to_string(),
                summary: if started {
                    "vault daemon started".to_string()
                } else {
                    "vault daemon already running".to_string()
                },
                args: serde_json::json!({
                    "socket": socket,
                    "started": started,
                    "already_running": !started,
                    "pid": pid,
                }),
            })
        }
        DaemonCommands::Status(args) => {
            let socket = args.resolve_socket()?;
            let status = daemon_status_probe(&socket)?;
            match status {
                Some(status) => Ok(CommandResult {
                    command: "vault.daemon.status".to_string(),
                    summary: "vault daemon status completed".to_string(),
                    args: serde_json::json!({
                        "socket": socket,
                        "running": true,
                        "state": "running",
                        "uptime_ms": status.uptime_ms,
                        "cached_connections": status.cached_connections,
                        "cached_kernels": status.cached_kernels,
                        "cached_results": status.cached_results,
                    }),
                }),
                None => Ok(CommandResult {
                    command: "vault.daemon.status".to_string(),
                    summary: "vault daemon status completed".to_string(),
                    args: serde_json::json!({
                        "socket": socket,
                        "running": false,
                        "state": daemon_socket_state_label(&socket),
                    }),
                }),
            }
        }
        DaemonCommands::Stop(args) => {
            let socket = args.resolve_socket()?;
            let status = daemon_status_probe(&socket)?;
            if status.is_none() {
                return Ok(CommandResult {
                    command: "vault.daemon.stop".to_string(),
                    summary: "vault daemon stop completed".to_string(),
                    args: serde_json::json!({
                        "socket": socket,
                        "stopped": false,
                        "running": false,
                    }),
                });
            }

            let response = daemon_request(&socket, &DaemonRequest::Shutdown)?;
            if !response.ok {
                let message = response
                    .error
                    .unwrap_or_else(|| "daemon returned unknown failure".to_string());
                return Err(anyhow!(message));
            }
            Ok(CommandResult {
                command: "vault.daemon.stop".to_string(),
                summary: "vault daemon stop completed".to_string(),
                args: serde_json::json!({
                    "socket": socket,
                    "stopped": true,
                }),
            })
        }
        DaemonCommands::StopAll(args) => handle_daemon_stop_all(args),
    }
}

pub(crate) fn handle_daemon_stop_all(args: DaemonStopAllArgs) -> Result<CommandResult> {
    let socket_dir = match args.socket_dir {
        Some(dir) => PathBuf::from(dir),
        None => default_daemon_socket_dir()?,
    };
    let sockets = list_managed_daemon_sockets(&socket_dir)?;
    let discovered_sockets = sockets.len();
    let mut running_before_stop = 0usize;
    let mut stopped = 0usize;
    let mut pruned = 0usize;
    let mut failed = Vec::new();

    for socket in sockets {
        let socket_label = socket.to_string_lossy().to_string();
        let status = match daemon_status_probe(&socket_label) {
            Ok(status) => status,
            Err(source) => {
                failed.push(format!("{socket_label}: {source}"));
                continue;
            }
        };

        if status.is_some() {
            running_before_stop += 1;
            match daemon_request(&socket_label, &DaemonRequest::Shutdown) {
                Ok(response) if response.ok => {
                    stopped += 1;
                }
                Ok(response) => {
                    let message = response
                        .error
                        .unwrap_or_else(|| "daemon returned unknown failure".to_string());
                    failed.push(format!("{socket_label}: {message}"));
                }
                Err(source) => failed.push(format!("{socket_label}: {source}")),
            }
            continue;
        }

        let state = daemon_socket_state_label(&socket_label);
        if matches!(state, "stale" | "dead") {
            match fs::remove_file(&socket) {
                Ok(()) => pruned += 1,
                Err(source) => failed.push(format!(
                    "{}: failed to remove stale socket: {}",
                    socket_label, source
                )),
            }
        }
    }

    Ok(CommandResult {
        command: "vault.daemon.stop_all".to_string(),
        summary: "vault daemon stop-all completed".to_string(),
        args: serde_json::json!({
            "socket_dir": socket_dir.to_string_lossy(),
            "discovered_sockets": discovered_sockets,
            "running_before_stop": running_before_stop,
            "stopped": stopped,
            "pruned_stale": pruned,
            "failed": failed,
        }),
    })
}

pub(crate) fn list_managed_daemon_sockets(socket_dir: &Path) -> Result<Vec<PathBuf>> {
    if !socket_dir.exists() {
        return Ok(Vec::new());
    }
    let entries = fs::read_dir(socket_dir)
        .with_context(|| format!("read daemon socket directory '{}'", socket_dir.display()))?;
    let mut sockets = Vec::new();
    for entry in entries {
        let entry = entry.with_context(|| {
            format!(
                "read daemon socket directory entry from '{}'",
                socket_dir.display()
            )
        })?;
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) == Some("sock") {
            sockets.push(path);
        }
    }
    sockets.sort();
    Ok(sockets)
}

pub(crate) fn daemon_status_probe(socket: &str) -> Result<Option<DaemonStatus>> {
    let response = match daemon_request(socket, &DaemonRequest::Status) {
        Ok(response) => response,
        Err(source) => {
            if daemon_socket_is_unavailable(&source) {
                return Ok(None);
            }
            return Err(source);
        }
    };
    if !response.ok {
        return Ok(None);
    }
    Ok(response.status)
}

#[cfg(unix)]
pub(crate) fn daemon_socket_state_label(socket: &str) -> &'static str {
    let path = Path::new(socket);
    if !path.exists() {
        return "stopped";
    }
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(_) => return "dead",
    };
    if metadata.file_type().is_socket() {
        "stale"
    } else {
        "dead"
    }
}

#[cfg(not(unix))]
pub(crate) fn daemon_socket_state_label(_socket: &str) -> &'static str {
    "stopped"
}

pub(crate) fn daemon_socket_is_unavailable(error: &anyhow::Error) -> bool {
    for source in error.chain() {
        if let Some(io_error) = source.downcast_ref::<std::io::Error>()
            && matches!(
                io_error.kind(),
                std::io::ErrorKind::NotFound
                    | std::io::ErrorKind::ConnectionRefused
                    | std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::ConnectionReset
            )
        {
            return true;
        }
        if let Some(io_error) = source.downcast_ref::<std::io::Error>()
            && io_error.raw_os_error() == Some(38)
        {
            return true;
        }
    }
    false
}

pub(crate) fn wait_for_daemon_startup(socket: &str, timeout_ms: u64) -> Result<()> {
    let start = Instant::now();
    let timeout = Duration::from_millis(timeout_ms.max(100));
    loop {
        if daemon_status_probe(socket)?.is_some() {
            return Ok(());
        }
        if start.elapsed() >= timeout {
            return Err(anyhow!(
                "daemon startup timed out after {}ms for socket '{}'",
                timeout_ms,
                socket
            ));
        }
        thread::sleep(Duration::from_millis(25));
    }
}

pub(crate) fn daemon_request(socket: &str, request: &DaemonRequest) -> Result<DaemonResponse> {
    #[cfg(not(unix))]
    {
        let _ = socket;
        let _ = request;
        return Err(anyhow!(
            "daemon sockets are only supported on unix platforms"
        ));
    }

    #[cfg(unix)]
    {
        let mut stream = UnixStream::connect(socket)
            .with_context(|| format!("connect daemon socket '{}'", socket))?;
        let payload = serde_json::to_vec(request).context("serialize daemon request")?;
        stream
            .write_all(&payload)
            .context("write daemon request payload")?;
        stream.flush().context("flush daemon request payload")?;
        stream
            .shutdown(std::net::Shutdown::Write)
            .context("shutdown daemon request write half")?;

        let mut response_bytes = Vec::new();
        stream
            .read_to_end(&mut response_bytes)
            .context("read daemon response payload")?;
        serde_json::from_slice::<DaemonResponse>(&response_bytes)
            .context("parse daemon response payload")
    }
}

pub(crate) fn read_bounded_bytes(reader: &mut impl Read, max_bytes: u64) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    let mut limited = reader.by_ref().take(max_bytes.saturating_add(1));
    limited
        .read_to_end(&mut bytes)
        .context("read bounded payload")?;
    if bytes.len() as u64 > max_bytes {
        return Err(anyhow!(
            "request payload exceeds maximum size ({max_bytes} bytes)"
        ));
    }
    Ok(bytes)
}

pub(crate) fn run_daemon_server(socket: &str) -> Result<()> {
    #[cfg(not(unix))]
    {
        let _ = socket;
        return Err(anyhow!(
            "daemon sockets are only supported on unix platforms"
        ));
    }

    #[cfg(unix)]
    {
        let socket_path = prepare_daemon_socket_path(socket)?;

        let listener = UnixListener::bind(&socket_path)
            .with_context(|| format!("bind daemon socket '{socket}'"))?;
        let mut runtime = RuntimeMode::Daemon(Box::<RuntimeCache>::default());
        let started_at = Instant::now();
        let mut should_shutdown = false;

        // Defense-in-depth: bound request payloads to prevent unbounded memory
        // allocation from malformed or malicious local-socket clients.
        const MAX_DAEMON_REQUEST_BYTES: u64 = 16 * 1024 * 1024; // 16 MiB

        while !should_shutdown {
            let (mut stream, _) = listener.accept().context("accept daemon request stream")?;
            let request_bytes = match read_bounded_bytes(&mut stream, MAX_DAEMON_REQUEST_BYTES) {
                Ok(bytes) => bytes,
                Err(source) => {
                    let response = DaemonResponse {
                        ok: false,
                        output: None,
                        error: Some(source.to_string()),
                        status: None,
                    };
                    let bytes = serde_json::to_vec(&response)
                        .context("serialize daemon oversize response")?;
                    stream.write_all(&bytes).ok();
                    stream.flush().ok();
                    continue;
                }
            };
            let request = serde_json::from_slice::<DaemonRequest>(&request_bytes)
                .context("parse daemon request payload")?;

            let response = match request {
                DaemonRequest::Execute { payload } => {
                    let started_at = Instant::now();
                    let policy = daemon_execution_policy(&payload.command);
                    if policy.refreshes_runtime() {
                        maybe_refresh_daemon_state(&payload.command, &mut runtime)?;
                    }
                    let resolved = resolve_command_vault_paths(&payload.command)?;
                    let runtime_key = resolved.as_ref().map(runtime_cache_key);
                    let cache_key = if policy.uses_result_cache() {
                        daemon_cache_key(&payload.command).ok()
                    } else {
                        None
                    };
                    let cached =
                        if let (RuntimeMode::Daemon(cache), Some(key)) = (&runtime, &cache_key) {
                            cache
                                .command_results
                                .get(key)
                                .map(|entry| entry.result.clone())
                        } else {
                            None
                        };

                    let result = match cached {
                        Some(result) => Ok(result),
                        None => dispatch_with_runtime(
                            payload.command.clone(),
                            payload.allow_writes,
                            &mut runtime,
                        ),
                    };

                    match result {
                        Ok(result) => {
                            update_daemon_command_cache(
                                &mut runtime,
                                policy,
                                runtime_key.as_deref(),
                                cache_key,
                                &result,
                            );

                            if payload.json {
                                match maybe_render_streaming_output_for_command(
                                    &payload.command,
                                    payload.json_stream,
                                    &mut runtime,
                                ) {
                                    Ok(Some(output)) => DaemonResponse {
                                        ok: true,
                                        output: Some(output),
                                        error: None,
                                        status: None,
                                    },
                                    Ok(None) => match render_output_with_elapsed(
                                        payload.json,
                                        &result,
                                        started_at.elapsed(),
                                    ) {
                                        Ok(output) => DaemonResponse {
                                            ok: true,
                                            output: Some(output),
                                            error: None,
                                            status: None,
                                        },
                                        Err(source) => DaemonResponse {
                                            ok: false,
                                            output: None,
                                            error: Some(source.to_string()),
                                            status: None,
                                        },
                                    },
                                    Err(source) => DaemonResponse {
                                        ok: false,
                                        output: None,
                                        error: Some(source.to_string()),
                                        status: None,
                                    },
                                }
                            } else {
                                match render_output_with_elapsed(
                                    payload.json,
                                    &result,
                                    started_at.elapsed(),
                                ) {
                                    Ok(output) => DaemonResponse {
                                        ok: true,
                                        output: Some(output),
                                        error: None,
                                        status: None,
                                    },
                                    Err(source) => DaemonResponse {
                                        ok: false,
                                        output: None,
                                        error: Some(source.to_string()),
                                        status: None,
                                    },
                                }
                            }
                        }
                        Err(source) => DaemonResponse {
                            ok: false,
                            output: None,
                            error: Some(source.to_string()),
                            status: None,
                        },
                    }
                }
                DaemonRequest::Status => {
                    let status = match &runtime {
                        RuntimeMode::OneShot => DaemonStatus {
                            uptime_ms: started_at.elapsed().as_millis(),
                            cached_connections: 0,
                            cached_kernels: 0,
                            cached_results: 0,
                        },
                        RuntimeMode::Daemon(cache) => DaemonStatus {
                            uptime_ms: started_at.elapsed().as_millis(),
                            cached_connections: cache.connections.len(),
                            cached_kernels: cache.kernels.len(),
                            cached_results: cache.command_results.len(),
                        },
                    };
                    DaemonResponse {
                        ok: true,
                        output: None,
                        error: None,
                        status: Some(status),
                    }
                }
                DaemonRequest::Shutdown => {
                    should_shutdown = true;
                    DaemonResponse {
                        ok: true,
                        output: Some(String::from("daemon shutdown acknowledged")),
                        error: None,
                        status: None,
                    }
                }
            };

            let bytes =
                serde_json::to_vec(&response).context("serialize daemon response payload")?;
            stream
                .write_all(&bytes)
                .context("write daemon response payload")?;
            stream.flush().context("flush daemon response payload")?;
        }

        drop(listener);
        if socket_path.exists() {
            fs::remove_file(&socket_path)
                .with_context(|| format!("remove daemon socket '{}'", socket_path.display()))?;
        }
        Ok(())
    }
}

#[cfg(unix)]
pub(crate) fn prepare_daemon_socket_path(socket: &str) -> Result<PathBuf> {
    let socket_path = Path::new(socket);
    if let Some(parent) = socket_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create daemon socket parent '{}'", parent.display()))?;
    }
    if socket_path.exists() {
        fs::remove_file(socket_path)
            .with_context(|| format!("remove stale daemon socket '{}'", socket))?;
    }
    Ok(socket_path.to_path_buf())
}
