use super::*;

const MAX_DAEMON_CACHED_RESULTS: usize = 256;
pub(crate) const MAX_DAEMON_CACHE_BYTES: usize = 32 * 1024 * 1024;
pub(crate) const MAX_DAEMON_RESPONSE_BYTES: u64 = 16 * 1024 * 1024;
// JSON transports the public envelope as an escaped string; reserve bounded
// expansion without reducing the public response allowance on this backend.
pub(crate) const MAX_DAEMON_RESPONSE_FRAME_BYTES: u64 = MAX_DAEMON_RESPONSE_BYTES * 2 + 64 * 1024;
pub(crate) const DAEMON_PROTOCOL_VERSION: u32 = 2;
const DAEMON_IO_TIMEOUT_MS: u64 = 120_000;

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
    pub(crate) json: bool,
    pub(crate) json_stream: bool,
    #[serde(default)]
    pub(crate) context: Option<ExecutionContext>,
    #[serde(default)]
    pub(crate) protocol: u32,
    #[serde(default)]
    pub(crate) build: String,
    #[serde(default)]
    pub(crate) request_id: String,
    #[serde(default)]
    pub(crate) timeout_ms: u64,
    #[serde(default)]
    pub(crate) no_result_cache: bool,
    #[serde(default)]
    pub(crate) output_toon: bool,
    pub(crate) continuation: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum DaemonRequest {
    Execute { payload: Box<DaemonExecuteRequest> },
    Status,
    Shutdown,
    Cancel { request_id: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DaemonStatus {
    pub(crate) uptime_ms: u128,
    pub(crate) cached_connections: usize,
    pub(crate) cached_kernels: usize,
    pub(crate) cached_results: usize,
    #[serde(default)]
    pub(crate) cached_result_bytes: usize,
    #[serde(default)]
    pub(crate) active_requests: usize,
    #[serde(default)]
    pub(crate) active_reads: usize,
    #[serde(default)]
    pub(crate) writer_busy: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DaemonResponse {
    pub(crate) ok: bool,
    pub(crate) output: Option<String>,
    pub(crate) error: Option<String>,
    pub(crate) status: Option<DaemonStatus>,
    #[serde(default)]
    pub(crate) failure: Option<RemoteCliError>,
    #[serde(default)]
    pub(crate) protocol: u32,
    #[serde(default)]
    pub(crate) build: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RemoteCliError {
    pub(crate) exit_code: i32,
    pub(crate) code: String,
    pub(crate) message: String,
    pub(crate) hint: Option<String>,
    pub(crate) details: Option<JsonValue>,
}

impl std::fmt::Display for RemoteCliError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}
impl std::error::Error for RemoteCliError {}

impl DaemonResponse {
    pub(crate) fn success(output: Option<String>, status: Option<DaemonStatus>) -> Self {
        Self {
            ok: true,
            output,
            error: None,
            status,
            failure: None,
            protocol: DAEMON_PROTOCOL_VERSION,
            build: env!("TAO_BUILD_ID").to_string(),
        }
    }
    pub(crate) fn failed(source: anyhow::Error) -> Self {
        let error = classify_cli_error(&source);
        Self {
            ok: false,
            output: None,
            status: None,
            error: Some(error.error.message.clone()),
            failure: Some(RemoteCliError {
                exit_code: error.exit_kind.code(),
                code: error.error.code,
                message: error.error.message,
                hint: error.error.hint,
                details: error.error.details,
            }),
            protocol: DAEMON_PROTOCOL_VERSION,
            build: env!("TAO_BUILD_ID").to_string(),
        }
    }
    pub(crate) fn check_compatibility(&self) -> Result<()> {
        if self.protocol != DAEMON_PROTOCOL_VERSION || self.build != env!("TAO_BUILD_ID") {
            return Err(CliContractError::blocked(
                "daemon_incompatible",
                "daemon protocol or build differs from this executable",
                Some("stop the existing daemon or use --execution-mode direct".to_string()),
                None,
            )
            .into());
        }
        Ok(())
    }
}

pub(crate) fn maybe_forward_to_daemon(cli: &Cli) -> Result<Option<String>> {
    if cli.execution_mode == ExecutionMode::Direct
        || is_daemon_control_command(&cli.command)
        || !command_supports_daemon_forwarding(&cli.command)
    {
        return Ok(None);
    }
    #[cfg(not(unix))]
    {
        if cli.execution_mode == ExecutionMode::RequiredDaemon {
            return Err(CliContractError::daemon_unavailable(
                "daemon execution requires Unix sockets",
            )
            .into());
        }
        return Ok(None);
    }
    #[cfg(unix)]
    {
        let resolved = resolve_command_vault_paths(&cli.command)?;
        let Some(socket) = resolve_daemon_socket_for_cli(cli)? else {
            return Ok(None);
        };
        // Establish compatibility before submitting work. Only connection failure before
        // submission is retryable; an uncertain executed request is never replayed.
        let status = match daemon_status_probe(&socket) {
            Ok(status) => status,
            Err(error) if cli.execution_mode == ExecutionMode::Auto => {
                let _ = error;
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        if status.is_none() {
            if cli.execution_mode == ExecutionMode::RequiredDaemon {
                return Err(
                    CliContractError::daemon_unavailable("required daemon is not running").into(),
                );
            }
            if daemon_execution_policy(&cli.command) == DaemonExecutionPolicy::ObservationalFresh {
                return Ok(None);
            }
            if ensure_daemon_running(&socket, DEFAULT_DAEMON_STARTUP_TIMEOUT_MS).is_err() {
                return Ok(None);
            }
        }
        let request_id = new_request_id();
        let request = DaemonRequest::Execute {
            payload: Box::new(DaemonExecuteRequest {
                command: cli.command.clone(),
                json: cli.json,
                json_stream: cli.json_stream,
                context: resolved.as_ref().map(ExecutionContext::from),
                protocol: DAEMON_PROTOCOL_VERSION,
                build: env!("TAO_BUILD_ID").to_string(),
                request_id: request_id.clone(),
                timeout_ms: cli.timeout_ms,
                no_result_cache: cli.no_result_cache,
                output_toon: cli.toon,
                continuation: cli.continuation.clone(),
            }),
        };
        let response = match daemon_request_with_timeout(
            &socket,
            &request,
            cli.timeout_ms.saturating_add(1000),
        ) {
            Ok(response) => response,
            Err(error) => {
                let _ = daemon_request_with_timeout(
                    &socket,
                    &DaemonRequest::Cancel { request_id },
                    500,
                );
                return Err(error);
            }
        };
        response.check_compatibility()?;
        if !response.ok {
            return Err(response
                .failure
                .map(anyhow::Error::from)
                .unwrap_or_else(|| {
                    anyhow!(
                        response
                            .error
                            .unwrap_or_else(|| "daemon failed without diagnostic".to_string())
                    )
                }));
        }
        let output = response
            .output
            .ok_or_else(|| anyhow!("daemon execute response missing output payload"))?;
        if cli.toon {
            let envelope: JsonValue = serde_json::from_str(&output)?;
            return Ok(Some(toon_format::encode_default(&envelope)?));
        }
        Ok(Some(output))
    }
}

pub(crate) fn new_request_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(1);
    let time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!(
        "{}-{time:x}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

pub(crate) fn command_supports_daemon_forwarding(command: &Commands) -> bool {
    !matches!(command, Commands::Tools(_) | Commands::Config { .. })
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
        Commands::Config { .. } => return Ok(None),
        Commands::Health(args) => args.resolve().map_err(|source| {
            commands::health::health_blocked_error(
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
        })?,
        Commands::Doc { command } => match command {
            DocCommands::Read(args) => args.resolve()?,
            DocCommands::List(args) => args.resolve()?,
        },
        Commands::Base { command } => match command {
            BaseCommands::List(args) => args.resolve()?,
            BaseCommands::View(args) => args.resolve()?,
            BaseCommands::Schema(args) => args.resolve()?,
        },
        Commands::Graph { command } => match command {
            GraphCommands::Links(args) => args.resolve()?,
            GraphCommands::Audit(args) => args.resolve()?,
            GraphCommands::Path(args) => args.resolve()?,
            GraphCommands::Walk(args) => args.resolve()?,
        },
        Commands::Meta { command } => match command {
            MetaCommands::Properties(args) => args.resolve()?,
            MetaCommands::Tags(args) => args.resolve()?,
            MetaCommands::Aliases(args) => args.resolve()?,
        },
        Commands::Task { command } => match command {
            TaskCommands::List(args) => args.resolve()?,
        },
        Commands::Validate(args) => args.resolve()?,
        Commands::Search(args) => args.resolve()?,
        Commands::Query(args) => args.resolve()?,
        Commands::Vault { command } => match command {
            VaultCommands::Open(args) => args.resolve()?,
            VaultCommands::Preflight(args) => args.resolve()?,
            VaultCommands::Reindex(args) => args.resolve()?,
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
    let mut normalized = serde_json::to_value(command)?;
    if let Some(args) = normalized
        .as_object_mut()
        .and_then(|value| value.values_mut().next())
        && let Some(query) = args.get_mut("query")
        && let Some(text) = query.as_str()
    {
        *query = JsonValue::String(text.trim().to_string());
    }
    serde_json::to_string(&normalized).context("serialize command cache key")
}

pub(crate) fn daemon_execution_policy(command: &Commands) -> DaemonExecutionPolicy {
    match command {
        Commands::Tools(_) => DaemonExecutionPolicy::ExplicitWork,
        Commands::Config { .. } => DaemonExecutionPolicy::ExplicitWork,
        Commands::Health(_) => DaemonExecutionPolicy::ObservationalFresh,
        Commands::Doc { command } => match command {
            DocCommands::Read(_) | DocCommands::List(_) => {
                DaemonExecutionPolicy::CachedReadWithRefresh
            }
        },
        Commands::Base { .. } => DaemonExecutionPolicy::CachedReadWithRefresh,
        Commands::Graph { .. } => DaemonExecutionPolicy::CachedReadWithRefresh,
        Commands::Meta { .. } => DaemonExecutionPolicy::CachedReadWithRefresh,
        Commands::Task { command } => match command {
            TaskCommands::List(_) => DaemonExecutionPolicy::CachedReadWithRefresh,
        },
        Commands::Validate(_) => DaemonExecutionPolicy::ObservationalFresh,
        Commands::Search(_) => DaemonExecutionPolicy::CachedReadWithRefresh,
        Commands::Query(args) if args.explain && !args.execute => {
            DaemonExecutionPolicy::ObservationalFresh
        }
        Commands::Query(_) => DaemonExecutionPolicy::CachedReadWithRefresh,
        Commands::Vault { command } => match command {
            VaultCommands::Reindex(args) if args.dry_run => {
                DaemonExecutionPolicy::ObservationalFresh
            }
            VaultCommands::Preflight(_) => DaemonExecutionPolicy::ObservationalFresh,
            VaultCommands::Open(_)
            | VaultCommands::Reindex(_)
            | VaultCommands::Daemon { .. }
            | VaultCommands::DaemonServe(_) => DaemonExecutionPolicy::ExplicitWork,
        },
    }
}

#[cfg(test)]
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
    refresh_daemon_runtime_state(runtime, &resolved)
}

pub(crate) fn observe_daemon_runtime(
    runtime: &mut RuntimeMode,
    resolved: &ResolvedVaultPathArgs,
) -> Result<(u64, bool)> {
    let runtime_key = runtime_cache_key(resolved);
    let observation = if let RuntimeMode::Daemon(cache) = runtime {
        let first_observation = if !cache.change_monitors.contains_key(&runtime_key) {
            let monitor = VaultChangeMonitor::start_with_case_policy(
                Path::new(&resolved.vault_root),
                resolved.case_policy,
            )
            .with_context(|| {
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
    Ok(observation)
}

pub(crate) fn acknowledge_daemon_refresh(
    runtime: &mut RuntimeMode,
    resolved: &ResolvedVaultPathArgs,
    generation: u64,
) {
    if let RuntimeMode::Daemon(cache) = runtime {
        let key = runtime_cache_key(resolved);
        cache
            .last_reconciled_generation
            .insert(key.clone(), generation);
        if let Some(monitor) = cache.change_monitors.get(&key) {
            let _ = monitor.acknowledge_reconciled(generation);
        }
        clear_cached_results_for_runtime(cache, &key);
    }
}

pub(crate) fn daemon_refresh_pending(
    runtime: &RuntimeMode,
    resolved: &ResolvedVaultPathArgs,
) -> bool {
    let RuntimeMode::Daemon(cache) = runtime else {
        return false;
    };
    let key = runtime_cache_key(resolved);
    let observed = cache
        .change_monitors
        .get(&key)
        .map(VaultChangeMonitor::generation);
    observed.is_none() || cache.last_reconciled_generation.get(&key).copied() != observed
}

pub(crate) fn refresh_daemon_runtime_state(
    runtime: &mut RuntimeMode,
    resolved: &ResolvedVaultPathArgs,
) -> Result<bool> {
    let runtime_key = runtime_cache_key(resolved);
    let (current_generation, _) = observe_daemon_runtime(runtime, resolved)?;
    let first_observation = matches!(runtime, RuntimeMode::Daemon(cache) if !cache.last_reconciled_generation.contains_key(&runtime_key));

    if first_observation {
        let refreshed = with_connection(runtime, resolved, |connection| {
            let outcome = IndexRefreshService
                .refresh(
                    Path::new(&resolved.vault_root),
                    connection,
                    resolved.case_policy,
                    IndexRefreshOptions::default(),
                )
                .map_err(|source| anyhow!("daemon initial index refresh failed: {source}"))?;
            if matches!(outcome.mode, IndexRefreshMode::Current) {
                Ok(None)
            } else {
                Ok(Some(outcome.reason.unwrap_or(outcome.mode.label())))
            }
        })?;
        if let RuntimeMode::Daemon(cache) = runtime {
            cache
                .last_reconciled_generation
                .insert(runtime_key.clone(), current_generation);
            if let Some(monitor) = cache.change_monitors.get(&runtime_key) {
                let _ = monitor.acknowledge_reconciled(current_generation);
            }
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

    let reconcile = with_connection(runtime, resolved, |connection| {
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
        if let Some(monitor) = cache.change_monitors.get(&runtime_key) {
            let _ = monitor.acknowledge_reconciled(current_generation);
        }
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
            watcher_last_error: None,
        },
        RuntimeMode::Snapshot(cache) => {
            let (daemon_running, monitor_running, monitor_error) = request_daemon_state();
            CliRuntimeState {
                backend: if daemon_running { "daemon" } else { "direct" },
                daemon_running,
                change_monitor_initialized: monitor_running,
                watcher_last_error: monitor_error,
                cached_connection: cache.connections.contains_key(&runtime_cache_key(resolved)),
            }
        }
        RuntimeMode::Daemon(cache) => {
            let runtime_key = runtime_cache_key(resolved);
            CliRuntimeState {
                backend: "daemon",
                daemon_running: true,
                change_monitor_initialized: cache.change_monitors.contains_key(&runtime_key),
                watcher_last_error: cache
                    .change_monitors
                    .get(&runtime_key)
                    .and_then(|monitor| monitor.health().last_error),
                cached_connection: cache.connections.contains_key(&runtime_key),
            }
        }
    }
}

pub(crate) fn watcher_status_for_runtime_state(runtime_state: &CliRuntimeState) -> WatcherStatus {
    if let Some(reason) = &runtime_state.watcher_last_error {
        WatcherStatus::Degraded {
            reason: reason.clone(),
        }
    } else if runtime_state.change_monitor_initialized {
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
        let bytes = serde_json::to_vec(result)
            .map(|value| value.len())
            .unwrap_or(usize::MAX);
        if bytes > MAX_DAEMON_CACHE_BYTES / 8 {
            return;
        }
        let cache_runtime_key = runtime_key.unwrap_or("<global>").to_string();
        cache.command_result_sizes.insert(key.clone(), bytes);
        cache
            .command_result_order
            .retain(|existing| existing != &key);
        cache.command_result_order.push_back(key.clone());
        cache.command_results.insert(
            key,
            CachedCommandResult {
                runtime_key: cache_runtime_key,
                result: result.clone(),
            },
        );
        evict_daemon_command_cache(cache);
        return;
    }

    if policy.clears_runtime_cache_on_success() {
        if let Some(runtime_key) = runtime_key {
            clear_cached_results_for_runtime(cache, runtime_key);
        } else {
            cache.command_results.clear();
            cache.command_result_order.clear();
            cache.command_result_sizes.clear();
            cache.result_bytes = 0;
        }
    }
}

pub(crate) fn clear_cached_results_for_runtime(cache: &mut RuntimeCache, runtime_key: &str) {
    cache
        .command_results
        .retain(|_, entry| entry.runtime_key != runtime_key);
    cache
        .command_result_order
        .retain(|key| cache.command_results.contains_key(key));
    cache
        .command_result_sizes
        .retain(|key, _| cache.command_results.contains_key(key));
    cache.result_bytes = cache.command_result_sizes.values().sum();
}

fn evict_daemon_command_cache(cache: &mut RuntimeCache) {
    cache.result_bytes = cache.command_result_sizes.values().sum();
    while cache.command_results.len() > MAX_DAEMON_CACHED_RESULTS
        || cache.result_bytes > MAX_DAEMON_CACHE_BYTES
    {
        let Some(oldest) = cache.command_result_order.pop_front() else {
            break;
        };
        cache.command_results.remove(&oldest);
        cache.result_bytes = cache
            .result_bytes
            .saturating_sub(cache.command_result_sizes.remove(&oldest).unwrap_or(0));
    }
    cache
        .command_result_order
        .retain(|key| cache.command_results.contains_key(key));
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
            match prune_stale_daemon_socket(&socket_label) {
                Ok(true) => pruned += 1,
                Ok(false) => {}
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

fn is_socket_type(file_type: &std::fs::FileType) -> bool {
    #[cfg(unix)]
    {
        file_type.is_socket()
    }
    #[cfg(not(unix))]
    {
        let _ = file_type;
        false
    }
}

fn prune_stale_daemon_socket(socket: &str) -> Result<bool> {
    #[cfg(unix)]
    {
        daemon_socket::prune_stale(socket)
    }
    #[cfg(not(unix))]
    {
        let _ = socket;
        Ok(false)
    }
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
        if path.extension().and_then(|value| value.to_str()) == Some("sock")
            && is_socket_type(&entry.file_type()?)
            && fs::symlink_metadata(path.with_extension("sock.lock"))
                .is_ok_and(|metadata| metadata.file_type().is_file())
        {
            sockets.push(path);
        }
    }
    sockets.sort();
    Ok(sockets)
}

pub(crate) fn daemon_status_probe(socket: &str) -> Result<Option<DaemonStatus>> {
    let response = match daemon_request_with_timeout(socket, &DaemonRequest::Status, 1000) {
        Ok(response) => response,
        Err(source) => {
            if daemon_socket_is_unavailable(&source) {
                return Ok(None);
            }
            return Err(source);
        }
    };
    response.check_compatibility()?;
    if !response.ok {
        return Err(response
            .failure
            .map(anyhow::Error::from)
            .unwrap_or_else(|| anyhow!("daemon status failed")));
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
        if let Some(contract_error) = source.downcast_ref::<CliContractError>()
            && contract_error.code == "daemon_unavailable"
        {
            return true;
        }
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
            return Err(CliContractError::daemon_unavailable(format!(
                "daemon startup timed out after {}ms for socket '{}'",
                timeout_ms, socket
            ))
            .into());
        }
        thread::sleep(Duration::from_millis(25));
    }
}

pub(crate) fn daemon_request(socket: &str, request: &DaemonRequest) -> Result<DaemonResponse> {
    daemon_request_with_timeout(socket, request, DAEMON_IO_TIMEOUT_MS)
}

pub(crate) fn daemon_request_with_timeout(
    socket: &str,
    request: &DaemonRequest,
    timeout_ms: u64,
) -> Result<DaemonResponse> {
    #[cfg(not(unix))]
    {
        let _ = (socket, request, timeout_ms);
        Err(anyhow!("daemon sockets require Unix"))
    }
    #[cfg(unix)]
    {
        let mut stream = UnixStream::connect(socket).map_err(|source| {
            CliContractError::daemon_unavailable(format!(
                "connect daemon socket '{socket}': {source}"
            ))
        })?;
        let deadline = Instant::now() + Duration::from_millis(timeout_ms.max(1));
        let payload = serde_json::to_vec(request).context("serialize daemon request")?;
        if payload.len() > 1024 * 1024 {
            return Err(runtime_error(
                "request_too_large",
                "request exceeds 1 MiB framing limit",
            ));
        }
        write_socket_frame(&mut stream, &payload, deadline)?;
        stream
            .shutdown(std::net::Shutdown::Write)
            .context("finish daemon request")?;
        let bytes = read_socket_frame(&mut stream, MAX_DAEMON_RESPONSE_FRAME_BYTES, deadline)?;
        serde_json::from_slice(&bytes).context("parse daemon response payload")
    }
}

#[cfg(unix)]
pub(crate) fn write_socket_frame(
    stream: &mut UnixStream,
    mut bytes: &[u8],
    deadline: Instant,
) -> Result<()> {
    stream
        .set_nonblocking(true)
        .context("configure daemon frame writer")?;
    while !bytes.is_empty() {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(runtime_error(
                "request_timeout",
                "socket delivery deadline exceeded",
            ));
        }
        match stream.write(bytes) {
            Ok(0) => {
                return Err(runtime_error(
                    "connection_closed",
                    "peer stopped accepting the response",
                ));
            }
            Ok(count) => bytes = &bytes[count..],
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted
                ) =>
            {
                let timeout =
                    rustix::event::Timespec::try_from(remaining.min(Duration::from_millis(100)))?;
                let mut descriptors = [rustix::event::PollFd::new(
                    &*stream,
                    rustix::event::PollFlags::OUT,
                )];
                match rustix::event::poll(&mut descriptors, Some(&timeout)) {
                    Ok(_) | Err(rustix::io::Errno::INTR) => {}
                    Err(error) => return Err(anyhow!("wait for daemon delivery: {error}")),
                }
            }
            Err(error) => return Err(error).context("write daemon frame"),
        }
    }
    Ok(())
}

#[cfg(unix)]
pub(crate) fn read_socket_frame(
    stream: &mut UnixStream,
    max_bytes: u64,
    deadline: Instant,
) -> Result<Vec<u8>> {
    // Darwin rejects SO_RCVTIMEO changes after the peer closes; nonblocking
    // reads keep EOF and absolute deadlines reliable without resetting it.
    stream
        .set_nonblocking(true)
        .context("configure daemon frame reader")?;
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 8192];
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(runtime_error(
                "request_timeout",
                "socket frame deadline exceeded",
            ));
        }
        match stream.read(&mut buffer) {
            Ok(0) => return Ok(bytes),
            Ok(count) => {
                if bytes.len().saturating_add(count) as u64 > max_bytes {
                    return Err(runtime_error(
                        "request_too_large",
                        format!("frame exceeds maximum size ({max_bytes} bytes)"),
                    ));
                }
                bytes.extend_from_slice(&buffer[..count]);
            }
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock
                        | std::io::ErrorKind::TimedOut
                        | std::io::ErrorKind::Interrupted
                ) =>
            {
                let timeout =
                    rustix::event::Timespec::try_from(remaining.min(Duration::from_millis(100)))?;
                let mut descriptors = [rustix::event::PollFd::new(
                    &*stream,
                    rustix::event::PollFlags::IN,
                )];
                match rustix::event::poll(&mut descriptors, Some(&timeout)) {
                    Ok(_) | Err(rustix::io::Errno::INTR) => {}
                    Err(error) => return Err(anyhow!("wait for daemon frame: {error}")),
                }
            }
            Err(error) => return Err(error).context("read daemon frame"),
        }
    }
}

#[cfg(test)]
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
    #[cfg(unix)]
    {
        super::daemon_server::run(socket)
    }
    #[cfg(not(unix))]
    {
        let _ = socket;
        Err(anyhow!("daemon sockets require Unix"))
    }
}

#[cfg(all(unix, test))]
pub(crate) fn prepare_daemon_socket_path(socket: &str) -> Result<PathBuf> {
    super::daemon_socket::prepare_path(socket)
}
