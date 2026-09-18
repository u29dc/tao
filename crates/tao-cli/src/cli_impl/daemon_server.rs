use super::*;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, TryLockError};

const MAX_CLIENTS: usize = 32;
const CONTROL_RESERVE: usize = 4;
const MAX_READERS: usize = 4;
const MAX_RUNTIMES: usize = 8;
const RUNTIME_IDLE: Duration = Duration::from_secs(300);
const FRAME_TIMEOUT: Duration = Duration::from_secs(3);

struct Server {
    runtime: Mutex<RuntimeMode>,
    writer_queue: Mutex<VecDeque<u64>>,
    next_writer: AtomicU64,
    contexts: Mutex<HashMap<String, RegisteredRuntime>>,
    shutdown: Arc<AtomicBool>,
    clients: AtomicUsize,
    readers: AtomicUsize,
    writer_busy: AtomicBool,
    connections: AtomicUsize,
    kernels: AtomicUsize,
    cached: AtomicUsize,
    cached_bytes: AtomicUsize,
    requests: Mutex<HashMap<String, Arc<AtomicBool>>>,
    monitors: Mutex<HashMap<String, (bool, Option<String>)>>,
    started: Instant,
}

#[derive(Clone)]
struct RegisteredRuntime {
    resolved: ResolvedVaultPathArgs,
    last_used: Instant,
    refresh_error: Option<String>,
    content_pending: bool,
}

struct WriterTicket<'a> {
    server: &'a Server,
    id: u64,
}
impl Drop for WriterTicket<'_> {
    fn drop(&mut self) {
        if let Ok(mut queue) = self.server.writer_queue.lock() {
            queue.retain(|id| *id != self.id);
        }
    }
}
struct WriterGuard<'a> {
    runtime: MutexGuard<'a, RuntimeMode>,
    _ticket: WriterTicket<'a>,
}
impl std::ops::Deref for WriterGuard<'_> {
    type Target = RuntimeMode;
    fn deref(&self) -> &RuntimeMode {
        &self.runtime
    }
}
impl std::ops::DerefMut for WriterGuard<'_> {
    fn deref_mut(&mut self) -> &mut RuntimeMode {
        &mut self.runtime
    }
}

struct Permit<'a>(&'a AtomicUsize);
impl Drop for Permit<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

struct WriterFlag<'a>(&'a AtomicBool);
impl Drop for WriterFlag<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

fn acquire_permit(counter: &AtomicUsize, maximum: usize) -> Result<Permit<'_>> {
    counter
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            (current < maximum).then_some(current + 1)
        })
        .map_err(|_| {
            runtime_error(
                "runtime_overloaded",
                "runtime concurrency limit reached; retry later",
            )
        })?;
    Ok(Permit(counter))
}

impl Server {
    fn new() -> Self {
        Self {
            runtime: Mutex::new(RuntimeMode::Daemon(Box::<RuntimeCache>::default())),
            writer_queue: Mutex::new(VecDeque::new()),
            next_writer: AtomicU64::new(0),
            contexts: Mutex::new(HashMap::new()),
            shutdown: Arc::new(AtomicBool::new(false)),
            clients: AtomicUsize::new(0),
            readers: AtomicUsize::new(0),
            writer_busy: AtomicBool::new(false),
            connections: AtomicUsize::new(0),
            kernels: AtomicUsize::new(0),
            cached: AtomicUsize::new(0),
            cached_bytes: AtomicUsize::new(0),
            requests: Mutex::new(HashMap::new()),
            monitors: Mutex::new(HashMap::new()),
            started: Instant::now(),
        }
    }

    fn status(&self) -> DaemonStatus {
        DaemonStatus {
            uptime_ms: self.started.elapsed().as_millis(),
            cached_connections: self.connections.load(Ordering::Relaxed),
            cached_kernels: self.kernels.load(Ordering::Relaxed),
            cached_results: self.cached.load(Ordering::Relaxed),
            cached_result_bytes: self.cached_bytes.load(Ordering::Relaxed),
            active_requests: self.clients.load(Ordering::Relaxed),
            active_reads: self.readers.load(Ordering::Relaxed),
            writer_busy: self.writer_busy.load(Ordering::Acquire),
        }
    }

    fn publish_metrics(&self, runtime: &RuntimeMode) {
        if let RuntimeMode::Daemon(cache) = runtime {
            self.connections
                .store(cache.connections.len(), Ordering::Relaxed);
            self.kernels.store(cache.kernels.len(), Ordering::Relaxed);
            self.cached
                .store(cache.command_results.len(), Ordering::Relaxed);
            self.cached_bytes
                .store(cache.result_bytes, Ordering::Relaxed);
            if let Ok(mut monitors) = self.monitors.lock() {
                monitors.clear();
                monitors.extend(
                    cache
                        .change_monitors
                        .iter()
                        .map(|(key, monitor)| (key.clone(), (true, monitor.health().last_error))),
                );
            }
        }
    }

    fn writer(&self) -> Result<WriterGuard<'_>> {
        let id = self.next_writer.fetch_add(1, Ordering::Relaxed);
        self.writer_queue
            .lock()
            .map_err(|_| runtime_error("runtime_unavailable", "writer queue unavailable"))?
            .push_back(id);
        let ticket = WriterTicket { server: self, id };
        loop {
            check_request_deadline()?;
            if self.shutdown.load(Ordering::Acquire) {
                return Err(runtime_error(
                    "request_cancelled",
                    "daemon is shutting down",
                ));
            }
            if self
                .writer_queue
                .lock()
                .map_err(|_| runtime_error("runtime_unavailable", "writer queue unavailable"))?
                .front()
                != Some(&id)
            {
                thread::sleep(Duration::from_millis(5));
                continue;
            }
            match self.runtime.try_lock() {
                Ok(runtime) => {
                    return Ok(WriterGuard {
                        runtime,
                        _ticket: ticket,
                    });
                }
                Err(TryLockError::WouldBlock) => thread::sleep(Duration::from_millis(5)),
                Err(TryLockError::Poisoned(_)) => {
                    return Err(runtime_error(
                        "runtime_unavailable",
                        "runtime writer failed",
                    ));
                }
            }
        }
    }

    fn register_context(&self, resolved: &ResolvedVaultPathArgs) {
        if let Ok(mut contexts) = self.contexts.lock() {
            let key = runtime_cache_key(resolved);
            contexts
                .entry(key)
                .and_modify(|context| context.last_used = Instant::now())
                .or_insert_with(|| RegisteredRuntime {
                    resolved: resolved.clone(),
                    last_used: Instant::now(),
                    refresh_error: None,
                    content_pending: false,
                });
            contexts.retain(|_, context| {
                context.content_pending || context.last_used.elapsed() < RUNTIME_IDLE
            });
            while contexts.len() > MAX_RUNTIMES {
                let oldest = contexts
                    .iter()
                    .min_by_key(|(_, context)| context.last_used)
                    .map(|(key, _)| key.clone());
                if let Some(oldest) = oldest {
                    contexts.remove(&oldest);
                }
            }
        }
    }

    fn next_context(
        &self,
        previous: &str,
        include_content_work: bool,
    ) -> Option<(String, RegisteredRuntime)> {
        let mut contexts = self.contexts.lock().ok()?;
        contexts.retain(|_, context| {
            context.content_pending || context.last_used.elapsed() < RUNTIME_IDLE
        });
        let eligible = || {
            contexts.iter().filter_map(|(key, context)| {
                (include_content_work || context.last_used.elapsed() < RUNTIME_IDLE).then_some(key)
            })
        };
        let key = eligible()
            .filter(|key| key.as_str() > previous)
            .min()
            .or_else(|| eligible().min())?
            .clone();
        Some((key.clone(), contexts.get(&key)?.clone()))
    }

    fn set_content_pending(&self, key: &str, pending: bool) {
        if let Ok(mut contexts) = self.contexts.lock()
            && let Some(context) = contexts.get_mut(key)
        {
            context.content_pending = pending;
        }
    }
}

pub(super) fn run(socket: &str) -> Result<()> {
    let Some(owner) = super::daemon_socket::acquire(socket)? else {
        return Ok(());
    };
    owner.listener.set_nonblocking(true)?;
    let server = Arc::new(Server::new());
    let mut handlers = Vec::new();
    let background = Arc::clone(&server);
    handlers.push(thread::spawn(move || extraction_loop(&background)));
    let background = Arc::clone(&server);
    handlers.push(thread::spawn(move || refresh_loop(&background)));
    while !server.shutdown.load(Ordering::Acquire) {
        match owner.listener.accept() {
            Ok((mut stream, _)) => {
                let control_only = server.clients.load(Ordering::Acquire) >= MAX_CLIENTS;
                if server.clients.load(Ordering::Acquire) >= MAX_CLIENTS + CONTROL_RESERVE {
                    send_response(
                        &mut stream,
                        DaemonResponse::failed(runtime_error(
                            "runtime_overloaded",
                            "too many connected clients",
                        )),
                    );
                    continue;
                }
                server.clients.fetch_add(1, Ordering::AcqRel);
                let shared = Arc::clone(&server);
                handlers.push(thread::spawn(move || {
                    let _permit = Permit(&shared.clients);
                    let response =
                        read_socket_frame(&mut stream, if control_only { 4096 } else { 1024 * 1024 }, Instant::now() + if control_only { Duration::from_millis(250) } else { FRAME_TIMEOUT })
                            .and_then(|bytes| {
                                serde_json::from_slice::<DaemonRequest>(&bytes).map_err(|error| {
                                    runtime_error(
                                        "invalid_request",
                                        format!("invalid daemon request: {error}"),
                                    )
                                })
                            })
                            .and_then(|request| {
                                if control_only && matches!(&request, DaemonRequest::Execute { payload } if daemon_execution_policy(&payload.command) != DaemonExecutionPolicy::ObservationalFresh) {
                                    return Err(runtime_error("runtime_overloaded", "request queue is full; control capacity reserved"));
                                }
                                handle_request(&shared, request)
                            });
                    send_response(&mut stream, response.unwrap_or_else(DaemonResponse::failed));
                }));
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                if let Ok(mut runtime) = server.runtime.try_lock() {
                    evict_idle_runtimes(&mut runtime);
                    server.publish_metrics(&runtime);
                }
                let mut descriptors = [rustix::event::PollFd::new(
                    &owner.listener,
                    rustix::event::PollFlags::IN,
                )];
                let timeout = rustix::event::Timespec::try_from(Duration::from_millis(50))?;
                match rustix::event::poll(&mut descriptors, Some(&timeout)) {
                    Ok(_) | Err(rustix::io::Errno::INTR) => {}
                    Err(error) => return Err(anyhow!("wait for daemon connection: {error}")),
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error).context("accept daemon connection"),
        }
        let mut pending = Vec::new();
        for handler in handlers.drain(..) {
            if handler.is_finished() {
                let _ = handler.join();
            } else {
                pending.push(handler);
            }
        }
        handlers = pending;
    }
    if let Ok(requests) = server.requests.lock() {
        for cancelled in requests.values() {
            cancelled.store(true, Ordering::Release);
        }
    }
    // The process exit owns final teardown; do not let an uncooperative computation
    // keep the control/shutdown path waiting indefinitely.
    let stop_deadline = Instant::now() + Duration::from_millis(500);
    while handlers.iter().any(|handler| !handler.is_finished()) && Instant::now() < stop_deadline {
        thread::sleep(Duration::from_millis(5));
    }
    drop(owner);
    Ok(())
}

fn send_response(stream: &mut UnixStream, response: DaemonResponse) {
    let response = if response
        .output
        .as_ref()
        .is_some_and(|output| output.len() as u64 > MAX_DAEMON_RESPONSE_BYTES)
    {
        DaemonResponse::failed(runtime_error(
            "response_too_large",
            "response exceeds 16 MiB; request a smaller window",
        ))
    } else {
        response
    };
    let encoded = serde_json::to_vec(&response).and_then(|bytes| {
        if bytes.len() as u64 <= MAX_DAEMON_RESPONSE_FRAME_BYTES {
            return Ok(bytes);
        }
        serde_json::to_vec(&DaemonResponse::failed(runtime_error(
            "response_too_large",
            "response exceeds 16 MiB; request a smaller window",
        )))
    });
    if let Ok(bytes) = encoded {
        let _ = write_socket_frame(stream, &bytes, Instant::now() + Duration::from_secs(2));
    }
}

fn handle_request(server: &Server, request: DaemonRequest) -> Result<DaemonResponse> {
    match request {
        DaemonRequest::Status => Ok(DaemonResponse::success(None, Some(server.status()))),
        DaemonRequest::Shutdown => {
            server.shutdown.store(true, Ordering::Release);
            Ok(DaemonResponse::success(
                Some("daemon shutdown acknowledged".to_string()),
                None,
            ))
        }
        DaemonRequest::Cancel { request_id } => {
            if let Ok(requests) = server.requests.lock()
                && let Some(cancelled) = requests.get(&request_id)
            {
                cancelled.store(true, Ordering::Release);
            }
            Ok(DaemonResponse::success(None, None))
        }
        DaemonRequest::Execute { payload } => execute(server, *payload),
    }
}

fn execute(server: &Server, payload: DaemonExecuteRequest) -> Result<DaemonResponse> {
    if payload.protocol != DAEMON_PROTOCOL_VERSION || payload.build != env!("TAO_BUILD_ID") {
        return Err(CliContractError::blocked(
            "daemon_incompatible",
            "incompatible request protocol or build",
            Some("use the same Tao build for client and server".to_string()),
            None,
        )
        .into());
    }
    if payload.request_id.is_empty()
        || payload.request_id.len() > 128
        || !(1..=3_600_000).contains(&payload.timeout_ms)
    {
        return Err(
            CliContractError::invalid_argument("invalid request identity or deadline").into(),
        );
    }
    if is_daemon_control_command(&payload.command) {
        return Err(CliContractError::invalid_argument(
            "daemon lifecycle commands cannot be nested inside execute",
        )
        .into());
    }
    if let Commands::Query(args) = &payload.command {
        commands::query::validate_capabilities(args)?;
    }
    if let Commands::Doc { command } = &payload.command {
        commands::doc::validate_args(command)?;
    }
    validate_continuation_usage(&payload.command, payload.continuation.as_deref())?;
    let resolved = payload
        .context
        .as_ref()
        .map(ExecutionContext::resolved)
        .transpose()?;
    if resolved.is_none() && command_supports_daemon_forwarding(&payload.command) {
        return Err(CliContractError::invalid_argument(
            "vault operation requires resolved execution context",
        )
        .into());
    }
    let cancelled = Arc::new(AtomicBool::new(false));
    {
        let mut requests = server
            .requests
            .lock()
            .map_err(|_| runtime_error("runtime_unavailable", "request registry unavailable"))?;
        if requests.contains_key(&payload.request_id) {
            return Err(runtime_error(
                "duplicate_request",
                "request identity is already active",
            ));
        }
        requests.insert(payload.request_id.clone(), Arc::clone(&cancelled));
    }
    let result = execute_registered(server, &payload, resolved, cancelled);
    if let Ok(mut requests) = server.requests.lock() {
        requests.remove(&payload.request_id);
    }
    result
}

fn generation(runtime: &mut RuntimeMode, resolved: &ResolvedVaultPathArgs) -> Result<String> {
    published_generation(runtime, resolved)
}

fn execute_registered(
    server: &Server,
    payload: &DaemonExecuteRequest,
    resolved: Option<ResolvedVaultPathArgs>,
    cancelled: Arc<AtomicBool>,
) -> Result<DaemonResponse> {
    let started = Instant::now();
    let deadline = started + Duration::from_millis(payload.timeout_ms);
    let _scope = RequestScope::new(resolved.clone(), false, deadline, Arc::clone(&cancelled))
        .for_daemon(false, None)
        .enter();
    let policy = daemon_execution_policy(&payload.command);
    let runtime_key = resolved.as_ref().map(runtime_cache_key);
    if (policy.refreshes_runtime() || policy == DaemonExecutionPolicy::ExplicitWork)
        && let Some(resolved) = &resolved
    {
        server.register_context(resolved);
    }
    if policy == DaemonExecutionPolicy::ExplicitWork {
        let mut runtime = server.writer()?;
        server.writer_busy.store(true, Ordering::Release);
        let _busy = WriterFlag(&server.writer_busy);
        if let (RuntimeMode::Daemon(cache), Some(key)) = (&mut *runtime, runtime_key.as_deref()) {
            clear_cached_results_for_runtime(cache, key);
        }
        let observed = if matches!(&payload.command, Commands::Vault { command: VaultCommands::Reindex(args) } if !args.dry_run)
        {
            resolved
                .as_ref()
                .map(|resolved| {
                    observe_daemon_runtime(&mut runtime, resolved).map(|observation| observation.0)
                })
                .transpose()?
        } else {
            None
        };
        let result = dispatch_with_runtime(payload.command.clone(), &mut runtime);
        if result.is_ok()
            && let (Some(resolved), Some(observed)) = (&resolved, observed)
        {
            acknowledge_daemon_refresh(&mut runtime, resolved, observed);
        }
        if let (RuntimeMode::Daemon(cache), Some(key)) = (&mut *runtime, runtime_key.as_deref()) {
            clear_cached_results_for_runtime(cache, key);
        }
        server.publish_metrics(&runtime);
        check_request_deadline()?;
        return response_for_result(result?, started, "bypass", false, &payload.request_id, None);
    }
    // Diagnostics have a separate admission path and never wait for the writer.
    let _reader = if policy == DaemonExecutionPolicy::ObservationalFresh {
        None
    } else {
        Some(acquire_permit(&server.readers, MAX_READERS)?)
    };
    let mut refresh_pending = server.writer_busy.load(Ordering::Acquire);
    let mut cache_key = None;
    let mut expected_generation = None;
    let mut cached_result = None;
    let (mut monitor_running, mut monitor_error) = runtime_key
        .as_ref()
        .and_then(|key| {
            server
                .monitors
                .lock()
                .ok()
                .and_then(|monitors| monitors.get(key).cloned())
        })
        .unwrap_or((false, None));
    if policy.refreshes_runtime() {
        match server.runtime.try_lock() {
            Ok(mut runtime) => {
                if let (Some(resolved), Some(runtime_key)) = (&resolved, &runtime_key) {
                    observe_daemon_runtime(&mut runtime, resolved)?;
                    // Bootstrap only when no publication exists yet. Existing
                    // publications stay readable while the background lane reconciles.
                    let needs_bootstrap = !Path::new(&resolved.db_path).is_file()
                        || with_connection(&mut runtime, resolved, |connection| {
                            Ok(tao_sdk_storage::IndexStateRepository::get_by_key(
                                connection,
                                tao_sdk_service::LINK_RESOLUTION_VERSION_STATE_KEY,
                            )?
                            .is_none())
                        })?;
                    if needs_bootstrap {
                        server.writer_busy.store(true, Ordering::Release);
                        let _busy = WriterFlag(&server.writer_busy);
                        refresh_daemon_runtime_state(&mut runtime, resolved)?;
                    }
                    refresh_pending = daemon_refresh_pending(&runtime, resolved);
                    let version = generation(&mut runtime, resolved)?;
                    expected_generation = Some(version.clone());
                    let key = format!(
                        "{runtime_key}\u{1e}{version}\u{1e}{}",
                        daemon_cache_key(&payload.command)?
                    );
                    if let RuntimeMode::Daemon(cache) = &mut *runtime {
                        monitor_running = cache.change_monitors.contains_key(runtime_key);
                        monitor_error = cache
                            .change_monitors
                            .get(runtime_key)
                            .and_then(|monitor| monitor.health().last_error);
                        if !payload.no_result_cache
                            && !payload.json_stream
                            && let Some(entry) = cache.command_results.get(&key)
                            && entry.runtime_key == *runtime_key
                        {
                            cached_result = Some(entry.result.clone());
                            cache
                                .command_result_order
                                .retain(|existing| existing != &key);
                            cache.command_result_order.push_back(key.clone());
                            server.publish_metrics(&runtime);
                        }
                    }
                    cache_key = Some(key);
                }
                server.publish_metrics(&runtime);
            }
            Err(TryLockError::WouldBlock) => refresh_pending = true,
            Err(TryLockError::Poisoned(_)) => {
                return Err(runtime_error("runtime_unavailable", "writer unavailable"));
            }
        }
    } else if let (Ok(runtime), Some(key)) = (server.runtime.try_lock(), &runtime_key)
        && let RuntimeMode::Daemon(cache) = &*runtime
    {
        monitor_running = cache.change_monitors.contains_key(key);
        monitor_error = cache
            .change_monitors
            .get(key)
            .and_then(|monitor| monitor.health().last_error);
        if let Some(resolved) = &resolved {
            refresh_pending = daemon_refresh_pending(&runtime, resolved);
        }
    }
    if let Some(key) = &runtime_key
        && let Ok(contexts) = server.contexts.lock()
        && let Some(context) = contexts.get(key)
        && let Some(error) = &context.refresh_error
    {
        monitor_error = Some(error.clone());
        refresh_pending = true;
    }
    let read_scope = RequestScope::new(resolved.clone(), true, deadline, cancelled)
        .for_daemon(monitor_running, monitor_error)
        .enter();
    let mut snapshot = RuntimeMode::Snapshot(Box::<RuntimeCache>::default());
    let continuation = prepare_continuation(
        &mut snapshot,
        resolved.as_ref(),
        &payload.command,
        payload.continuation.as_deref(),
        payload.output_toon,
        payload.json_stream,
    )?;
    let snapshot_generation = if let Some(continuation) = &continuation {
        Some(continuation.generation.clone())
    } else if policy.refreshes_runtime() {
        resolved
            .as_ref()
            .map(|resolved| generation(&mut snapshot, resolved))
            .transpose()?
    } else {
        None
    };
    if expected_generation == snapshot_generation
        && let Some(result) = cached_result
    {
        return response_for_result(
            result,
            started,
            "hit",
            refresh_pending,
            &payload.request_id,
            continuation.as_ref(),
        );
    }
    if let (Some(version), Some(runtime_key)) = (&snapshot_generation, &runtime_key) {
        cache_key = Some(format!(
            "{runtime_key}\u{1e}{version}\u{1e}{}",
            daemon_cache_key(&payload.command)?
        ));
    }
    expected_generation = snapshot_generation;
    if let Some(output) = maybe_render_streaming_output_for_command(
        &payload.command,
        payload.json_stream,
        &mut snapshot,
    )? {
        check_request_deadline()?;
        return Ok(DaemonResponse::success(
            Some(decorate_continuation(
                decorate_runtime_output(
                    output,
                    "daemon",
                    "bypass",
                    refresh_pending,
                    &payload.request_id,
                )?,
                continuation.as_ref(),
            )?),
            None,
        ));
    }
    let result = dispatch_with_runtime(payload.command.clone(), &mut snapshot)?;
    drop(snapshot);
    drop(read_scope);
    check_request_deadline()?;
    if !payload.no_result_cache
        && policy.uses_result_cache()
        && let Ok(mut runtime) = server.runtime.try_lock()
        && let (Some(resolved), Some(expected)) = (&resolved, &expected_generation)
        && generation(&mut runtime, resolved)? == *expected
    {
        update_daemon_command_cache(
            &mut runtime,
            policy,
            runtime_key.as_deref(),
            cache_key,
            &result,
        );
        server.publish_metrics(&runtime);
    }
    response_for_result(
        result,
        started,
        if payload.no_result_cache || !policy.uses_result_cache() {
            "bypass"
        } else {
            "miss"
        },
        refresh_pending,
        &payload.request_id,
        continuation.as_ref(),
    )
}

fn response_for_result(
    result: CommandResult,
    started: Instant,
    cache: &str,
    pending: bool,
    request_id: &str,
    continuation: Option<&PageContinuation>,
) -> Result<DaemonResponse> {
    let output = render_output_with_elapsed(true, &result, started.elapsed())?;
    Ok(DaemonResponse::success(
        Some(decorate_continuation(
            decorate_runtime_output(output, "daemon", cache, pending, request_id)?,
            continuation,
        )?),
        None,
    ))
}

fn evict_idle_runtimes(runtime: &mut RuntimeMode) {
    let RuntimeMode::Daemon(cache) = runtime else {
        return;
    };
    let mut ages = cache
        .last_used
        .iter()
        .map(|(key, time)| (key.clone(), *time))
        .collect::<Vec<_>>();
    ages.sort_by_key(|(_, time)| *time);
    let mut remaining = ages.len();
    for (key, last_used) in ages {
        if remaining <= MAX_RUNTIMES && last_used.elapsed() < RUNTIME_IDLE {
            continue;
        }
        clear_cached_results_for_runtime(cache, &key);
        cache.connections.remove(&key);
        cache.kernels.remove(&key);
        cache.change_monitors.remove(&key);
        cache.last_reconciled_generation.remove(&key);
        cache.resolved_runtimes.remove(&key);
        cache.last_used.remove(&key);
        remaining = remaining.saturating_sub(1);
    }
}

fn refresh_loop(server: &Server) {
    let mut previous_key = String::new();
    while !server.shutdown.load(Ordering::Acquire) {
        thread::sleep(Duration::from_millis(100));
        let Some((key, context)) = server.next_context(&previous_key, false) else {
            continue;
        };
        previous_key = key.clone();
        let _scope = RequestScope::new(
            Some(context.resolved.clone()),
            false,
            Instant::now() + Duration::from_secs(120),
            Arc::new(AtomicBool::new(false)),
        )
        .with_parent_cancellation(&server.shutdown)
        .for_daemon(true, None)
        .enter();
        let Ok(mut runtime) = server.writer() else {
            continue;
        };
        if !Path::new(&context.resolved.db_path).is_file() {
            continue; // A foreground bootstrap owns absent state.
        }
        server.writer_busy.store(true, Ordering::Release);
        let _busy = WriterFlag(&server.writer_busy);
        let result = refresh_daemon_runtime_state(&mut runtime, &context.resolved);
        if let RuntimeMode::Daemon(cache) = &mut *runtime {
            // Background maintenance is not user activity.
            cache.last_used.insert(key.clone(), context.last_used);
        }
        if let Ok(mut contexts) = server.contexts.lock()
            && let Some(context) = contexts.get_mut(&key)
        {
            context.refresh_error = result
                .err()
                .map(|error| format!("index refresh failed: {error}"));
        }
        server.publish_metrics(&runtime);
    }
}

fn extraction_loop(server: &Server) {
    let mut previous_key = String::new();
    while !server.shutdown.load(Ordering::Acquire) {
        thread::sleep(Duration::from_millis(100));
        let Some((key, context)) = server.next_context(&previous_key, true) else {
            continue;
        };
        previous_key = key.clone();
        let resolved = context.resolved;
        if !Path::new(&resolved.db_path).is_file() {
            continue;
        }
        let _scope = RequestScope::new(
            Some(resolved.clone()),
            false,
            Instant::now() + Duration::from_secs(30),
            Arc::new(AtomicBool::new(false)),
        )
        .with_parent_cancellation(&server.shutdown)
        .for_daemon(true, None)
        .enter();
        // Never hold the foreground/index-refresh lock across external extraction.
        // Database leases bound a CPU-sized pool across processes. Short publication
        // transactions coordinate with indexing, and read caches key each snapshot's
        // committed publication generations.
        let Ok(mut connection) = open_initialized_connection(&resolved) else {
            continue;
        };
        let _ = (|| -> Result<()> {
            let status = tao_sdk_service::ContentIndexService.status(&connection)?;
            // Queue ownership outlives read-cache activity. An unattended vault
            // continues extracting without retaining idle readers or watchers.
            server.set_content_pending(&key, status.queued > 0 || status.running > 0);
            if status.queued == 0 && status.running == 0 {
                return Ok(());
            }
            let spool =
                tao_sdk_service::content_spool_root(&connection, Path::new(&resolved.vault_root));
            let status = tao_sdk_service::ContentIndexService.process_pending_cancellable(
                &mut connection,
                Path::new(&resolved.vault_root),
                &spool,
                Duration::from_secs(30),
                resolved.case_policy,
                &server.shutdown,
            )?;
            server.set_content_pending(&key, status.queued > 0 || status.running > 0);
            Ok(())
        })();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unattended_content_queue_outlives_idle_read_runtime_until_drained() {
        let server = Server::new();
        let resolved = ResolvedVaultPathArgs {
            vault_root: "/vault".to_string(),
            data_dir: "/vault/.tao".to_string(),
            db_path: "/vault/.tao/index.sqlite".to_string(),
            case_policy: CasePolicy::Sensitive,
        };
        let key = runtime_cache_key(&resolved);
        server.register_context(&resolved);
        server.set_content_pending(&key, true);
        server
            .contexts
            .lock()
            .unwrap()
            .get_mut(&key)
            .unwrap()
            .last_used = Instant::now() - RUNTIME_IDLE - Duration::from_secs(1);
        assert!(server.next_context("", false).is_none());
        assert_eq!(server.next_context("", true).unwrap().0, key);
        server.set_content_pending(&key, false);
        assert!(server.next_context("", true).is_none());
        assert!(server.contexts.lock().unwrap().is_empty());
    }

    #[test]
    fn result_cache_enforces_actual_byte_capacity_and_skips_oversized_entries() {
        let mut runtime = RuntimeMode::Daemon(Box::<RuntimeCache>::default());
        let payload = "x".repeat(MAX_DAEMON_CACHE_BYTES / 8 - 1024);
        for index in 0..9 {
            let result = CommandResult {
                command: "doc.read".to_string(),
                summary: String::new(),
                args: serde_json::json!({"content": payload}),
            };
            update_daemon_command_cache(
                &mut runtime,
                DaemonExecutionPolicy::CachedReadWithRefresh,
                Some("vault"),
                Some(format!("query-{index}")),
                &result,
            );
        }
        let RuntimeMode::Daemon(cache) = &runtime else {
            unreachable!()
        };
        assert!(cache.result_bytes <= MAX_DAEMON_CACHE_BYTES);
        assert_eq!(cache.command_results.len(), 8);
        assert!(!cache.command_results.contains_key("query-0"));
        let oversized = CommandResult {
            command: "doc.read".to_string(),
            summary: String::new(),
            args: serde_json::json!({"content": "x".repeat(MAX_DAEMON_CACHE_BYTES / 8)}),
        };
        update_daemon_command_cache(
            &mut runtime,
            DaemonExecutionPolicy::CachedReadWithRefresh,
            Some("vault"),
            Some("oversized".to_string()),
            &oversized,
        );
        let RuntimeMode::Daemon(cache) = &runtime else {
            unreachable!()
        };
        assert!(!cache.command_results.contains_key("oversized"));
    }

    #[test]
    fn runtime_eviction_drops_oldest_and_idle_contexts() {
        let mut cache = RuntimeCache::default();
        for index in 0..12 {
            cache.last_used.insert(
                format!("runtime-{index}"),
                Instant::now() - Duration::from_secs(index),
            );
        }
        cache.last_used.insert(
            "idle".to_string(),
            Instant::now() - RUNTIME_IDLE - Duration::from_secs(1),
        );
        let mut runtime = RuntimeMode::Daemon(Box::new(cache));
        evict_idle_runtimes(&mut runtime);
        let RuntimeMode::Daemon(cache) = runtime else {
            unreachable!()
        };
        assert_eq!(cache.last_used.len(), MAX_RUNTIMES);
        assert!(!cache.last_used.contains_key("idle"));
        assert!(cache.last_used.contains_key("runtime-0"));
        assert!(!cache.last_used.contains_key("runtime-11"));
    }
}
