use super::*;
use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

/// Owned request configuration; never read the daemon's inherited environment for a request.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ExecutionContext {
    pub(crate) vault_root: String,
    pub(crate) data_dir: String,
    pub(crate) db_path: String,
    pub(crate) case_insensitive: bool,
}

impl From<&ResolvedVaultPathArgs> for ExecutionContext {
    fn from(args: &ResolvedVaultPathArgs) -> Self {
        Self {
            vault_root: args.vault_root.clone(),
            data_dir: args.data_dir.clone(),
            db_path: args.db_path.clone(),
            case_insensitive: args.case_policy == CasePolicy::Insensitive,
        }
    }
}

impl ExecutionContext {
    pub(crate) fn resolved(&self) -> Result<ResolvedVaultPathArgs> {
        for path in [&self.vault_root, &self.data_dir, &self.db_path] {
            if !Path::new(path).is_absolute() {
                return Err(CliContractError::invalid_argument(
                    "request context requires absolute resolved paths",
                )
                .into());
            }
        }
        Ok(ResolvedVaultPathArgs {
            vault_root: self.vault_root.clone(),
            data_dir: self.data_dir.clone(),
            db_path: self.db_path.clone(),
            case_policy: if self.case_insensitive {
                CasePolicy::Insensitive
            } else {
                CasePolicy::Sensitive
            },
        })
    }
}

#[derive(Clone)]
pub(crate) struct RequestScope {
    pub(crate) resolved: Option<ResolvedVaultPathArgs>,
    pub(crate) observational: bool,
    pub(crate) deadline: Instant,
    pub(crate) cancelled: Arc<AtomicBool>,
    parent_cancelled: Option<Arc<AtomicBool>>,
    interrupts: Arc<Mutex<Vec<rusqlite::InterruptHandle>>>,
    daemon: bool,
    monitor_running: bool,
    monitor_error: Option<String>,
}

thread_local! {
    static REQUEST_SCOPE: RefCell<Option<RequestScope>> = const { RefCell::new(None) };
}

pub(crate) struct RequestScopeGuard {
    _index_scope: tao_sdk_service::IndexCancellationScope,
    previous: Option<RequestScope>,
    finished: Arc<AtomicBool>,
    interrupts: Arc<Mutex<Vec<rusqlite::InterruptHandle>>>,
}

impl Drop for RequestScopeGuard {
    fn drop(&mut self) {
        // Synchronize scope teardown with any in-flight interrupt so a timed-out
        // request cannot interrupt a cached connection after the next request owns it.
        let _interrupts = self.interrupts.lock();
        self.finished.store(true, Ordering::Release);
        REQUEST_SCOPE.with(|slot| *slot.borrow_mut() = self.previous.take());
    }
}

impl RequestScope {
    pub(crate) fn new(
        resolved: Option<ResolvedVaultPathArgs>,
        observational: bool,
        deadline: Instant,
        cancelled: Arc<AtomicBool>,
    ) -> Self {
        Self {
            resolved,
            observational,
            deadline,
            cancelled,
            parent_cancelled: None,
            interrupts: Arc::new(Mutex::new(Vec::new())),
            daemon: false,
            monitor_running: false,
            monitor_error: None,
        }
    }

    pub(crate) fn for_daemon(
        mut self,
        monitor_running: bool,
        monitor_error: Option<String>,
    ) -> Self {
        self.daemon = true;
        self.monitor_running = monitor_running;
        self.monitor_error = monitor_error;
        self
    }

    pub(crate) fn with_parent_cancellation(mut self, cancelled: &Arc<AtomicBool>) -> Self {
        self.parent_cancelled = Some(Arc::clone(cancelled));
        self
    }

    pub(crate) fn enter(self) -> RequestScopeGuard {
        let index_scope = tao_sdk_service::IndexCancellationScope::enter(
            self.deadline,
            Arc::clone(&self.cancelled),
        );
        let finished = Arc::new(AtomicBool::new(false));
        let done = Arc::clone(&finished);
        let scope = self.clone();
        // A bounded request owns this watchdog. SQLite work is interrupted even when a
        // caller is blocked inside a query, rather than only after it returns.
        thread::spawn(move || {
            while !done.load(Ordering::Acquire) {
                if scope.cancelled.load(Ordering::Acquire)
                    || scope
                        .parent_cancelled
                        .as_ref()
                        .is_some_and(|flag| flag.load(Ordering::Acquire))
                    || Instant::now() >= scope.deadline
                {
                    scope.cancelled.store(true, Ordering::Release);
                    if let Ok(handles) = scope.interrupts.lock() {
                        if done.load(Ordering::Acquire) {
                            break;
                        }
                        for handle in handles.iter() {
                            handle.interrupt();
                        }
                    }
                    break;
                }
                thread::sleep(Duration::from_millis(10));
            }
        });
        let interrupts = Arc::clone(&self.interrupts);
        let previous = REQUEST_SCOPE.with(|slot| slot.replace(Some(self)));
        RequestScopeGuard {
            _index_scope: index_scope,
            previous,
            finished,
            interrupts,
        }
    }
}

pub(crate) fn request_resolved_paths() -> Option<ResolvedVaultPathArgs> {
    REQUEST_SCOPE.with(|slot| {
        slot.borrow()
            .as_ref()
            .and_then(|scope| scope.resolved.clone())
    })
}

pub(crate) fn request_is_observational() -> bool {
    REQUEST_SCOPE.with(|slot| {
        slot.borrow()
            .as_ref()
            .is_some_and(|scope| scope.observational)
    })
}

pub(crate) fn request_daemon_state() -> (bool, bool, Option<String>) {
    REQUEST_SCOPE.with(|slot| {
        slot.borrow()
            .as_ref()
            .map(|scope| {
                (
                    scope.daemon,
                    scope.monitor_running,
                    scope.monitor_error.clone(),
                )
            })
            .unwrap_or((false, false, None))
    })
}

pub(crate) fn request_cancellation_flag() -> Arc<AtomicBool> {
    REQUEST_SCOPE.with(|slot| {
        slot.borrow()
            .as_ref()
            .map(|scope| Arc::clone(&scope.cancelled))
            .unwrap_or_else(|| Arc::new(AtomicBool::new(false)))
    })
}

pub(crate) fn request_remaining() -> Duration {
    REQUEST_SCOPE.with(|slot| {
        slot.borrow()
            .as_ref()
            .map(|scope| scope.deadline.saturating_duration_since(Instant::now()))
            .unwrap_or(Duration::from_secs(120))
    })
}

pub(crate) fn check_request_deadline() -> Result<()> {
    REQUEST_SCOPE.with(|slot| {
        if let Some(scope) = slot.borrow().as_ref() {
            if Instant::now() >= scope.deadline {
                return Err(runtime_error(
                    "request_timeout",
                    "request execution deadline exceeded",
                ));
            }
            if scope.cancelled.load(Ordering::Acquire) {
                return Err(runtime_error("request_cancelled", "request was cancelled"));
            }
        }
        Ok(())
    })
}

pub(crate) fn register_interrupt(handle: rusqlite::InterruptHandle) {
    REQUEST_SCOPE.with(|slot| {
        if let Some(scope) = slot.borrow().as_ref()
            && let Ok(mut handles) = scope.interrupts.lock()
        {
            handles.push(handle);
        }
    });
}

pub(crate) fn runtime_error(code: &'static str, message: impl Into<String>) -> anyhow::Error {
    CliContractError::failure(
        code,
        message,
        Some("retry within the configured runtime limits".to_string()),
        None,
    )
    .into()
}

pub(crate) fn decorate_runtime_output(
    output: String,
    backend: &str,
    cache: &str,
    refresh_pending: bool,
    request_id: &str,
) -> Result<String> {
    let mut envelope: JsonValue =
        serde_json::from_str(&output).context("parse runtime envelope")?;
    envelope["meta"]["runtime"] = serde_json::json!({
        "backend": backend,
        "resultCache": cache,
        "refreshPending": refresh_pending,
        "requestId": request_id,
        "protocolVersion": DAEMON_PROTOCOL_VERSION,
        "buildId": env!("TAO_BUILD_ID"),
    });
    serde_json::to_string(&envelope).context("serialize runtime envelope")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn background_deadlines_do_not_cancel_the_parent_but_shutdown_cancels_children() {
        let shutdown = Arc::new(AtomicBool::new(false));
        let expired = Arc::new(AtomicBool::new(false));
        let guard = RequestScope::new(None, false, Instant::now(), Arc::clone(&expired))
            .with_parent_cancellation(&shutdown)
            .enter();
        thread::sleep(Duration::from_millis(30));
        assert!(expired.load(Ordering::Acquire));
        assert!(!shutdown.load(Ordering::Acquire));
        drop(guard);
        let child = Arc::new(AtomicBool::new(false));
        let _guard = RequestScope::new(
            None,
            false,
            Instant::now() + Duration::from_secs(5),
            Arc::clone(&child),
        )
        .with_parent_cancellation(&shutdown)
        .enter();
        shutdown.store(true, Ordering::Release);
        thread::sleep(Duration::from_millis(30));
        assert!(child.load(Ordering::Acquire));
    }
}
