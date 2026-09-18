//! Reconciliation-backed watch adapter for filesystem drift repair.

use notify::{EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use rusqlite::Connection;
use std::path::{Component, Path, PathBuf};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, Instant};
use tao_sdk_service::ReconciliationScannerService;
use tao_sdk_vault::{CasePolicy, VaultInclusionPolicy};
use thiserror::Error;

/// Result payload for one reconciliation-backed watch pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchReconcileResult {
    /// Number of files scanned on disk.
    pub scanned_files: u64,
    /// Number of inserted paths detected.
    pub inserted_paths: u64,
    /// Number of updated paths detected.
    pub updated_paths: u64,
    /// Number of drift paths repaired.
    pub drift_paths: u64,
    /// Number of coalesced batches applied.
    pub batches_applied: u64,
    /// Number of files upserted by repair.
    pub upserted_files: u64,
    /// Number of files removed from index.
    pub removed_files: u64,
    /// Number of links reindexed.
    pub links_reindexed: u64,
    /// Number of properties reindexed.
    pub properties_reindexed: u64,
    /// Number of bases reindexed.
    pub bases_reindexed: u64,
}

/// Adapter exposing reconciliation scan as a watch-compatible primitive.
#[derive(Debug, Default, Clone, Copy)]
pub struct WatchReconcileService {
    scanner: ReconciliationScannerService,
}

impl WatchReconcileService {
    /// Run one drift-scan/repair pass for the provided vault root.
    pub fn reconcile_once(
        &self,
        vault_root: &Path,
        connection: &mut Connection,
        case_policy: CasePolicy,
    ) -> Result<WatchReconcileResult, WatchReconcileError> {
        const DEFAULT_MAX_BATCH_SIZE: usize = 128;
        let result = self
            .scanner
            .scan_and_repair(vault_root, connection, case_policy, DEFAULT_MAX_BATCH_SIZE)
            .map_err(|source| WatchReconcileError::ScanAndRepair { source })?;
        Ok(WatchReconcileResult {
            scanned_files: result.scanned_files,
            inserted_paths: result.inserted_paths,
            updated_paths: result.updated_paths,
            drift_paths: result.drift_paths,
            batches_applied: result.batches_applied,
            upserted_files: result.upserted_files,
            removed_files: result.removed_files,
            links_reindexed: result.links_reindexed,
            properties_reindexed: result.properties_reindexed,
            bases_reindexed: result.bases_reindexed,
        })
    }
}

/// Bounded watcher diagnostics. Event delivery alone cannot prove index freshness.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct VaultWatchHealth {
    /// A successful reconciliation is required before acknowledging this generation.
    pub requires_reconcile: bool,
    /// Number of backend or ignore-policy errors observed.
    pub backend_errors: u64,
    /// Most recent degradation reason, retained after successful reconciliation.
    pub last_error: Option<String>,
    /// Number of explicit backend/full-verification rescan requests.
    pub rescan_requests: u64,
}

#[derive(Debug)]
struct MonitorState {
    root: PathBuf,
    case_policy: CasePolicy,
    inclusion: VaultInclusionPolicy,
    health: VaultWatchHealth,
    last_verification: Instant,
}

impl MonitorState {
    fn degrade(&mut self, message: String) {
        self.health.backend_errors = self.health.backend_errors.saturating_add(1);
        // Retain one bounded diagnostic instead of an unbounded backend history.
        self.health.last_error = Some(message.chars().take(2048).collect());
        self.request_rescan();
    }

    fn request_rescan(&mut self) {
        self.health.requires_reconcile = true;
        self.health.rescan_requests = self.health.rescan_requests.saturating_add(1);
    }

    fn reload_inclusion(&mut self) {
        match VaultInclusionPolicy::load(&self.root, self.case_policy) {
            Ok(inclusion) => self.inclusion = inclusion,
            Err(error) => self.degrade(error.to_string()),
        }
    }

    fn observe(&mut self, result: notify::Result<notify::Event>) -> bool {
        let event = match result {
            Ok(event) => event,
            Err(error) => {
                self.degrade(error.to_string());
                return true;
            }
        };
        // Overflow can be attached to any event kind, including access events.
        if event.need_rescan() {
            self.reload_inclusion();
            self.request_rescan();
            return true;
        }
        if matches!(event.kind, EventKind::Access(_)) {
            return false;
        }
        if event.paths.is_empty() {
            self.reload_inclusion();
            self.request_rescan();
            return true;
        }
        let control_changed = event.paths.iter().any(|path| {
            self.inclusion
                .is_control_path(&normalize_event_path(&self.root, path))
        });
        if control_changed {
            self.reload_inclusion();
            self.request_rescan();
            return true;
        }
        let dirty = should_mark_dirty(&self.root, &self.inclusion, &event);
        self.health.requires_reconcile |= dirty;
        dirty
    }
}

/// Filesystem monitor with a bounded verification fallback for silently lost events.
pub struct VaultChangeMonitor {
    generation: Arc<AtomicU64>,
    state: Arc<Mutex<MonitorState>>,
    _watcher: RecommendedWatcher,
}

impl std::fmt::Debug for VaultChangeMonitor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("VaultChangeMonitor")
            .field("generation", &self.generation.load(Ordering::Acquire))
            .field("health", &self.health())
            .finish()
    }
}

impl VaultChangeMonitor {
    /// Start monitoring one vault with the default sensitive path policy.
    pub fn start(vault_root: &Path) -> Result<Self, VaultChangeMonitorError> {
        Self::start_with_case_policy(vault_root, CasePolicy::Sensitive)
    }

    /// Start monitoring with the same inclusion/case rules as the index scanner.
    pub fn start_with_case_policy(
        vault_root: &Path,
        case_policy: CasePolicy,
    ) -> Result<Self, VaultChangeMonitorError> {
        let canonical_root = std::fs::canonicalize(vault_root).map_err(|source| {
            VaultChangeMonitorError::CanonicalizeRoot {
                path: vault_root.to_path_buf(),
                source,
            }
        })?;
        let inclusion = VaultInclusionPolicy::load(&canonical_root, case_policy)
            .map_err(|source| VaultChangeMonitorError::Inclusion { source })?;
        let generation = Arc::new(AtomicU64::new(0));
        let state = Arc::new(Mutex::new(MonitorState {
            root: canonical_root.clone(),
            case_policy,
            inclusion,
            health: VaultWatchHealth::default(),
            last_verification: Instant::now(),
        }));
        let generation_ref = Arc::clone(&generation);
        let state_ref = Arc::clone(&state);
        let mut watcher =
            notify::recommended_watcher(move |result: notify::Result<notify::Event>| {
                let mut state = state_ref
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if state.observe(result) {
                    generation_ref.fetch_add(1, Ordering::Release);
                }
            })
            .map_err(|source| VaultChangeMonitorError::CreateWatcher { source })?;
        watcher
            .watch(&canonical_root, RecursiveMode::Recursive)
            .map_err(|source| VaultChangeMonitorError::WatchRoot {
                path: canonical_root,
                source,
            })?;
        Ok(Self {
            generation,
            state,
            _watcher: watcher,
        })
    }

    /// Return the change generation, requesting verification at least once a minute.
    /// The fallback runs on observation; idle monitors do not create background I/O.
    #[must_use]
    pub fn generation(&self) -> u64 {
        self.periodic_generation(Duration::from_secs(60))
    }

    /// Return the generation with an explicit bounded verification interval.
    /// A periodic tick reloads ignore rules and invalidates cached freshness even
    /// when a backend silently lost an event. Reconciliation supplies the truth.
    #[must_use]
    pub fn periodic_generation(&self, interval: Duration) -> u64 {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.last_verification.elapsed() >= interval {
            state.reload_inclusion();
            state.request_rescan();
            state.last_verification = Instant::now();
            self.generation.fetch_add(1, Ordering::Release);
        }
        self.generation.load(Ordering::Acquire)
    }

    /// Return the most recent bounded health diagnostics.
    #[must_use]
    pub fn health(&self) -> VaultWatchHealth {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .health
            .clone()
    }

    /// Acknowledge a successful full reconciliation only if no newer event arrived.
    /// Returns false when the caller must reconcile a newer generation.
    #[must_use]
    pub fn acknowledge_reconciled(&self, generation: u64) -> bool {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.generation.load(Ordering::Acquire) != generation {
            return false;
        }
        state.health.requires_reconcile = false;
        state.last_verification = Instant::now();
        true
    }
}

fn should_mark_dirty(
    canonical_root: &Path,
    inclusion: &VaultInclusionPolicy,
    event: &notify::Event,
) -> bool {
    if event.need_rescan() {
        return true;
    }
    if matches!(event.kind, EventKind::Access(_)) {
        return false;
    }
    if event.paths.is_empty() {
        return true;
    }
    event.paths.iter().any(|path| {
        let normalized_path = normalize_event_path(canonical_root, path);
        let is_dir = matches!(
            event.kind,
            EventKind::Create(notify::event::CreateKind::Folder)
                | EventKind::Remove(notify::event::RemoveKind::Folder)
        ) || std::fs::symlink_metadata(&normalized_path)
            .is_ok_and(|metadata| metadata.is_dir());
        inclusion.is_control_path(&normalized_path) || inclusion.includes(&normalized_path, is_dir)
    })
}

fn normalize_event_path(canonical_root: &Path, path: &Path) -> PathBuf {
    let rooted_path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        canonical_root.join(path)
    };
    lexical_normalize(&rooted_path)
}

fn lexical_normalize(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            Component::Normal(segment) => normalized.push(segment),
            Component::Prefix(_) | Component::RootDir => normalized.push(component.as_os_str()),
        }
    }
    normalized
}

/// Watch adapter failures.
#[derive(Debug, Error)]
pub enum WatchReconcileError {
    /// Reconciliation scanner failed.
    #[error("watch reconcile scan failed: {source}")]
    ScanAndRepair {
        /// Underlying reconciliation error.
        #[source]
        source: tao_sdk_service::ReconciliationScanError,
    },
}

/// Vault change monitor failures.
#[derive(Debug, Error)]
pub enum VaultChangeMonitorError {
    /// Loading the shared scanner inclusion policy failed.
    #[error("failed to load watcher inclusion policy: {source}")]
    Inclusion {
        /// Scanner policy error.
        #[source]
        source: tao_sdk_vault::VaultScanError,
    },
    /// Vault root canonicalization failed.
    #[error("failed to canonicalize vault root '{path}': {source}")]
    CanonicalizeRoot {
        /// Input vault path.
        path: PathBuf,
        /// Filesystem error.
        #[source]
        source: std::io::Error,
    },
    /// Watcher creation failed.
    #[error("failed to create filesystem watcher: {source}")]
    CreateWatcher {
        /// Watcher backend error.
        #[source]
        source: notify::Error,
    },
    /// Registering the vault root with the watcher failed.
    #[error("failed to watch vault root '{path}': {source}")]
    WatchRoot {
        /// Canonical watched root.
        path: PathBuf,
        /// Watcher backend error.
        #[source]
        source: notify::Error,
    },
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::thread;
    use std::time::{Duration, Instant};

    use rusqlite::Connection;
    use tao_sdk_service::FullIndexService;
    use tao_sdk_storage::run_migrations;
    use tao_sdk_vault::CasePolicy;
    use tempfile::tempdir;

    use super::{VaultChangeMonitor, WatchReconcileService, should_mark_dirty};

    #[test]
    fn reconcile_once_detects_changed_paths() {
        let temp = tempdir().expect("tempdir");
        let vault = temp.path().join("vault");
        fs::create_dir_all(vault.join("notes")).expect("create notes");
        fs::write(vault.join("notes/a.md"), "# A").expect("write seed");

        let mut connection = Connection::open(temp.path().join("index.sqlite")).expect("open db");
        run_migrations(&mut connection).expect("migrate");
        FullIndexService::default()
            .rebuild(&vault, &mut connection, CasePolicy::Sensitive)
            .expect("seed index");

        fs::write(vault.join("notes/a.md"), "# A\nupdated").expect("update note");

        let result = WatchReconcileService::default()
            .reconcile_once(&vault, &mut connection, CasePolicy::Sensitive)
            .expect("reconcile");
        assert!(result.updated_paths >= 1);
        assert!(result.upserted_files >= 1);
    }

    #[test]
    fn change_monitor_marks_generation_when_vault_content_changes() {
        let temp = tempdir().expect("tempdir");
        let vault = temp.path().join("vault");
        fs::create_dir_all(vault.join("notes")).expect("create notes");
        fs::write(vault.join("notes/a.md"), "# A").expect("write seed");

        let monitor = VaultChangeMonitor::start(&vault).expect("start monitor");
        let before = monitor.generation();
        fs::write(vault.join("notes/a.md"), "# A\nupdated").expect("update note");

        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline {
            if monitor.generation() > before {
                return;
            }
            thread::sleep(Duration::from_millis(25));
        }

        panic!("expected watcher generation to advance after note update");
    }

    #[test]
    fn change_monitor_ignores_runtime_paths_after_event_path_normalization() {
        let temp = tempdir().expect("tempdir");
        let vault = temp.path().join("vault");
        fs::create_dir_all(&vault).expect("create vault");
        let canonical_root = fs::canonicalize(&vault).expect("canonicalize vault");
        let ignored_runtime_root = canonical_root.join(".tao");
        let inclusion =
            tao_sdk_vault::VaultInclusionPolicy::load(&canonical_root, CasePolicy::Sensitive)
                .expect("inclusion");

        for path in [
            ignored_runtime_root.join("index.sqlite"),
            PathBuf::from(".tao/index.sqlite"),
            PathBuf::from("./.tao/index.sqlite"),
            PathBuf::from("notes/../.tao/index.sqlite"),
        ] {
            let event = notify::Event::default().add_path(path);
            assert!(!should_mark_dirty(&canonical_root, &inclusion, &event));
        }
    }

    #[test]
    fn change_monitor_marks_normalized_content_paths_dirty() {
        let temp = tempdir().expect("tempdir");
        let vault = temp.path().join("vault");
        fs::create_dir_all(&vault).expect("create vault");
        let canonical_root = fs::canonicalize(&vault).expect("canonicalize vault");
        let inclusion =
            tao_sdk_vault::VaultInclusionPolicy::load(&canonical_root, CasePolicy::Sensitive)
                .expect("inclusion");

        for path in [
            canonical_root.join("notes/a.md"),
            PathBuf::from("notes/a.md"),
            PathBuf::from(".tao/../notes/a.md"),
        ] {
            let event = notify::Event::default().add_path(path);
            assert!(should_mark_dirty(&canonical_root, &inclusion, &event));
        }
    }

    fn monitor_state(root: &std::path::Path) -> super::MonitorState {
        super::MonitorState {
            root: root.to_path_buf(),
            case_policy: CasePolicy::Sensitive,
            inclusion: tao_sdk_vault::VaultInclusionPolicy::load(root, CasePolicy::Sensitive)
                .expect("policy"),
            health: super::VaultWatchHealth::default(),
            last_verification: Instant::now(),
        }
    }

    #[test]
    fn scanner_exclusions_and_read_access_do_not_dirty_watcher() {
        let temp = tempdir().expect("tempdir");
        fs::write(temp.path().join(".taoignore"), "scratch/\n*.bak\n").expect("ignore");
        let root = fs::canonicalize(temp.path()).expect("root");
        let mut state = monitor_state(&root);
        for path in [
            ".git/HEAD",
            ".obsidian/app.json",
            ".tao/index.sqlite",
            "scratch/a.md",
            "notes/a.bak",
        ] {
            assert!(!state.observe(Ok(notify::Event::default().add_path(PathBuf::from(path)))));
        }
        let access = notify::Event::new(notify::EventKind::Access(notify::event::AccessKind::Read))
            .add_path(PathBuf::from("notes/a.md"));
        assert!(!state.observe(Ok(access)));
        assert!(!state.health.requires_reconcile);
        assert!(state.observe(Ok(
            notify::Event::default().add_path(PathBuf::from("notes/a.md"))
        )));
        assert!(state.health.requires_reconcile);
    }

    #[test]
    fn ignore_control_changes_reload_policy_and_preserve_parse_failures() {
        let temp = tempdir().expect("tempdir");
        let root = fs::canonicalize(temp.path()).expect("root");
        let mut state = monitor_state(&root);
        fs::write(root.join(".taoignore"), "scratch/\n").expect("ignore");
        assert!(state.observe(Ok(
            notify::Event::default().add_path(PathBuf::from(".taoignore"))
        )));
        assert!(!state.observe(Ok(
            notify::Event::default().add_path(PathBuf::from("scratch/note.md"))
        )));
        fs::write(root.join(".taoignore"), "{foo,bar\n").expect("malformed ignore");
        assert!(state.observe(Ok(
            notify::Event::default().add_path(PathBuf::from(".taoignore"))
        )));
        assert_eq!(state.health.backend_errors, 1);
        assert!(
            state
                .health
                .last_error
                .as_deref()
                .expect("diagnostic")
                .contains("failed to parse .taoignore")
        );
        fs::remove_file(root.join(".taoignore")).expect("delete ignore");
        assert!(state.observe(Ok(
            notify::Event::default().add_path(PathBuf::from(".taoignore"))
        )));
        assert!(state.observe(Ok(
            notify::Event::default().add_path(PathBuf::from("scratch/note.md"))
        )));
    }

    #[test]
    fn rescan_empty_path_and_backend_errors_require_authoritative_repair() {
        let temp = tempdir().expect("tempdir");
        let root = fs::canonicalize(temp.path()).expect("root");
        let mut state = monitor_state(&root);
        let rescan = notify::Event::new(notify::EventKind::Access(notify::event::AccessKind::Read))
            .set_flag(notify::event::Flag::Rescan);
        assert!(state.observe(Ok(rescan)));
        assert!(
            state.observe(Ok(notify::Event::new(notify::EventKind::Modify(
                notify::event::ModifyKind::Any
            ))))
        );
        assert!(state.observe(Err(notify::Error::generic("backend queue overflow"))));
        assert_eq!(state.health.rescan_requests, 3);
        assert_eq!(state.health.backend_errors, 1);
        assert!(state.health.requires_reconcile);
        assert_eq!(
            state.health.last_error.as_deref(),
            Some("backend queue overflow")
        );
        assert!(
            !state.observe(Ok(notify::Event::new(notify::EventKind::Access(
                notify::event::AccessKind::Read
            ))))
        );
    }

    #[test]
    fn periodic_fallback_and_generation_acknowledgement_are_race_safe() {
        let temp = tempdir().expect("tempdir");
        let monitor = VaultChangeMonitor::start(temp.path()).expect("monitor");
        let before = monitor.generation();
        let requested = monitor.periodic_generation(Duration::ZERO);
        assert!(requested > before);
        assert!(monitor.health().requires_reconcile);
        assert!(!monitor.acknowledge_reconciled(before));
        assert!(monitor.acknowledge_reconciled(requested));
        assert!(!monitor.health().requires_reconcile);
        assert_eq!(monitor.generation(), requested);
    }
}
