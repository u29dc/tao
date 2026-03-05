use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fs;
use std::io::{Read, Write};
#[cfg(unix)]
use std::os::unix::fs::FileTypeExt;
#[cfg(unix)]
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::process::{Command as ProcessCommand, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use clap::{Args, Parser, Subcommand};
use rusqlite::Connection;
use serde::ser::{SerializeMap, SerializeSeq};
use serde::{Deserialize, Serialize, Serializer};
use serde_json::Value as JsonValue;
use tao_sdk_bases::{
    BaseDocument, BaseTableQueryPlanner, BaseViewRegistry, TableQueryPlanRequest,
    parse_base_document,
};
use tao_sdk_bridge::{BridgeEnvelope, BridgeKernel};
use tao_sdk_search::{
    LogicalPlanBuilder, LogicalQueryPlanRequest, PhysicalPlanBuilder, PhysicalPlanOptimizer,
    SearchQueryProjectedItem, SearchQueryProjection, SearchQueryRequest, SearchQueryService,
    SortKey, WhereExpr, apply_sort, apply_where_filter, parse_sort_keys,
    parse_where_expression_opt,
};
use tao_sdk_service::{
    BacklinkGraphService, BaseTableExecutionOptions, BaseTableExecutorService, GraphWalkDirection,
    GraphWalkRequest, HealthSnapshotService, SdkConfigLoader, SdkConfigOverrides, WatcherStatus,
    ensure_runtime_paths,
};
use tao_sdk_storage::{
    BasesRepository, FilesRepository, LinksRepository, PropertiesRepository, TasksRepository,
    preflight_migrations, run_migrations,
};
use tao_sdk_vault::{CasePolicy, PathCanonicalizationService, validate_relative_vault_path};
use tao_sdk_watch::{VaultChangeMonitor, WatchReconcileService};

mod commands;

const DEFAULT_DAEMON_STARTUP_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_DAEMON_SOCKET_DIR: &str = ".tools/tao/daemons";
const QUERY_DOCS_POST_FILTER_PAGE_LIMIT: u64 = 1_000;

#[derive(Debug, Clone, Parser, Serialize, Deserialize)]
#[command(name = "tao", version, about = "tao cli")]
struct Cli {
    /// Emit one JSON envelope to stdout.
    #[arg(long, global = true)]
    json: bool,
    /// Stream JSON envelope serialization for supported large read commands.
    #[arg(long, global = true, default_value_t = false)]
    json_stream: bool,
    /// Allow vault content write operations (disabled by default).
    #[arg(long, global = true, default_value_t = false)]
    allow_writes: bool,
    /// Route command execution through a warm daemon socket.
    #[arg(long, global = true)]
    daemon_socket: Option<String>,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Clone, Subcommand, Serialize, Deserialize)]
enum Commands {
    /// Compact document operations.
    Doc {
        #[command(subcommand)]
        command: DocCommands,
    },
    /// Compact base operations.
    Base {
        #[command(subcommand)]
        command: BaseCommands,
    },
    /// Compact graph operations.
    Graph {
        #[command(subcommand)]
        command: GraphCommands,
    },
    /// Compact metadata operations.
    Meta {
        #[command(subcommand)]
        command: MetaCommands,
    },
    /// Task extraction and state operations.
    Task {
        #[command(subcommand)]
        command: TaskCommands,
    },
    /// Unified read query entrypoint.
    Query(QueryArgs),
    /// Vault lifecycle and indexing operations.
    Vault {
        #[command(subcommand)]
        command: VaultCommands,
    },
}

#[derive(Debug, Clone, Subcommand, Serialize, Deserialize)]
enum DocCommands {
    /// Return one note by normalized path.
    Read(NotePathArgs),
    /// Create or update one note.
    Write(NotePutArgs),
    /// List markdown note windows.
    List(VaultPathArgs),
}

#[derive(Debug, Clone, Subcommand, Serialize, Deserialize)]
enum BaseCommands {
    /// List indexed bases.
    List(VaultPathArgs),
    /// Query one base table view.
    View(BaseViewArgs),
    /// Return one base schema contract.
    Schema(BaseSchemaArgs),
}

#[derive(Debug, Clone, Subcommand, Serialize, Deserialize)]
enum GraphCommands {
    /// Return outgoing links for one note.
    Outgoing(NotePathArgs),
    /// Return backlinks for one note.
    Backlinks(NotePathArgs),
    /// Return unresolved graph links.
    Unresolved(GraphWindowArgs),
    /// Return notes with no outgoing resolved edges.
    Deadends(GraphWindowArgs),
    /// Return isolated notes with no incoming/outgoing resolved edges.
    Orphans(GraphWindowArgs),
    /// Return connected components across resolved graph edges.
    Components(GraphComponentsArgs),
    /// Return one-hop neighbors for one note.
    Neighbors(GraphNeighborsArgs),
    /// Return shortest path between two notes.
    Path(GraphPathArgs),
    /// Walk graph neighbors from one root note.
    Walk(GraphWalkArgs),
}

#[derive(Debug, Clone, Subcommand, Serialize, Deserialize)]
enum MetaCommands {
    /// Aggregate property keys across vault.
    Properties(GraphWindowArgs),
    /// Aggregate tags across vault.
    Tags(GraphWindowArgs),
    /// Aggregate aliases across vault.
    Aliases(GraphWindowArgs),
    /// Aggregate task counts across vault.
    Tasks(TaskListArgs),
}

#[derive(Debug, Clone, Subcommand, Serialize, Deserialize)]
enum TaskCommands {
    /// List extracted markdown tasks.
    List(TaskListArgs),
    /// Update checkbox state on one task line.
    SetState(TaskSetStateArgs),
}

#[derive(Debug, Clone, Subcommand, Serialize, Deserialize)]
enum VaultCommands {
    /// Open one vault path and initialize sqlite state.
    Open(VaultPathArgs),
    /// Return vault health snapshot.
    Stats(VaultPathArgs),
    /// Validate migration state/checksums before startup migration apply.
    Preflight(VaultPathArgs),
    /// Run smart reindex (reconcile drift and refresh index totals).
    Reindex(VaultPathArgs),
    /// Apply incremental reconcile pass.
    Reconcile(VaultPathArgs),
    /// Manage persistent warm-runtime daemon.
    Daemon {
        #[command(subcommand)]
        command: DaemonCommands,
    },
    /// Internal daemon server loop.
    #[command(hide = true)]
    DaemonServe(DaemonSocketArgs),
}

#[derive(Debug, Clone, Subcommand, Serialize, Deserialize)]
enum DaemonCommands {
    /// Start daemon in background process.
    Start(DaemonStartArgs),
    /// Query daemon runtime status.
    Status(DaemonSocketArgs),
    /// Stop daemon and terminate warm runtime.
    Stop(DaemonSocketArgs),
    /// Stop all managed daemons and prune stale socket files.
    StopAll(DaemonStopAllArgs),
}

#[derive(Debug, Clone, Args, Serialize, Deserialize)]
struct VaultPathArgs {
    /// Optional absolute vault root path. Falls back to config/env defaults.
    #[arg(long)]
    vault_root: Option<String>,
    /// Optional sqlite database file path override.
    #[arg(long)]
    db_path: Option<String>,
}

#[derive(Debug, Clone, Args, Serialize, Deserialize)]
struct NotePathArgs {
    /// Optional absolute vault root path. Falls back to config/env defaults.
    #[arg(long)]
    vault_root: Option<String>,
    /// Optional sqlite database file path override.
    #[arg(long)]
    db_path: Option<String>,
    /// Vault-relative normalized note path.
    #[arg(long)]
    path: String,
}

#[derive(Debug, Clone, Args, Serialize, Deserialize)]
struct NotePutArgs {
    /// Optional absolute vault root path. Falls back to config/env defaults.
    #[arg(long)]
    vault_root: Option<String>,
    /// Optional sqlite database file path override.
    #[arg(long)]
    db_path: Option<String>,
    /// Vault-relative normalized note path.
    #[arg(long)]
    path: String,
    /// Full markdown content payload.
    #[arg(long)]
    content: String,
}

#[derive(Debug, Clone, Args, Serialize, Deserialize)]
struct BaseViewArgs {
    /// Optional absolute vault root path. Falls back to config/env defaults.
    #[arg(long)]
    vault_root: Option<String>,
    /// Optional sqlite database file path override.
    #[arg(long)]
    db_path: Option<String>,
    /// Base id or normalized base file path.
    #[arg(long)]
    path_or_id: String,
    /// View name to query.
    #[arg(long)]
    view_name: String,
    /// One-based page number.
    #[arg(long, default_value_t = 1)]
    page: u32,
    /// Page size.
    #[arg(long, default_value_t = 50)]
    page_size: u32,
}

#[derive(Debug, Clone, Args, Serialize, Deserialize)]
struct BaseSchemaArgs {
    /// Optional absolute vault root path. Falls back to config/env defaults.
    #[arg(long)]
    vault_root: Option<String>,
    /// Optional sqlite database file path override.
    #[arg(long)]
    db_path: Option<String>,
    /// Base id or normalized base file path.
    #[arg(long)]
    path_or_id: String,
}

#[derive(Debug, Clone, Args, Serialize, Deserialize)]
struct GraphWindowArgs {
    /// Optional absolute vault root path. Falls back to config/env defaults.
    #[arg(long)]
    vault_root: Option<String>,
    /// Optional sqlite database file path override.
    #[arg(long)]
    db_path: Option<String>,
    /// Window size.
    #[arg(long, default_value_t = 100)]
    limit: u32,
    /// Window offset.
    #[arg(long, default_value_t = 0)]
    offset: u32,
}

#[derive(Debug, Clone, Args, Serialize, Deserialize)]
struct GraphWalkArgs {
    /// Optional absolute vault root path. Falls back to config/env defaults.
    #[arg(long)]
    vault_root: Option<String>,
    /// Optional sqlite database file path override.
    #[arg(long)]
    db_path: Option<String>,
    /// Root note path.
    #[arg(long)]
    path: String,
    /// Maximum BFS depth.
    #[arg(long, default_value_t = 2)]
    depth: u32,
    /// Maximum row count.
    #[arg(long, default_value_t = 200)]
    limit: u32,
    /// Include unresolved links in traversal.
    #[arg(long, default_value_t = false)]
    include_unresolved: bool,
    /// Include folder hierarchy overlay edges.
    #[arg(long, default_value_t = false)]
    include_folders: bool,
}

#[derive(Debug, Clone, Args, Serialize, Deserialize)]
struct GraphComponentsArgs {
    /// Optional absolute vault root path. Falls back to config/env defaults.
    #[arg(long)]
    vault_root: Option<String>,
    /// Optional sqlite database file path override.
    #[arg(long)]
    db_path: Option<String>,
    /// Window size.
    #[arg(long, default_value_t = 100)]
    limit: u32,
    /// Window offset.
    #[arg(long, default_value_t = 0)]
    offset: u32,
    /// Include full member path list for each component (slower on large vaults).
    #[arg(long, default_value_t = false)]
    include_members: bool,
    /// Number of member paths to include when `--include-members` is not set.
    #[arg(long, default_value_t = 64)]
    sample_size: u32,
    /// Component mode selector: weak|strong.
    #[arg(long, default_value = "weak")]
    mode: String,
}

#[derive(Debug, Clone, Args, Serialize, Deserialize)]
struct GraphNeighborsArgs {
    /// Optional absolute vault root path. Falls back to config/env defaults.
    #[arg(long)]
    vault_root: Option<String>,
    /// Optional sqlite database file path override.
    #[arg(long)]
    db_path: Option<String>,
    /// Root note path.
    #[arg(long)]
    path: String,
    /// Direction selector: all|outgoing|incoming.
    #[arg(long, default_value = "all")]
    direction: String,
    /// Window size.
    #[arg(long, default_value_t = 100)]
    limit: u32,
    /// Window offset.
    #[arg(long, default_value_t = 0)]
    offset: u32,
}

#[derive(Debug, Clone, Args, Serialize, Deserialize)]
struct GraphPathArgs {
    /// Optional absolute vault root path. Falls back to config/env defaults.
    #[arg(long)]
    vault_root: Option<String>,
    /// Optional sqlite database file path override.
    #[arg(long)]
    db_path: Option<String>,
    /// Start note path.
    #[arg(long)]
    from: String,
    /// End note path.
    #[arg(long)]
    to: String,
    /// Maximum traversal depth.
    #[arg(long, default_value_t = 8)]
    max_depth: u32,
    /// Maximum number of explored nodes before abort.
    #[arg(long, default_value_t = 10_000)]
    max_nodes: u32,
}

#[derive(Debug, Clone, Args, Serialize, Deserialize)]
struct TaskListArgs {
    /// Optional absolute vault root path. Falls back to config/env defaults.
    #[arg(long)]
    vault_root: Option<String>,
    /// Optional sqlite database file path override.
    #[arg(long)]
    db_path: Option<String>,
    /// Optional state filter: open|done|cancelled.
    #[arg(long)]
    state: Option<String>,
    /// Optional text filter.
    #[arg(long)]
    query: Option<String>,
    /// Window size.
    #[arg(long, default_value_t = 100)]
    limit: u32,
    /// Window offset.
    #[arg(long, default_value_t = 0)]
    offset: u32,
}

#[derive(Debug, Clone, Args, Serialize, Deserialize)]
struct TaskSetStateArgs {
    /// Optional absolute vault root path. Falls back to config/env defaults.
    #[arg(long)]
    vault_root: Option<String>,
    /// Optional sqlite database file path override.
    #[arg(long)]
    db_path: Option<String>,
    /// Vault-relative normalized note path.
    #[arg(long)]
    path: String,
    /// One-based line number of task.
    #[arg(long)]
    line: usize,
    /// Target state: open|done|cancelled.
    #[arg(long)]
    state: String,
}

#[derive(Debug, Clone, Args, Serialize, Deserialize)]
struct QueryArgs {
    /// Optional absolute vault root path. Falls back to config/env defaults.
    #[arg(long)]
    vault_root: Option<String>,
    /// Optional sqlite database file path override.
    #[arg(long)]
    db_path: Option<String>,
    /// Scope selector: docs|graph|task|meta:tags|meta:aliases|meta:properties|base:<id-or-path>
    #[arg(long)]
    from: String,
    /// Optional free text query.
    #[arg(long)]
    query: Option<String>,
    /// Optional note path (for graph outgoing/backlinks).
    #[arg(long)]
    path: Option<String>,
    /// Optional base view name when `--from base:<id>`.
    #[arg(long)]
    view_name: Option<String>,
    /// Optional projected column list for docs scope (`file_id,path,title,matched_in`).
    #[arg(long)]
    select: Option<String>,
    /// Optional typed where expression, for example: `priority >= 2 and done == false`.
    #[arg(long = "where")]
    where_clause: Option<String>,
    /// Optional multi-key sort expression, for example: `priority:desc:nulls_last,path:asc`.
    #[arg(long)]
    sort: Option<String>,
    /// Return logical/physical plan metadata.
    #[arg(long, default_value_t = false)]
    explain: bool,
    /// Execute query rows when used with `--explain`.
    #[arg(long, default_value_t = false)]
    execute: bool,
    /// Window size.
    #[arg(long, default_value_t = 100)]
    limit: u32,
    /// Window offset.
    #[arg(long, default_value_t = 0)]
    offset: u32,
}

#[derive(Debug, Clone, Args, Serialize, Deserialize)]
struct DaemonSocketArgs {
    /// Optional unix domain socket path override for tao daemon.
    #[arg(long)]
    socket: Option<String>,
    /// Optional absolute vault root path used to derive deterministic daemon socket.
    #[arg(long)]
    vault_root: Option<String>,
    /// Optional sqlite database file path override used with `--vault-root`.
    #[arg(long)]
    db_path: Option<String>,
}

#[derive(Debug, Clone, Args, Serialize, Deserialize)]
struct DaemonStartArgs {
    /// Optional unix domain socket path override for tao daemon.
    #[arg(long)]
    socket: Option<String>,
    /// Optional absolute vault root path used to derive deterministic daemon socket.
    #[arg(long)]
    vault_root: Option<String>,
    /// Optional sqlite database file path override used with `--vault-root`.
    #[arg(long)]
    db_path: Option<String>,
    /// Run daemon in foreground (blocks current process).
    #[arg(long, default_value_t = false)]
    foreground: bool,
    /// Maximum wait window for daemon startup when backgrounded.
    #[arg(long, default_value_t = DEFAULT_DAEMON_STARTUP_TIMEOUT_MS)]
    startup_timeout_ms: u64,
}

#[derive(Debug, Clone, Args, Serialize, Deserialize)]
struct DaemonStopAllArgs {
    /// Optional daemon socket directory override.
    #[arg(long)]
    socket_dir: Option<String>,
}

impl VaultPathArgs {
    fn resolve(&self) -> Result<ResolvedVaultPathArgs> {
        resolve_vault_paths(self.vault_root.as_deref(), self.db_path.as_deref())
    }
}

impl NotePathArgs {
    fn resolve(&self) -> Result<ResolvedVaultPathArgs> {
        resolve_vault_paths(self.vault_root.as_deref(), self.db_path.as_deref())
    }
}

impl NotePutArgs {
    fn resolve(&self) -> Result<ResolvedVaultPathArgs> {
        resolve_vault_paths(self.vault_root.as_deref(), self.db_path.as_deref())
    }
}

impl BaseViewArgs {
    fn resolve(&self) -> Result<ResolvedVaultPathArgs> {
        resolve_vault_paths(self.vault_root.as_deref(), self.db_path.as_deref())
    }
}

impl BaseSchemaArgs {
    fn resolve(&self) -> Result<ResolvedVaultPathArgs> {
        resolve_vault_paths(self.vault_root.as_deref(), self.db_path.as_deref())
    }
}

impl GraphWindowArgs {
    fn resolve(&self) -> Result<ResolvedVaultPathArgs> {
        resolve_vault_paths(self.vault_root.as_deref(), self.db_path.as_deref())
    }
}

impl GraphWalkArgs {
    fn resolve(&self) -> Result<ResolvedVaultPathArgs> {
        resolve_vault_paths(self.vault_root.as_deref(), self.db_path.as_deref())
    }
}

impl GraphComponentsArgs {
    fn resolve(&self) -> Result<ResolvedVaultPathArgs> {
        resolve_vault_paths(self.vault_root.as_deref(), self.db_path.as_deref())
    }
}

impl GraphNeighborsArgs {
    fn resolve(&self) -> Result<ResolvedVaultPathArgs> {
        resolve_vault_paths(self.vault_root.as_deref(), self.db_path.as_deref())
    }
}

impl GraphPathArgs {
    fn resolve(&self) -> Result<ResolvedVaultPathArgs> {
        resolve_vault_paths(self.vault_root.as_deref(), self.db_path.as_deref())
    }
}

impl TaskListArgs {
    fn resolve(&self) -> Result<ResolvedVaultPathArgs> {
        resolve_vault_paths(self.vault_root.as_deref(), self.db_path.as_deref())
    }
}

impl TaskSetStateArgs {
    fn resolve(&self) -> Result<ResolvedVaultPathArgs> {
        resolve_vault_paths(self.vault_root.as_deref(), self.db_path.as_deref())
    }
}

impl QueryArgs {
    fn resolve(&self) -> Result<ResolvedVaultPathArgs> {
        resolve_vault_paths(self.vault_root.as_deref(), self.db_path.as_deref())
    }
}

impl DaemonSocketArgs {
    fn resolve_socket(&self) -> Result<String> {
        resolve_daemon_socket(
            self.socket.as_deref(),
            self.vault_root.as_deref(),
            self.db_path.as_deref(),
        )
    }
}

impl DaemonStartArgs {
    fn resolve_socket(&self) -> Result<String> {
        resolve_daemon_socket(
            self.socket.as_deref(),
            self.vault_root.as_deref(),
            self.db_path.as_deref(),
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum QueryDocsColumn {
    FileId,
    Path,
    Title,
    MatchedIn,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GraphNeighborDirection {
    All,
    Outgoing,
    Incoming,
}

impl GraphNeighborDirection {
    fn parse(raw: &str) -> Result<Self> {
        match raw {
            "all" => Ok(Self::All),
            "outgoing" => Ok(Self::Outgoing),
            "incoming" => Ok(Self::Incoming),
            _ => Err(anyhow!(
                "unsupported --direction '{}'; expected one of: all|outgoing|incoming",
                raw
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GraphComponentModeArg {
    Weak,
    Strong,
}

impl GraphComponentModeArg {
    fn parse(raw: &str) -> Result<Self> {
        match raw {
            "weak" => Ok(Self::Weak),
            "strong" => Ok(Self::Strong),
            _ => Err(anyhow!(
                "unsupported --mode '{}'; expected one of: weak|strong",
                raw
            )),
        }
    }

    fn as_service_mode(self) -> tao_sdk_service::GraphComponentMode {
        match self {
            Self::Weak => tao_sdk_service::GraphComponentMode::Weak,
            Self::Strong => tao_sdk_service::GraphComponentMode::Strong,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Weak => "weak",
            Self::Strong => "strong",
        }
    }
}

impl QueryDocsColumn {
    fn key(self) -> &'static str {
        match self {
            Self::FileId => "file_id",
            Self::Path => "path",
            Self::Title => "title",
            Self::MatchedIn => "matched_in",
        }
    }
}

fn parse_query_docs_columns(select: Option<&str>) -> Result<Vec<QueryDocsColumn>> {
    let mut columns = Vec::new();
    let mut seen = HashSet::new();

    if let Some(raw) = select {
        let tokens = raw
            .split(',')
            .map(str::trim)
            .filter(|token| !token.is_empty())
            .collect::<Vec<_>>();
        if tokens.is_empty() {
            return Err(anyhow!(
                "--select must include at least one docs column from: file_id,path,title,matched_in"
            ));
        }
        for token in tokens {
            let column = match token {
                "file_id" => QueryDocsColumn::FileId,
                "path" => QueryDocsColumn::Path,
                "title" => QueryDocsColumn::Title,
                "matched_in" => QueryDocsColumn::MatchedIn,
                _ => {
                    return Err(anyhow!(
                        "unsupported docs projection column '{}'; allowed: file_id,path,title,matched_in",
                        token
                    ));
                }
            };
            if seen.insert(column) {
                columns.push(column);
            }
        }
    } else {
        columns = vec![
            QueryDocsColumn::FileId,
            QueryDocsColumn::Path,
            QueryDocsColumn::Title,
            QueryDocsColumn::MatchedIn,
        ];
    }

    Ok(columns)
}

fn project_query_docs_row(
    item: SearchQueryProjectedItem,
    columns: &[QueryDocsColumn],
) -> JsonValue {
    let mut map = serde_json::Map::with_capacity(columns.len());
    for column in columns {
        match column {
            QueryDocsColumn::FileId => {
                map.insert(
                    QueryDocsColumn::FileId.key().to_string(),
                    JsonValue::String(item.file_id.clone().unwrap_or_default()),
                );
            }
            QueryDocsColumn::Path => {
                map.insert(
                    QueryDocsColumn::Path.key().to_string(),
                    JsonValue::String(item.path.clone().unwrap_or_default()),
                );
            }
            QueryDocsColumn::Title => {
                map.insert(
                    QueryDocsColumn::Title.key().to_string(),
                    JsonValue::String(item.title.clone().unwrap_or_default()),
                );
            }
            QueryDocsColumn::MatchedIn => {
                map.insert(
                    QueryDocsColumn::MatchedIn.key().to_string(),
                    JsonValue::Array(
                        item.matched_in
                            .clone()
                            .unwrap_or_default()
                            .iter()
                            .cloned()
                            .map(JsonValue::String)
                            .collect::<Vec<_>>(),
                    ),
                );
            }
        }
    }
    JsonValue::Object(map)
}

struct QueryDocsStreamingEnvelope<'a> {
    page: &'a tao_sdk_search::SearchQueryProjectedPage,
    columns: &'a [QueryDocsColumn],
}

struct QueryDocsStreamingValue<'a> {
    page: &'a tao_sdk_search::SearchQueryProjectedPage,
    columns: &'a [QueryDocsColumn],
}

struct QueryDocsStreamingArgs<'a> {
    page: &'a tao_sdk_search::SearchQueryProjectedPage,
    columns: &'a [QueryDocsColumn],
}

struct QueryDocsStreamingRows<'a> {
    items: &'a [SearchQueryProjectedItem],
    columns: &'a [QueryDocsColumn],
}

struct QueryDocsStreamingRow<'a> {
    item: &'a SearchQueryProjectedItem,
    columns: &'a [QueryDocsColumn],
}

fn query_docs_projection(columns: &[QueryDocsColumn]) -> SearchQueryProjection {
    SearchQueryProjection {
        include_file_id: columns.contains(&QueryDocsColumn::FileId),
        include_path: columns.contains(&QueryDocsColumn::Path),
        include_title: columns.contains(&QueryDocsColumn::Title),
        include_matched_in: columns.contains(&QueryDocsColumn::MatchedIn),
    }
}

#[derive(Debug)]
struct QueryPostFilterAccumulator {
    offset: usize,
    limit: usize,
    sort_keys: Vec<SortKey>,
    total: u64,
    rows: Vec<serde_json::Map<String, JsonValue>>,
}

impl QueryPostFilterAccumulator {
    fn new(offset: u32, limit: u32, sort_keys: &[SortKey]) -> Self {
        Self {
            offset: offset as usize,
            limit: limit as usize,
            sort_keys: sort_keys.to_vec(),
            total: 0,
            rows: Vec::new(),
        }
    }

    fn push_batch(&mut self, batch: Vec<serde_json::Map<String, JsonValue>>) {
        if self.sort_keys.is_empty() {
            for row in batch {
                let row_index = usize::try_from(self.total).unwrap_or(usize::MAX);
                self.total = self.total.saturating_add(1);
                if row_index < self.offset {
                    continue;
                }
                if self.rows.len() < self.limit {
                    self.rows.push(row);
                }
            }
            return;
        }

        let window_size = self.offset.saturating_add(self.limit);
        for row in batch {
            self.total = self.total.saturating_add(1);
            if window_size == 0 {
                continue;
            }
            self.rows.push(row);
            apply_sort(&mut self.rows, &self.sort_keys);
            if self.rows.len() > window_size {
                self.rows.pop();
            }
        }
    }

    fn finish(mut self) -> (u64, Vec<JsonValue>) {
        let rows = if self.sort_keys.is_empty() {
            self.rows
        } else {
            self.rows = self
                .rows
                .into_iter()
                .skip(self.offset)
                .take(self.limit)
                .collect::<Vec<_>>();
            self.rows
        };
        (
            self.total,
            rows.into_iter().map(JsonValue::Object).collect::<Vec<_>>(),
        )
    }
}

fn apply_post_filter_batch(
    batch: Vec<serde_json::Map<String, JsonValue>>,
    where_expr: Option<&WhereExpr>,
) -> Result<Vec<serde_json::Map<String, JsonValue>>> {
    apply_where_filter(batch, where_expr)
        .map_err(|source| anyhow!("evaluate --where failed: {source}"))
}

fn flatten_base_query_row(
    row: serde_json::Map<String, JsonValue>,
) -> serde_json::Map<String, JsonValue> {
    let mut flattened = serde_json::Map::<String, JsonValue>::new();
    if let Some(file_id) = row.get("file_id") {
        flattened.insert("file_id".to_string(), file_id.clone());
    }
    if let Some(file_path) = row.get("file_path") {
        flattened.insert("path".to_string(), file_path.clone());
    }
    if let Some(values) = row.get("values").and_then(JsonValue::as_object) {
        for (key, value) in values {
            flattened.insert(key.clone(), value.clone());
        }
    }
    flattened
}

fn collect_docs_rows_for_where_only(
    runtime: &mut RuntimeMode,
    resolved: &ResolvedVaultPathArgs,
    query: &str,
    columns: &[QueryDocsColumn],
    where_expr: &WhereExpr,
    limit: u32,
    offset: u32,
) -> Result<(u64, Vec<JsonValue>)> {
    with_connection(runtime, resolved, |connection| {
        let mut query_offset = 0_u64;
        let mut total = 0_u64;
        let mut rows = Vec::new();

        loop {
            let page = SearchQueryService.query_projected(
                Path::new(&resolved.vault_root),
                connection,
                SearchQueryRequest {
                    query: query.to_string(),
                    limit: QUERY_DOCS_POST_FILTER_PAGE_LIMIT,
                    offset: query_offset,
                },
                SearchQueryProjection::default(),
            )?;
            let batch_count = u64::try_from(page.items.len()).unwrap_or(u64::MAX);
            if batch_count == 0 {
                break;
            }

            let batch_rows = page
                .items
                .into_iter()
                .filter_map(|item| match project_query_docs_row(item, columns) {
                    JsonValue::Object(map) => Some(map),
                    _ => None,
                })
                .collect::<Vec<_>>();
            let filtered = apply_where_filter(batch_rows, Some(where_expr))
                .map_err(|source| anyhow!("evaluate --where failed: {source}"))?;
            for row in filtered {
                if total >= u64::from(offset) && rows.len() < limit as usize {
                    rows.push(JsonValue::Object(row));
                }
                total = total.saturating_add(1);
            }

            query_offset = query_offset.saturating_add(batch_count);
            if query_offset >= page.total {
                break;
            }
        }

        Ok((total, rows))
    })
}

impl Serialize for QueryDocsStreamingEnvelope<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(3))?;
        map.serialize_entry("ok", &true)?;
        map.serialize_entry(
            "value",
            &QueryDocsStreamingValue {
                page: self.page,
                columns: self.columns,
            },
        )?;
        map.serialize_entry("error", &Option::<JsonValue>::None)?;
        map.end()
    }
}

impl Serialize for QueryDocsStreamingValue<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(3))?;
        map.serialize_entry("command", "query.run")?;
        map.serialize_entry("summary", "query run completed")?;
        map.serialize_entry(
            "args",
            &QueryDocsStreamingArgs {
                page: self.page,
                columns: self.columns,
            },
        )?;
        map.end()
    }
}

impl Serialize for QueryDocsStreamingArgs<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(6))?;
        let column_names = self
            .columns
            .iter()
            .map(|column| column.key())
            .collect::<Vec<_>>();
        map.serialize_entry("from", "docs")?;
        map.serialize_entry("columns", &column_names)?;
        map.serialize_entry(
            "rows",
            &QueryDocsStreamingRows {
                items: &self.page.items,
                columns: self.columns,
            },
        )?;
        map.serialize_entry("total", &self.page.total)?;
        map.serialize_entry("limit", &self.page.limit)?;
        map.serialize_entry("offset", &self.page.offset)?;
        map.end()
    }
}

impl Serialize for QueryDocsStreamingRows<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut sequence = serializer.serialize_seq(Some(self.items.len()))?;
        for item in self.items {
            sequence.serialize_element(&QueryDocsStreamingRow {
                item,
                columns: self.columns,
            })?;
        }
        sequence.end()
    }
}

impl Serialize for QueryDocsStreamingRow<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.columns.len()))?;
        for column in self.columns {
            match column {
                QueryDocsColumn::FileId => {
                    map.serialize_entry(
                        column.key(),
                        &self.item.file_id.clone().unwrap_or_default(),
                    )?;
                }
                QueryDocsColumn::Path => {
                    map.serialize_entry(column.key(), &self.item.path.clone().unwrap_or_default())?;
                }
                QueryDocsColumn::Title => {
                    map.serialize_entry(
                        column.key(),
                        &self.item.title.clone().unwrap_or_default(),
                    )?;
                }
                QueryDocsColumn::MatchedIn => {
                    map.serialize_entry(
                        column.key(),
                        &self.item.matched_in.clone().unwrap_or_default(),
                    )?;
                }
            }
        }
        map.end()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CommandResult {
    command: String,
    summary: String,
    args: JsonValue,
}

#[derive(Debug, Clone)]
struct ResolvedVaultPathArgs {
    vault_root: String,
    data_dir: String,
    db_path: String,
    case_policy: CasePolicy,
    read_only: bool,
}

#[derive(Debug, Clone, Serialize)]
struct ExtractedTask {
    path: String,
    line: usize,
    state: String,
    text: String,
}

#[derive(Debug, Default)]
struct RuntimeCache {
    kernels: HashMap<String, BridgeKernel>,
    connections: HashMap<String, Connection>,
    command_results: HashMap<String, CachedCommandResult>,
    change_monitors: HashMap<String, VaultChangeMonitor>,
    last_reconciled_generation: HashMap<String, u64>,
}

#[derive(Debug, Clone)]
struct CachedCommandResult {
    runtime_key: String,
    result: CommandResult,
}

#[derive(Debug)]
enum RuntimeMode {
    OneShot,
    Daemon(Box<RuntimeCache>),
}

#[derive(Debug, Serialize)]
struct JsonEnvelope<T: Serialize> {
    ok: bool,
    value: Option<T>,
    error: Option<JsonError>,
}

#[derive(Debug, Serialize)]
struct JsonError {
    code: String,
    message: String,
    hint: Option<String>,
    context: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DaemonExecuteRequest {
    command: Commands,
    allow_writes: bool,
    json: bool,
    json_stream: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum DaemonRequest {
    Execute { payload: Box<DaemonExecuteRequest> },
    Status,
    Shutdown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DaemonStatus {
    uptime_ms: u128,
    cached_connections: usize,
    cached_kernels: usize,
    cached_results: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DaemonResponse {
    ok: bool,
    output: Option<String>,
    error: Option<String>,
    status: Option<DaemonStatus>,
}

impl<T: Serialize> JsonEnvelope<T> {
    fn success(value: T) -> Self {
        Self {
            ok: true,
            value: Some(value),
            error: None,
        }
    }

    fn failure(error: JsonError) -> Self {
        Self {
            ok: false,
            value: None,
            error: Some(error),
        }
    }
}

pub fn run() -> Result<()> {
    let cli = Cli::parse();
    let run = || -> Result<String> {
        if cli.json_stream && !cli.json {
            return Err(anyhow!("--json-stream requires --json"));
        }
        if let Some(output) = maybe_forward_to_daemon(&cli)? {
            return Ok(output);
        }
        if let Some(output) = maybe_render_streaming_output(&cli)? {
            return Ok(output);
        }

        let result = dispatch(cli.command.clone(), cli.allow_writes)?;
        render_output(cli.json, &result)
    };

    match run() {
        Ok(output) => {
            println!("{output}");
            Ok(())
        }
        Err(source) => {
            if cli.json {
                let rendered = render_error_output(&source)?;
                println!("{rendered}");
                return Ok(());
            }
            Err(source)
        }
    }
}

fn render_error_output(error: &anyhow::Error) -> Result<String> {
    let envelope = JsonEnvelope::<CommandResult>::failure(classify_cli_error(error));
    serde_json::to_string(&envelope).context("serialize json error envelope")
}

fn classify_cli_error(error: &anyhow::Error) -> JsonError {
    let message = error.to_string();
    let (code, hint) = if message.contains("--allow-writes") {
        (
            "write_disabled",
            Some("pass --allow-writes to enable write operations".to_string()),
        )
    } else if message.contains("parse --where failed") || message.contains("parse --sort failed") {
        (
            "query_parse_error",
            Some("fix query expression syntax and retry".to_string()),
        )
    } else if message.contains("connect daemon socket") {
        (
            "daemon_unavailable",
            Some("daemon auto-start failed; check socket path permissions or override --daemon-socket".to_string()),
        )
    } else if message.contains("unsupported query scope")
        || message.contains("requires --view-name")
        || message.contains("must not")
    {
        (
            "invalid_argument",
            Some("check command arguments and retry".to_string()),
        )
    } else {
        (
            "command_failed",
            Some("inspect message and rerun with corrected inputs".to_string()),
        )
    };

    JsonError {
        code: code.to_string(),
        message,
        hint,
        context: BTreeMap::new(),
    }
}

fn maybe_render_streaming_output(cli: &Cli) -> Result<Option<String>> {
    let mut runtime = RuntimeMode::OneShot;
    maybe_render_streaming_output_for_command(&cli.command, cli.json_stream, &mut runtime)
}

fn maybe_render_streaming_output_for_command(
    command: &Commands,
    json_stream: bool,
    runtime: &mut RuntimeMode,
) -> Result<Option<String>> {
    if !json_stream {
        return Ok(None);
    }

    let Commands::Query(args) = command else {
        return Ok(None);
    };
    if !args.from.trim().eq_ignore_ascii_case("docs") {
        return Ok(None);
    }
    if args.where_clause.is_some() || args.sort.is_some() {
        // Streaming path intentionally bypasses post-filter plans; fallback to regular path.
        return Ok(None);
    }

    let columns = parse_query_docs_columns(args.select.as_deref())?;
    let projection = query_docs_projection(&columns);
    let resolved = args.resolve()?;
    let page = with_connection(runtime, &resolved, |connection| {
        Ok(SearchQueryService.query_projected(
            Path::new(&resolved.vault_root),
            connection,
            SearchQueryRequest {
                query: args.query.clone().unwrap_or_default(),
                limit: u64::from(args.limit.max(1)),
                offset: u64::from(args.offset),
            },
            projection,
        )?)
    })
    .map_err(|source| anyhow!("query docs failed: {source}"))?;
    let rendered = serde_json::to_string(&QueryDocsStreamingEnvelope {
        page: &page,
        columns: &columns,
    })
    .context("serialize streamed docs query envelope")?;
    Ok(Some(rendered))
}

fn dispatch(command: Commands, allow_writes: bool) -> Result<CommandResult> {
    let mut runtime = RuntimeMode::OneShot;
    dispatch_with_runtime(command, allow_writes, &mut runtime)
}

fn dispatch_with_runtime(
    command: Commands,
    allow_writes: bool,
    runtime: &mut RuntimeMode,
) -> Result<CommandResult> {
    match command {
        Commands::Doc { command } => commands::doc::dispatch(command, allow_writes, runtime),
        Commands::Base { command } => commands::base::dispatch(command, runtime),
        Commands::Graph { command } => commands::graph::dispatch(command, runtime),
        Commands::Meta { command } => commands::meta::dispatch(command, runtime),
        Commands::Task { command } => commands::task::dispatch(command, allow_writes, runtime),
        Commands::Query(args) => commands::query::dispatch(args, runtime),
        Commands::Vault { command } => commands::vault::dispatch(command, runtime),
    }
}

fn render_output(json: bool, result: &CommandResult) -> Result<String> {
    if json {
        Ok(serde_json::to_string(&JsonEnvelope::success(result))?)
    } else {
        Ok(result.summary.clone())
    }
}

fn retag_result(mut result: CommandResult, command: &str, summary: &str) -> CommandResult {
    result.command = command.to_string();
    result.summary = summary.to_string();
    result
}

fn handle_doc(
    command: DocCommands,
    allow_writes: bool,
    runtime: &mut RuntimeMode,
) -> Result<CommandResult> {
    match command {
        DocCommands::Read(args) => {
            let resolved = args.resolve()?;
            let note = with_kernel(runtime, &resolved, |kernel| {
                expect_bridge_value(kernel.note_get(&args.path), "doc.read")
            })?;
            Ok(CommandResult {
                command: "doc.read".to_string(),
                summary: "doc read completed".to_string(),
                args: serde_json::json!({
                    "path": note.path,
                    "title": note.title,
                    "front_matter": note.front_matter,
                    "body": note.body,
                    "headings_total": note.headings_total,
                }),
            })
        }
        DocCommands::Write(args) => {
            let resolved = args.resolve()?;
            ensure_writes_enabled(allow_writes, resolved.read_only, "doc.write")?;
            let ack = with_kernel(runtime, &resolved, |kernel| {
                expect_bridge_value(
                    kernel.note_put_with_policy(&args.path, &args.content, true),
                    "doc.write",
                )
            })?;
            Ok(CommandResult {
                command: "doc.write".to_string(),
                summary: "doc write completed".to_string(),
                args: serde_json::json!({
                    "path": ack.path,
                    "file_id": ack.file_id,
                    "action": ack.action,
                    "index_synced": ack.index_synced,
                    "event_logged": ack.event_logged,
                    "warnings": ack.warnings,
                }),
            })
        }
        DocCommands::List(args) => {
            let resolved = args.resolve()?;
            let mut after_path: Option<String> = None;
            let mut items = Vec::new();
            loop {
                let page = with_kernel(runtime, &resolved, |kernel| {
                    expect_bridge_value(kernel.notes_list(after_path.as_deref(), 1000), "doc.list")
                })?;
                after_path = page.next_cursor;
                items.extend(page.items.into_iter().map(|item| {
                    serde_json::json!({
                        "file_id": item.file_id,
                        "path": item.path,
                        "title": item.title,
                        "updated_at": item.updated_at,
                    })
                }));
                if after_path.is_none() {
                    break;
                }
            }
            Ok(CommandResult {
                command: "doc.list".to_string(),
                summary: "doc list completed".to_string(),
                args: serde_json::json!({
                    "total": items.len(),
                    "items": items,
                }),
            })
        }
    }
}

fn handle_base(command: BaseCommands, runtime: &mut RuntimeMode) -> Result<CommandResult> {
    match command {
        BaseCommands::List(args) => {
            let resolved = args.resolve()?;
            let bases = with_connection(runtime, &resolved, |connection| {
                Ok(BasesRepository::list_with_paths(connection)?)
            })
            .map_err(|source| anyhow!("query bases failed: {source}"))?;
            let items = bases
                .into_iter()
                .map(|base| -> Result<JsonValue> {
                    let document = decode_base_document(&base.config_json)?;
                    Ok(serde_json::json!({
                        "base_id": base.base_id,
                        "file_path": base.file_path,
                        "views": document
                            .views
                            .into_iter()
                            .map(|view| view.name)
                            .collect::<Vec<_>>(),
                        "updated_at": base.updated_at,
                    }))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(CommandResult {
                command: "base.list".to_string(),
                summary: "base list completed".to_string(),
                args: serde_json::json!({
                    "total": items.len(),
                    "items": items,
                }),
            })
        }
        BaseCommands::View(args) => {
            let resolved = args.resolve()?;
            let base = with_connection(runtime, &resolved, |connection| {
                Ok(BasesRepository::list_with_paths(connection)?)
            })
            .map_err(|source| anyhow!("query bases failed: {source}"))?
            .into_iter()
            .find(|base| base.base_id == args.path_or_id || base.file_path == args.path_or_id)
            .ok_or_else(|| anyhow!("base id/path not found: {}", args.path_or_id))?;
            let document = decode_base_document(&base.config_json)?;
            let registry = BaseViewRegistry::from_document(&document)
                .map_err(|source| anyhow!("decode base view registry failed: {source}"))?;
            let plan = BaseTableQueryPlanner
                .compile(
                    &registry,
                    &TableQueryPlanRequest {
                        view_name: args.view_name.clone(),
                        page: args.page,
                        page_size: args.page_size,
                    },
                )
                .map_err(|source| anyhow!("compile base table query plan failed: {source}"))?;
            let page = with_connection(runtime, &resolved, |connection| {
                Ok(BaseTableExecutorService.execute_with_options(
                    connection,
                    &plan,
                    BaseTableExecutionOptions {
                        include_summaries: false,
                        coercion_mode: tao_sdk_bases::BaseCoercionMode::Permissive,
                    },
                )?)
            })
            .map_err(|source| anyhow!("execute base table query failed: {source}"))?;
            let has_more = (args.page as usize * args.page_size as usize) < page.total as usize;
            let rows = page
                .rows
                .into_iter()
                .map(|row| {
                    serde_json::json!({
                        "file_id": row.file_id,
                        "file_path": row.file_path,
                        "values": row.values,
                    })
                })
                .collect::<Vec<_>>();
            Ok(CommandResult {
                command: "base.view".to_string(),
                summary: "base view completed".to_string(),
                args: serde_json::json!({
                    "base_id": base.base_id,
                    "file_path": base.file_path,
                    "view_name": plan.view_name,
                    "page": args.page,
                    "page_size": args.page_size,
                    "total": page.total,
                    "has_more": has_more,
                    "columns": plan.columns,
                    "sorts": plan.sorts,
                    "grouping": page.grouping,
                    "relation_diagnostics": page.relation_diagnostics,
                    "execution": page.execution,
                    "rows": rows,
                }),
            })
        }
        BaseCommands::Schema(args) => {
            let resolved = args.resolve()?;
            let base = with_connection(runtime, &resolved, |connection| {
                Ok(BasesRepository::list_with_paths(connection)?)
            })
            .map_err(|source| anyhow!("query bases failed: {source}"))?
            .into_iter()
            .find(|base| base.base_id == args.path_or_id || base.file_path == args.path_or_id)
            .ok_or_else(|| anyhow!("base id/path not found: {}", args.path_or_id))?;
            let document = decode_base_document(&base.config_json)?;
            let views = document
                .views
                .iter()
                .map(|view| {
                    serde_json::json!({
                        "name": view.name,
                        "kind": view.kind.as_str(),
                        "source": view.source,
                        "columns": view.columns.iter().map(|column| {
                            serde_json::json!({
                                "name": column.key,
                                "label": column.label,
                                "hidden": column.hidden,
                                "width": column.width,
                                "filterable": true,
                                "sortable": true,
                            })
                        }).collect::<Vec<_>>(),
                    })
                })
                .collect::<Vec<_>>();
            Ok(CommandResult {
                command: "base.schema".to_string(),
                summary: "base schema completed".to_string(),
                args: serde_json::json!({
                    "base_id": base.base_id,
                    "file_path": base.file_path,
                    "views": views,
                }),
            })
        }
    }
}

fn handle_graph(command: GraphCommands, runtime: &mut RuntimeMode) -> Result<CommandResult> {
    match command {
        GraphCommands::Outgoing(args) => {
            let resolved = args.resolve()?;
            let panels = with_kernel(runtime, &resolved, |kernel| {
                expect_bridge_value(kernel.note_links(&args.path), "graph.outgoing")
            })?;
            let items = panels
                .outgoing
                .into_iter()
                .map(|link| {
                    serde_json::json!({
                        "source_path": link.source_path,
                        "target_path": link.target_path,
                        "heading": link.heading,
                        "block_id": link.block_id,
                        "display_text": link.display_text,
                        "kind": link.kind,
                        "resolved": link.resolved,
                    })
                })
                .collect::<Vec<_>>();
            Ok(CommandResult {
                command: "graph.outgoing".to_string(),
                summary: "graph outgoing completed".to_string(),
                args: serde_json::json!({
                    "path": args.path,
                    "total": items.len(),
                    "items": items,
                }),
            })
        }
        GraphCommands::Backlinks(args) => {
            let resolved = args.resolve()?;
            let panels = with_kernel(runtime, &resolved, |kernel| {
                expect_bridge_value(kernel.note_links(&args.path), "graph.backlinks")
            })?;
            let items = panels
                .backlinks
                .into_iter()
                .map(|link| {
                    serde_json::json!({
                        "source_path": link.source_path,
                        "target_path": link.target_path,
                        "heading": link.heading,
                        "block_id": link.block_id,
                        "display_text": link.display_text,
                        "kind": link.kind,
                        "resolved": link.resolved,
                    })
                })
                .collect::<Vec<_>>();
            Ok(CommandResult {
                command: "graph.backlinks".to_string(),
                summary: "graph backlinks completed".to_string(),
                args: serde_json::json!({
                    "path": args.path,
                    "total": items.len(),
                    "items": items,
                }),
            })
        }
        GraphCommands::Unresolved(args) => {
            let resolved = args.resolve()?;
            let (total, rows) = with_connection(runtime, &resolved, |connection| {
                Ok(BacklinkGraphService.unresolved_links_page(
                    connection,
                    args.limit,
                    args.offset,
                )?)
            })
            .map_err(|source| anyhow!("query unresolved links failed: {source}"))?;
            let items = rows.into_iter().map(link_edge_to_json).collect::<Vec<_>>();
            Ok(CommandResult {
                command: "graph.unresolved".to_string(),
                summary: "graph unresolved completed".to_string(),
                args: serde_json::json!({
                    "total": total,
                    "limit": args.limit,
                    "offset": args.offset,
                    "items": items,
                }),
            })
        }
        GraphCommands::Deadends(args) => {
            let resolved = args.resolve()?;
            let (total, rows) = with_connection(runtime, &resolved, |connection| {
                Ok(BacklinkGraphService.deadends_page(connection, args.limit, args.offset)?)
            })
            .map_err(|source| anyhow!("query deadends failed: {source}"))?;
            let items = rows
                .into_iter()
                .map(|row| {
                    serde_json::json!({
                        "file_id": row.file_id,
                        "path": row.path,
                        "incoming_resolved": row.incoming_resolved,
                        "outgoing_resolved": row.outgoing_resolved,
                    })
                })
                .collect::<Vec<_>>();
            Ok(CommandResult {
                command: "graph.deadends".to_string(),
                summary: "graph deadends completed".to_string(),
                args: serde_json::json!({
                    "total": total,
                    "limit": args.limit,
                    "offset": args.offset,
                    "items": items,
                }),
            })
        }
        GraphCommands::Orphans(args) => {
            let resolved = args.resolve()?;
            let (total, rows) = with_connection(runtime, &resolved, |connection| {
                Ok(BacklinkGraphService.orphans_page(connection, args.limit, args.offset)?)
            })
            .map_err(|source| anyhow!("query orphans failed: {source}"))?;
            let items = rows
                .into_iter()
                .map(|row| {
                    serde_json::json!({
                        "file_id": row.file_id,
                        "path": row.path,
                        "incoming_resolved": row.incoming_resolved,
                        "outgoing_resolved": row.outgoing_resolved,
                    })
                })
                .collect::<Vec<_>>();
            Ok(CommandResult {
                command: "graph.orphans".to_string(),
                summary: "graph orphans completed".to_string(),
                args: serde_json::json!({
                    "total": total,
                    "limit": args.limit,
                    "offset": args.offset,
                    "items": items,
                }),
            })
        }
        GraphCommands::Components(args) => {
            let resolved = args.resolve()?;
            let mode = GraphComponentModeArg::parse(args.mode.trim())?;
            let (total, rows) = with_connection(runtime, &resolved, |connection| {
                Ok(BacklinkGraphService.components_page(
                    connection,
                    mode.as_service_mode(),
                    args.limit,
                    args.offset,
                    args.include_members,
                    args.sample_size as usize,
                )?)
            })
            .map_err(|source| anyhow!("query graph components failed: {source}"))?;
            let items = rows
                .into_iter()
                .map(|row| {
                    serde_json::json!({
                        "size": row.size,
                        "paths": row.paths,
                        "truncated": row.truncated,
                    })
                })
                .collect::<Vec<_>>();
            Ok(CommandResult {
                command: "graph.components".to_string(),
                summary: "graph components completed".to_string(),
                args: serde_json::json!({
                    "mode": mode.as_str(),
                    "total": total,
                    "limit": args.limit,
                    "offset": args.offset,
                    "include_members": args.include_members,
                    "sample_size": args.sample_size,
                    "items": items,
                }),
            })
        }
        GraphCommands::Neighbors(args) => {
            let resolved = args.resolve()?;
            let direction = GraphNeighborDirection::parse(args.direction.trim())?;
            let (total, items) = with_connection(runtime, &resolved, |connection| {
                let mut rows = Vec::<serde_json::Value>::new();

                if matches!(
                    direction,
                    GraphNeighborDirection::All | GraphNeighborDirection::Outgoing
                ) {
                    let outgoing =
                        BacklinkGraphService.outgoing_for_path(connection, &args.path)?;
                    for edge in outgoing {
                        let Some(target_path) = edge.resolved_path.clone() else {
                            continue;
                        };
                        rows.push(serde_json::json!({
                            "path": target_path,
                            "direction": "outgoing",
                            "link_id": edge.link_id,
                            "source_path": edge.source_path,
                            "raw_target": edge.raw_target,
                        }));
                    }
                }

                if matches!(
                    direction,
                    GraphNeighborDirection::All | GraphNeighborDirection::Incoming
                ) {
                    let incoming =
                        BacklinkGraphService.backlinks_for_path(connection, &args.path)?;
                    for edge in incoming {
                        rows.push(serde_json::json!({
                            "path": edge.source_path,
                            "direction": "incoming",
                            "link_id": edge.link_id,
                            "source_path": edge.source_path,
                            "raw_target": edge.raw_target,
                        }));
                    }
                }

                rows.sort_by(|left, right| {
                    let left_path = left
                        .get("path")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or_default();
                    let right_path = right
                        .get("path")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or_default();
                    let left_direction = left
                        .get("direction")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or_default();
                    let right_direction = right
                        .get("direction")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or_default();
                    left_path
                        .cmp(right_path)
                        .then_with(|| left_direction.cmp(right_direction))
                });
                rows.dedup_by(|left, right| {
                    left.get("path") == right.get("path")
                        && left.get("direction") == right.get("direction")
                });

                let total = u64::try_from(rows.len()).unwrap_or(u64::MAX);
                let items = paginate_json_items(rows, args.limit, args.offset);
                Ok((total, items))
            })
            .map_err(|source| anyhow!("graph neighbors failed: {source}"))?;
            Ok(CommandResult {
                command: "graph.neighbors".to_string(),
                summary: "graph neighbors completed".to_string(),
                args: serde_json::json!({
                    "path": args.path,
                    "direction": args.direction,
                    "total": total,
                    "limit": args.limit,
                    "offset": args.offset,
                    "items": items,
                }),
            })
        }
        GraphCommands::Path(args) => {
            if args.max_nodes == 0 {
                return Err(anyhow!("--max-nodes must be greater than zero"));
            }
            let resolved = args.resolve()?;
            let (found, explored_nodes, path) = with_connection(runtime, &resolved, |connection| {
                let Some(from_file) = FilesRepository::get_by_normalized_path(connection, &args.from)? else {
                    return Ok((false, 0_u32, Vec::<String>::new()));
                };
                let Some(to_file) = FilesRepository::get_by_normalized_path(connection, &args.to)? else {
                    return Ok((false, 0_u32, Vec::<String>::new()));
                };

                if from_file.file_id == to_file.file_id {
                    return Ok((true, 1_u32, vec![from_file.normalized_path]));
                }

                let file_rows = FilesRepository::list_all(connection)?;
                let mut path_by_file_id = HashMap::<String, String>::new();
                for row in file_rows {
                    if row.is_markdown {
                        path_by_file_id.insert(row.file_id, row.normalized_path);
                    }
                }

                let pairs = LinksRepository::list_resolved_pairs(connection)?;
                let mut adjacency = HashMap::<String, Vec<String>>::new();
                for pair in pairs {
                    adjacency
                        .entry(pair.source_file_id.clone())
                        .or_default()
                        .push(pair.target_file_id.clone());
                    adjacency
                        .entry(pair.target_file_id)
                        .or_default()
                        .push(pair.source_file_id);
                }
                for neighbors in adjacency.values_mut() {
                    neighbors.sort();
                    neighbors.dedup();
                }

                let from_id = from_file.file_id;
                let to_id = to_file.file_id;
                let mut queue = VecDeque::<String>::from([from_id.clone()]);
                let mut depth_by_id = HashMap::<String, u32>::new();
                let mut parent_by_id = HashMap::<String, String>::new();
                depth_by_id.insert(from_id.clone(), 0);
                let mut explored_nodes: u32 = 1;

                while let Some(current) = queue.pop_front() {
                    if current == to_id {
                        break;
                    }
                    let current_depth = *depth_by_id.get(&current).unwrap_or(&0);
                    if current_depth >= args.max_depth {
                        continue;
                    }
                    if let Some(neighbors) = adjacency.get(&current) {
                        for next in neighbors {
                            if depth_by_id.contains_key(next) {
                                continue;
                            }
                            explored_nodes = explored_nodes.saturating_add(1);
                            if explored_nodes > args.max_nodes {
                                return Err(anyhow!(
                                    "graph path aborted after exploring {} nodes; increase --max-nodes",
                                    args.max_nodes
                                ));
                            }
                            depth_by_id.insert(next.clone(), current_depth + 1);
                            parent_by_id.insert(next.clone(), current.clone());
                            queue.push_back(next.clone());
                        }
                    }
                }

                if !depth_by_id.contains_key(&to_id) {
                    return Ok((false, explored_nodes, Vec::<String>::new()));
                }

                let mut path_ids = vec![to_id.clone()];
                let mut cursor = to_id;
                while let Some(parent) = parent_by_id.get(&cursor).cloned() {
                    path_ids.push(parent.clone());
                    if parent == from_id {
                        break;
                    }
                    cursor = parent;
                }
                path_ids.reverse();
                let path = path_ids
                    .into_iter()
                    .filter_map(|file_id| path_by_file_id.get(&file_id).cloned())
                    .collect::<Vec<_>>();
                Ok((true, explored_nodes, path))
            })
            .map_err(|source| anyhow!("graph path failed: {source}"))?;
            let edge_count = path.len().saturating_sub(1);
            Ok(CommandResult {
                command: "graph.path".to_string(),
                summary: "graph path completed".to_string(),
                args: serde_json::json!({
                    "from": args.from,
                    "to": args.to,
                    "found": found,
                    "max_depth": args.max_depth,
                    "max_nodes": args.max_nodes,
                    "explored_nodes": explored_nodes,
                    "edge_count": edge_count,
                    "path": path,
                }),
            })
        }
        GraphCommands::Walk(args) => {
            let resolved = args.resolve()?;
            let traversed = with_connection(runtime, &resolved, |connection| {
                Ok(BacklinkGraphService.walk(
                    connection,
                    &GraphWalkRequest {
                        path: args.path.clone(),
                        depth: args.depth,
                        limit: args.limit,
                        include_unresolved: args.include_unresolved,
                        include_folders: args.include_folders,
                    },
                )?)
            })
            .map_err(|source| anyhow!("graph walk failed: {source}"))?;
            let items = traversed
                .into_iter()
                .map(|step| {
                    let direction = match step.direction {
                        GraphWalkDirection::Outgoing => "outgoing",
                        GraphWalkDirection::Incoming => "incoming",
                    };
                    let edge_type = match step.edge_type {
                        tao_sdk_service::GraphWalkEdgeType::Wikilink => "wikilink",
                        tao_sdk_service::GraphWalkEdgeType::FolderParent => "folder-parent",
                        tao_sdk_service::GraphWalkEdgeType::FolderSibling => "folder-sibling",
                    };
                    serde_json::json!({
                        "depth": step.depth,
                        "direction": direction,
                        "edge_type": edge_type,
                        "link_id": step.link_id,
                        "source_path": step.source_path,
                        "target_path": step.target_path,
                        "raw_target": step.raw_target,
                        "resolved": step.resolved,
                    })
                })
                .collect::<Vec<_>>();
            Ok(CommandResult {
                command: "graph.walk".to_string(),
                summary: "graph walk completed".to_string(),
                args: serde_json::json!({
                    "path": args.path,
                    "depth": args.depth,
                    "include_folders": args.include_folders,
                    "total": items.len(),
                    "items": items,
                }),
            })
        }
    }
}

fn handle_meta(command: MetaCommands, runtime: &mut RuntimeMode) -> Result<CommandResult> {
    match command {
        MetaCommands::Properties(args) => {
            let resolved = args.resolve()?;
            let items = with_connection(runtime, &resolved, |connection| {
                let mut statement = connection
                    .prepare(
                        "SELECT key, COUNT(*) AS total FROM properties GROUP BY key ORDER BY key ASC",
                    )
                    .context("prepare properties aggregate query")?;
                let rows = statement
                    .query_map([], |row| {
                        Ok(serde_json::json!({
                            "key": row.get::<_, String>(0)?,
                            "total": row.get::<_, u64>(1)?,
                        }))
                    })
                    .context("query properties aggregate rows")?;
                let mut items = Vec::new();
                for row in rows {
                    items.push(row.context("map properties aggregate row")?);
                }
                Ok(items)
            })?;
            let total = items.len();
            let items = paginate_json_items(items, args.limit, args.offset);
            Ok(CommandResult {
                command: "meta.properties".to_string(),
                summary: "meta properties completed".to_string(),
                args: serde_json::json!({
                    "total": total,
                    "limit": args.limit,
                    "offset": args.offset,
                    "items": items,
                }),
            })
        }
        MetaCommands::Tags(args) => handle_meta_token_aggregate(args, "tags", "meta.tags", runtime),
        MetaCommands::Aliases(args) => {
            handle_meta_token_aggregate(args, "aliases", "meta.aliases", runtime)
        }
        MetaCommands::Tasks(args) => {
            let result = handle_task(TaskCommands::List(args), false, runtime)?;
            Ok(retag_result(result, "meta.tasks", "meta tasks completed"))
        }
    }
}

fn handle_task(
    command: TaskCommands,
    allow_writes: bool,
    runtime: &mut RuntimeMode,
) -> Result<CommandResult> {
    match command {
        TaskCommands::List(args) => {
            let resolved = args.resolve()?;
            let state = args
                .state
                .as_deref()
                .map(str::trim)
                .filter(|state| !state.is_empty());
            let query = args
                .query
                .as_deref()
                .map(str::trim)
                .filter(|query| !query.is_empty());
            let (total, rows) = with_connection(runtime, &resolved, |connection| {
                let total = TasksRepository::count_with_paths(connection, state, query, None)
                    .map_err(|source| anyhow!("count tasks failed: {source}"))?;
                let rows = TasksRepository::list_with_paths(
                    connection,
                    state,
                    query,
                    None,
                    args.limit,
                    args.offset,
                )
                .map_err(|source| anyhow!("list tasks failed: {source}"))?;
                Ok((total, rows))
            })?;
            let items = rows
                .into_iter()
                .map(|row| {
                    let line = usize::try_from(row.line_number).unwrap_or(0);
                    serde_json::to_value(ExtractedTask {
                        path: row.file_path,
                        line,
                        state: row.state,
                        text: row.text,
                    })
                    .unwrap_or(JsonValue::Null)
                })
                .collect::<Vec<_>>();
            Ok(CommandResult {
                command: "task.list".to_string(),
                summary: "task list completed".to_string(),
                args: serde_json::json!({
                    "total": total,
                    "limit": args.limit,
                    "offset": args.offset,
                    "items": items,
                }),
            })
        }
        TaskCommands::SetState(args) => {
            let resolved = args.resolve()?;
            ensure_writes_enabled(allow_writes, resolved.read_only, "task.set-state")?;
            let absolute = resolve_existing_vault_note_path(&resolved, &args.path)
                .map_err(|source| anyhow!("resolve task note path '{}': {source}", args.path))?;
            let markdown = fs::read_to_string(&absolute)
                .with_context(|| format!("read markdown note '{}'", absolute.display()))?;
            let mut lines = markdown.lines().map(str::to_string).collect::<Vec<_>>();
            if args.line == 0 || args.line > lines.len() {
                return Err(anyhow!(
                    "task line is out of range for '{}': {}",
                    args.path,
                    args.line
                ));
            }
            let index = args.line - 1;
            let updated = update_task_line_state(&lines[index], &args.state)?;
            lines[index] = updated;
            let mut rebuilt = lines.join("\n");
            if markdown.ends_with('\n') {
                rebuilt.push('\n');
            }
            fs::write(&absolute, rebuilt)
                .with_context(|| format!("write markdown note '{}'", absolute.display()))?;

            with_connection(runtime, &resolved, |connection| {
                WatchReconcileService::default()
                    .reconcile_once(
                        Path::new(&resolved.vault_root),
                        connection,
                        resolved.case_policy,
                    )
                    .map_err(|source| anyhow!("reconcile after task state update failed: {source}"))
            })?;

            Ok(CommandResult {
                command: "task.set-state".to_string(),
                summary: "task set-state completed".to_string(),
                args: serde_json::json!({
                    "path": args.path,
                    "line": args.line,
                    "state": args.state,
                }),
            })
        }
    }
}

fn handle_query(args: QueryArgs, runtime: &mut RuntimeMode) -> Result<CommandResult> {
    let from = args.from.trim();
    let limit = args.limit.max(1);
    let where_expr = parse_where_expression_opt(args.where_clause.as_deref())
        .map_err(|source| anyhow!("parse --where failed: {source}"))?;
    let sort_keys = parse_sort_keys(args.sort.as_deref())
        .map_err(|source| anyhow!("parse --sort failed: {source}"))?;
    if from.eq_ignore_ascii_case("docs") {
        let columns = parse_query_docs_columns(args.select.as_deref())?;
        let projection = query_docs_projection(&columns);
        let logical_plan = LogicalPlanBuilder
            .build(LogicalQueryPlanRequest {
                from: from.to_string(),
                query: args.query.clone(),
                where_expr: where_expr.clone(),
                sort_keys: sort_keys.clone(),
                projection: columns
                    .iter()
                    .map(|column| column.key().to_string())
                    .collect(),
                limit: u64::from(limit),
                offset: u64::from(args.offset),
                execute: !args.explain || args.execute,
            })
            .map_err(|source| anyhow!("build logical query plan failed: {source}"))?;
        let physical_plan = PhysicalPlanOptimizer.optimize(
            PhysicalPlanBuilder
                .build(&logical_plan)
                .map_err(|source| anyhow!("build physical query plan failed: {source}"))?,
        );
        if args.explain && !args.execute {
            return Ok(CommandResult {
                command: "query.run".to_string(),
                summary: "query explain completed".to_string(),
                args: serde_json::json!({
                    "from": "docs",
                    "logical_plan": {
                        "scope": logical_plan.scope.label(),
                        "query": logical_plan.query,
                        "has_where": logical_plan.where_expr.is_some(),
                        "sort_keys": logical_plan.sort_keys.iter().map(|sort| {
                            serde_json::json!({
                                "field": sort.field,
                                "direction": match sort.direction {
                                    tao_sdk_search::SortDirection::Asc => "asc",
                                    tao_sdk_search::SortDirection::Desc => "desc",
                                },
                                "null_order": match sort.null_order {
                                    tao_sdk_search::NullOrder::First => "first",
                                    tao_sdk_search::NullOrder::Last => "last",
                                },
                            })
                        }).collect::<Vec<_>>(),
                        "projection": logical_plan.projection,
                        "limit": logical_plan.limit,
                        "offset": logical_plan.offset,
                        "execute": logical_plan.execute,
                    },
                    "physical_plan": {
                        "adapter": physical_plan.adapter.label(),
                        "stages": physical_plan.filter_stages,
                        "limit": physical_plan.limit,
                        "offset": physical_plan.offset,
                        "execute": physical_plan.execute,
                    }
                }),
            });
        }
        let resolved = args.resolve()?;
        let apply_post_filters = where_expr.is_some() || !sort_keys.is_empty();
        let query = args.query.clone().unwrap_or_default();

        let (total, rows) = if apply_post_filters {
            if sort_keys.is_empty() {
                collect_docs_rows_for_where_only(
                    runtime,
                    &resolved,
                    &query,
                    &columns,
                    where_expr
                        .as_ref()
                        .expect("apply_post_filters implies where expr when no sort keys"),
                    limit,
                    args.offset,
                )?
            } else {
                let mut accumulator =
                    QueryPostFilterAccumulator::new(args.offset, limit, &sort_keys);
                with_connection(runtime, &resolved, |connection| {
                    let mut query_offset = 0_u64;

                    loop {
                        let page = SearchQueryService.query_projected(
                            Path::new(&resolved.vault_root),
                            connection,
                            SearchQueryRequest {
                                query: query.clone(),
                                limit: QUERY_DOCS_POST_FILTER_PAGE_LIMIT,
                                offset: query_offset,
                            },
                            SearchQueryProjection::default(),
                        )?;
                        let batch_count = u64::try_from(page.items.len()).unwrap_or(u64::MAX);
                        if batch_count == 0 {
                            break;
                        }

                        let batch_rows = page
                            .items
                            .into_iter()
                            .filter_map(|item| match project_query_docs_row(item, &columns) {
                                JsonValue::Object(map) => Some(map),
                                _ => None,
                            })
                            .collect::<Vec<_>>();
                        let filtered = apply_post_filter_batch(batch_rows, where_expr.as_ref())?;
                        accumulator.push_batch(filtered);

                        query_offset = query_offset.saturating_add(batch_count);
                        if query_offset >= page.total {
                            break;
                        }
                    }

                    Ok::<(), anyhow::Error>(())
                })
                .map_err(|source| anyhow!("query docs failed: {source}"))?;
                accumulator.finish()
            }
        } else {
            let page = with_connection(runtime, &resolved, |connection| {
                Ok(SearchQueryService.query_projected(
                    Path::new(&resolved.vault_root),
                    connection,
                    SearchQueryRequest {
                        query,
                        limit: u64::from(limit),
                        offset: u64::from(args.offset),
                    },
                    projection,
                )?)
            })
            .map_err(|source| anyhow!("query docs failed: {source}"))?;
            let rows = page
                .items
                .into_iter()
                .filter_map(|item| match project_query_docs_row(item, &columns) {
                    JsonValue::Object(map) => Some(JsonValue::Object(map)),
                    _ => None,
                })
                .collect::<Vec<_>>();
            (page.total, rows)
        };
        let selected_columns = columns
            .iter()
            .map(|column| column.key())
            .collect::<Vec<_>>();
        let mut args_payload = serde_json::json!({
            "from": "docs",
            "columns": selected_columns,
            "rows": rows,
            "total": total,
            "limit": limit,
            "offset": args.offset,
        });
        if args.explain {
            args_payload["explain"] = serde_json::json!({
                "adapter": physical_plan.adapter.label(),
                "stages": physical_plan.filter_stages,
            });
        }
        return Ok(CommandResult {
            command: "query.run".to_string(),
            summary: "query run completed".to_string(),
            args: args_payload,
        });
    }

    if let Some(base_id_or_path) = from.strip_prefix("base:") {
        let view_name = args
            .view_name
            .clone()
            .ok_or_else(|| anyhow!("query base scope requires --view-name"))?;
        let logical_plan = LogicalPlanBuilder
            .build(LogicalQueryPlanRequest {
                from: from.to_string(),
                query: args.query.clone(),
                where_expr: where_expr.clone(),
                sort_keys: sort_keys.clone(),
                projection: Vec::new(),
                limit: u64::from(limit),
                offset: u64::from(args.offset),
                execute: !args.explain || args.execute,
            })
            .map_err(|source| anyhow!("build logical query plan failed: {source}"))?;
        let physical_plan = PhysicalPlanOptimizer.optimize(
            PhysicalPlanBuilder
                .build(&logical_plan)
                .map_err(|source| anyhow!("build physical query plan failed: {source}"))?,
        );
        if args.explain && !args.execute {
            return Ok(CommandResult {
                command: "query.run".to_string(),
                summary: "query explain completed".to_string(),
                args: serde_json::json!({
                    "from": from,
                    "logical_plan": {
                        "scope": logical_plan.scope.label(),
                        "has_where": logical_plan.where_expr.is_some(),
                        "sort_keys": logical_plan.sort_keys.iter().map(|sort| {
                            serde_json::json!({
                                "field": sort.field,
                                "direction": match sort.direction {
                                    tao_sdk_search::SortDirection::Asc => "asc",
                                    tao_sdk_search::SortDirection::Desc => "desc",
                                },
                                "null_order": match sort.null_order {
                                    tao_sdk_search::NullOrder::First => "first",
                                    tao_sdk_search::NullOrder::Last => "last",
                                },
                            })
                        }).collect::<Vec<_>>(),
                        "limit": logical_plan.limit,
                        "offset": logical_plan.offset,
                        "execute": logical_plan.execute,
                    },
                    "physical_plan": {
                        "adapter": physical_plan.adapter.label(),
                        "stages": physical_plan.filter_stages,
                        "limit": physical_plan.limit,
                        "offset": physical_plan.offset,
                        "execute": physical_plan.execute,
                    }
                }),
            });
        }

        let fast_page_size = args.offset.saturating_add(limit);
        let (base_id, file_path, view_name, total, rows) = if where_expr.is_none()
            && sort_keys.is_empty()
        {
            let result = handle_base(
                BaseCommands::View(BaseViewArgs {
                    vault_root: args.vault_root.clone(),
                    db_path: args.db_path.clone(),
                    path_or_id: base_id_or_path.to_string(),
                    view_name: view_name.clone(),
                    page: 1,
                    page_size: fast_page_size.max(1),
                }),
                runtime,
            )?;
            let base_id = result
                .args
                .get("base_id")
                .cloned()
                .unwrap_or_else(|| JsonValue::String(base_id_or_path.to_string()));
            let file_path = result
                .args
                .get("file_path")
                .cloned()
                .unwrap_or(JsonValue::Null);
            let view_name = result
                .args
                .get("view_name")
                .cloned()
                .unwrap_or_else(|| JsonValue::String(view_name.clone()));
            let total = result
                .args
                .get("total")
                .and_then(JsonValue::as_u64)
                .unwrap_or(0);
            let rows = result
                .args
                .get("rows")
                .and_then(JsonValue::as_array)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .skip(args.offset as usize)
                .take(limit as usize)
                .collect::<Vec<_>>();
            (base_id, file_path, view_name, total, rows)
        } else {
            const QUERY_BASE_PAGE_SIZE: u32 = 512;
            let mut accumulator = QueryPostFilterAccumulator::new(args.offset, limit, &sort_keys);
            let mut page = 1_u32;

            loop {
                let result = handle_base(
                    BaseCommands::View(BaseViewArgs {
                        vault_root: args.vault_root.clone(),
                        db_path: args.db_path.clone(),
                        path_or_id: base_id_or_path.to_string(),
                        view_name: view_name.clone(),
                        page,
                        page_size: QUERY_BASE_PAGE_SIZE,
                    }),
                    runtime,
                )?;
                let batch_rows = result
                    .args
                    .get("rows")
                    .and_then(JsonValue::as_array)
                    .cloned()
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(|row| row.as_object().cloned())
                    .map(flatten_base_query_row)
                    .collect::<Vec<_>>();
                let filtered = apply_post_filter_batch(batch_rows, where_expr.as_ref())?;
                accumulator.push_batch(filtered);

                let has_more = result
                    .args
                    .get("has_more")
                    .and_then(JsonValue::as_bool)
                    .unwrap_or(false);
                if !has_more {
                    let (total, rows) = accumulator.finish();
                    let rows = rows
                        .into_iter()
                        .filter_map(|row| row.as_object().cloned())
                        .map(|mut row| {
                            let file_id = row.remove("file_id").unwrap_or(JsonValue::Null);
                            let file_path = row.remove("path").unwrap_or(JsonValue::Null);
                            serde_json::json!({
                                "file_id": file_id,
                                "file_path": file_path,
                                "values": row,
                            })
                        })
                        .collect::<Vec<_>>();
                    break (
                        result
                            .args
                            .get("base_id")
                            .cloned()
                            .unwrap_or_else(|| JsonValue::String(base_id_or_path.to_string())),
                        result
                            .args
                            .get("file_path")
                            .cloned()
                            .unwrap_or(JsonValue::Null),
                        result
                            .args
                            .get("view_name")
                            .cloned()
                            .unwrap_or_else(|| JsonValue::String(view_name.clone())),
                        total,
                        rows,
                    );
                }
                page = page.saturating_add(1);
            }
        };

        let mut args_payload = serde_json::json!({
            "from": from,
            "base_id": base_id,
            "file_path": file_path,
            "view_name": view_name,
            "rows": rows,
            "total": total,
            "limit": limit,
            "offset": args.offset,
        });
        if args.explain {
            args_payload["explain"] = serde_json::json!({
                "adapter": physical_plan.adapter.label(),
                "stages": physical_plan.filter_stages,
            });
        }
        return Ok(CommandResult {
            command: "query.run".to_string(),
            summary: "query run completed".to_string(),
            args: args_payload,
        });
    }

    if from.eq_ignore_ascii_case("graph") {
        let graph_result = if let Some(path) = &args.path {
            let resolved = args.resolve()?;
            let panels = with_kernel(runtime, &resolved, |kernel| {
                expect_bridge_value(kernel.note_links(path), "query.graph")
            })?;
            let outgoing = panels
                .outgoing
                .iter()
                .map(|link| {
                    serde_json::json!({
                        "direction": "outgoing",
                        "source_path": link.source_path,
                        "target_path": link.target_path,
                        "heading": link.heading,
                        "block_id": link.block_id,
                        "display_text": link.display_text,
                        "kind": link.kind,
                        "resolved": link.resolved,
                    })
                })
                .collect::<Vec<_>>();
            let backlinks = panels
                .backlinks
                .iter()
                .map(|link| {
                    serde_json::json!({
                        "direction": "backlinks",
                        "source_path": link.source_path,
                        "target_path": link.target_path,
                        "heading": link.heading,
                        "block_id": link.block_id,
                        "display_text": link.display_text,
                        "kind": link.kind,
                        "resolved": link.resolved,
                    })
                })
                .collect::<Vec<_>>();
            let mut items = outgoing.clone();
            items.extend(backlinks.clone());
            CommandResult {
                command: "graph.links".to_string(),
                summary: "graph links completed".to_string(),
                args: serde_json::json!({
                    "path": path,
                    "outgoing_total": outgoing.len(),
                    "backlinks_total": backlinks.len(),
                    "total": outgoing.len() + backlinks.len(),
                    "outgoing": outgoing,
                    "backlinks": backlinks,
                    "items": items,
                }),
            }
        } else {
            handle_graph(
                GraphCommands::Unresolved(GraphWindowArgs {
                    vault_root: args.vault_root.clone(),
                    db_path: args.db_path.clone(),
                    limit: args.limit,
                    offset: args.offset,
                }),
                runtime,
            )?
        };
        return Ok(retag_result(
            graph_result,
            "query.run",
            "query run completed",
        ));
    }

    if from.eq_ignore_ascii_case("task") {
        let task_result = handle_task(
            TaskCommands::List(TaskListArgs {
                vault_root: args.vault_root.clone(),
                db_path: args.db_path.clone(),
                state: None,
                query: args.query.clone(),
                limit: args.limit,
                offset: args.offset,
            }),
            false,
            runtime,
        )?;
        return Ok(retag_result(
            task_result,
            "query.run",
            "query run completed",
        ));
    }

    if from.eq_ignore_ascii_case("meta:tags") {
        let result = handle_meta(
            MetaCommands::Tags(GraphWindowArgs {
                vault_root: args.vault_root.clone(),
                db_path: args.db_path.clone(),
                limit: args.limit,
                offset: args.offset,
            }),
            runtime,
        )?;
        return Ok(retag_result(result, "query.run", "query run completed"));
    }

    if from.eq_ignore_ascii_case("meta:aliases") {
        let result = handle_meta(
            MetaCommands::Aliases(GraphWindowArgs {
                vault_root: args.vault_root.clone(),
                db_path: args.db_path.clone(),
                limit: args.limit,
                offset: args.offset,
            }),
            runtime,
        )?;
        return Ok(retag_result(result, "query.run", "query run completed"));
    }

    if from.eq_ignore_ascii_case("meta:properties") {
        let result = handle_meta(
            MetaCommands::Properties(GraphWindowArgs {
                vault_root: args.vault_root,
                db_path: args.db_path,
                limit: args.limit,
                offset: args.offset,
            }),
            runtime,
        )?;
        return Ok(retag_result(result, "query.run", "query run completed"));
    }

    Err(anyhow!(
        "unsupported query scope '{}'; supported scopes: docs, graph, task, meta:tags, meta:aliases, meta:properties, base:<id-or-path>",
        from
    ))
}

fn handle_vault(command: VaultCommands, runtime: &mut RuntimeMode) -> Result<CommandResult> {
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
            let snapshot = with_connection(runtime, &resolved, |connection| {
                Ok(HealthSnapshotService.snapshot(
                    Path::new(&resolved.vault_root),
                    connection,
                    0,
                    WatcherStatus::Stopped,
                )?)
            })
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
            let (reconcile, totals) = with_connection(runtime, &resolved, |connection| {
                let reconcile = WatchReconcileService::default()
                    .reconcile_once(
                        Path::new(&resolved.vault_root),
                        connection,
                        resolved.case_policy,
                    )
                    .map_err(|source| anyhow!("vault reindex failed: {source}"))?;
                let totals = query_index_totals(connection)
                    .map_err(|source| anyhow!("vault reindex total query failed: {source}"))?;
                Ok((reconcile, totals))
            })?;
            Ok(CommandResult {
                command: "vault.reindex".to_string(),
                summary: "vault reindex completed".to_string(),
                args: serde_json::json!({
                    "indexed_files": totals.indexed_files,
                    "markdown_files": totals.markdown_files,
                    "links_total": totals.links_total,
                    "unresolved_links": totals.unresolved_links,
                    "properties_total": totals.properties_total,
                    "bases_total": totals.bases_total,
                    "drift_paths": reconcile.drift_paths,
                    "batches_applied": reconcile.batches_applied,
                    "upserted_files": reconcile.upserted_files,
                    "removed_files": reconcile.removed_files,
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

fn maybe_forward_to_daemon(cli: &Cli) -> Result<Option<String>> {
    if is_daemon_control_command(&cli.command) {
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

fn resolve_daemon_socket_for_cli(cli: &Cli) -> Result<Option<String>> {
    if let Some(socket) = cli.daemon_socket.as_ref() {
        return Ok(Some(socket.clone()));
    }
    let Some(vault) = resolve_command_vault_paths(&cli.command)? else {
        return Ok(None);
    };
    Ok(Some(derive_daemon_socket_for_vault(&vault.vault_root)?))
}

fn resolve_command_vault_paths(command: &Commands) -> Result<Option<ResolvedVaultPathArgs>> {
    let resolved = match command {
        Commands::Doc { command } => match command {
            DocCommands::Read(args) => args.resolve()?,
            DocCommands::Write(args) => args.resolve()?,
            DocCommands::List(args) => args.resolve()?,
        },
        Commands::Base { command } => match command {
            BaseCommands::List(args) => args.resolve()?,
            BaseCommands::View(args) => args.resolve()?,
            BaseCommands::Schema(args) => args.resolve()?,
        },
        Commands::Graph { command } => match command {
            GraphCommands::Outgoing(args) => args.resolve()?,
            GraphCommands::Backlinks(args) => args.resolve()?,
            GraphCommands::Unresolved(args) => args.resolve()?,
            GraphCommands::Deadends(args) => args.resolve()?,
            GraphCommands::Orphans(args) => args.resolve()?,
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

fn is_daemon_control_command(command: &Commands) -> bool {
    matches!(
        command,
        Commands::Vault {
            command: VaultCommands::Daemon { .. } | VaultCommands::DaemonServe(_)
        }
    )
}

fn daemon_cache_key(command: &Commands) -> Result<String> {
    serde_json::to_string(command).context("serialize command cache key")
}

fn command_is_cacheable(command: &Commands) -> bool {
    match command {
        Commands::Doc { command } => matches!(command, DocCommands::Read(_) | DocCommands::List(_)),
        Commands::Base { .. } => true,
        Commands::Graph { .. } => true,
        Commands::Meta { .. } => true,
        Commands::Task { command } => matches!(command, TaskCommands::List(_)),
        Commands::Query(_) => true,
        Commands::Vault { command } => matches!(
            command,
            VaultCommands::Stats(_) | VaultCommands::Preflight(_)
        ),
    }
}

fn maybe_refresh_daemon_state(command: &Commands, runtime: &mut RuntimeMode) -> Result<bool> {
    let RuntimeMode::Daemon(_) = runtime else {
        return Ok(false);
    };
    if !command_is_cacheable(command) {
        return Ok(false);
    }

    let Some(resolved) = resolve_command_vault_paths(command)? else {
        return Ok(false);
    };
    let runtime_key = runtime_cache_key(&resolved);
    let current_generation = if let RuntimeMode::Daemon(cache) = runtime {
        if !cache.change_monitors.contains_key(&runtime_key) {
            let monitor =
                VaultChangeMonitor::start(Path::new(&resolved.vault_root)).with_context(|| {
                    format!(
                        "start daemon filesystem monitor for vault '{}'",
                        resolved.vault_root
                    )
                })?;
            cache.change_monitors.insert(runtime_key.clone(), monitor);
        }
        cache
            .change_monitors
            .get(&runtime_key)
            .map(VaultChangeMonitor::generation)
            .unwrap_or(0)
    } else {
        0
    };

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

fn clear_cached_results_for_runtime(cache: &mut RuntimeCache, runtime_key: &str) {
    cache
        .command_results
        .retain(|_, entry| entry.runtime_key != runtime_key);
}

fn resolve_daemon_socket(
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

fn derive_daemon_socket_for_vault(vault_root: &str) -> Result<String> {
    let socket_dir = default_daemon_socket_dir()?;
    let hash = blake3::hash(vault_root.as_bytes()).to_hex().to_string();
    let file_name = format!("vault-{}.sock", &hash[..16]);
    Ok(socket_dir.join(file_name).to_string_lossy().to_string())
}

fn default_daemon_socket_dir() -> Result<PathBuf> {
    if let Some(home) = std::env::var_os("HOME") {
        return Ok(PathBuf::from(home).join(DEFAULT_DAEMON_SOCKET_DIR));
    }
    let cwd = std::env::current_dir().context("resolve cwd for daemon socket dir fallback")?;
    Ok(cwd.join(".tao/daemons"))
}

fn ensure_daemon_running(socket: &str, startup_timeout_ms: u64) -> Result<Option<u32>> {
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

fn handle_daemon(command: DaemonCommands) -> Result<CommandResult> {
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

fn handle_daemon_stop_all(args: DaemonStopAllArgs) -> Result<CommandResult> {
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

fn list_managed_daemon_sockets(socket_dir: &Path) -> Result<Vec<PathBuf>> {
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

fn daemon_status_probe(socket: &str) -> Result<Option<DaemonStatus>> {
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
fn daemon_socket_state_label(socket: &str) -> &'static str {
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
fn daemon_socket_state_label(_socket: &str) -> &'static str {
    "stopped"
}

fn daemon_socket_is_unavailable(error: &anyhow::Error) -> bool {
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

fn wait_for_daemon_startup(socket: &str, timeout_ms: u64) -> Result<()> {
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

fn daemon_request(socket: &str, request: &DaemonRequest) -> Result<DaemonResponse> {
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

fn run_daemon_server(socket: &str) -> Result<()> {
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

        while !should_shutdown {
            let (mut stream, _) = listener.accept().context("accept daemon request stream")?;
            let mut request_bytes = Vec::new();
            stream
                .read_to_end(&mut request_bytes)
                .context("read daemon request payload")?;
            let request = serde_json::from_slice::<DaemonRequest>(&request_bytes)
                .context("parse daemon request payload")?;

            let response = match request {
                DaemonRequest::Execute { payload } => {
                    let cacheable = command_is_cacheable(&payload.command);
                    maybe_refresh_daemon_state(&payload.command, &mut runtime)?;
                    let resolved = resolve_command_vault_paths(&payload.command)?;
                    let runtime_key = resolved.as_ref().map(runtime_cache_key);
                    let cache_key = if cacheable {
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
                            if let RuntimeMode::Daemon(cache) = &mut runtime {
                                if let Some(key) = cache_key {
                                    let cache_runtime_key = runtime_key
                                        .clone()
                                        .unwrap_or_else(|| "<global>".to_string());
                                    cache.command_results.insert(
                                        key,
                                        CachedCommandResult {
                                            runtime_key: cache_runtime_key,
                                            result: result.clone(),
                                        },
                                    );
                                } else if let Some(runtime_key) = runtime_key.as_deref() {
                                    clear_cached_results_for_runtime(cache, runtime_key);
                                } else {
                                    cache.command_results.clear();
                                }
                            }

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
                                    Ok(None) => match render_output(payload.json, &result) {
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
                                match render_output(payload.json, &result) {
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
fn prepare_daemon_socket_path(socket: &str) -> Result<PathBuf> {
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

fn ensure_writes_enabled(allow_writes: bool, read_only: bool, command: &str) -> Result<()> {
    if allow_writes || !read_only {
        return Ok(());
    }
    Err(anyhow!(
        "{command} is disabled by default; pass --allow-writes or set [security].read_only=false to enable vault content mutations"
    ))
}

fn paginate_json_items(items: Vec<JsonValue>, limit: u32, offset: u32) -> Vec<JsonValue> {
    items
        .into_iter()
        .skip(offset as usize)
        .take(limit as usize)
        .collect()
}

fn link_edge_to_json(edge: tao_sdk_service::LinkGraphEdge) -> JsonValue {
    serde_json::json!({
        "link_id": edge.link_id,
        "source_file_id": edge.source_file_id,
        "source_path": edge.source_path,
        "raw_target": edge.raw_target,
        "resolved_file_id": edge.resolved_file_id,
        "resolved_path": edge.resolved_path,
        "heading_slug": edge.heading_slug,
        "block_id": edge.block_id,
        "is_unresolved": edge.is_unresolved,
        "unresolved_reason": edge.unresolved_reason,
        "source_field": edge.source_field,
    })
}

fn handle_meta_token_aggregate(
    args: GraphWindowArgs,
    property_key: &str,
    command: &str,
    runtime: &mut RuntimeMode,
) -> Result<CommandResult> {
    let resolved = args.resolve()?;
    let rows = with_connection(runtime, &resolved, |connection| {
        Ok(PropertiesRepository::list_by_key_with_paths(
            connection,
            property_key,
        )?)
    })
    .map_err(|source| anyhow!("query property key '{}' failed: {source}", property_key))?;
    let mut counts = HashMap::<String, usize>::new();
    for row in rows {
        for token in extract_property_tokens(&row.value_json) {
            *counts.entry(token).or_insert(0) += 1;
        }
    }
    let mut items = counts
        .into_iter()
        .map(|(token, total)| serde_json::json!({ "token": token, "total": total }))
        .collect::<Vec<_>>();
    items.sort_by(|left, right| {
        right["total"]
            .as_u64()
            .unwrap_or(0)
            .cmp(&left["total"].as_u64().unwrap_or(0))
            .then_with(|| {
                left["token"]
                    .as_str()
                    .unwrap_or_default()
                    .cmp(right["token"].as_str().unwrap_or_default())
            })
    });
    let total = items.len();
    let items = paginate_json_items(items, args.limit, args.offset);
    Ok(CommandResult {
        command: command.to_string(),
        summary: format!("{command} completed"),
        args: serde_json::json!({
            "total": total,
            "limit": args.limit,
            "offset": args.offset,
            "items": items,
        }),
    })
}

fn extract_property_tokens(value_json: &str) -> Vec<String> {
    let parsed = serde_json::from_str::<JsonValue>(value_json)
        .unwrap_or_else(|_| JsonValue::String(value_json.to_string()));
    let mut tokens = Vec::new();
    collect_json_string_tokens(&parsed, &mut tokens);
    let mut deduped = Vec::new();
    let mut seen = HashSet::<String>::new();
    for token in tokens {
        let key = token.to_ascii_lowercase();
        if seen.insert(key) {
            deduped.push(token);
        }
    }
    deduped
}

fn collect_json_string_tokens(value: &JsonValue, out: &mut Vec<String>) {
    match value {
        JsonValue::String(raw) => {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                return;
            }
            for token in trimmed
                .split([',', ' '])
                .map(str::trim)
                .filter(|token| !token.is_empty())
            {
                out.push(token.trim_start_matches('#').to_string());
            }
        }
        JsonValue::Array(values) => {
            for item in values {
                collect_json_string_tokens(item, out);
            }
        }
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::Object(_) => {}
    }
}

fn update_task_line_state(line: &str, state: &str) -> Result<String> {
    let trimmed = line.trim_start();
    let indent_len = line.len() - trimmed.len();
    let indent = &line[..indent_len];
    let content = if let Some(rest) = trimmed.strip_prefix("- [ ] ") {
        rest
    } else if let Some(rest) = trimmed
        .strip_prefix("- [x] ")
        .or_else(|| trimmed.strip_prefix("- [X] "))
    {
        rest
    } else if let Some(rest) = trimmed.strip_prefix("- [-] ") {
        rest
    } else {
        return Err(anyhow!("line does not contain a markdown checkbox task"));
    };
    let marker = match state.to_ascii_lowercase().as_str() {
        "open" => "[ ]",
        "done" => "[x]",
        "cancelled" => "[-]",
        _ => return Err(anyhow!("unsupported task state '{}'", state)),
    };
    Ok(format!("{indent}- {marker} {content}"))
}

fn resolve_existing_vault_note_path(
    resolved: &ResolvedVaultPathArgs,
    path: &str,
) -> Result<PathBuf> {
    validate_relative_vault_path(path).map_err(|source| anyhow!(source.to_string()))?;
    let canonicalizer =
        PathCanonicalizationService::new(&resolved.vault_root, resolved.case_policy)
            .map_err(|source| anyhow!("create vault canonicalizer failed: {source}"))?;
    canonicalizer
        .canonicalize(Path::new(path))
        .map(|canonical| canonical.absolute)
        .map_err(|source| anyhow!("canonicalize vault note path failed: {source}"))
}

fn runtime_cache_key(args: &ResolvedVaultPathArgs) -> String {
    format!(
        "{}\u{1f}{}\u{1f}{}\u{1f}{:?}\u{1f}{}",
        args.vault_root, args.data_dir, args.db_path, args.case_policy, args.read_only
    )
}

fn with_connection<T>(
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

fn with_kernel<T>(
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

fn resolve_vault_paths(
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

fn open_bridge_kernel(args: &ResolvedVaultPathArgs) -> Result<BridgeKernel> {
    ensure_runtime_paths_for_args(args)?;
    BridgeKernel::open_with_case_policy(&args.vault_root, &args.db_path, args.case_policy)
        .map_err(|source| anyhow!("open bridge kernel failed: {source}"))
}

fn expect_bridge_value<T>(envelope: BridgeEnvelope<T>, command: &str) -> Result<T> {
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

fn decode_base_document(config_json: &str) -> Result<BaseDocument> {
    if let Ok(document) = serde_json::from_str::<BaseDocument>(config_json) {
        return Ok(document);
    }

    let raw_value = serde_json::from_str::<JsonValue>(config_json)
        .map_err(|source| anyhow!("parse base config json failed: {source}"))?;
    let Some(raw_yaml) = raw_value.get("raw").and_then(JsonValue::as_str) else {
        return Err(anyhow!(
            "base config json is not a supported document payload"
        ));
    };
    parse_base_document(raw_yaml).map_err(|source| anyhow!("parse base yaml failed: {source}"))
}

fn open_initialized_connection(args: &ResolvedVaultPathArgs) -> Result<Connection> {
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

fn ensure_runtime_paths_for_args(args: &ResolvedVaultPathArgs) -> Result<()> {
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
struct IndexTotals {
    indexed_files: u64,
    markdown_files: u64,
    links_total: u64,
    unresolved_links: u64,
    properties_total: u64,
    bases_total: u64,
}

fn query_index_totals(connection: &Connection) -> Result<IndexTotals> {
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

#[cfg(test)]
mod tests {
    use std::env;
    use std::fs;
    #[cfg(unix)]
    use std::os::unix::net::UnixListener;
    use std::path::Path;
    use std::sync::{Mutex, OnceLock};

    use super::{
        Cli, Commands, DaemonCommands, DaemonSocketArgs, DaemonStopAllArgs, DocCommands,
        NotePutArgs, QueryArgs, RuntimeCache, RuntimeMode, VaultPathArgs, command_is_cacheable,
        derive_daemon_socket_for_vault, dispatch, dispatch_with_runtime, handle_daemon,
        maybe_forward_to_daemon, maybe_refresh_daemon_state, maybe_render_streaming_output,
        prepare_daemon_socket_path, render_error_output, render_output,
        resolve_command_vault_paths, resolve_daemon_socket_for_cli, runtime_cache_key,
    };
    use clap::{CommandFactory, Parser};
    use serde_json::Value as JsonValue;

    #[test]
    fn cli_help_contains_grouped_command_names() {
        let mut command = Cli::command();
        let mut output = Vec::new();
        command
            .write_long_help(&mut output)
            .expect("render long help");
        let rendered = String::from_utf8(output).expect("utf8 help");

        assert!(rendered.contains("vault"));
        assert!(rendered.contains("doc"));
        assert!(rendered.contains("base"));
        assert!(rendered.contains("graph"));
        assert!(rendered.contains("meta"));
        assert!(rendered.contains("task"));
        assert!(rendered.contains("query"));
        assert!(!rendered.contains("note"));
        assert!(!rendered.contains("links"));
        assert!(!rendered.contains("properties"));
        assert!(!rendered.contains("bases"));
        assert!(!rendered.contains("search"));
        assert!(!rendered.contains("hubs"));
    }

    #[test]
    fn json_output_is_one_envelope_object() {
        with_temp_cwd(|| {
            let tempdir = tempfile::tempdir().expect("create tempdir");
            let vault_root = tempdir.path().to_path_buf();
            let cli = Cli::parse_from([
                "tao".to_string(),
                "--json".to_string(),
                "vault".to_string(),
                "open".to_string(),
                "--vault-root".to_string(),
                vault_root.to_string_lossy().to_string(),
            ]);
            let result = dispatch(cli.command, cli.allow_writes).expect("dispatch");
            let output = render_output(cli.json, &result).expect("render output");
            let value: serde_json::Value = serde_json::from_str(&output).expect("parse output");

            assert_eq!(
                value.get("ok").and_then(serde_json::Value::as_bool),
                Some(true)
            );
            assert_eq!(
                value
                    .get("value")
                    .and_then(|raw| raw.get("command"))
                    .and_then(serde_json::Value::as_str),
                Some("vault.open")
            );
            assert!(value.get("error").is_some_and(serde_json::Value::is_null));
        });
    }

    #[test]
    fn json_contract_is_stable_for_all_grouped_json_commands() {
        with_temp_cwd(|| {
            let tempdir = tempfile::tempdir().expect("create tempdir");
            let vault_root = tempdir.path().join("vault");
            let notes_dir = vault_root.join("notes");
            let projects_dir = notes_dir.join("projects");
            let views_dir = vault_root.join("views");

            fs::create_dir_all(&projects_dir).expect("create projects dir");
            fs::create_dir_all(&views_dir).expect("create views dir");
            fs::write(
                projects_dir.join("project-a.md"),
                "---\nstatus: active\npriority: 4\ntags: [work, active]\naliases: [\"Project Alpha\"]\n---\n# Project A\n",
            )
            .expect("write project-a note");
            fs::write(
                projects_dir.join("project-b.md"),
                "---\nstatus: paused\npriority: 2\n---\n# Project B\n",
            )
            .expect("write project-b note");
            fs::write(notes_dir.join("alpha.md"), "# Alpha\n[[project-a]]\n")
                .expect("write alpha note");
            fs::write(notes_dir.join("tasks.md"), "- [ ] ship tao cli\n")
                .expect("write tasks note");
            fs::write(
                views_dir.join("projects.base"),
                "views:\n  - name: ActiveProjects\n    type: table\n    source: notes/projects\n    filters:\n      - key: status\n        op: eq\n        value: active\n    sorts:\n      - key: priority\n        direction: desc\n    columns:\n      - title\n      - status\n      - priority\n",
            )
            .expect("write projects base");

            let vault_root_string = vault_root.to_string_lossy().to_string();

            let scenarios = [
                (
                    "vault.open",
                    vec![
                        "tao",
                        "--json",
                        "vault",
                        "open",
                        "--vault-root",
                        &vault_root_string,
                    ],
                ),
                (
                    "vault.stats",
                    vec![
                        "tao",
                        "--json",
                        "vault",
                        "stats",
                        "--vault-root",
                        &vault_root_string,
                    ],
                ),
                (
                    "vault.preflight",
                    vec![
                        "tao",
                        "--json",
                        "vault",
                        "preflight",
                        "--vault-root",
                        &vault_root_string,
                    ],
                ),
                (
                    "vault.reindex",
                    vec![
                        "tao",
                        "--json",
                        "vault",
                        "reindex",
                        "--vault-root",
                        &vault_root_string,
                    ],
                ),
                (
                    "doc.read",
                    vec![
                        "tao",
                        "--json",
                        "doc",
                        "read",
                        "--vault-root",
                        &vault_root_string,
                        "--path",
                        "notes/alpha.md",
                    ],
                ),
                (
                    "doc.list",
                    vec![
                        "tao",
                        "--json",
                        "doc",
                        "list",
                        "--vault-root",
                        &vault_root_string,
                    ],
                ),
                (
                    "doc.write",
                    vec![
                        "tao",
                        "--json",
                        "--allow-writes",
                        "doc",
                        "write",
                        "--vault-root",
                        &vault_root_string,
                        "--path",
                        "notes/new.md",
                        "--content",
                        "# New\nbody",
                    ],
                ),
                (
                    "graph.outgoing",
                    vec![
                        "tao",
                        "--json",
                        "graph",
                        "outgoing",
                        "--vault-root",
                        &vault_root_string,
                        "--path",
                        "notes/alpha.md",
                    ],
                ),
                (
                    "graph.backlinks",
                    vec![
                        "tao",
                        "--json",
                        "graph",
                        "backlinks",
                        "--vault-root",
                        &vault_root_string,
                        "--path",
                        "notes/projects/project-a.md",
                    ],
                ),
                (
                    "graph.unresolved",
                    vec![
                        "tao",
                        "--json",
                        "graph",
                        "unresolved",
                        "--vault-root",
                        &vault_root_string,
                    ],
                ),
                (
                    "graph.deadends",
                    vec![
                        "tao",
                        "--json",
                        "graph",
                        "deadends",
                        "--vault-root",
                        &vault_root_string,
                    ],
                ),
                (
                    "graph.orphans",
                    vec![
                        "tao",
                        "--json",
                        "graph",
                        "orphans",
                        "--vault-root",
                        &vault_root_string,
                    ],
                ),
                (
                    "graph.components",
                    vec![
                        "tao",
                        "--json",
                        "graph",
                        "components",
                        "--vault-root",
                        &vault_root_string,
                    ],
                ),
                (
                    "graph.neighbors",
                    vec![
                        "tao",
                        "--json",
                        "graph",
                        "neighbors",
                        "--vault-root",
                        &vault_root_string,
                        "--path",
                        "notes/alpha.md",
                    ],
                ),
                (
                    "graph.path",
                    vec![
                        "tao",
                        "--json",
                        "graph",
                        "path",
                        "--vault-root",
                        &vault_root_string,
                        "--from",
                        "notes/alpha.md",
                        "--to",
                        "notes/projects/project-a.md",
                    ],
                ),
                (
                    "graph.walk",
                    vec![
                        "tao",
                        "--json",
                        "graph",
                        "walk",
                        "--vault-root",
                        &vault_root_string,
                        "--path",
                        "notes/alpha.md",
                        "--depth",
                        "2",
                        "--limit",
                        "20",
                    ],
                ),
                (
                    "base.list",
                    vec![
                        "tao",
                        "--json",
                        "base",
                        "list",
                        "--vault-root",
                        &vault_root_string,
                    ],
                ),
                (
                    "base.schema",
                    vec![
                        "tao",
                        "--json",
                        "base",
                        "schema",
                        "--vault-root",
                        &vault_root_string,
                        "--path-or-id",
                        "views/projects.base",
                    ],
                ),
                (
                    "base.view",
                    vec![
                        "tao",
                        "--json",
                        "base",
                        "view",
                        "--vault-root",
                        &vault_root_string,
                        "--path-or-id",
                        "views/projects.base",
                        "--view-name",
                        "ActiveProjects",
                        "--page",
                        "1",
                        "--page-size",
                        "10",
                    ],
                ),
                (
                    "meta.properties",
                    vec![
                        "tao",
                        "--json",
                        "meta",
                        "properties",
                        "--vault-root",
                        &vault_root_string,
                    ],
                ),
                (
                    "meta.tags",
                    vec![
                        "tao",
                        "--json",
                        "meta",
                        "tags",
                        "--vault-root",
                        &vault_root_string,
                    ],
                ),
                (
                    "meta.aliases",
                    vec![
                        "tao",
                        "--json",
                        "meta",
                        "aliases",
                        "--vault-root",
                        &vault_root_string,
                    ],
                ),
                (
                    "meta.tasks",
                    vec![
                        "tao",
                        "--json",
                        "meta",
                        "tasks",
                        "--vault-root",
                        &vault_root_string,
                    ],
                ),
                (
                    "task.list",
                    vec![
                        "tao",
                        "--json",
                        "task",
                        "list",
                        "--vault-root",
                        &vault_root_string,
                    ],
                ),
                (
                    "task.set-state",
                    vec![
                        "tao",
                        "--json",
                        "--allow-writes",
                        "task",
                        "set-state",
                        "--vault-root",
                        &vault_root_string,
                        "--path",
                        "notes/tasks.md",
                        "--line",
                        "1",
                        "--state",
                        "done",
                    ],
                ),
                (
                    "query.run",
                    vec![
                        "tao",
                        "--json",
                        "query",
                        "--vault-root",
                        &vault_root_string,
                        "--from",
                        "docs",
                        "--query",
                        "project",
                        "--limit",
                        "10",
                        "--offset",
                        "0",
                    ],
                ),
                (
                    "vault.reconcile",
                    vec![
                        "tao",
                        "--json",
                        "vault",
                        "reconcile",
                        "--vault-root",
                        &vault_root_string,
                    ],
                ),
            ];

            for (expected_command, args) in scenarios {
                let cli = Cli::parse_from(args);
                let result = dispatch(cli.command, cli.allow_writes)
                    .expect("dispatch json contract scenario");
                let output = render_output(cli.json, &result).expect("render json output");
                let envelope: JsonValue = serde_json::from_str(&output).expect("parse json output");
                assert_json_contract(&envelope, expected_command);
            }
        });
    }

    fn assert_json_contract(value: &JsonValue, expected_command: &str) {
        let envelope = value.as_object().expect("envelope must be object");
        assert_eq!(envelope.len(), 3);
        assert!(envelope.contains_key("ok"));
        assert!(envelope.contains_key("value"));
        assert!(envelope.contains_key("error"));
        assert_eq!(
            envelope.get("ok").and_then(JsonValue::as_bool),
            Some(true),
            "expected ok=true for command {expected_command}",
        );
        assert!(envelope.get("error").is_some_and(JsonValue::is_null));

        let payload = envelope
            .get("value")
            .and_then(JsonValue::as_object)
            .expect("value payload must be object");
        assert_eq!(payload.len(), 3);
        assert_eq!(
            payload.get("command").and_then(JsonValue::as_str),
            Some(expected_command)
        );
        assert!(payload.get("summary").is_some_and(JsonValue::is_string));
        assert!(payload.get("args").is_some_and(JsonValue::is_object));
    }

    #[test]
    fn json_error_envelope_uses_stable_write_disabled_code() {
        let cli = Cli::parse_from([
            "tao",
            "--json",
            "doc",
            "write",
            "--vault-root",
            "/tmp",
            "--path",
            "notes/test.md",
            "--content",
            "# test",
        ]);
        let error = dispatch(cli.command, cli.allow_writes).expect_err("write must fail");
        let output = render_error_output(&error).expect("render error output");
        let envelope: JsonValue = serde_json::from_str(&output).expect("parse output");
        assert_eq!(envelope.get("ok").and_then(JsonValue::as_bool), Some(false));
        assert!(envelope.get("value").is_some_and(JsonValue::is_null));
        let error_payload = envelope
            .get("error")
            .and_then(JsonValue::as_object)
            .expect("error object");
        assert_eq!(
            error_payload.get("code").and_then(JsonValue::as_str),
            Some("write_disabled")
        );
        assert!(
            error_payload
                .get("message")
                .and_then(JsonValue::as_str)
                .is_some_and(|message| !message.contains("panicked at")),
            "error message should not include stack traces"
        );
    }

    #[test]
    fn json_error_envelope_uses_stable_query_parse_error_code() {
        let cli = Cli::parse_from([
            "tao",
            "--json",
            "query",
            "--vault-root",
            "/tmp",
            "--from",
            "docs",
            "--query",
            "project",
            "--where",
            "title = 'alpha'",
        ]);
        let error = dispatch(cli.command, cli.allow_writes).expect_err("parse must fail");
        let output = render_error_output(&error).expect("render error output");
        let envelope: JsonValue = serde_json::from_str(&output).expect("parse output");
        let error_payload = envelope
            .get("error")
            .and_then(JsonValue::as_object)
            .expect("error object");
        assert_eq!(
            error_payload.get("code").and_then(JsonValue::as_str),
            Some("query_parse_error")
        );
        assert!(error_payload.get("hint").is_some_and(JsonValue::is_string));
    }

    #[test]
    fn write_commands_are_blocked_without_allow_writes_flag() {
        with_temp_cwd(|| {
            let tempdir = tempfile::tempdir().expect("create tempdir");
            let vault_root = tempdir.path().join("vault");
            let notes_dir = vault_root.join("notes");
            fs::create_dir_all(&vault_root).expect("create vault dir");
            fs::create_dir_all(&notes_dir).expect("create notes dir");
            fs::write(notes_dir.join("tasks.md"), "- [ ] blocked task\n")
                .expect("write task fixture");

            let doc_write = Cli::parse_from([
                "tao",
                "--json",
                "doc",
                "write",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
                "--path",
                "notes/blocked.md",
                "--content",
                "# blocked",
            ]);
            let doc_write_error = dispatch(doc_write.command, doc_write.allow_writes)
                .expect_err("doc.write should require --allow-writes");
            assert!(doc_write_error.to_string().contains("--allow-writes"));

            let task_set_state = Cli::parse_from([
                "tao",
                "--json",
                "task",
                "set-state",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
                "--path",
                "notes/tasks.md",
                "--line",
                "1",
                "--state",
                "done",
            ]);
            let task_error = dispatch(task_set_state.command, task_set_state.allow_writes)
                .expect_err("task.set-state should require --allow-writes");
            assert!(task_error.to_string().contains("--allow-writes"));
        });
    }

    #[test]
    fn task_set_state_rejects_paths_outside_the_vault_boundary() {
        with_temp_cwd(|| {
            let tempdir = tempfile::tempdir().expect("create tempdir");
            let vault_root = tempdir.path().join("vault");
            let outside_path = tempdir.path().join("outside.md");
            fs::create_dir_all(vault_root.join("notes")).expect("create notes");
            fs::write(vault_root.join("notes/tasks.md"), "- [ ] inside task\n")
                .expect("write note");
            fs::write(&outside_path, "- [ ] outside task\n").expect("write outside");

            let absolute = Cli::parse_from([
                "tao",
                "--json",
                "--allow-writes",
                "task",
                "set-state",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
                "--path",
                outside_path.to_string_lossy().as_ref(),
                "--line",
                "1",
                "--state",
                "done",
            ]);
            let absolute_error = dispatch(absolute.command, absolute.allow_writes)
                .expect_err("absolute path should be rejected");
            assert!(absolute_error.to_string().contains("vault-relative"));
            assert_eq!(
                fs::read_to_string(&outside_path).expect("read outside after absolute attempt"),
                "- [ ] outside task\n"
            );

            let parent = Cli::parse_from([
                "tao",
                "--json",
                "--allow-writes",
                "task",
                "set-state",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
                "--path",
                "../outside.md",
                "--line",
                "1",
                "--state",
                "done",
            ]);
            let parent_error = dispatch(parent.command, parent.allow_writes)
                .expect_err("parent traversal should be rejected");
            assert!(parent_error.to_string().contains("traverse"));
            assert_eq!(
                fs::read_to_string(&outside_path).expect("read outside after traversal attempt"),
                "- [ ] outside task\n"
            );
        });
    }

    #[test]
    fn vault_commands_use_configured_default_root_when_vault_root_arg_is_omitted() {
        with_temp_cwd(|| {
            let tempdir = tempfile::tempdir().expect("create tempdir");
            let vault_root = tempdir.path().join("vault");
            fs::create_dir_all(vault_root.join("notes")).expect("create notes");
            fs::write(vault_root.join("notes/a.md"), "# A\n").expect("write note");

            fs::write(
                Path::new("config.toml"),
                format!(
                    r#"[vault]
root = "{}"

[security]
read_only = true
"#,
                    vault_root.display()
                ),
            )
            .expect("write root config");

            let cli = Cli::parse_from(["tao", "--json", "vault", "stats"]);
            let result = dispatch(cli.command, cli.allow_writes).expect("dispatch");
            let output = render_output(cli.json, &result).expect("render output");
            let envelope: JsonValue = serde_json::from_str(&output).expect("parse output");
            let resolved_root = envelope
                .get("value")
                .and_then(|raw| raw.get("args"))
                .and_then(|raw| raw.get("vault_root"))
                .and_then(JsonValue::as_str)
                .expect("resolved vault root");

            assert_eq!(
                Path::new(resolved_root),
                fs::canonicalize(vault_root)
                    .expect("canonical vault")
                    .as_path()
            );
        });
    }

    #[test]
    fn write_commands_are_enabled_when_read_only_policy_is_disabled_in_config() {
        with_temp_cwd(|| {
            let tempdir = tempfile::tempdir().expect("create tempdir");
            let vault_root = tempdir.path().join("vault");
            fs::create_dir_all(vault_root.join("notes")).expect("create notes");
            fs::write(vault_root.join("notes/a.md"), "# A\n").expect("write note");

            fs::write(
                Path::new("config.toml"),
                format!(
                    r#"[vault]
root = "{}"

[security]
read_only = false
"#,
                    vault_root.display()
                ),
            )
            .expect("write root config");

            let cli = Cli::parse_from([
                "tao",
                "--json",
                "doc",
                "write",
                "--path",
                "notes/policy-write.md",
                "--content",
                "# policy",
            ]);
            let result = dispatch(cli.command, cli.allow_writes).expect("dispatch doc write");
            let output = render_output(cli.json, &result).expect("render output");
            let envelope: JsonValue = serde_json::from_str(&output).expect("parse output");
            assert_eq!(envelope.get("ok").and_then(JsonValue::as_bool), Some(true));
            assert!(
                vault_root.join("notes/policy-write.md").exists(),
                "write should succeed when read_only=false"
            );
        });
    }

    #[test]
    fn query_docs_select_projects_requested_columns_only() {
        with_temp_cwd(|| {
            let tempdir = tempfile::tempdir().expect("create tempdir");
            let vault_root = tempdir.path().join("vault");
            fs::create_dir_all(vault_root.join("notes/projects")).expect("create notes");
            fs::write(
                vault_root.join("notes/projects/alpha.md"),
                "# Alpha\nproject roadmap",
            )
            .expect("write alpha");
            fs::write(
                vault_root.join("notes/projects/beta.md"),
                "# Beta\nproject updates",
            )
            .expect("write beta");

            let open = Cli::parse_from([
                "tao",
                "--json",
                "vault",
                "open",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
            ]);
            dispatch(open.command, open.allow_writes).expect("open vault");
            let reindex = Cli::parse_from([
                "tao",
                "--json",
                "vault",
                "reindex",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
            ]);
            dispatch(reindex.command, reindex.allow_writes).expect("reindex vault");

            let cli = Cli::parse_from([
                "tao",
                "--json",
                "query",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
                "--from",
                "docs",
                "--query",
                "project",
                "--select",
                "path,title",
                "--limit",
                "10",
                "--offset",
                "0",
            ]);
            let result = dispatch(cli.command, cli.allow_writes).expect("dispatch docs query");
            let output = render_output(cli.json, &result).expect("render output");
            let envelope: JsonValue = serde_json::from_str(&output).expect("parse output");
            let columns = envelope
                .get("value")
                .and_then(|value| value.get("args"))
                .and_then(|args| args.get("columns"))
                .and_then(JsonValue::as_array)
                .expect("columns array");
            let column_names = columns
                .iter()
                .filter_map(JsonValue::as_str)
                .collect::<Vec<_>>();
            assert_eq!(column_names, vec!["path", "title"]);
            let rows = envelope
                .get("value")
                .and_then(|value| value.get("args"))
                .and_then(|args| args.get("rows"))
                .and_then(JsonValue::as_array)
                .expect("rows array");
            assert!(!rows.is_empty(), "expected at least one query row");
            for row in rows {
                let object = row.as_object().expect("row object");
                assert!(object.contains_key("path"));
                assert!(object.contains_key("title"));
                assert!(!object.contains_key("file_id"));
                assert!(!object.contains_key("matched_in"));
            }
        });
    }

    #[test]
    fn query_docs_where_and_sort_are_applied_deterministically() {
        with_temp_cwd(|| {
            let tempdir = tempfile::tempdir().expect("create tempdir");
            let vault_root = tempdir.path().join("vault");
            fs::create_dir_all(vault_root.join("notes/projects")).expect("create notes");
            fs::write(
                vault_root.join("notes/projects/alpha.md"),
                "# Alpha\nproject roadmap",
            )
            .expect("write alpha");
            fs::write(
                vault_root.join("notes/projects/beta.md"),
                "# Beta\nproject roadmap",
            )
            .expect("write beta");
            fs::write(
                vault_root.join("notes/projects/gamma.md"),
                "# Gamma\nproject roadmap",
            )
            .expect("write gamma");

            let open = Cli::parse_from([
                "tao",
                "--json",
                "vault",
                "open",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
            ]);
            dispatch(open.command, open.allow_writes).expect("open vault");
            let reindex = Cli::parse_from([
                "tao",
                "--json",
                "vault",
                "reindex",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
            ]);
            dispatch(reindex.command, reindex.allow_writes).expect("reindex vault");

            let cli = Cli::parse_from([
                "tao",
                "--json",
                "query",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
                "--from",
                "docs",
                "--query",
                "project",
                "--where",
                "title starts_with 'a' or title starts_with 'b'",
                "--sort",
                "title:desc,path:asc",
                "--select",
                "path,title",
                "--limit",
                "10",
                "--offset",
                "0",
            ]);
            let result = dispatch(cli.command, cli.allow_writes).expect("dispatch docs query");
            let output = render_output(cli.json, &result).expect("render output");
            let envelope: JsonValue = serde_json::from_str(&output).expect("parse output");
            let rows = envelope
                .get("value")
                .and_then(|value| value.get("args"))
                .and_then(|args| args.get("rows"))
                .and_then(JsonValue::as_array)
                .expect("rows array");
            assert_eq!(rows.len(), 2);
            assert_eq!(
                rows[0]
                    .get("title")
                    .and_then(JsonValue::as_str)
                    .unwrap_or_default(),
                "beta"
            );
            assert_eq!(
                rows[1]
                    .get("title")
                    .and_then(JsonValue::as_str)
                    .unwrap_or_default(),
                "alpha"
            );
        });
    }

    #[test]
    fn query_docs_where_scans_full_match_set_before_post_filtering() {
        with_temp_cwd(|| {
            let tempdir = tempfile::tempdir().expect("create tempdir");
            let vault_root = tempdir.path().join("vault");
            let notes_dir = vault_root.join("notes");
            fs::create_dir_all(&notes_dir).expect("create notes");

            for index in 0..40_u32 {
                let stem = format!("note-{index:03}");
                fs::write(
                    notes_dir.join(format!("{stem}.md")),
                    format!("# {stem}\nproject"),
                )
                .expect("write note");
            }

            let open = Cli::parse_from([
                "tao",
                "--json",
                "vault",
                "open",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
            ]);
            dispatch(open.command, open.allow_writes).expect("open vault");
            let reindex = Cli::parse_from([
                "tao",
                "--json",
                "vault",
                "reindex",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
            ]);
            dispatch(reindex.command, reindex.allow_writes).expect("reindex vault");

            let cli = Cli::parse_from([
                "tao",
                "--json",
                "query",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
                "--from",
                "docs",
                "--query",
                "project",
                "--where",
                "title == 'note-025'",
                "--select",
                "path,title",
                "--limit",
                "5",
                "--offset",
                "0",
            ]);
            let result = dispatch(cli.command, cli.allow_writes).expect("dispatch docs query");
            let output = render_output(cli.json, &result).expect("render output");
            let envelope: JsonValue = serde_json::from_str(&output).expect("parse output");
            let args = envelope
                .get("value")
                .and_then(|value| value.get("args"))
                .and_then(JsonValue::as_object)
                .expect("args object");
            assert_eq!(args.get("total").and_then(JsonValue::as_u64), Some(1));
            let rows = args
                .get("rows")
                .and_then(JsonValue::as_array)
                .expect("rows array");
            assert_eq!(rows.len(), 1);
            assert_eq!(
                rows[0].get("title").and_then(JsonValue::as_str),
                Some("note-025")
            );
        });
    }

    #[test]
    fn query_docs_sort_scans_full_match_set_before_pagination() {
        with_temp_cwd(|| {
            let tempdir = tempfile::tempdir().expect("create tempdir");
            let vault_root = tempdir.path().join("vault");
            let notes_dir = vault_root.join("notes");
            fs::create_dir_all(&notes_dir).expect("create notes");

            for index in 0..1105_u32 {
                let stem = format!("note-{index:04}");
                fs::write(
                    notes_dir.join(format!("{stem}.md")),
                    format!("# {stem}\nproject"),
                )
                .expect("write note");
            }

            let open = Cli::parse_from([
                "tao",
                "--json",
                "vault",
                "open",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
            ]);
            dispatch(open.command, open.allow_writes).expect("open vault");
            let reindex = Cli::parse_from([
                "tao",
                "--json",
                "vault",
                "reindex",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
            ]);
            dispatch(reindex.command, reindex.allow_writes).expect("reindex vault");

            let cli = Cli::parse_from([
                "tao",
                "--json",
                "query",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
                "--from",
                "docs",
                "--query",
                "project",
                "--sort",
                "path:asc",
                "--select",
                "path,title",
                "--limit",
                "5",
                "--offset",
                "1000",
            ]);
            let result = dispatch(cli.command, cli.allow_writes).expect("dispatch docs query");
            let output = render_output(cli.json, &result).expect("render output");
            let envelope: JsonValue = serde_json::from_str(&output).expect("parse output");
            let args = envelope
                .get("value")
                .and_then(|value| value.get("args"))
                .and_then(JsonValue::as_object)
                .expect("args object");
            assert_eq!(args.get("total").and_then(JsonValue::as_u64), Some(1105));
            let rows = args
                .get("rows")
                .and_then(JsonValue::as_array)
                .expect("rows array");
            let paths = rows
                .iter()
                .map(|row| {
                    row.get("path")
                        .and_then(JsonValue::as_str)
                        .expect("path")
                        .to_string()
                })
                .collect::<Vec<_>>();
            assert_eq!(
                paths,
                vec![
                    "notes/note-1000.md",
                    "notes/note-1001.md",
                    "notes/note-1002.md",
                    "notes/note-1003.md",
                    "notes/note-1004.md",
                ]
            );
        });
    }

    #[test]
    fn query_docs_explain_returns_plan_without_rows_when_not_executing() {
        with_temp_cwd(|| {
            let tempdir = tempfile::tempdir().expect("create tempdir");
            let vault_root = tempdir.path().join("vault");
            fs::create_dir_all(vault_root.join("notes")).expect("create notes");
            fs::write(vault_root.join("notes/a.md"), "# Alpha\nproject").expect("write note");

            let open = Cli::parse_from([
                "tao",
                "--json",
                "vault",
                "open",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
            ]);
            dispatch(open.command, open.allow_writes).expect("open vault");
            let reindex = Cli::parse_from([
                "tao",
                "--json",
                "vault",
                "reindex",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
            ]);
            dispatch(reindex.command, reindex.allow_writes).expect("reindex vault");

            let cli = Cli::parse_from([
                "tao",
                "--json",
                "query",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
                "--from",
                "docs",
                "--query",
                "project",
                "--where",
                "title contains 'Alpha'",
                "--sort",
                "path:asc",
                "--explain",
            ]);
            let result = dispatch(cli.command, cli.allow_writes).expect("dispatch explain query");
            let output = render_output(cli.json, &result).expect("render output");
            let envelope: JsonValue = serde_json::from_str(&output).expect("parse output");
            let args = envelope
                .get("value")
                .and_then(|value| value.get("args"))
                .and_then(JsonValue::as_object)
                .expect("args object");
            assert!(args.contains_key("logical_plan"));
            assert!(args.contains_key("physical_plan"));
            assert!(!args.contains_key("rows"));
        });
    }

    #[test]
    fn query_base_where_and_sort_execute_over_base_scope() {
        with_temp_cwd(|| {
            let tempdir = tempfile::tempdir().expect("create tempdir");
            let vault_root = tempdir.path().join("vault");
            fs::create_dir_all(vault_root.join("notes/projects")).expect("create notes");
            fs::create_dir_all(vault_root.join("views")).expect("create views");

            fs::write(
                vault_root.join("views/projects.base"),
                r#"
views:
  - name: AllProjects
    type: table
    source: notes/projects
    columns:
      - title
      - status
      - priority
"#,
            )
            .expect("write base");
            fs::write(
                vault_root.join("notes/projects/a.md"),
                r#"---
status: active
priority: 1
---
# A
"#,
            )
            .expect("write a");
            fs::write(
                vault_root.join("notes/projects/b.md"),
                r#"---
status: paused
priority: 3
---
# B
"#,
            )
            .expect("write b");
            fs::write(
                vault_root.join("notes/projects/c.md"),
                r#"---
status: active
priority: 2
---
# C
"#,
            )
            .expect("write c");

            let open = Cli::parse_from([
                "tao",
                "--json",
                "vault",
                "open",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
            ]);
            dispatch(open.command, open.allow_writes).expect("open vault");
            let reindex = Cli::parse_from([
                "tao",
                "--json",
                "vault",
                "reindex",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
            ]);
            dispatch(reindex.command, reindex.allow_writes).expect("reindex vault");

            let cli = Cli::parse_from([
                "tao",
                "--json",
                "query",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
                "--from",
                "base:views/projects.base",
                "--view-name",
                "AllProjects",
                "--where",
                "status == 'active'",
                "--sort",
                "priority:desc",
                "--limit",
                "10",
                "--offset",
                "0",
            ]);
            let result = dispatch(cli.command, cli.allow_writes).expect("dispatch base query");
            let output = render_output(cli.json, &result).expect("render output");
            let envelope: JsonValue = serde_json::from_str(&output).expect("parse output");
            let args = envelope
                .get("value")
                .and_then(|value| value.get("args"))
                .and_then(JsonValue::as_object)
                .expect("args object");
            assert_eq!(args.get("total").and_then(JsonValue::as_u64), Some(2));
            let rows = args
                .get("rows")
                .and_then(JsonValue::as_array)
                .expect("rows array");
            assert_eq!(rows.len(), 2);
            assert!(rows.iter().all(|row| {
                row.get("values")
                    .and_then(|value| value.get("status"))
                    .and_then(JsonValue::as_str)
                    == Some("active")
            }));
        });
    }

    #[test]
    fn query_base_where_and_sort_scan_all_base_pages_before_pagination() {
        with_temp_cwd(|| {
            let tempdir = tempfile::tempdir().expect("create tempdir");
            let vault_root = tempdir.path().join("vault");
            fs::create_dir_all(vault_root.join("notes/projects")).expect("create projects");
            fs::create_dir_all(vault_root.join("views")).expect("create views");

            fs::write(
                vault_root.join("views/projects.base"),
                r#"
views:
  - name: AllProjects
    type: table
    source: notes/projects
    columns:
      - title
      - priority
"#,
            )
            .expect("write base");

            for priority in 1..=700_u32 {
                fs::write(
                    vault_root.join(format!("notes/projects/p-{priority:04}.md")),
                    format!("---\npriority: {priority}\n---\n# P-{priority:04}\n"),
                )
                .expect("write project note");
            }

            let open = Cli::parse_from([
                "tao",
                "--json",
                "vault",
                "open",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
            ]);
            dispatch(open.command, open.allow_writes).expect("open vault");
            let reindex = Cli::parse_from([
                "tao",
                "--json",
                "vault",
                "reindex",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
            ]);
            dispatch(reindex.command, reindex.allow_writes).expect("reindex vault");

            let cli = Cli::parse_from([
                "tao",
                "--json",
                "query",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
                "--from",
                "base:views/projects.base",
                "--view-name",
                "AllProjects",
                "--sort",
                "priority:desc",
                "--limit",
                "5",
                "--offset",
                "650",
            ]);
            let result = dispatch(cli.command, cli.allow_writes).expect("dispatch base query");
            let output = render_output(cli.json, &result).expect("render output");
            let envelope: JsonValue = serde_json::from_str(&output).expect("parse output");
            let args = envelope
                .get("value")
                .and_then(|value| value.get("args"))
                .and_then(JsonValue::as_object)
                .expect("args object");

            assert_eq!(args.get("total").and_then(JsonValue::as_u64), Some(700));
            let rows = args
                .get("rows")
                .and_then(JsonValue::as_array)
                .expect("rows array");
            let priorities = rows
                .iter()
                .map(|row| {
                    row.get("values")
                        .and_then(|value| value.get("priority"))
                        .map(|value| {
                            value
                                .as_str()
                                .map(ToString::to_string)
                                .or_else(|| value.as_i64().map(|number| number.to_string()))
                                .or_else(|| value.as_f64().map(|number| format!("{number:.0}")))
                                .expect("priority")
                        })
                        .expect("priority value")
                })
                .collect::<Vec<_>>();
            assert_eq!(priorities, vec!["50", "49", "48", "47", "46"]);
        });
    }

    #[test]
    fn query_matrix_covers_docs_graph_and_base_relation_cases() {
        with_temp_cwd(|| {
            let tempdir = tempfile::tempdir().expect("create tempdir");
            let vault_root = tempdir.path().join("vault");
            fs::create_dir_all(vault_root.join("notes/projects")).expect("create projects");
            fs::create_dir_all(vault_root.join("notes/links")).expect("create links");
            fs::create_dir_all(vault_root.join("views")).expect("create views");

            fs::write(
                vault_root.join("views/projects.base"),
                r#"
views:
  - name: ProjectTable
    type: table
    source: notes/projects
    columns:
      - title
      - status
      - priority
      - related
"#,
            )
            .expect("write base");
            fs::write(
                vault_root.join("notes/projects/alpha.md"),
                r#"---
status: active
priority: 2
related:
  - "[[notes/links/target.md]]"
---
# Alpha
project alpha
"#,
            )
            .expect("write alpha");
            fs::write(
                vault_root.join("notes/projects/beta.md"),
                r#"---
status: paused
priority: 1
related:
  - "[[notes/links/target.md]]"
---
# Beta
project beta
"#,
            )
            .expect("write beta");
            fs::write(
                vault_root.join("notes/links/source.md"),
                r#"---
related:
  - "[[notes/links/target.md]]"
---
# Source
links fixture
"#,
            )
            .expect("write source");
            fs::write(vault_root.join("notes/links/target.md"), "# Target\n")
                .expect("write target");

            let open = Cli::parse_from([
                "tao",
                "--json",
                "vault",
                "open",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
            ]);
            dispatch(open.command, open.allow_writes).expect("open vault");
            let reindex = Cli::parse_from([
                "tao",
                "--json",
                "vault",
                "reindex",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
            ]);
            dispatch(reindex.command, reindex.allow_writes).expect("reindex vault");

            let cases = vec![
                (
                    "docs",
                    Cli::parse_from([
                        "tao",
                        "--json",
                        "query",
                        "--vault-root",
                        vault_root.to_string_lossy().as_ref(),
                        "--from",
                        "docs",
                        "--query",
                        "project",
                        "--where",
                        "title starts_with 'a' or title starts_with 'b'",
                        "--sort",
                        "title:desc",
                        "--limit",
                        "10",
                        "--offset",
                        "0",
                    ]),
                    2_u64,
                ),
                (
                    "base",
                    Cli::parse_from([
                        "tao",
                        "--json",
                        "query",
                        "--vault-root",
                        vault_root.to_string_lossy().as_ref(),
                        "--from",
                        "base:views/projects.base",
                        "--view-name",
                        "ProjectTable",
                        "--where",
                        "related contains 'target' and status == 'active'",
                        "--sort",
                        "priority:desc",
                        "--limit",
                        "10",
                        "--offset",
                        "0",
                    ]),
                    1_u64,
                ),
                (
                    "graph",
                    Cli::parse_from([
                        "tao",
                        "--json",
                        "query",
                        "--vault-root",
                        vault_root.to_string_lossy().as_ref(),
                        "--from",
                        "graph",
                        "--path",
                        "notes/links/source.md",
                        "--limit",
                        "10",
                        "--offset",
                        "0",
                    ]),
                    1_u64,
                ),
            ];

            for (scope, cli, expected_total) in cases {
                let result =
                    dispatch(cli.command, cli.allow_writes).expect("dispatch matrix query case");
                let output = render_output(cli.json, &result).expect("render matrix output");
                let envelope: JsonValue =
                    serde_json::from_str(&output).expect("parse matrix output");
                let total = envelope
                    .get("value")
                    .and_then(|value| value.get("args"))
                    .and_then(|args| args.get("total"))
                    .and_then(JsonValue::as_u64)
                    .unwrap_or(0);
                assert_eq!(
                    total, expected_total,
                    "matrix mismatch for scope '{}': expected {}, got {}",
                    scope, expected_total, total
                );
            }
        });
    }

    #[test]
    fn query_graph_path_returns_outgoing_and_backlinks_panels() {
        with_temp_cwd(|| {
            let tempdir = tempfile::tempdir().expect("create tempdir");
            let vault_root = tempdir.path().join("vault");
            fs::create_dir_all(vault_root.join("notes")).expect("create notes");
            fs::write(vault_root.join("notes/a.md"), "# A\n[[b]]\n").expect("write a");
            fs::write(vault_root.join("notes/b.md"), "# B\n[[c]]\n").expect("write b");
            fs::write(vault_root.join("notes/c.md"), "# C\n[[b]]\n").expect("write c");

            let open = Cli::parse_from([
                "tao",
                "--json",
                "vault",
                "open",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
            ]);
            dispatch(open.command, open.allow_writes).expect("open vault");
            let reindex = Cli::parse_from([
                "tao",
                "--json",
                "vault",
                "reindex",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
            ]);
            dispatch(reindex.command, reindex.allow_writes).expect("reindex vault");

            let cli = Cli::parse_from([
                "tao",
                "--json",
                "query",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
                "--from",
                "graph",
                "--path",
                "notes/b.md",
                "--limit",
                "10",
                "--offset",
                "0",
            ]);
            let result = dispatch(cli.command, cli.allow_writes).expect("dispatch graph query");
            let output = render_output(cli.json, &result).expect("render graph query");
            let envelope: JsonValue = serde_json::from_str(&output).expect("parse output");
            let args = envelope
                .get("value")
                .and_then(|value| value.get("args"))
                .expect("query args");
            assert_eq!(
                args.get("outgoing_total").and_then(JsonValue::as_u64),
                Some(1)
            );
            assert_eq!(
                args.get("backlinks_total").and_then(JsonValue::as_u64),
                Some(2)
            );
            assert_eq!(args.get("total").and_then(JsonValue::as_u64), Some(3));
            assert_eq!(
                args.get("outgoing")
                    .and_then(JsonValue::as_array)
                    .map(Vec::len),
                Some(1)
            );
            assert_eq!(
                args.get("backlinks")
                    .and_then(JsonValue::as_array)
                    .map(Vec::len),
                Some(2)
            );
        });
    }

    #[test]
    fn json_stream_docs_query_uses_streaming_envelope() {
        with_temp_cwd(|| {
            let tempdir = tempfile::tempdir().expect("create tempdir");
            let vault_root = tempdir.path().join("vault");
            fs::create_dir_all(vault_root.join("notes/projects")).expect("create notes");
            fs::write(
                vault_root.join("notes/projects/alpha.md"),
                "# Alpha\nproject roadmap",
            )
            .expect("write alpha");

            let open = Cli::parse_from([
                "tao",
                "--json",
                "vault",
                "open",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
            ]);
            dispatch(open.command, open.allow_writes).expect("open vault");
            let reindex = Cli::parse_from([
                "tao",
                "--json",
                "vault",
                "reindex",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
            ]);
            dispatch(reindex.command, reindex.allow_writes).expect("reindex vault");

            let cli = Cli::parse_from([
                "tao",
                "--json",
                "--json-stream",
                "query",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
                "--from",
                "docs",
                "--query",
                "project",
                "--select",
                "path,title",
                "--limit",
                "10",
                "--offset",
                "0",
            ]);
            let output = maybe_render_streaming_output(&cli)
                .expect("render streaming output")
                .expect("streaming output expected");
            let envelope: JsonValue = serde_json::from_str(&output).expect("parse streaming json");
            assert_eq!(
                envelope
                    .get("value")
                    .and_then(|value| value.get("command"))
                    .and_then(JsonValue::as_str),
                Some("query.run")
            );
            let columns = envelope
                .get("value")
                .and_then(|value| value.get("args"))
                .and_then(|args| args.get("columns"))
                .and_then(JsonValue::as_array)
                .expect("columns");
            assert_eq!(
                columns
                    .iter()
                    .filter_map(JsonValue::as_str)
                    .collect::<Vec<_>>(),
                vec!["path", "title"]
            );
        });
    }

    #[test]
    fn graph_neighbors_supports_direction_filtering() {
        with_temp_cwd(|| {
            let tempdir = tempfile::tempdir().expect("create tempdir");
            let vault_root = tempdir.path().join("vault");
            fs::create_dir_all(vault_root.join("notes")).expect("create notes dir");
            fs::write(vault_root.join("notes/a.md"), "# A\n[[notes/b.md]]\n").expect("write a");
            fs::write(vault_root.join("notes/b.md"), "# B\n[[notes/c.md]]\n").expect("write b");
            fs::write(vault_root.join("notes/c.md"), "# C\n").expect("write c");
            fs::write(vault_root.join("notes/d.md"), "# D\n[[notes/a.md]]\n").expect("write d");

            let open = Cli::parse_from([
                "tao",
                "--json",
                "vault",
                "open",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
            ]);
            dispatch(open.command, open.allow_writes).expect("open vault");
            let reindex = Cli::parse_from([
                "tao",
                "--json",
                "vault",
                "reindex",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
            ]);
            dispatch(reindex.command, reindex.allow_writes).expect("reindex vault");

            let neighbors = Cli::parse_from([
                "tao",
                "--json",
                "graph",
                "neighbors",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
                "--path",
                "notes/a.md",
                "--direction",
                "incoming",
            ]);
            let output = render_output(
                neighbors.json,
                &dispatch(neighbors.command, neighbors.allow_writes).expect("dispatch neighbors"),
            )
            .expect("render neighbors");
            let envelope: JsonValue = serde_json::from_str(&output).expect("parse neighbors");
            let items = envelope
                .get("value")
                .and_then(|value| value.get("args"))
                .and_then(|args| args.get("items"))
                .and_then(JsonValue::as_array)
                .expect("neighbors items");
            assert_eq!(items.len(), 1);
            assert_eq!(
                items[0].get("path").and_then(JsonValue::as_str),
                Some("notes/d.md")
            );
            assert_eq!(
                items[0].get("direction").and_then(JsonValue::as_str),
                Some("incoming")
            );
        });
    }

    #[test]
    fn graph_path_reports_found_not_found_and_guardrail_errors() {
        with_temp_cwd(|| {
            let tempdir = tempfile::tempdir().expect("create tempdir");
            let vault_root = tempdir.path().join("vault");
            fs::create_dir_all(vault_root.join("notes")).expect("create notes dir");
            fs::write(vault_root.join("notes/a.md"), "# A\n[[notes/b.md]]\n").expect("write a");
            fs::write(vault_root.join("notes/b.md"), "# B\n[[notes/c.md]]\n").expect("write b");
            fs::write(vault_root.join("notes/c.md"), "# C\n").expect("write c");
            fs::write(vault_root.join("notes/e.md"), "# E\n").expect("write e");

            let open = Cli::parse_from([
                "tao",
                "--json",
                "vault",
                "open",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
            ]);
            dispatch(open.command, open.allow_writes).expect("open vault");
            let reindex = Cli::parse_from([
                "tao",
                "--json",
                "vault",
                "reindex",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
            ]);
            dispatch(reindex.command, reindex.allow_writes).expect("reindex vault");

            let found = Cli::parse_from([
                "tao",
                "--json",
                "graph",
                "path",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
                "--from",
                "notes/a.md",
                "--to",
                "notes/a.md",
            ]);
            let found_output = render_output(
                found.json,
                &dispatch(found.command, found.allow_writes).expect("dispatch found path"),
            )
            .expect("render found path");
            let found_envelope: JsonValue =
                serde_json::from_str(&found_output).expect("parse found path");
            assert_eq!(
                found_envelope
                    .get("value")
                    .and_then(|value| value.get("args"))
                    .and_then(|args| args.get("found"))
                    .and_then(JsonValue::as_bool),
                Some(true)
            );

            let missing = Cli::parse_from([
                "tao",
                "--json",
                "graph",
                "path",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
                "--from",
                "notes/a.md",
                "--to",
                "notes/e.md",
            ]);
            let missing_output = render_output(
                missing.json,
                &dispatch(missing.command, missing.allow_writes).expect("dispatch missing path"),
            )
            .expect("render missing path");
            let missing_envelope: JsonValue =
                serde_json::from_str(&missing_output).expect("parse missing path");
            assert_eq!(
                missing_envelope
                    .get("value")
                    .and_then(|value| value.get("args"))
                    .and_then(|args| args.get("found"))
                    .and_then(JsonValue::as_bool),
                Some(false)
            );

            let guardrail = Cli::parse_from([
                "tao",
                "--json",
                "graph",
                "path",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
                "--from",
                "notes/a.md",
                "--to",
                "notes/c.md",
                "--max-nodes",
                "0",
            ]);
            let error = dispatch(guardrail.command, guardrail.allow_writes)
                .expect_err("guardrail should fail");
            assert!(
                error
                    .to_string()
                    .contains("--max-nodes must be greater than zero")
            );
        });
    }

    #[test]
    fn graph_components_supports_weak_and_strong_modes() {
        with_temp_cwd(|| {
            let tempdir = tempfile::tempdir().expect("create tempdir");
            let vault_root = tempdir.path().join("vault");
            fs::create_dir_all(vault_root.join("notes")).expect("create notes dir");
            fs::write(vault_root.join("notes/a.md"), "# A\n[[b]]\n").expect("write a");
            fs::write(vault_root.join("notes/b.md"), "# B\n[[a]]\n[[c]]\n").expect("write b");
            fs::write(vault_root.join("notes/c.md"), "# C\n").expect("write c");

            let open = Cli::parse_from([
                "tao",
                "--json",
                "vault",
                "open",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
            ]);
            dispatch(open.command, open.allow_writes).expect("open vault");
            let reindex = Cli::parse_from([
                "tao",
                "--json",
                "vault",
                "reindex",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
            ]);
            dispatch(reindex.command, reindex.allow_writes).expect("reindex vault");

            let weak = Cli::parse_from([
                "tao",
                "--json",
                "graph",
                "components",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
                "--mode",
                "weak",
                "--include-members",
            ]);
            let weak_output = render_output(
                weak.json,
                &dispatch(weak.command, weak.allow_writes).expect("dispatch weak components"),
            )
            .expect("render weak components");
            let weak_json: JsonValue = serde_json::from_str(&weak_output).expect("parse weak json");
            let weak_items = weak_json
                .get("value")
                .and_then(|value| value.get("args"))
                .and_then(|args| args.get("items"))
                .and_then(JsonValue::as_array)
                .expect("weak items");
            assert_eq!(weak_items.len(), 1);
            assert_eq!(
                weak_items[0].get("size").and_then(JsonValue::as_u64),
                Some(3)
            );

            let strong = Cli::parse_from([
                "tao",
                "--json",
                "graph",
                "components",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
                "--mode",
                "strong",
                "--include-members",
            ]);
            let strong_output = render_output(
                strong.json,
                &dispatch(strong.command, strong.allow_writes).expect("dispatch strong components"),
            )
            .expect("render strong components");
            let strong_json: JsonValue =
                serde_json::from_str(&strong_output).expect("parse strong json");
            let strong_items = strong_json
                .get("value")
                .and_then(|value| value.get("args"))
                .and_then(|args| args.get("items"))
                .and_then(JsonValue::as_array)
                .expect("strong items");
            let strong_sizes = strong_items
                .iter()
                .filter_map(|item| item.get("size").and_then(JsonValue::as_u64))
                .collect::<Vec<_>>();
            assert_eq!(strong_sizes, vec![2, 1]);
        });
    }

    #[test]
    fn graph_walk_can_include_folder_overlay_edges() {
        with_temp_cwd(|| {
            let tempdir = tempfile::tempdir().expect("create tempdir");
            let vault_root = tempdir.path().join("vault");
            fs::create_dir_all(vault_root.join("notes/projects")).expect("create projects dir");
            fs::create_dir_all(vault_root.join("notes/meetings")).expect("create meetings dir");
            fs::write(vault_root.join("notes/projects/a.md"), "# A\n").expect("write a");
            fs::write(vault_root.join("notes/projects/b.md"), "# B\n").expect("write b");
            fs::write(vault_root.join("notes/meetings/m1.md"), "# M1\n").expect("write m1");

            let open = Cli::parse_from([
                "tao",
                "--json",
                "vault",
                "open",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
            ]);
            dispatch(open.command, open.allow_writes).expect("open vault");
            let reindex = Cli::parse_from([
                "tao",
                "--json",
                "vault",
                "reindex",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
            ]);
            dispatch(reindex.command, reindex.allow_writes).expect("reindex vault");

            let plain_walk = Cli::parse_from([
                "tao",
                "--json",
                "graph",
                "walk",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
                "--path",
                "notes/projects/a.md",
                "--depth",
                "2",
            ]);
            let plain_output = render_output(
                plain_walk.json,
                &dispatch(plain_walk.command, plain_walk.allow_writes)
                    .expect("dispatch plain walk"),
            )
            .expect("render plain walk");
            let plain_json: JsonValue =
                serde_json::from_str(&plain_output).expect("parse plain walk");
            let plain_items = plain_json
                .get("value")
                .and_then(|value| value.get("args"))
                .and_then(|args| args.get("items"))
                .and_then(JsonValue::as_array)
                .expect("plain items");
            assert!(
                plain_items.is_empty(),
                "expected no wikilink steps in plain walk fixture"
            );

            let folder_walk = Cli::parse_from([
                "tao",
                "--json",
                "graph",
                "walk",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
                "--path",
                "notes/projects/a.md",
                "--depth",
                "2",
                "--include-folders",
            ]);
            let folder_output = render_output(
                folder_walk.json,
                &dispatch(folder_walk.command, folder_walk.allow_writes)
                    .expect("dispatch folder walk"),
            )
            .expect("render folder walk");
            let folder_json: JsonValue =
                serde_json::from_str(&folder_output).expect("parse folder walk");
            let folder_items = folder_json
                .get("value")
                .and_then(|value| value.get("args"))
                .and_then(|args| args.get("items"))
                .and_then(JsonValue::as_array)
                .expect("folder walk items");
            assert!(!folder_items.is_empty(), "expected folder overlay edges");
            assert!(folder_items.iter().any(|item| {
                item.get("edge_type").and_then(JsonValue::as_str) == Some("folder-sibling")
            }));
        });
    }

    #[test]
    fn graph_unresolved_includes_reason_and_source_fields() {
        with_temp_cwd(|| {
            let tempdir = tempfile::tempdir().expect("create tempdir");
            let vault_root = tempdir.path().join("vault");
            fs::create_dir_all(vault_root.join("notes")).expect("create notes dir");
            fs::write(
                vault_root.join("notes/a.md"),
                "---\nrefs:\n  - \"[[missing-frontmatter]]\"\n---\n# A\n[[missing-body]]\n",
            )
            .expect("write a");

            let open = Cli::parse_from([
                "tao",
                "--json",
                "vault",
                "open",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
            ]);
            dispatch(open.command, open.allow_writes).expect("open vault");
            let reindex = Cli::parse_from([
                "tao",
                "--json",
                "vault",
                "reindex",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
            ]);
            dispatch(reindex.command, reindex.allow_writes).expect("reindex vault");

            let unresolved = Cli::parse_from([
                "tao",
                "--json",
                "graph",
                "unresolved",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
                "--limit",
                "20",
                "--offset",
                "0",
            ]);
            let output = render_output(
                unresolved.json,
                &dispatch(unresolved.command, unresolved.allow_writes)
                    .expect("dispatch unresolved"),
            )
            .expect("render unresolved");
            let payload: JsonValue = serde_json::from_str(&output).expect("parse unresolved");
            let items = payload
                .get("value")
                .and_then(|value| value.get("args"))
                .and_then(|args| args.get("items"))
                .and_then(JsonValue::as_array)
                .expect("unresolved items");
            assert!(
                items
                    .iter()
                    .all(|item| item.get("unresolved_reason").is_some())
            );
            assert!(items.iter().all(|item| item.get("source_field").is_some()));
            assert!(items.iter().any(|item| {
                item.get("source_field").and_then(JsonValue::as_str) == Some("body")
            }));
            assert!(items.iter().any(|item| {
                item.get("source_field")
                    .and_then(JsonValue::as_str)
                    .is_some_and(|value| value.starts_with("frontmatter:"))
            }));
        });
    }

    #[test]
    fn graph_snapshot_contracts_match_golden_outputs() {
        with_temp_cwd(|| {
            let fixture_root = Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../vault/fixtures/graph-parity")
                .canonicalize()
                .expect("canonicalize graph parity fixture");
            let expected_root = fixture_root.join("expected");

            let tempdir = tempfile::tempdir().expect("create tempdir");
            let vault_root = tempdir.path().join("vault");
            copy_dir_recursive(&fixture_root, &vault_root).expect("copy graph parity fixture");
            let _ = fs::remove_dir_all(vault_root.join("expected"));

            let open = Cli::parse_from([
                "tao",
                "--json",
                "vault",
                "open",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
            ]);
            dispatch(open.command, open.allow_writes).expect("open vault");
            let reindex = Cli::parse_from([
                "tao",
                "--json",
                "vault",
                "reindex",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
            ]);
            dispatch(reindex.command, reindex.allow_writes).expect("reindex vault");

            let assert_snapshot = |expected_name: &str, cli: Cli| {
                let rendered = render_output(
                    cli.json,
                    &dispatch(cli.command, cli.allow_writes).expect("dispatch"),
                )
                .expect("render output");
                let actual: JsonValue =
                    serde_json::from_str(&rendered).expect("parse json envelope");
                let actual_args = actual
                    .get("value")
                    .and_then(|value| value.get("args"))
                    .expect("value.args");
                let expected_raw = fs::read_to_string(expected_root.join(expected_name))
                    .expect("read expected snapshot");
                let expected: JsonValue =
                    serde_json::from_str(&expected_raw).expect("parse expected snapshot");
                assert_eq!(
                    actual_args, &expected,
                    "snapshot mismatch for {expected_name}"
                );
            };

            assert_snapshot(
                "outgoing.json",
                Cli::parse_from([
                    "tao",
                    "--json",
                    "graph",
                    "outgoing",
                    "--vault-root",
                    vault_root.to_string_lossy().as_ref(),
                    "--path",
                    "notes/root.md",
                ]),
            );
            assert_snapshot(
                "backlinks.json",
                Cli::parse_from([
                    "tao",
                    "--json",
                    "graph",
                    "backlinks",
                    "--vault-root",
                    vault_root.to_string_lossy().as_ref(),
                    "--path",
                    "notes/beta.md",
                ]),
            );
            assert_snapshot(
                "unresolved.json",
                Cli::parse_from([
                    "tao",
                    "--json",
                    "graph",
                    "unresolved",
                    "--vault-root",
                    vault_root.to_string_lossy().as_ref(),
                    "--limit",
                    "100",
                    "--offset",
                    "0",
                ]),
            );
            assert_snapshot(
                "deadends.json",
                Cli::parse_from([
                    "tao",
                    "--json",
                    "graph",
                    "deadends",
                    "--vault-root",
                    vault_root.to_string_lossy().as_ref(),
                    "--limit",
                    "100",
                    "--offset",
                    "0",
                ]),
            );
            assert_snapshot(
                "orphans.json",
                Cli::parse_from([
                    "tao",
                    "--json",
                    "graph",
                    "orphans",
                    "--vault-root",
                    vault_root.to_string_lossy().as_ref(),
                    "--limit",
                    "100",
                    "--offset",
                    "0",
                ]),
            );
            assert_snapshot(
                "walk.json",
                Cli::parse_from([
                    "tao",
                    "--json",
                    "graph",
                    "walk",
                    "--vault-root",
                    vault_root.to_string_lossy().as_ref(),
                    "--path",
                    "notes/root.md",
                    "--depth",
                    "2",
                    "--limit",
                    "50",
                    "--include-unresolved",
                ]),
            );
        });
    }

    #[test]
    fn daemon_control_commands_bypass_client_forwarding() {
        let cli = Cli::parse_from([
            "tao",
            "--json",
            "--daemon-socket",
            "/tmp/tao-test.sock",
            "vault",
            "daemon",
            "status",
            "--socket",
            "/tmp/tao-test.sock",
        ]);
        let forwarded = maybe_forward_to_daemon(&cli).expect("daemon control should not forward");
        assert!(forwarded.is_none());
    }

    #[test]
    fn daemon_socket_resolution_prefers_explicit_override() {
        let cli = Cli::parse_from([
            "tao",
            "--json",
            "--daemon-socket",
            "/tmp/tao-explicit.sock",
            "vault",
            "open",
            "--vault-root",
            "/tmp",
        ]);
        let socket = resolve_daemon_socket_for_cli(&cli)
            .expect("resolve socket")
            .expect("socket should be resolved");
        assert_eq!(socket, "/tmp/tao-explicit.sock");
    }

    #[test]
    fn daemon_socket_resolution_derives_deterministic_per_vault_path() {
        with_temp_cwd(|| {
            let tempdir = tempfile::tempdir().expect("create tempdir");
            let vault_root = tempdir.path().join("vault");
            fs::create_dir_all(&vault_root).expect("create vault root");

            let cli = Cli::parse_from([
                "tao",
                "--json",
                "vault",
                "open",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
            ]);

            let socket = resolve_daemon_socket_for_cli(&cli)
                .expect("resolve socket")
                .expect("socket should be resolved");
            let resolved = resolve_command_vault_paths(&cli.command)
                .expect("resolve command vault paths")
                .expect("vault path should resolve");
            let expected =
                derive_daemon_socket_for_vault(&resolved.vault_root).expect("derive socket");
            assert_eq!(socket, expected);
            assert!(socket.ends_with(".sock"));
        });
    }

    #[test]
    fn daemon_status_reports_stale_and_dead_socket_states() {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let stale_socket = tempdir.path().join("stale.sock");
        #[cfg(unix)]
        {
            let listener = UnixListener::bind(&stale_socket).expect("bind stale socket");
            drop(listener);
        }
        let stale = handle_daemon(DaemonCommands::Status(DaemonSocketArgs {
            socket: Some(stale_socket.to_string_lossy().to_string()),
            vault_root: None,
            db_path: None,
        }))
        .expect("daemon status");
        assert_eq!(
            stale.args.get("state").and_then(JsonValue::as_str),
            Some("stale")
        );
        assert_eq!(
            stale.args.get("running").and_then(JsonValue::as_bool),
            Some(false)
        );

        let dead_path = tempdir.path().join("dead.sock");
        fs::write(&dead_path, "not-a-socket").expect("write dead socket placeholder");
        let dead = handle_daemon(DaemonCommands::Status(DaemonSocketArgs {
            socket: Some(dead_path.to_string_lossy().to_string()),
            vault_root: None,
            db_path: None,
        }))
        .expect("daemon status");
        assert_eq!(
            dead.args.get("state").and_then(JsonValue::as_str),
            Some("dead")
        );
        assert_eq!(
            dead.args.get("running").and_then(JsonValue::as_bool),
            Some(false)
        );
    }

    #[test]
    fn daemon_socket_prepare_removes_stale_entry_before_bind() {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let stale_path = tempdir.path().join("prepare.sock");
        fs::write(&stale_path, "stale").expect("write stale file");
        assert!(stale_path.exists());
        let prepared =
            prepare_daemon_socket_path(stale_path.to_string_lossy().as_ref()).expect("prepare");
        assert_eq!(prepared, stale_path);
        assert!(!stale_path.exists(), "stale socket path should be removed");
    }

    #[test]
    fn daemon_stop_all_prunes_stale_socket_files() {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let socket_dir = tempdir.path().join("daemons");
        fs::create_dir_all(&socket_dir).expect("create daemon socket dir");
        let dead_socket = socket_dir.join("dead.sock");
        fs::write(&dead_socket, "stale").expect("write dead socket marker");
        assert!(dead_socket.exists(), "test precondition");

        let result = handle_daemon(DaemonCommands::StopAll(DaemonStopAllArgs {
            socket_dir: Some(socket_dir.to_string_lossy().to_string()),
        }))
        .expect("daemon stop-all");
        assert_eq!(
            result
                .args
                .get("discovered_sockets")
                .and_then(JsonValue::as_u64),
            Some(1)
        );
        assert_eq!(
            result.args.get("pruned_stale").and_then(JsonValue::as_u64),
            Some(1)
        );
        assert!(!dead_socket.exists(), "stale socket should be removed");
    }

    #[test]
    fn daemon_cacheability_matrix_blocks_mutating_commands() {
        let cacheable_query = Commands::Query(QueryArgs {
            vault_root: Some("/tmp".to_string()),
            db_path: None,
            from: "docs".to_string(),
            query: Some("project".to_string()),
            path: None,
            view_name: None,
            select: None,
            where_clause: None,
            sort: None,
            explain: false,
            execute: false,
            limit: 10,
            offset: 0,
        });
        assert!(command_is_cacheable(&cacheable_query));

        let doc_write = Commands::Doc {
            command: DocCommands::Write(NotePutArgs {
                vault_root: Some("/tmp".to_string()),
                db_path: None,
                path: "notes/x.md".to_string(),
                content: "# x".to_string(),
            }),
        };
        assert!(!command_is_cacheable(&doc_write));
    }

    #[test]
    fn vault_open_creates_default_db_when_db_path_is_omitted() {
        with_temp_cwd(|| {
            let tempdir = tempfile::tempdir().expect("create tempdir");
            let vault_root = tempdir.path().join("vault");
            fs::create_dir_all(&vault_root).expect("create vault dir");

            let cli = Cli::parse_from([
                "tao",
                "--json",
                "vault",
                "open",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
            ]);
            let result = dispatch(cli.command, cli.allow_writes).expect("dispatch");
            let output = render_output(cli.json, &result).expect("render output");
            let envelope: JsonValue = serde_json::from_str(&output).expect("parse output");

            let db_path = envelope
                .get("value")
                .and_then(|raw| raw.get("args"))
                .and_then(|raw| raw.get("db_path"))
                .and_then(JsonValue::as_str)
                .expect("db_path in response");

            assert!(
                Path::new(db_path).exists(),
                "expected default sqlite file to be created at {db_path}"
            );
        });
    }

    #[test]
    fn vault_open_respects_db_path_override() {
        with_temp_cwd(|| {
            let tempdir = tempfile::tempdir().expect("create tempdir");
            let vault_root = tempdir.path().join("vault");
            fs::create_dir_all(&vault_root).expect("create vault dir");
            let custom_db = tempdir.path().join("custom").join("tao.sqlite");

            let cli = Cli::parse_from([
                "tao",
                "--json",
                "vault",
                "open",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
                "--db-path",
                custom_db.to_string_lossy().as_ref(),
            ]);
            let result = dispatch(cli.command, cli.allow_writes).expect("dispatch");
            let output = render_output(cli.json, &result).expect("render output");
            let envelope: JsonValue = serde_json::from_str(&output).expect("parse output");

            let db_path = envelope
                .get("value")
                .and_then(|raw| raw.get("args"))
                .and_then(|raw| raw.get("db_path"))
                .and_then(JsonValue::as_str)
                .expect("db_path in response");

            assert_eq!(Path::new(db_path), custom_db.as_path());
            assert!(custom_db.exists(), "expected override sqlite path to exist");
        });
    }

    fn with_temp_cwd<T>(operation: impl FnOnce() -> T) -> T {
        static CWD_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        let lock = CWD_LOCK.get_or_init(|| Mutex::new(()));
        let _guard = lock.lock().expect("lock cwd mutex");

        let original_dir = env::current_dir().expect("get original cwd");
        let sandbox = tempfile::tempdir().expect("create cwd sandbox");
        env::set_current_dir(sandbox.path()).expect("set temp cwd");
        let result = operation();
        env::set_current_dir(&original_dir).expect("restore cwd");
        result
    }

    #[test]
    fn daemon_refresh_uses_filesystem_monitor_to_pick_up_external_note_changes() {
        with_temp_cwd(|| {
            let tempdir = tempfile::tempdir().expect("create tempdir");
            let vault_root = tempdir.path().join("vault");
            fs::create_dir_all(vault_root.join("notes")).expect("create notes");
            fs::write(vault_root.join("notes/a.md"), "# A").expect("write a");

            let command = Commands::Doc {
                command: DocCommands::List(VaultPathArgs {
                    vault_root: Some(vault_root.to_string_lossy().to_string()),
                    db_path: None,
                }),
            };

            let mut runtime = RuntimeMode::Daemon(Box::<RuntimeCache>::default());
            maybe_refresh_daemon_state(&command, &mut runtime).expect("prime daemon refresh");
            let first = dispatch_with_runtime(command.clone(), false, &mut runtime)
                .expect("dispatch first daemon list");
            assert_eq!(first.args.get("total").and_then(JsonValue::as_u64), Some(1));

            let resolved = resolve_command_vault_paths(&command)
                .expect("resolve paths")
                .expect("resolved args");
            let runtime_key = runtime_cache_key(&resolved);
            if let RuntimeMode::Daemon(cache) = &mut runtime {
                let cache_key = serde_json::to_string(&command).expect("cache key");
                cache.command_results.insert(
                    cache_key,
                    super::CachedCommandResult {
                        runtime_key,
                        result: first,
                    },
                );
            }

            fs::write(vault_root.join("notes/b.md"), "# B").expect("write b");
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
            let mut refreshed = false;
            while std::time::Instant::now() < deadline {
                if maybe_refresh_daemon_state(&command, &mut runtime).expect("refresh daemon state")
                {
                    refreshed = true;
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(25));
            }
            assert!(
                refreshed,
                "expected daemon refresh after external note change"
            );

            if let RuntimeMode::Daemon(cache) = &runtime {
                assert!(
                    cache.command_results.is_empty(),
                    "stale cached command results should be invalidated"
                );
            }

            let second = dispatch_with_runtime(command, false, &mut runtime)
                .expect("dispatch refreshed daemon list");
            assert_eq!(
                second.args.get("total").and_then(JsonValue::as_u64),
                Some(2)
            );
        });
    }

    fn copy_dir_recursive(source: &Path, destination: &Path) -> std::io::Result<()> {
        fs::create_dir_all(destination)?;
        for entry in fs::read_dir(source)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            let source_path = entry.path();
            let destination_path = destination.join(entry.file_name());
            if file_type.is_dir() {
                copy_dir_recursive(&source_path, &destination_path)?;
            } else if file_type.is_file() {
                if let Some(parent) = destination_path.parent() {
                    fs::create_dir_all(parent)?;
                }
                fs::copy(&source_path, &destination_path)?;
            }
        }
        Ok(())
    }
}
