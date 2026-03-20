use super::*;

#[derive(Debug, Clone, Parser, Serialize, Deserialize)]
#[command(name = "tao", version, about = "tao cli")]
pub(crate) struct Cli {
    /// Emit plain-text summaries instead of JSON envelopes.
    #[arg(
        long = "text",
        global = true,
        default_value_t = true,
        action = ArgAction::SetFalse
    )]
    pub(crate) json: bool,
    /// Stream JSON envelope serialization for supported large read commands.
    #[arg(long, global = true, default_value_t = false)]
    pub(crate) json_stream: bool,
    /// Allow vault content write operations (disabled by default).
    #[arg(long, global = true, default_value_t = false)]
    pub(crate) allow_writes: bool,
    /// Route command execution through a warm daemon socket.
    #[arg(long, global = true)]
    pub(crate) daemon_socket: Option<String>,
    #[command(subcommand)]
    pub(crate) command: Commands,
}

#[derive(Debug, Clone, Subcommand, Serialize, Deserialize)]
pub(crate) enum Commands {
    /// Return public tool registry metadata.
    Tools(ToolsArgs),
    /// Return machine-oriented runtime readiness.
    Health(VaultPathArgs),
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

#[derive(Debug, Clone, Args, Serialize, Deserialize)]
pub(crate) struct ToolsArgs {
    /// Optional dotted tool name to inspect.
    pub(crate) name: Option<String>,
}

#[derive(Debug, Clone, Subcommand, Serialize, Deserialize)]
pub(crate) enum DocCommands {
    /// Return one note by normalized path.
    Read(NotePathArgs),
    /// Create or update one note.
    Write(NotePutArgs),
    /// List markdown note windows.
    List(VaultPathArgs),
}

#[derive(Debug, Clone, Subcommand, Serialize, Deserialize)]
pub(crate) enum BaseCommands {
    /// List indexed bases.
    List(VaultPathArgs),
    /// Query one base table view.
    View(BaseViewArgs),
    /// Return one base schema contract.
    Schema(BaseSchemaArgs),
    /// Validate one base config and return diagnostics.
    Validate(BaseSchemaArgs),
}

#[derive(Debug, Clone, Subcommand, Serialize, Deserialize)]
pub(crate) enum GraphCommands {
    /// Return outgoing links for one note.
    Outgoing(NotePathArgs),
    /// Return backlinks for one note.
    Backlinks(NotePathArgs),
    /// Return scoped inbound-link counts for file audits.
    InboundScope(GraphInboundScopeArgs),
    /// Return unresolved graph links.
    Unresolved(GraphWindowArgs),
    /// Return notes with no outgoing resolved edges.
    Deadends(GraphWindowArgs),
    /// Return isolated notes with no incoming/outgoing resolved edges.
    Orphans(GraphWindowArgs),
    /// Return strict floating files with built-in graph-view filtering.
    Floating(GraphWindowArgs),
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
pub(crate) enum MetaCommands {
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
pub(crate) enum TaskCommands {
    /// List extracted markdown tasks.
    List(TaskListArgs),
    /// Update checkbox state on one task line.
    SetState(TaskSetStateArgs),
}

#[derive(Debug, Clone, Subcommand, Serialize, Deserialize)]
pub(crate) enum VaultCommands {
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
pub(crate) enum DaemonCommands {
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
pub(crate) struct VaultPathArgs {
    /// Optional absolute vault root path. Falls back to config/env defaults.
    #[arg(long)]
    pub(crate) vault_root: Option<String>,
    /// Optional sqlite database file path override.
    #[arg(long)]
    pub(crate) db_path: Option<String>,
}

#[derive(Debug, Clone, Args, Serialize, Deserialize)]
pub(crate) struct NotePathArgs {
    /// Optional absolute vault root path. Falls back to config/env defaults.
    #[arg(long)]
    pub(crate) vault_root: Option<String>,
    /// Optional sqlite database file path override.
    #[arg(long)]
    pub(crate) db_path: Option<String>,
    /// Vault-relative normalized note path.
    #[arg(long)]
    pub(crate) path: String,
}

#[derive(Debug, Clone, Args, Serialize, Deserialize)]
pub(crate) struct NotePutArgs {
    /// Optional absolute vault root path. Falls back to config/env defaults.
    #[arg(long)]
    pub(crate) vault_root: Option<String>,
    /// Optional sqlite database file path override.
    #[arg(long)]
    pub(crate) db_path: Option<String>,
    /// Vault-relative normalized note path.
    #[arg(long)]
    pub(crate) path: String,
    /// Full markdown content payload.
    #[arg(long)]
    pub(crate) content: String,
}

#[derive(Debug, Clone, Args, Serialize, Deserialize)]
pub(crate) struct BaseViewArgs {
    /// Optional absolute vault root path. Falls back to config/env defaults.
    #[arg(long)]
    pub(crate) vault_root: Option<String>,
    /// Optional sqlite database file path override.
    #[arg(long)]
    pub(crate) db_path: Option<String>,
    /// Base id or normalized base file path.
    #[arg(long)]
    pub(crate) path_or_id: String,
    /// View name to query.
    #[arg(long)]
    pub(crate) view_name: String,
    /// One-based page number.
    #[arg(long, default_value_t = 1)]
    pub(crate) page: u32,
    /// Page size.
    #[arg(long, default_value_t = 50)]
    pub(crate) page_size: u32,
}

#[derive(Debug, Clone, Args, Serialize, Deserialize)]
pub(crate) struct BaseSchemaArgs {
    /// Optional absolute vault root path. Falls back to config/env defaults.
    #[arg(long)]
    pub(crate) vault_root: Option<String>,
    /// Optional sqlite database file path override.
    #[arg(long)]
    pub(crate) db_path: Option<String>,
    /// Base id or normalized base file path.
    #[arg(long)]
    pub(crate) path_or_id: String,
}

#[derive(Debug, Clone, Args, Serialize, Deserialize)]
pub(crate) struct GraphWindowArgs {
    /// Optional absolute vault root path. Falls back to config/env defaults.
    #[arg(long)]
    pub(crate) vault_root: Option<String>,
    /// Optional sqlite database file path override.
    #[arg(long)]
    pub(crate) db_path: Option<String>,
    /// Window size.
    #[arg(long, default_value_t = 100)]
    pub(crate) limit: u32,
    /// Window offset.
    #[arg(long, default_value_t = 0)]
    pub(crate) offset: u32,
}

#[derive(Debug, Clone, Args, Serialize, Deserialize)]
pub(crate) struct GraphInboundScopeArgs {
    /// Optional absolute vault root path. Falls back to config/env defaults.
    #[arg(long)]
    pub(crate) vault_root: Option<String>,
    /// Optional sqlite database file path override.
    #[arg(long)]
    pub(crate) db_path: Option<String>,
    /// Vault-relative folder/file prefix to audit.
    #[arg(long)]
    pub(crate) scope: String,
    /// Include markdown files in scoped audit.
    #[arg(long, default_value_t = false)]
    pub(crate) include_markdown: bool,
    /// Include non-markdown files in scoped audit.
    #[arg(long, default_value_t = false)]
    pub(crate) include_non_md: bool,
    /// Optional exclude path prefixes (repeatable).
    #[arg(long)]
    pub(crate) exclude_prefix: Vec<String>,
    /// Window size.
    #[arg(long, default_value_t = 100)]
    pub(crate) limit: u32,
    /// Window offset.
    #[arg(long, default_value_t = 0)]
    pub(crate) offset: u32,
}

#[derive(Debug, Clone, Args, Serialize, Deserialize)]
pub(crate) struct GraphWalkArgs {
    /// Optional absolute vault root path. Falls back to config/env defaults.
    #[arg(long)]
    pub(crate) vault_root: Option<String>,
    /// Optional sqlite database file path override.
    #[arg(long)]
    pub(crate) db_path: Option<String>,
    /// Root note path.
    #[arg(long)]
    pub(crate) path: String,
    /// Maximum BFS depth.
    #[arg(long, default_value_t = 2)]
    pub(crate) depth: u32,
    /// Maximum row count.
    #[arg(long, default_value_t = 200)]
    pub(crate) limit: u32,
    /// Include unresolved links in traversal.
    #[arg(long, default_value_t = false)]
    pub(crate) include_unresolved: bool,
    /// Include folder hierarchy overlay edges.
    #[arg(long, default_value_t = false)]
    pub(crate) include_folders: bool,
}

#[derive(Debug, Clone, Args, Serialize, Deserialize)]
pub(crate) struct GraphComponentsArgs {
    /// Optional absolute vault root path. Falls back to config/env defaults.
    #[arg(long)]
    pub(crate) vault_root: Option<String>,
    /// Optional sqlite database file path override.
    #[arg(long)]
    pub(crate) db_path: Option<String>,
    /// Window size.
    #[arg(long, default_value_t = 100)]
    pub(crate) limit: u32,
    /// Window offset.
    #[arg(long, default_value_t = 0)]
    pub(crate) offset: u32,
    /// Include full member path list for each component (slower on large vaults).
    #[arg(long, default_value_t = false)]
    pub(crate) include_members: bool,
    /// Number of member paths to include when `--include-members` is not set.
    #[arg(long, default_value_t = 64)]
    pub(crate) sample_size: u32,
    /// Component mode selector: weak|strong.
    #[arg(long, default_value = "weak")]
    pub(crate) mode: String,
}

#[derive(Debug, Clone, Args, Serialize, Deserialize)]
pub(crate) struct GraphNeighborsArgs {
    /// Optional absolute vault root path. Falls back to config/env defaults.
    #[arg(long)]
    pub(crate) vault_root: Option<String>,
    /// Optional sqlite database file path override.
    #[arg(long)]
    pub(crate) db_path: Option<String>,
    /// Root note path.
    #[arg(long)]
    pub(crate) path: String,
    /// Direction selector: all|outgoing|incoming.
    #[arg(long, default_value = "all")]
    pub(crate) direction: String,
    /// Window size.
    #[arg(long, default_value_t = 100)]
    pub(crate) limit: u32,
    /// Window offset.
    #[arg(long, default_value_t = 0)]
    pub(crate) offset: u32,
}

#[derive(Debug, Clone, Args, Serialize, Deserialize)]
pub(crate) struct GraphPathArgs {
    /// Optional absolute vault root path. Falls back to config/env defaults.
    #[arg(long)]
    pub(crate) vault_root: Option<String>,
    /// Optional sqlite database file path override.
    #[arg(long)]
    pub(crate) db_path: Option<String>,
    /// Start note path.
    #[arg(long)]
    pub(crate) from: String,
    /// End note path.
    #[arg(long)]
    pub(crate) to: String,
    /// Maximum traversal depth.
    #[arg(long, default_value_t = 8)]
    pub(crate) max_depth: u32,
    /// Maximum number of explored nodes before abort.
    #[arg(long, default_value_t = 10_000)]
    pub(crate) max_nodes: u32,
}

#[derive(Debug, Clone, Args, Serialize, Deserialize)]
pub(crate) struct TaskListArgs {
    /// Optional absolute vault root path. Falls back to config/env defaults.
    #[arg(long)]
    pub(crate) vault_root: Option<String>,
    /// Optional sqlite database file path override.
    #[arg(long)]
    pub(crate) db_path: Option<String>,
    /// Optional state filter: open|done|cancelled.
    #[arg(long)]
    pub(crate) state: Option<String>,
    /// Optional text filter.
    #[arg(long)]
    pub(crate) query: Option<String>,
    /// Window size.
    #[arg(long, default_value_t = 100)]
    pub(crate) limit: u32,
    /// Window offset.
    #[arg(long, default_value_t = 0)]
    pub(crate) offset: u32,
}

#[derive(Debug, Clone, Args, Serialize, Deserialize)]
pub(crate) struct TaskSetStateArgs {
    /// Optional absolute vault root path. Falls back to config/env defaults.
    #[arg(long)]
    pub(crate) vault_root: Option<String>,
    /// Optional sqlite database file path override.
    #[arg(long)]
    pub(crate) db_path: Option<String>,
    /// Vault-relative normalized note path.
    #[arg(long)]
    pub(crate) path: String,
    /// One-based line number of task.
    #[arg(long)]
    pub(crate) line: usize,
    /// Target state: open|done|cancelled.
    #[arg(long)]
    pub(crate) state: String,
}

#[derive(Debug, Clone, Args, Serialize, Deserialize)]
pub(crate) struct QueryArgs {
    /// Optional absolute vault root path. Falls back to config/env defaults.
    #[arg(long)]
    pub(crate) vault_root: Option<String>,
    /// Optional sqlite database file path override.
    #[arg(long)]
    pub(crate) db_path: Option<String>,
    /// Scope selector: docs|graph|task|meta:tags|meta:aliases|meta:properties|base:<id-or-path>
    #[arg(long)]
    pub(crate) from: String,
    /// Optional free text query.
    #[arg(long)]
    pub(crate) query: Option<String>,
    /// Optional note path (for graph outgoing/backlinks).
    #[arg(long)]
    pub(crate) path: Option<String>,
    /// Optional base view name when `--from base:<id>`.
    #[arg(long)]
    pub(crate) view_name: Option<String>,
    /// Optional projected column list for docs scope (`file_id,path,title,matched_in`).
    #[arg(long)]
    pub(crate) select: Option<String>,
    /// Optional typed where expression, for example: `priority >= 2 and done == false`.
    #[arg(long = "where")]
    pub(crate) where_clause: Option<String>,
    /// Optional multi-key sort expression, for example: `priority:desc:nulls_last,path:asc`.
    #[arg(long)]
    pub(crate) sort: Option<String>,
    /// Return logical/physical plan metadata.
    #[arg(long, default_value_t = false)]
    pub(crate) explain: bool,
    /// Execute query rows when used with `--explain`.
    #[arg(long, default_value_t = false)]
    pub(crate) execute: bool,
    /// Window size.
    #[arg(long, default_value_t = 100)]
    pub(crate) limit: u32,
    /// Window offset.
    #[arg(long, default_value_t = 0)]
    pub(crate) offset: u32,
}

#[derive(Debug, Clone, Args, Serialize, Deserialize)]
pub(crate) struct DaemonSocketArgs {
    /// Optional unix domain socket path override for tao daemon.
    #[arg(long)]
    pub(crate) socket: Option<String>,
    /// Optional absolute vault root path used to derive deterministic daemon socket.
    #[arg(long)]
    pub(crate) vault_root: Option<String>,
    /// Optional sqlite database file path override used with `--vault-root`.
    #[arg(long)]
    pub(crate) db_path: Option<String>,
}

#[derive(Debug, Clone, Args, Serialize, Deserialize)]
pub(crate) struct DaemonStartArgs {
    /// Optional unix domain socket path override for tao daemon.
    #[arg(long)]
    pub(crate) socket: Option<String>,
    /// Optional absolute vault root path used to derive deterministic daemon socket.
    #[arg(long)]
    pub(crate) vault_root: Option<String>,
    /// Optional sqlite database file path override used with `--vault-root`.
    #[arg(long)]
    pub(crate) db_path: Option<String>,
    /// Run daemon in foreground (blocks current process).
    #[arg(long, default_value_t = false)]
    pub(crate) foreground: bool,
    /// Maximum wait window for daemon startup when backgrounded.
    #[arg(long, default_value_t = DEFAULT_DAEMON_STARTUP_TIMEOUT_MS)]
    pub(crate) startup_timeout_ms: u64,
}

#[derive(Debug, Clone, Args, Serialize, Deserialize)]
pub(crate) struct DaemonStopAllArgs {
    /// Optional daemon socket directory override.
    #[arg(long)]
    pub(crate) socket_dir: Option<String>,
}

impl VaultPathArgs {
    pub(crate) fn resolve(&self) -> Result<ResolvedVaultPathArgs> {
        resolve_vault_paths(self.vault_root.as_deref(), self.db_path.as_deref())
    }
}

impl NotePathArgs {
    pub(crate) fn resolve(&self) -> Result<ResolvedVaultPathArgs> {
        resolve_vault_paths(self.vault_root.as_deref(), self.db_path.as_deref())
    }
}

impl NotePutArgs {
    pub(crate) fn resolve(&self) -> Result<ResolvedVaultPathArgs> {
        resolve_vault_paths(self.vault_root.as_deref(), self.db_path.as_deref())
    }
}

impl BaseViewArgs {
    pub(crate) fn resolve(&self) -> Result<ResolvedVaultPathArgs> {
        resolve_vault_paths(self.vault_root.as_deref(), self.db_path.as_deref())
    }
}

impl BaseSchemaArgs {
    pub(crate) fn resolve(&self) -> Result<ResolvedVaultPathArgs> {
        resolve_vault_paths(self.vault_root.as_deref(), self.db_path.as_deref())
    }
}

impl GraphWindowArgs {
    pub(crate) fn resolve(&self) -> Result<ResolvedVaultPathArgs> {
        resolve_vault_paths(self.vault_root.as_deref(), self.db_path.as_deref())
    }
}

impl GraphInboundScopeArgs {
    pub(crate) fn resolve(&self) -> Result<ResolvedVaultPathArgs> {
        resolve_vault_paths(self.vault_root.as_deref(), self.db_path.as_deref())
    }
}

impl GraphWalkArgs {
    pub(crate) fn resolve(&self) -> Result<ResolvedVaultPathArgs> {
        resolve_vault_paths(self.vault_root.as_deref(), self.db_path.as_deref())
    }
}

impl GraphComponentsArgs {
    pub(crate) fn resolve(&self) -> Result<ResolvedVaultPathArgs> {
        resolve_vault_paths(self.vault_root.as_deref(), self.db_path.as_deref())
    }
}

impl GraphNeighborsArgs {
    pub(crate) fn resolve(&self) -> Result<ResolvedVaultPathArgs> {
        resolve_vault_paths(self.vault_root.as_deref(), self.db_path.as_deref())
    }
}

impl GraphPathArgs {
    pub(crate) fn resolve(&self) -> Result<ResolvedVaultPathArgs> {
        resolve_vault_paths(self.vault_root.as_deref(), self.db_path.as_deref())
    }
}

impl TaskListArgs {
    pub(crate) fn resolve(&self) -> Result<ResolvedVaultPathArgs> {
        resolve_vault_paths(self.vault_root.as_deref(), self.db_path.as_deref())
    }
}

impl TaskSetStateArgs {
    pub(crate) fn resolve(&self) -> Result<ResolvedVaultPathArgs> {
        resolve_vault_paths(self.vault_root.as_deref(), self.db_path.as_deref())
    }
}

impl QueryArgs {
    pub(crate) fn resolve(&self) -> Result<ResolvedVaultPathArgs> {
        resolve_vault_paths(self.vault_root.as_deref(), self.db_path.as_deref())
    }
}

impl DaemonSocketArgs {
    pub(crate) fn resolve_socket(&self) -> Result<String> {
        resolve_daemon_socket(
            self.socket.as_deref(),
            self.vault_root.as_deref(),
            self.db_path.as_deref(),
        )
    }
}

impl DaemonStartArgs {
    pub(crate) fn resolve_socket(&self) -> Result<String> {
        resolve_daemon_socket(
            self.socket.as_deref(),
            self.vault_root.as_deref(),
            self.db_path.as_deref(),
        )
    }
}
