use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::io::{Read, Write};
#[cfg(unix)]
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::process::{Command as ProcessCommand, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use clap::{Args, Parser, Subcommand};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use tao_sdk_bases::{
    BaseDocument, BaseTableQueryPlanner, BaseViewRegistry, TableQueryPlanRequest,
    parse_base_document,
};
use tao_sdk_bridge::{BridgeEnvelope, BridgeKernel};
use tao_sdk_search::{SearchQueryRequest, SearchQueryService};
use tao_sdk_service::{
    BacklinkGraphService, BaseTableExecutionOptions, BaseTableExecutorService, GraphWalkDirection,
    GraphWalkRequest, HealthSnapshotService, SdkConfigLoader, SdkConfigOverrides, WatcherStatus,
};
use tao_sdk_storage::{
    BasesRepository, PropertiesRepository, TasksRepository, preflight_migrations, run_migrations,
};
use tao_sdk_vault::CasePolicy;
use tao_sdk_watch::WatchReconcileService;

const DEFAULT_DAEMON_SOCKET: &str = "/tmp/tao-daemon.sock";

#[derive(Debug, Clone, Parser, Serialize, Deserialize)]
#[command(name = "tao", version, about = "tao cli")]
struct Cli {
    /// Emit one JSON envelope to stdout.
    #[arg(long, global = true)]
    json: bool,
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
}

#[derive(Debug, Clone, Args, Serialize, Deserialize)]
struct VaultPathArgs {
    /// Absolute vault root path.
    #[arg(long)]
    vault_root: String,
    /// Optional sqlite database file path override.
    #[arg(long)]
    db_path: Option<String>,
}

#[derive(Debug, Clone, Args, Serialize, Deserialize)]
struct NotePathArgs {
    /// Absolute vault root path.
    #[arg(long)]
    vault_root: String,
    /// Optional sqlite database file path override.
    #[arg(long)]
    db_path: Option<String>,
    /// Vault-relative normalized note path.
    #[arg(long)]
    path: String,
}

#[derive(Debug, Clone, Args, Serialize, Deserialize)]
struct NotePutArgs {
    /// Absolute vault root path.
    #[arg(long)]
    vault_root: String,
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
    /// Absolute vault root path.
    #[arg(long)]
    vault_root: String,
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
    /// Absolute vault root path.
    #[arg(long)]
    vault_root: String,
    /// Optional sqlite database file path override.
    #[arg(long)]
    db_path: Option<String>,
    /// Base id or normalized base file path.
    #[arg(long)]
    path_or_id: String,
}

#[derive(Debug, Clone, Args, Serialize, Deserialize)]
struct GraphWindowArgs {
    /// Absolute vault root path.
    #[arg(long)]
    vault_root: String,
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
    /// Absolute vault root path.
    #[arg(long)]
    vault_root: String,
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
}

#[derive(Debug, Clone, Args, Serialize, Deserialize)]
struct GraphComponentsArgs {
    /// Absolute vault root path.
    #[arg(long)]
    vault_root: String,
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
}

#[derive(Debug, Clone, Args, Serialize, Deserialize)]
struct TaskListArgs {
    /// Absolute vault root path.
    #[arg(long)]
    vault_root: String,
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
    /// Absolute vault root path.
    #[arg(long)]
    vault_root: String,
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
    /// Absolute vault root path.
    #[arg(long)]
    vault_root: String,
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
    /// Window size.
    #[arg(long, default_value_t = 100)]
    limit: u32,
    /// Window offset.
    #[arg(long, default_value_t = 0)]
    offset: u32,
}

#[derive(Debug, Clone, Args, Serialize, Deserialize)]
struct DaemonSocketArgs {
    /// Unix domain socket path for tao daemon.
    #[arg(long, default_value = DEFAULT_DAEMON_SOCKET)]
    socket: String,
}

#[derive(Debug, Clone, Args, Serialize, Deserialize)]
struct DaemonStartArgs {
    /// Unix domain socket path for tao daemon.
    #[arg(long, default_value = DEFAULT_DAEMON_SOCKET)]
    socket: String,
    /// Run daemon in foreground (blocks current process).
    #[arg(long, default_value_t = false)]
    foreground: bool,
    /// Maximum wait window for daemon startup when backgrounded.
    #[arg(long, default_value_t = 5_000)]
    startup_timeout_ms: u64,
}

impl VaultPathArgs {
    fn resolve(&self) -> Result<ResolvedVaultPathArgs> {
        resolve_vault_paths(&self.vault_root, self.db_path.as_deref())
    }
}

impl NotePathArgs {
    fn resolve(&self) -> Result<ResolvedVaultPathArgs> {
        resolve_vault_paths(&self.vault_root, self.db_path.as_deref())
    }
}

impl NotePutArgs {
    fn resolve(&self) -> Result<ResolvedVaultPathArgs> {
        resolve_vault_paths(&self.vault_root, self.db_path.as_deref())
    }
}

impl BaseViewArgs {
    fn resolve(&self) -> Result<ResolvedVaultPathArgs> {
        resolve_vault_paths(&self.vault_root, self.db_path.as_deref())
    }
}

impl BaseSchemaArgs {
    fn resolve(&self) -> Result<ResolvedVaultPathArgs> {
        resolve_vault_paths(&self.vault_root, self.db_path.as_deref())
    }
}

impl GraphWindowArgs {
    fn resolve(&self) -> Result<ResolvedVaultPathArgs> {
        resolve_vault_paths(&self.vault_root, self.db_path.as_deref())
    }
}

impl GraphWalkArgs {
    fn resolve(&self) -> Result<ResolvedVaultPathArgs> {
        resolve_vault_paths(&self.vault_root, self.db_path.as_deref())
    }
}

impl GraphComponentsArgs {
    fn resolve(&self) -> Result<ResolvedVaultPathArgs> {
        resolve_vault_paths(&self.vault_root, self.db_path.as_deref())
    }
}

impl TaskListArgs {
    fn resolve(&self) -> Result<ResolvedVaultPathArgs> {
        resolve_vault_paths(&self.vault_root, self.db_path.as_deref())
    }
}

impl TaskSetStateArgs {
    fn resolve(&self) -> Result<ResolvedVaultPathArgs> {
        resolve_vault_paths(&self.vault_root, self.db_path.as_deref())
    }
}

impl QueryArgs {
    fn resolve(&self) -> Result<ResolvedVaultPathArgs> {
        resolve_vault_paths(&self.vault_root, self.db_path.as_deref())
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
    db_path: String,
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
    command_results: HashMap<String, CommandResult>,
}

#[derive(Debug)]
enum RuntimeMode {
    OneShot,
    Daemon(RuntimeCache),
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum DaemonRequest {
    Execute { payload: DaemonExecuteRequest },
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
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    if let Some(output) = maybe_forward_to_daemon(&cli)? {
        println!("{output}");
        return Ok(());
    }

    let result = dispatch(cli.command, cli.allow_writes)?;
    let output = render_output(cli.json, &result)?;
    println!("{output}");
    Ok(())
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
        Commands::Doc { command } => handle_doc(command, allow_writes, runtime),
        Commands::Base { command } => handle_base(command, runtime),
        Commands::Graph { command } => handle_graph(command, runtime),
        Commands::Meta { command } => handle_meta(command, runtime),
        Commands::Task { command } => handle_task(command, allow_writes, runtime),
        Commands::Query(args) => handle_query(args, runtime),
        Commands::Vault { command } => handle_vault(command, runtime),
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
            ensure_writes_enabled(allow_writes, "doc.write")?;
            let resolved = args.resolve()?;
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
            let (total, rows) = with_connection(runtime, &resolved, |connection| {
                Ok(BacklinkGraphService.components_page(
                    connection,
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
                    "total": total,
                    "limit": args.limit,
                    "offset": args.offset,
                    "include_members": args.include_members,
                    "sample_size": args.sample_size,
                    "items": items,
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
                    serde_json::json!({
                        "depth": step.depth,
                        "direction": direction,
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
            ensure_writes_enabled(allow_writes, "task.set-state")?;
            let resolved = args.resolve()?;
            let absolute = Path::new(&resolved.vault_root).join(&args.path);
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
                        CasePolicy::Sensitive,
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
    if from.eq_ignore_ascii_case("docs") {
        let resolved = args.resolve()?;
        let page = with_connection(runtime, &resolved, |connection| {
            Ok(SearchQueryService.query(
                Path::new(&resolved.vault_root),
                connection,
                SearchQueryRequest {
                    query: args.query.clone().unwrap_or_default(),
                    limit: u64::from(limit),
                    offset: u64::from(args.offset),
                },
            )?)
        })
        .map_err(|source| anyhow!("query docs failed: {source}"))?;
        let rows = page
            .items
            .into_iter()
            .map(|item| {
                serde_json::json!({
                    "file_id": item.file_id,
                    "path": item.path,
                    "title": item.title,
                    "matched_in": item.matched_in,
                })
            })
            .collect::<Vec<_>>();
        return Ok(CommandResult {
            command: "query.run".to_string(),
            summary: "query run completed".to_string(),
            args: serde_json::json!({
                "from": "docs",
                "columns": ["file_id", "path", "title", "matched_in"],
                "rows": rows,
                "total": page.total,
                "limit": page.limit,
                "offset": page.offset,
            }),
        });
    }

    if let Some(base_id_or_path) = from.strip_prefix("base:") {
        let view_name = args
            .view_name
            .clone()
            .ok_or_else(|| anyhow!("query base scope requires --view-name"))?;
        let result = handle_base(
            BaseCommands::View(BaseViewArgs {
                vault_root: args.vault_root.clone(),
                db_path: args.db_path.clone(),
                path_or_id: base_id_or_path.to_string(),
                view_name,
                page: (args.offset / limit) + 1,
                page_size: limit,
            }),
            runtime,
        )?;
        return Ok(retag_result(result, "query.run", "query run completed"));
    }

    if from.eq_ignore_ascii_case("graph") {
        let graph_result = if let Some(path) = &args.path {
            handle_graph(
                GraphCommands::Outgoing(NotePathArgs {
                    vault_root: args.vault_root.clone(),
                    db_path: args.db_path.clone(),
                    path: path.clone(),
                }),
                runtime,
            )?
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
                        CasePolicy::Sensitive,
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
                        CasePolicy::Sensitive,
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
            run_daemon_server(&args.socket)?;
            Ok(CommandResult {
                command: "vault.daemon.serve".to_string(),
                summary: "vault daemon serve stopped".to_string(),
                args: serde_json::json!({
                    "socket": args.socket,
                    "stopped": true,
                }),
            })
        }
    }
}

fn maybe_forward_to_daemon(cli: &Cli) -> Result<Option<String>> {
    let Some(socket) = cli.daemon_socket.as_deref() else {
        return Ok(None);
    };
    if is_daemon_control_command(&cli.command) {
        return Ok(None);
    }

    let response = daemon_request(
        socket,
        &DaemonRequest::Execute {
            payload: DaemonExecuteRequest {
                command: cli.command.clone(),
                allow_writes: cli.allow_writes,
                json: cli.json,
            },
        },
    )?;
    if !response.ok {
        let message = response
            .error
            .unwrap_or_else(|| "daemon returned unknown failure".to_string());
        return Err(anyhow!(message));
    }
    response
        .output
        .map(Some)
        .ok_or_else(|| anyhow!("daemon execute response missing output payload"))
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

fn handle_daemon(command: DaemonCommands) -> Result<CommandResult> {
    match command {
        DaemonCommands::Start(args) => {
            if args.foreground {
                run_daemon_server(&args.socket)?;
                return Ok(CommandResult {
                    command: "vault.daemon.start".to_string(),
                    summary: "vault daemon foreground session stopped".to_string(),
                    args: serde_json::json!({
                        "socket": args.socket,
                        "foreground": true,
                        "stopped": true,
                    }),
                });
            }

            let status = daemon_status_probe(&args.socket)?;
            if status.is_some() {
                return Ok(CommandResult {
                    command: "vault.daemon.start".to_string(),
                    summary: "vault daemon already running".to_string(),
                    args: serde_json::json!({
                        "socket": args.socket,
                        "started": false,
                        "already_running": true,
                    }),
                });
            }

            let current_exe = std::env::current_exe().context("resolve current executable path")?;
            let child = ProcessCommand::new(current_exe)
                .arg("vault")
                .arg("daemon-serve")
                .arg("--socket")
                .arg(&args.socket)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .with_context(|| format!("spawn background daemon at '{}'", args.socket))?;
            let pid = child.id();
            wait_for_daemon_startup(&args.socket, args.startup_timeout_ms)?;

            Ok(CommandResult {
                command: "vault.daemon.start".to_string(),
                summary: "vault daemon started".to_string(),
                args: serde_json::json!({
                    "socket": args.socket,
                    "started": true,
                    "pid": pid,
                }),
            })
        }
        DaemonCommands::Status(args) => {
            let status = daemon_status_probe(&args.socket)?;
            match status {
                Some(status) => Ok(CommandResult {
                    command: "vault.daemon.status".to_string(),
                    summary: "vault daemon status completed".to_string(),
                    args: serde_json::json!({
                        "socket": args.socket,
                        "running": true,
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
                        "socket": args.socket,
                        "running": false,
                    }),
                }),
            }
        }
        DaemonCommands::Stop(args) => {
            let status = daemon_status_probe(&args.socket)?;
            if status.is_none() {
                return Ok(CommandResult {
                    command: "vault.daemon.stop".to_string(),
                    summary: "vault daemon stop completed".to_string(),
                    args: serde_json::json!({
                        "socket": args.socket,
                        "stopped": false,
                        "running": false,
                    }),
                });
            }

            let response = daemon_request(&args.socket, &DaemonRequest::Shutdown)?;
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
                    "socket": args.socket,
                    "stopped": true,
                }),
            })
        }
    }
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

fn daemon_socket_is_unavailable(error: &anyhow::Error) -> bool {
    for source in error.chain() {
        if let Some(io_error) = source.downcast_ref::<std::io::Error>()
            && matches!(
                io_error.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
            )
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
        let socket_path = Path::new(socket);
        if let Some(parent) = socket_path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("create daemon socket parent '{}'", parent.display()))?;
        }
        if socket_path.exists() {
            fs::remove_file(socket_path)
                .with_context(|| format!("remove stale daemon socket '{}'", socket))?;
        }

        let listener = UnixListener::bind(socket_path)
            .with_context(|| format!("bind daemon socket '{socket}'"))?;
        let mut runtime = RuntimeMode::Daemon(RuntimeCache::default());
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
                    let cache_key = if cacheable {
                        daemon_cache_key(&payload.command).ok()
                    } else {
                        None
                    };
                    let cached =
                        if let (RuntimeMode::Daemon(cache), Some(key)) = (&runtime, &cache_key) {
                            cache.command_results.get(key).cloned()
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
                                    cache.command_results.insert(key, result.clone());
                                } else {
                                    cache.command_results.clear();
                                }
                            }

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
            fs::remove_file(socket_path)
                .with_context(|| format!("remove daemon socket '{}'", socket_path.display()))?;
        }
        Ok(())
    }
}

fn ensure_writes_enabled(allow_writes: bool, command: &str) -> Result<()> {
    if allow_writes {
        return Ok(());
    }
    Err(anyhow!(
        "{command} is disabled by default; pass --allow-writes to enable vault content mutations"
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

fn runtime_cache_key(args: &ResolvedVaultPathArgs) -> String {
    format!("{}\u{1f}{}", args.vault_root, args.db_path)
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
    vault_root: &str,
    db_path_override: Option<&str>,
) -> Result<ResolvedVaultPathArgs> {
    let config = SdkConfigLoader::load(SdkConfigOverrides {
        vault_root: Some(PathBuf::from(vault_root)),
        db_path: db_path_override.map(PathBuf::from),
        ..SdkConfigOverrides::default()
    })
    .map_err(|source| anyhow!("resolve sdk config failed: {source}"))?;

    Ok(ResolvedVaultPathArgs {
        vault_root: config.vault_root.to_string_lossy().to_string(),
        db_path: config.db_path.to_string_lossy().to_string(),
    })
}

fn open_bridge_kernel(args: &ResolvedVaultPathArgs) -> Result<BridgeKernel> {
    BridgeKernel::open(&args.vault_root, &args.db_path)
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

    let mut connection = Connection::open(&args.db_path)
        .with_context(|| format!("open sqlite database '{}'", args.db_path))?;
    run_migrations(&mut connection).map_err(|source| anyhow!("run migrations failed: {source}"))?;
    Ok(connection)
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
    use std::path::Path;
    use std::sync::{Mutex, OnceLock};

    use super::{
        Cli, Commands, DocCommands, NotePutArgs, QueryArgs, command_is_cacheable, dispatch,
        maybe_forward_to_daemon, render_output,
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
    fn daemon_forwarding_reports_missing_socket_for_non_control_commands() {
        let cli = Cli::parse_from([
            "tao",
            "--json",
            "--daemon-socket",
            "/tmp/does-not-exist.sock",
            "vault",
            "open",
            "--vault-root",
            "/tmp",
        ]);
        let error = maybe_forward_to_daemon(&cli).expect_err("forwarding should fail");
        assert!(
            error.to_string().contains("connect daemon socket"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn daemon_cacheability_matrix_blocks_mutating_commands() {
        let cacheable_query = Commands::Query(QueryArgs {
            vault_root: "/tmp".to_string(),
            db_path: None,
            from: "docs".to_string(),
            query: Some("project".to_string()),
            path: None,
            view_name: None,
            limit: 10,
            offset: 0,
        });
        assert!(command_is_cacheable(&cacheable_query));

        let doc_write = Commands::Doc {
            command: DocCommands::Write(NotePutArgs {
                vault_root: "/tmp".to_string(),
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
}
