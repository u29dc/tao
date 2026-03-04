use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use clap::{Args, Parser, Subcommand};
use rusqlite::Connection;
use serde::Serialize;
use serde_json::Value as JsonValue;
use tao_sdk_bases::{
    BaseDocument, BaseTableQueryPlanner, BaseViewRegistry, TableQueryPlanRequest,
    parse_base_document,
};
use tao_sdk_bridge::{BridgeEnvelope, BridgeKernel};
use tao_sdk_properties::TypedPropertyValue;
use tao_sdk_search::{SearchQueryRequest, SearchQueryService};
use tao_sdk_service::{
    BacklinkGraphService, BaseTableExecutorService, HealthSnapshotService, PropertyUpdateService,
    SdkConfigLoader, SdkConfigOverrides, WatcherStatus,
};
use tao_sdk_storage::{
    BasesRepository, FilesRepository, PropertiesRepository, TasksRepository, preflight_migrations,
    run_migrations,
};
use tao_sdk_vault::CasePolicy;
use tao_sdk_watch::WatchReconcileService;

#[derive(Debug, Parser)]
#[command(name = "tao", version, about = "tao cli")]
struct Cli {
    /// Emit one JSON envelope to stdout.
    #[arg(long, global = true)]
    json: bool,
    /// Allow vault content write operations (disabled by default).
    #[arg(long, global = true, default_value_t = false)]
    allow_writes: bool,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
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
    /// Note read/write and listing operations.
    Note {
        #[command(subcommand)]
        command: NoteCommands,
    },
    /// Link graph operations.
    Links {
        #[command(subcommand)]
        command: LinksCommands,
    },
    /// Frontmatter property operations.
    Properties {
        #[command(subcommand)]
        command: PropertiesCommands,
    },
    /// Base metadata and table operations.
    Bases {
        #[command(subcommand)]
        command: BasesCommands,
    },
    /// Search operations.
    Search {
        #[command(subcommand)]
        command: SearchCommands,
    },
}

#[derive(Debug, Subcommand)]
enum DocCommands {
    /// Return one note by normalized path.
    Read(NotePathArgs),
    /// Create or update one note.
    Write(NotePutArgs),
    /// List markdown note windows.
    List(VaultPathArgs),
}

#[derive(Debug, Subcommand)]
enum BaseCommands {
    /// List indexed bases.
    List(VaultPathArgs),
    /// Query one base table view.
    View(BaseViewArgs),
    /// Return one base schema contract.
    Schema(BaseSchemaArgs),
}

#[derive(Debug, Subcommand)]
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
    Components(GraphWindowArgs),
    /// Walk graph neighbors from one root note.
    Walk(GraphWalkArgs),
}

#[derive(Debug, Subcommand)]
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

#[derive(Debug, Subcommand)]
enum TaskCommands {
    /// List extracted markdown tasks.
    List(TaskListArgs),
    /// Update checkbox state on one task line.
    SetState(TaskSetStateArgs),
}

#[derive(Debug, Subcommand)]
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
}

#[derive(Debug, Subcommand)]
enum NoteCommands {
    /// Return one note by normalized path.
    Get(NotePathArgs),
    /// Create or update one note.
    Put(NotePutArgs),
    /// List markdown note windows.
    List(VaultPathArgs),
}

#[derive(Debug, Subcommand)]
enum LinksCommands {
    /// Return outgoing links for one note.
    Outgoing(NotePathArgs),
    /// Return backlinks for one note.
    Backlinks(NotePathArgs),
}

#[derive(Debug, Subcommand)]
enum PropertiesCommands {
    /// Return parsed properties for one note.
    Get(NotePathArgs),
    /// Set one property key/value for one note.
    Set(PropertySetArgs),
}

#[derive(Debug, Subcommand)]
enum BasesCommands {
    /// List indexed bases.
    List(VaultPathArgs),
    /// Query one base table view.
    View(BaseViewArgs),
}

#[derive(Debug, Subcommand)]
enum SearchCommands {
    /// Run one search query over indexed content.
    Query(SearchQueryArgs),
}

#[derive(Debug, Clone, Args, Serialize)]
struct VaultPathArgs {
    /// Absolute vault root path.
    #[arg(long)]
    vault_root: String,
    /// Optional sqlite database file path override.
    #[arg(long)]
    db_path: Option<String>,
}

#[derive(Debug, Clone, Args, Serialize)]
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

#[derive(Debug, Clone, Args, Serialize)]
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

#[derive(Debug, Clone, Args, Serialize)]
struct PropertySetArgs {
    /// Absolute vault root path.
    #[arg(long)]
    vault_root: String,
    /// Optional sqlite database file path override.
    #[arg(long)]
    db_path: Option<String>,
    /// Vault-relative normalized note path.
    #[arg(long)]
    path: String,
    /// Property key.
    #[arg(long)]
    key: String,
    /// Property value payload as string.
    #[arg(long)]
    value: String,
}

#[derive(Debug, Clone, Args, Serialize)]
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

#[derive(Debug, Clone, Args, Serialize)]
struct SearchQueryArgs {
    /// Absolute vault root path.
    #[arg(long)]
    vault_root: String,
    /// Optional sqlite database file path override.
    #[arg(long)]
    db_path: Option<String>,
    /// Query text.
    #[arg(long)]
    query: String,
    /// Window size.
    #[arg(long, default_value_t = 50)]
    limit: u32,
    /// Window offset.
    #[arg(long, default_value_t = 0)]
    offset: u32,
}

#[derive(Debug, Clone, Args, Serialize)]
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

#[derive(Debug, Clone, Args, Serialize)]
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

#[derive(Debug, Clone, Args, Serialize)]
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

#[derive(Debug, Clone, Args, Serialize)]
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

#[derive(Debug, Clone, Args, Serialize)]
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

#[derive(Debug, Clone, Args, Serialize)]
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

impl PropertySetArgs {
    fn resolve(&self) -> Result<ResolvedVaultPathArgs> {
        resolve_vault_paths(&self.vault_root, self.db_path.as_deref())
    }
}

impl BaseViewArgs {
    fn resolve(&self) -> Result<ResolvedVaultPathArgs> {
        resolve_vault_paths(&self.vault_root, self.db_path.as_deref())
    }
}

impl SearchQueryArgs {
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

#[derive(Debug, Serialize)]
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

#[derive(Debug, Clone)]
struct ResolvedEdge {
    link_id: String,
    source_file_id: String,
    source_path: String,
    target_file_id: Option<String>,
    target_path: Option<String>,
    raw_target: String,
    is_unresolved: bool,
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
    let result = dispatch(cli.command, cli.allow_writes)?;
    let output = render_output(cli.json, &result)?;
    println!("{output}");
    Ok(())
}

fn dispatch(command: Commands, allow_writes: bool) -> Result<CommandResult> {
    match command {
        Commands::Doc { command } => handle_doc(command, allow_writes),
        Commands::Base { command } => handle_base(command),
        Commands::Graph { command } => handle_graph(command),
        Commands::Meta { command } => handle_meta(command),
        Commands::Task { command } => handle_task(command, allow_writes),
        Commands::Query(args) => handle_query(args),
        Commands::Vault { command } => handle_vault(command),
        Commands::Note { command } => handle_note(command, allow_writes),
        Commands::Links { command } => handle_links(command),
        Commands::Properties { command } => handle_properties(command, allow_writes),
        Commands::Bases { command } => handle_bases(command),
        Commands::Search { command } => handle_search(command),
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

fn handle_doc(command: DocCommands, allow_writes: bool) -> Result<CommandResult> {
    match command {
        DocCommands::Read(args) => Ok(retag_result(
            handle_note(NoteCommands::Get(args), allow_writes)?,
            "doc.read",
            "doc read completed",
        )),
        DocCommands::Write(args) => Ok(retag_result(
            handle_note(NoteCommands::Put(args), allow_writes)?,
            "doc.write",
            "doc write completed",
        )),
        DocCommands::List(args) => Ok(retag_result(
            handle_note(NoteCommands::List(args), allow_writes)?,
            "doc.list",
            "doc list completed",
        )),
    }
}

fn handle_base(command: BaseCommands) -> Result<CommandResult> {
    match command {
        BaseCommands::List(args) => Ok(retag_result(
            handle_bases(BasesCommands::List(args))?,
            "base.list",
            "base list completed",
        )),
        BaseCommands::View(args) => Ok(retag_result(
            handle_bases(BasesCommands::View(args))?,
            "base.view",
            "base view completed",
        )),
        BaseCommands::Schema(args) => {
            let resolved = args.resolve()?;
            let connection = open_initialized_connection(&resolved)?;
            let base = BasesRepository::list_with_paths(&connection)
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

fn handle_graph(command: GraphCommands) -> Result<CommandResult> {
    match command {
        GraphCommands::Outgoing(args) => Ok(retag_result(
            handle_links(LinksCommands::Outgoing(args))?,
            "graph.outgoing",
            "graph outgoing completed",
        )),
        GraphCommands::Backlinks(args) => Ok(retag_result(
            handle_links(LinksCommands::Backlinks(args))?,
            "graph.backlinks",
            "graph backlinks completed",
        )),
        GraphCommands::Unresolved(args) => {
            let resolved = args.resolve()?;
            let connection = open_initialized_connection(&resolved)?;
            let (total, rows) = BacklinkGraphService
                .unresolved_links_page(&connection, args.limit, args.offset)
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
            let connection = open_initialized_connection(&resolved)?;
            let (paths_by_id, edges) = load_graph_snapshot(&connection)?;
            let mut incoming_counts = HashMap::<String, usize>::new();
            let mut outgoing_counts = HashMap::<String, usize>::new();
            for edge in edges.iter().filter(|edge| !edge.is_unresolved) {
                *outgoing_counts
                    .entry(edge.source_file_id.clone())
                    .or_insert(0) += 1;
                if let Some(target_id) = &edge.target_file_id {
                    *incoming_counts.entry(target_id.clone()).or_insert(0) += 1;
                }
            }
            let mut items = paths_by_id
                .iter()
                .filter_map(|(file_id, path)| {
                    let incoming = *incoming_counts.get(file_id).unwrap_or(&0);
                    let outgoing = *outgoing_counts.get(file_id).unwrap_or(&0);
                    (incoming > 0 && outgoing == 0).then_some(serde_json::json!({
                        "file_id": file_id,
                        "path": path,
                        "incoming_resolved": incoming,
                        "outgoing_resolved": outgoing,
                    }))
                })
                .collect::<Vec<_>>();
            items.sort_by(|left, right| {
                left["path"]
                    .as_str()
                    .unwrap_or_default()
                    .cmp(right["path"].as_str().unwrap_or_default())
            });
            let total = items.len();
            let items = paginate_json_items(items, args.limit, args.offset);
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
            let connection = open_initialized_connection(&resolved)?;
            let (paths_by_id, edges) = load_graph_snapshot(&connection)?;
            let mut degree = HashMap::<String, usize>::new();
            for edge in edges.iter().filter(|edge| !edge.is_unresolved) {
                *degree.entry(edge.source_file_id.clone()).or_insert(0) += 1;
                if let Some(target_id) = &edge.target_file_id {
                    *degree.entry(target_id.clone()).or_insert(0) += 1;
                }
            }
            let mut items = paths_by_id
                .iter()
                .filter_map(|(file_id, path)| {
                    (*degree.get(file_id).unwrap_or(&0) == 0).then_some(serde_json::json!({
                        "file_id": file_id,
                        "path": path,
                    }))
                })
                .collect::<Vec<_>>();
            items.sort_by(|left, right| {
                left["path"]
                    .as_str()
                    .unwrap_or_default()
                    .cmp(right["path"].as_str().unwrap_or_default())
            });
            let total = items.len();
            let items = paginate_json_items(items, args.limit, args.offset);
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
            let connection = open_initialized_connection(&resolved)?;
            let (paths_by_id, edges) = load_graph_snapshot(&connection)?;
            let mut adjacency = HashMap::<String, Vec<String>>::new();
            for edge in edges.iter().filter(|edge| !edge.is_unresolved) {
                if let Some(target_id) = &edge.target_file_id {
                    adjacency
                        .entry(edge.source_file_id.clone())
                        .or_default()
                        .push(target_id.clone());
                    adjacency
                        .entry(target_id.clone())
                        .or_default()
                        .push(edge.source_file_id.clone());
                }
            }
            let mut visited = HashSet::<String>::new();
            let mut components = Vec::new();
            let mut ids = paths_by_id.keys().cloned().collect::<Vec<_>>();
            ids.sort();
            for root in ids {
                if visited.contains(&root) {
                    continue;
                }
                let mut queue = VecDeque::from([root.clone()]);
                let mut members = Vec::new();
                visited.insert(root.clone());
                while let Some(current) = queue.pop_front() {
                    members.push(current.clone());
                    for next in adjacency.get(&current).into_iter().flatten() {
                        if visited.insert(next.clone()) {
                            queue.push_back(next.clone());
                        }
                    }
                }
                let mut paths = members
                    .iter()
                    .filter_map(|file_id| paths_by_id.get(file_id).cloned())
                    .collect::<Vec<_>>();
                paths.sort();
                components.push(serde_json::json!({
                    "size": members.len(),
                    "paths": paths,
                }));
            }
            components.sort_by(|left, right| {
                right["size"]
                    .as_u64()
                    .unwrap_or(0)
                    .cmp(&left["size"].as_u64().unwrap_or(0))
            });
            let total = components.len();
            let items = paginate_json_items(components, args.limit, args.offset);
            Ok(CommandResult {
                command: "graph.components".to_string(),
                summary: "graph components completed".to_string(),
                args: serde_json::json!({
                    "total": total,
                    "limit": args.limit,
                    "offset": args.offset,
                    "items": items,
                }),
            })
        }
        GraphCommands::Walk(args) => {
            let resolved = args.resolve()?;
            let connection = open_initialized_connection(&resolved)?;
            let (paths_by_id, edges) = load_graph_snapshot(&connection)?;
            let source_by_path = paths_by_id
                .iter()
                .map(|(file_id, path)| (path.clone(), file_id.clone()))
                .collect::<HashMap<_, _>>();
            let Some(start_id) = source_by_path.get(&args.path) else {
                return Ok(CommandResult {
                    command: "graph.walk".to_string(),
                    summary: "graph walk completed".to_string(),
                    args: serde_json::json!({
                        "path": args.path,
                        "depth": args.depth,
                        "total": 0,
                        "items": [],
                    }),
                });
            };

            let mut outgoing = HashMap::<String, Vec<&ResolvedEdge>>::new();
            let mut incoming = HashMap::<String, Vec<&ResolvedEdge>>::new();
            for edge in &edges {
                outgoing
                    .entry(edge.source_file_id.clone())
                    .or_default()
                    .push(edge);
                if let Some(target_id) = &edge.target_file_id {
                    incoming.entry(target_id.clone()).or_default().push(edge);
                }
            }

            let mut queue = VecDeque::from([(start_id.clone(), 0_u32)]);
            let mut visited_depth = HashMap::<String, u32>::from([(start_id.clone(), 0)]);
            let mut traversed = Vec::new();
            while let Some((node_id, depth)) = queue.pop_front() {
                if depth >= args.depth {
                    continue;
                }

                for edge in outgoing.get(&node_id).into_iter().flatten() {
                    if edge.is_unresolved && !args.include_unresolved {
                        continue;
                    }
                    let next_id = edge.target_file_id.clone();
                    traversed.push(serde_json::json!({
                        "depth": depth + 1,
                        "direction": "outgoing",
                        "link_id": edge.link_id,
                        "source_path": edge.source_path,
                        "target_path": edge.target_path,
                        "raw_target": edge.raw_target,
                        "resolved": !edge.is_unresolved && edge.target_file_id.is_some(),
                    }));
                    if let Some(next_id) = next_id {
                        let next_depth = depth + 1;
                        let should_visit = visited_depth
                            .get(&next_id)
                            .map(|seen_depth| next_depth < *seen_depth)
                            .unwrap_or(true);
                        if should_visit {
                            visited_depth.insert(next_id.clone(), next_depth);
                            queue.push_back((next_id, next_depth));
                        }
                    }
                    if traversed.len() >= args.limit as usize {
                        break;
                    }
                }
                if traversed.len() >= args.limit as usize {
                    break;
                }

                for edge in incoming.get(&node_id).into_iter().flatten() {
                    traversed.push(serde_json::json!({
                        "depth": depth + 1,
                        "direction": "incoming",
                        "link_id": edge.link_id,
                        "source_path": edge.source_path,
                        "target_path": edge.target_path,
                        "raw_target": edge.raw_target,
                        "resolved": !edge.is_unresolved && edge.target_file_id.is_some(),
                    }));
                    let next_id = edge.source_file_id.clone();
                    let next_depth = depth + 1;
                    let should_visit = visited_depth
                        .get(&next_id)
                        .map(|seen_depth| next_depth < *seen_depth)
                        .unwrap_or(true);
                    if should_visit {
                        visited_depth.insert(next_id.clone(), next_depth);
                        queue.push_back((next_id, next_depth));
                    }
                    if traversed.len() >= args.limit as usize {
                        break;
                    }
                }
                if traversed.len() >= args.limit as usize {
                    break;
                }
            }
            Ok(CommandResult {
                command: "graph.walk".to_string(),
                summary: "graph walk completed".to_string(),
                args: serde_json::json!({
                    "path": args.path,
                    "depth": args.depth,
                    "total": traversed.len(),
                    "items": traversed,
                }),
            })
        }
    }
}

fn handle_meta(command: MetaCommands) -> Result<CommandResult> {
    match command {
        MetaCommands::Properties(args) => {
            let resolved = args.resolve()?;
            let connection = open_initialized_connection(&resolved)?;
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
        MetaCommands::Tags(args) => handle_meta_token_aggregate(args, "tags", "meta.tags"),
        MetaCommands::Aliases(args) => handle_meta_token_aggregate(args, "aliases", "meta.aliases"),
        MetaCommands::Tasks(args) => {
            let result = handle_task(TaskCommands::List(args), false)?;
            Ok(retag_result(result, "meta.tasks", "meta tasks completed"))
        }
    }
}

fn handle_task(command: TaskCommands, allow_writes: bool) -> Result<CommandResult> {
    match command {
        TaskCommands::List(args) => {
            let resolved = args.resolve()?;
            let connection = open_initialized_connection(&resolved)?;
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
            let total = TasksRepository::count_with_paths(&connection, state, query, None)
                .map_err(|source| anyhow!("count tasks failed: {source}"))?;
            let rows = TasksRepository::list_with_paths(
                &connection,
                state,
                query,
                None,
                args.limit,
                args.offset,
            )
            .map_err(|source| anyhow!("list tasks failed: {source}"))?;
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

            let mut connection = open_initialized_connection(&resolved)?;
            WatchReconcileService::default()
                .reconcile_once(
                    Path::new(&resolved.vault_root),
                    &mut connection,
                    CasePolicy::Sensitive,
                )
                .map_err(|source| anyhow!("reconcile after task state update failed: {source}"))?;

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

fn handle_query(args: QueryArgs) -> Result<CommandResult> {
    let from = args.from.trim();
    let limit = args.limit.max(1);
    if from.eq_ignore_ascii_case("docs") {
        let resolved = args.resolve()?;
        let connection = open_initialized_connection(&resolved)?;
        let page = SearchQueryService
            .query(
                Path::new(&resolved.vault_root),
                &connection,
                SearchQueryRequest {
                    query: args.query.clone().unwrap_or_default(),
                    limit: u64::from(limit),
                    offset: u64::from(args.offset),
                },
            )
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
        let result = handle_base(BaseCommands::View(BaseViewArgs {
            vault_root: args.vault_root.clone(),
            db_path: args.db_path.clone(),
            path_or_id: base_id_or_path.to_string(),
            view_name,
            page: (args.offset / limit) + 1,
            page_size: limit,
        }))?;
        return Ok(retag_result(result, "query.run", "query run completed"));
    }

    if from.eq_ignore_ascii_case("graph") {
        let graph_result = if let Some(path) = &args.path {
            handle_graph(GraphCommands::Outgoing(NotePathArgs {
                vault_root: args.vault_root.clone(),
                db_path: args.db_path.clone(),
                path: path.clone(),
            }))?
        } else {
            handle_graph(GraphCommands::Unresolved(GraphWindowArgs {
                vault_root: args.vault_root.clone(),
                db_path: args.db_path.clone(),
                limit: args.limit,
                offset: args.offset,
            }))?
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
        )?;
        return Ok(retag_result(
            task_result,
            "query.run",
            "query run completed",
        ));
    }

    if from.eq_ignore_ascii_case("meta:tags") {
        let result = handle_meta(MetaCommands::Tags(GraphWindowArgs {
            vault_root: args.vault_root.clone(),
            db_path: args.db_path.clone(),
            limit: args.limit,
            offset: args.offset,
        }))?;
        return Ok(retag_result(result, "query.run", "query run completed"));
    }

    if from.eq_ignore_ascii_case("meta:aliases") {
        let result = handle_meta(MetaCommands::Aliases(GraphWindowArgs {
            vault_root: args.vault_root.clone(),
            db_path: args.db_path.clone(),
            limit: args.limit,
            offset: args.offset,
        }))?;
        return Ok(retag_result(result, "query.run", "query run completed"));
    }

    if from.eq_ignore_ascii_case("meta:properties") {
        let result = handle_meta(MetaCommands::Properties(GraphWindowArgs {
            vault_root: args.vault_root,
            db_path: args.db_path,
            limit: args.limit,
            offset: args.offset,
        }))?;
        return Ok(retag_result(result, "query.run", "query run completed"));
    }

    Err(anyhow!(
        "unsupported query scope '{}'; supported scopes: docs, graph, task, meta:tags, meta:aliases, meta:properties, base:<id-or-path>",
        from
    ))
}

fn handle_vault(command: VaultCommands) -> Result<CommandResult> {
    match command {
        VaultCommands::Open(args) => {
            let resolved = args.resolve()?;
            let connection = open_initialized_connection(&resolved)?;
            let migration_count: i64 = connection
                .query_row("SELECT COUNT(*) FROM schema_migrations", [], |row| {
                    row.get(0)
                })
                .context("query migration count")?;
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
            let connection = open_initialized_connection(&resolved)?;
            let snapshot = HealthSnapshotService
                .snapshot(
                    Path::new(&resolved.vault_root),
                    &connection,
                    0,
                    WatcherStatus::Stopped,
                )
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
            let mut connection = open_initialized_connection(&resolved)?;
            let reconcile = WatchReconcileService::default()
                .reconcile_once(
                    Path::new(&resolved.vault_root),
                    &mut connection,
                    CasePolicy::Sensitive,
                )
                .map_err(|source| anyhow!("vault reindex failed: {source}"))?;
            let totals = query_index_totals(&connection)
                .map_err(|source| anyhow!("vault reindex total query failed: {source}"))?;
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
            let mut connection = open_initialized_connection(&resolved)?;
            let result = WatchReconcileService::default()
                .reconcile_once(
                    Path::new(&resolved.vault_root),
                    &mut connection,
                    CasePolicy::Sensitive,
                )
                .map_err(|source| anyhow!("vault reconcile failed: {source}"))?;
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
    }
}

fn handle_note(command: NoteCommands, allow_writes: bool) -> Result<CommandResult> {
    match command {
        NoteCommands::Get(args) => {
            let resolved = args.resolve()?;
            let kernel = open_bridge_kernel(&resolved)?;
            let note = expect_bridge_value(kernel.note_get(&args.path), "note.get")?;
            Ok(CommandResult {
                command: "note.get".to_string(),
                summary: "note get completed".to_string(),
                args: serde_json::json!({
                    "path": note.path,
                    "title": note.title,
                    "front_matter": note.front_matter,
                    "body": note.body,
                    "headings_total": note.headings_total,
                }),
            })
        }
        NoteCommands::Put(args) => {
            ensure_writes_enabled(allow_writes, "note.put")?;
            let resolved = args.resolve()?;
            let mut kernel = open_bridge_kernel(&resolved)?;
            let ack = expect_bridge_value(
                kernel.note_put_with_policy(&args.path, &args.content, true),
                "note.put",
            )?;
            Ok(CommandResult {
                command: "note.put".to_string(),
                summary: "note put completed".to_string(),
                args: serde_json::json!({
                    "path": ack.path,
                    "file_id": ack.file_id,
                    "action": ack.action,
                }),
            })
        }
        NoteCommands::List(args) => {
            let resolved = args.resolve()?;
            let kernel = open_bridge_kernel(&resolved)?;
            let mut after_path: Option<String> = None;
            let mut items = Vec::new();
            loop {
                let page = expect_bridge_value(
                    kernel.notes_list(after_path.as_deref(), 1000),
                    "note.list",
                )?;
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
                command: "note.list".to_string(),
                summary: "note list completed".to_string(),
                args: serde_json::json!({
                    "total": items.len(),
                    "items": items,
                }),
            })
        }
    }
}

fn handle_links(command: LinksCommands) -> Result<CommandResult> {
    match command {
        LinksCommands::Outgoing(args) => {
            let resolved = args.resolve()?;
            let kernel = open_bridge_kernel(&resolved)?;
            let panels = expect_bridge_value(kernel.note_links(&args.path), "links.outgoing")?;
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
                command: "links.outgoing".to_string(),
                summary: "links outgoing completed".to_string(),
                args: serde_json::json!({
                    "path": args.path,
                    "total": items.len(),
                    "items": items,
                }),
            })
        }
        LinksCommands::Backlinks(args) => {
            let resolved = args.resolve()?;
            let kernel = open_bridge_kernel(&resolved)?;
            let panels = expect_bridge_value(kernel.note_links(&args.path), "links.backlinks")?;
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
                command: "links.backlinks".to_string(),
                summary: "links backlinks completed".to_string(),
                args: serde_json::json!({
                    "path": args.path,
                    "total": items.len(),
                    "items": items,
                }),
            })
        }
    }
}

fn handle_properties(command: PropertiesCommands, allow_writes: bool) -> Result<CommandResult> {
    match command {
        PropertiesCommands::Get(args) => {
            let resolved = args.resolve()?;
            let connection = open_initialized_connection(&resolved)?;
            let file = FilesRepository::get_by_normalized_path(&connection, &args.path)
                .map_err(|source| anyhow!("lookup note metadata failed: {source}"))?;
            let Some(file) = file else {
                return Ok(CommandResult {
                    command: "properties.get".to_string(),
                    summary: "properties get completed".to_string(),
                    args: serde_json::json!({
                        "path": args.path,
                        "total": 0,
                        "items": [],
                    }),
                });
            };

            let rows = PropertiesRepository::list_for_file_with_path(&connection, &file.file_id)
                .map_err(|source| anyhow!("query properties failed: {source}"))?;
            let items = rows
                .into_iter()
                .map(|row| {
                    let parsed_value = serde_json::from_str::<JsonValue>(&row.value_json)
                        .unwrap_or_else(|_| JsonValue::String(row.value_json.clone()));
                    serde_json::json!({
                        "property_id": row.property_id,
                        "file_id": row.file_id,
                        "file_path": row.file_path,
                        "key": row.key,
                        "value_type": row.value_type,
                        "value": parsed_value,
                        "value_json": row.value_json,
                        "updated_at": row.updated_at,
                    })
                })
                .collect::<Vec<_>>();

            Ok(CommandResult {
                command: "properties.get".to_string(),
                summary: "properties get completed".to_string(),
                args: serde_json::json!({
                    "path": args.path,
                    "file_id": file.file_id,
                    "total": items.len(),
                    "items": items,
                }),
            })
        }
        PropertiesCommands::Set(args) => {
            ensure_writes_enabled(allow_writes, "properties.set")?;
            let resolved = args.resolve()?;
            let mut connection = open_initialized_connection(&resolved)?;
            let file = FilesRepository::get_by_normalized_path(&connection, &args.path)
                .map_err(|source| anyhow!("lookup note metadata failed: {source}"))?;
            let Some(file) = file else {
                return Err(anyhow!(
                    "note path is not indexed; run vault reindex first: {}",
                    args.path
                ));
            };

            let typed_value = parse_cli_property_value(&args.value)?;
            let result = PropertyUpdateService::default()
                .set_property(
                    Path::new(&resolved.vault_root),
                    &mut connection,
                    &file.file_id,
                    &args.key,
                    typed_value,
                )
                .map_err(|source| anyhow!("property set failed: {source}"))?;

            Ok(CommandResult {
                command: "properties.set".to_string(),
                summary: "properties set completed".to_string(),
                args: serde_json::json!({
                    "path": args.path,
                    "file_id": result.file_id,
                    "key": result.key,
                    "value": typed_property_value_to_json(&result.value),
                    "title": result.parsed.title,
                    "headings_total": result.parsed.headings.len(),
                }),
            })
        }
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

fn handle_bases(command: BasesCommands) -> Result<CommandResult> {
    match command {
        BasesCommands::List(args) => {
            let resolved = args.resolve()?;
            let connection = open_initialized_connection(&resolved)?;
            let bases = BasesRepository::list_with_paths(&connection)
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
                command: "bases.list".to_string(),
                summary: "bases list completed".to_string(),
                args: serde_json::json!({
                    "total": items.len(),
                    "items": items,
                }),
            })
        }
        BasesCommands::View(args) => {
            let resolved = args.resolve()?;
            let connection = open_initialized_connection(&resolved)?;
            let base = BasesRepository::list_with_paths(&connection)
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
            let page = BaseTableExecutorService
                .execute(&connection, &plan)
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
                command: "bases.view".to_string(),
                summary: "bases view completed".to_string(),
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
    }
}

fn handle_search(command: SearchCommands) -> Result<CommandResult> {
    match command {
        SearchCommands::Query(args) => {
            let resolved = args.resolve()?;
            let connection = open_initialized_connection(&resolved)?;
            let page = SearchQueryService
                .query(
                    Path::new(&resolved.vault_root),
                    &connection,
                    SearchQueryRequest {
                        query: args.query.clone(),
                        limit: u64::from(args.limit),
                        offset: u64::from(args.offset),
                    },
                )
                .map_err(|source| anyhow!("search query failed: {source}"))?;
            let items = page
                .items
                .into_iter()
                .map(|item| {
                    serde_json::json!({
                        "file_id": item.file_id,
                        "path": item.path,
                        "title": item.title,
                        "indexed_at": item.indexed_at,
                        "matched_in": item.matched_in,
                    })
                })
                .collect::<Vec<_>>();
            Ok(CommandResult {
                command: "search.query".to_string(),
                summary: "search query completed".to_string(),
                args: serde_json::json!({
                    "query": page.query,
                    "limit": page.limit,
                    "offset": page.offset,
                    "total": page.total,
                    "items": items,
                }),
            })
        }
    }
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

fn load_graph_snapshot(
    connection: &Connection,
) -> Result<(HashMap<String, String>, Vec<ResolvedEdge>)> {
    let files = FilesRepository::list_all(connection)
        .map_err(|source| anyhow!("list files for graph snapshot failed: {source}"))?;
    let mut paths_by_id = HashMap::new();
    for file in files.into_iter().filter(|file| file.is_markdown) {
        paths_by_id.insert(file.file_id, file.normalized_path);
    }

    let mut statement = connection
        .prepare(
            "SELECT l.link_id, l.source_file_id, sf.normalized_path AS source_path, \
                    l.resolved_file_id, tf.normalized_path AS target_path, \
                    l.raw_target, l.is_unresolved \
             FROM links l \
             JOIN files sf ON sf.file_id = l.source_file_id \
             LEFT JOIN files tf ON tf.file_id = l.resolved_file_id \
             ORDER BY l.link_id ASC",
        )
        .context("prepare graph snapshot query")?;
    let rows = statement
        .query_map([], |row| {
            let is_unresolved: i64 = row.get("is_unresolved")?;
            Ok(ResolvedEdge {
                link_id: row.get("link_id")?,
                source_file_id: row.get("source_file_id")?,
                source_path: row.get("source_path")?,
                target_file_id: row.get("resolved_file_id")?,
                target_path: row.get("target_path")?,
                raw_target: row.get("raw_target")?,
                is_unresolved: is_unresolved != 0,
            })
        })
        .context("query graph snapshot rows")?;
    let mut edges = Vec::new();
    for row in rows {
        edges.push(row.context("map graph snapshot row")?);
    }
    Ok((paths_by_id, edges))
}

fn handle_meta_token_aggregate(
    args: GraphWindowArgs,
    property_key: &str,
    command: &str,
) -> Result<CommandResult> {
    let resolved = args.resolve()?;
    let connection = open_initialized_connection(&resolved)?;
    let rows = PropertiesRepository::list_by_key_with_paths(&connection, property_key)
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

fn parse_cli_property_value(raw: &str) -> Result<TypedPropertyValue> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(TypedPropertyValue::String(String::new()));
    }
    if trimmed.eq_ignore_ascii_case("null") {
        return Ok(TypedPropertyValue::Null);
    }
    if trimmed.eq_ignore_ascii_case("true") {
        return Ok(TypedPropertyValue::Bool(true));
    }
    if trimmed.eq_ignore_ascii_case("false") {
        return Ok(TypedPropertyValue::Bool(false));
    }
    if let Ok(value) = trimmed.parse::<f64>() {
        return Ok(TypedPropertyValue::Number(value));
    }
    if let Ok(value) = serde_json::from_str::<JsonValue>(trimmed) {
        return json_to_typed_property_value(&value);
    }
    Ok(TypedPropertyValue::String(raw.to_string()))
}

fn json_to_typed_property_value(value: &JsonValue) -> Result<TypedPropertyValue> {
    match value {
        JsonValue::Null => Ok(TypedPropertyValue::Null),
        JsonValue::Bool(value) => Ok(TypedPropertyValue::Bool(*value)),
        JsonValue::Number(value) => value
            .as_f64()
            .map(TypedPropertyValue::Number)
            .ok_or_else(|| anyhow!("property numeric value is out of supported range")),
        JsonValue::String(value) => Ok(TypedPropertyValue::String(value.clone())),
        JsonValue::Array(values) => {
            let typed_values = values
                .iter()
                .map(json_to_typed_property_value)
                .collect::<Result<Vec<_>>>()?;
            Ok(TypedPropertyValue::List(typed_values))
        }
        JsonValue::Object(_) => Ok(TypedPropertyValue::String(value.to_string())),
    }
}

fn typed_property_value_to_json(value: &TypedPropertyValue) -> JsonValue {
    match value {
        TypedPropertyValue::Bool(value) => JsonValue::Bool(*value),
        TypedPropertyValue::Number(value) => serde_json::Number::from_f64(*value)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        TypedPropertyValue::Date(value) | TypedPropertyValue::String(value) => {
            JsonValue::String(value.clone())
        }
        TypedPropertyValue::List(values) => {
            JsonValue::Array(values.iter().map(typed_property_value_to_json).collect())
        }
        TypedPropertyValue::Null => JsonValue::Null,
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

    use super::{Cli, dispatch, render_output};
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
        assert!(rendered.contains("note"));
        assert!(rendered.contains("links"));
        assert!(rendered.contains("properties"));
        assert!(rendered.contains("bases"));
        assert!(rendered.contains("search"));
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
                "---\nstatus: active\npriority: 4\n---\n# Project A\n",
            )
            .expect("write project-a note");
            fs::write(
                projects_dir.join("project-b.md"),
                "---\nstatus: paused\npriority: 2\n---\n# Project B\n",
            )
            .expect("write project-b note");
            fs::write(notes_dir.join("alpha.md"), "# Alpha\n[[project-a]]\n")
                .expect("write alpha note");
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
                    "note.get",
                    vec![
                        "tao",
                        "--json",
                        "note",
                        "get",
                        "--vault-root",
                        &vault_root_string,
                        "--path",
                        "notes/alpha.md",
                    ],
                ),
                (
                    "note.list",
                    vec![
                        "tao",
                        "--json",
                        "note",
                        "list",
                        "--vault-root",
                        &vault_root_string,
                    ],
                ),
                (
                    "note.put",
                    vec![
                        "tao",
                        "--json",
                        "--allow-writes",
                        "note",
                        "put",
                        "--vault-root",
                        &vault_root_string,
                        "--path",
                        "notes/new.md",
                        "--content",
                        "# New\nbody",
                    ],
                ),
                (
                    "links.outgoing",
                    vec![
                        "tao",
                        "--json",
                        "links",
                        "outgoing",
                        "--vault-root",
                        &vault_root_string,
                        "--path",
                        "notes/alpha.md",
                    ],
                ),
                (
                    "links.backlinks",
                    vec![
                        "tao",
                        "--json",
                        "links",
                        "backlinks",
                        "--vault-root",
                        &vault_root_string,
                        "--path",
                        "notes/projects/project-a.md",
                    ],
                ),
                (
                    "properties.get",
                    vec![
                        "tao",
                        "--json",
                        "properties",
                        "get",
                        "--vault-root",
                        &vault_root_string,
                        "--path",
                        "notes/projects/project-a.md",
                    ],
                ),
                (
                    "properties.set",
                    vec![
                        "tao",
                        "--json",
                        "--allow-writes",
                        "properties",
                        "set",
                        "--vault-root",
                        &vault_root_string,
                        "--path",
                        "notes/projects/project-a.md",
                        "--key",
                        "status",
                        "--value",
                        "active",
                    ],
                ),
                (
                    "bases.list",
                    vec![
                        "tao",
                        "--json",
                        "bases",
                        "list",
                        "--vault-root",
                        &vault_root_string,
                    ],
                ),
                (
                    "bases.view",
                    vec![
                        "tao",
                        "--json",
                        "bases",
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
                    "search.query",
                    vec![
                        "tao",
                        "--json",
                        "search",
                        "query",
                        "--vault-root",
                        &vault_root_string,
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
            fs::create_dir_all(&vault_root).expect("create vault dir");

            let note_put = Cli::parse_from([
                "tao",
                "--json",
                "note",
                "put",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
                "--path",
                "notes/blocked.md",
                "--content",
                "# blocked",
            ]);
            let note_put_error = dispatch(note_put.command, note_put.allow_writes)
                .expect_err("note.put should require --allow-writes");
            assert!(note_put_error.to_string().contains("--allow-writes"));

            let properties_set = Cli::parse_from([
                "tao",
                "--json",
                "properties",
                "set",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
                "--path",
                "notes/blocked.md",
                "--key",
                "status",
                "--value",
                "draft",
            ]);
            let properties_error = dispatch(properties_set.command, properties_set.allow_writes)
                .expect_err("properties.set should require --allow-writes");
            assert!(properties_error.to_string().contains("--allow-writes"));
        });
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
