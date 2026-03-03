use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{Context, Result, anyhow};
use clap::{Args, Parser, Subcommand};
use obs_sdk_bases::{
    BaseDocument, BaseTableQueryPlanner, BaseViewRegistry, TableQueryPlanRequest,
    parse_base_document,
};
use obs_sdk_bridge::{BridgeEnvelope, BridgeKernel};
use obs_sdk_properties::TypedPropertyValue;
use obs_sdk_service::{
    BaseTableExecutorService, FullIndexService, HealthSnapshotService, PropertyUpdateService,
    ReconcileService, WatcherStatus,
};
use obs_sdk_storage::{BasesRepository, FilesRepository, PropertiesRepository, run_migrations};
use obs_sdk_vault::CasePolicy;
use rusqlite::Connection;
use serde::Serialize;
use serde_json::Value as JsonValue;

#[derive(Debug, Parser)]
#[command(name = "obs", version, about = "obs cli")]
struct Cli {
    /// Emit one JSON envelope to stdout.
    #[arg(long, global = true)]
    json: bool,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
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
enum VaultCommands {
    /// Open one vault path and initialize sqlite state.
    Open(VaultPathArgs),
    /// Return vault health snapshot.
    Stats(VaultPathArgs),
    /// Rebuild full index.
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
    /// SQLite database file path.
    #[arg(long)]
    db_path: String,
}

#[derive(Debug, Clone, Args, Serialize)]
struct NotePathArgs {
    /// Absolute vault root path.
    #[arg(long)]
    vault_root: String,
    /// SQLite database file path.
    #[arg(long)]
    db_path: String,
    /// Vault-relative normalized note path.
    #[arg(long)]
    path: String,
}

#[derive(Debug, Clone, Args, Serialize)]
struct NotePutArgs {
    /// Absolute vault root path.
    #[arg(long)]
    vault_root: String,
    /// SQLite database file path.
    #[arg(long)]
    db_path: String,
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
    /// SQLite database file path.
    #[arg(long)]
    db_path: String,
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
    /// SQLite database file path.
    #[arg(long)]
    db_path: String,
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
    /// SQLite database file path.
    #[arg(long)]
    db_path: String,
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

#[derive(Debug, Serialize)]
struct CommandResult {
    command: String,
    summary: String,
    args: JsonValue,
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
    let result = dispatch(cli.command)?;
    let output = render_output(cli.json, &result)?;
    println!("{output}");
    Ok(())
}

fn dispatch(command: Commands) -> Result<CommandResult> {
    match command {
        Commands::Vault { command } => handle_vault(command),
        Commands::Note { command } => handle_note(command),
        Commands::Links { command } => handle_links(command),
        Commands::Properties { command } => handle_properties(command),
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

fn handle_vault(command: VaultCommands) -> Result<CommandResult> {
    match command {
        VaultCommands::Open(args) => {
            let connection = open_initialized_connection(&args)?;
            let migration_count: i64 = connection
                .query_row("SELECT COUNT(*) FROM schema_migrations", [], |row| {
                    row.get(0)
                })
                .context("query migration count")?;
            Ok(CommandResult {
                command: "vault.open".to_string(),
                summary: "vault open completed".to_string(),
                args: serde_json::json!({
                    "vault_root": args.vault_root,
                    "db_path": args.db_path,
                    "db_ready": true,
                    "migrations_applied": migration_count,
                }),
            })
        }
        VaultCommands::Stats(args) => {
            let connection = open_initialized_connection(&args)?;
            let snapshot = HealthSnapshotService
                .snapshot(
                    Path::new(&args.vault_root),
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
        VaultCommands::Reindex(args) => {
            let mut connection = open_initialized_connection(&args)?;
            let result = FullIndexService::default()
                .rebuild(
                    Path::new(&args.vault_root),
                    &mut connection,
                    CasePolicy::Sensitive,
                )
                .map_err(|source| anyhow!("vault reindex failed: {source}"))?;
            Ok(CommandResult {
                command: "vault.reindex".to_string(),
                summary: "vault reindex completed".to_string(),
                args: serde_json::json!({
                    "indexed_files": result.indexed_files,
                    "markdown_files": result.markdown_files,
                    "links_total": result.links_total,
                    "unresolved_links": result.unresolved_links,
                    "properties_total": result.properties_total,
                    "bases_total": result.bases_total,
                }),
            })
        }
        VaultCommands::Reconcile(args) => {
            let mut connection = open_initialized_connection(&args)?;
            let result = ReconcileService
                .reconcile_vault(
                    Path::new(&args.vault_root),
                    &mut connection,
                    CasePolicy::Sensitive,
                )
                .map_err(|source| anyhow!("vault reconcile failed: {source}"))?;
            Ok(CommandResult {
                command: "vault.reconcile".to_string(),
                summary: "vault reconcile completed".to_string(),
                args: serde_json::json!({
                    "scanned_files": result.scanned_files,
                    "inserted_files": result.inserted_files,
                    "updated_files": result.updated_files,
                    "removed_files": result.removed_files,
                    "unchanged_files": result.unchanged_files,
                }),
            })
        }
    }
}

fn handle_note(command: NoteCommands) -> Result<CommandResult> {
    match command {
        NoteCommands::Get(args) => {
            let kernel = open_bridge_kernel(&args.vault_root, &args.db_path)?;
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
            let mut kernel = open_bridge_kernel(&args.vault_root, &args.db_path)?;
            let ack = expect_bridge_value(kernel.note_put(&args.path, &args.content), "note.put")?;
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
            let kernel = open_bridge_kernel(&args.vault_root, &args.db_path)?;
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
            let kernel = open_bridge_kernel(&args.vault_root, &args.db_path)?;
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
            let kernel = open_bridge_kernel(&args.vault_root, &args.db_path)?;
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

fn handle_properties(command: PropertiesCommands) -> Result<CommandResult> {
    match command {
        PropertiesCommands::Get(args) => {
            let vault_args = VaultPathArgs {
                vault_root: args.vault_root.clone(),
                db_path: args.db_path.clone(),
            };
            let connection = open_initialized_connection(&vault_args)?;
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
            let vault_args = VaultPathArgs {
                vault_root: args.vault_root.clone(),
                db_path: args.db_path.clone(),
            };
            let mut connection = open_initialized_connection(&vault_args)?;
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
                    Path::new(&args.vault_root),
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

fn handle_bases(command: BasesCommands) -> Result<CommandResult> {
    match command {
        BasesCommands::List(args) => {
            let connection = open_initialized_connection(&args)?;
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
            let vault_args = VaultPathArgs {
                vault_root: args.vault_root.clone(),
                db_path: args.db_path.clone(),
            };
            let connection = open_initialized_connection(&vault_args)?;
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
            let query = args.query.trim();
            if query.is_empty() {
                return Err(anyhow!("search query must not be empty"));
            }

            let vault_args = VaultPathArgs {
                vault_root: args.vault_root.clone(),
                db_path: args.db_path.clone(),
            };
            let connection = open_initialized_connection(&vault_args)?;

            let query_lc = query.to_ascii_lowercase();
            let matches = FilesRepository::list_all(&connection)
                .map_err(|source| anyhow!("query indexed files for search failed: {source}"))?
                .into_iter()
                .filter(|file| file.is_markdown)
                .filter(|file| {
                    let path_lc = file.normalized_path.to_ascii_lowercase();
                    if path_lc.contains(&query_lc) {
                        return true;
                    }
                    search_title_from_path(&file.normalized_path)
                        .to_ascii_lowercase()
                        .contains(&query_lc)
                })
                .map(|file| {
                    serde_json::json!({
                        "file_id": file.file_id,
                        "path": file.normalized_path,
                        "title": search_title_from_path(&file.normalized_path),
                        "indexed_at": file.indexed_at,
                    })
                })
                .collect::<Vec<_>>();

            let total = matches.len();
            let offset = args.offset as usize;
            let limit = args.limit as usize;
            let items = matches
                .into_iter()
                .skip(offset)
                .take(limit)
                .collect::<Vec<_>>();
            Ok(CommandResult {
                command: "search.query".to_string(),
                summary: "search query completed".to_string(),
                args: serde_json::json!({
                    "query": query,
                    "limit": args.limit,
                    "offset": args.offset,
                    "total": total,
                    "items": items,
                }),
            })
        }
    }
}

fn open_bridge_kernel(vault_root: &str, db_path: &str) -> Result<BridgeKernel> {
    BridgeKernel::open(vault_root, db_path)
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

fn search_title_from_path(path: &str) -> String {
    Path::new(path)
        .file_stem()
        .and_then(|stem| stem.to_str())
        .map(std::string::ToString::to_string)
        .unwrap_or_else(|| path.to_string())
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

fn open_initialized_connection(args: &VaultPathArgs) -> Result<Connection> {
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

#[cfg(test)]
mod tests {
    use std::fs;

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
        assert!(rendered.contains("note"));
        assert!(rendered.contains("links"));
        assert!(rendered.contains("properties"));
        assert!(rendered.contains("bases"));
        assert!(rendered.contains("search"));
    }

    #[test]
    fn json_output_is_one_envelope_object() {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let vault_root = tempdir.path().to_path_buf();
        let db_path = vault_root.join("obs.sqlite");
        let cli = Cli::parse_from([
            "obs".to_string(),
            "--json".to_string(),
            "vault".to_string(),
            "open".to_string(),
            "--vault-root".to_string(),
            vault_root.to_string_lossy().to_string(),
            "--db-path".to_string(),
            db_path.to_string_lossy().to_string(),
        ]);
        let result = dispatch(cli.command).expect("dispatch");
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
    }

    #[test]
    fn json_contract_is_stable_for_all_grouped_json_commands() {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let vault_root = tempdir.path().join("vault");
        let notes_dir = vault_root.join("notes");
        let projects_dir = notes_dir.join("projects");
        let views_dir = vault_root.join("views");
        let db_path = tempdir.path().join("obs.sqlite");

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
        let db_path_string = db_path.to_string_lossy().to_string();

        let scenarios = [
            (
                "vault.open",
                vec![
                    "obs",
                    "--json",
                    "vault",
                    "open",
                    "--vault-root",
                    &vault_root_string,
                    "--db-path",
                    &db_path_string,
                ],
            ),
            (
                "vault.stats",
                vec![
                    "obs",
                    "--json",
                    "vault",
                    "stats",
                    "--vault-root",
                    &vault_root_string,
                    "--db-path",
                    &db_path_string,
                ],
            ),
            (
                "vault.reindex",
                vec![
                    "obs",
                    "--json",
                    "vault",
                    "reindex",
                    "--vault-root",
                    &vault_root_string,
                    "--db-path",
                    &db_path_string,
                ],
            ),
            (
                "note.get",
                vec![
                    "obs",
                    "--json",
                    "note",
                    "get",
                    "--vault-root",
                    &vault_root_string,
                    "--db-path",
                    &db_path_string,
                    "--path",
                    "notes/alpha.md",
                ],
            ),
            (
                "note.list",
                vec![
                    "obs",
                    "--json",
                    "note",
                    "list",
                    "--vault-root",
                    &vault_root_string,
                    "--db-path",
                    &db_path_string,
                ],
            ),
            (
                "note.put",
                vec![
                    "obs",
                    "--json",
                    "note",
                    "put",
                    "--vault-root",
                    &vault_root_string,
                    "--db-path",
                    &db_path_string,
                    "--path",
                    "notes/new.md",
                    "--content",
                    "# New\nbody",
                ],
            ),
            (
                "links.outgoing",
                vec![
                    "obs",
                    "--json",
                    "links",
                    "outgoing",
                    "--vault-root",
                    &vault_root_string,
                    "--db-path",
                    &db_path_string,
                    "--path",
                    "notes/alpha.md",
                ],
            ),
            (
                "links.backlinks",
                vec![
                    "obs",
                    "--json",
                    "links",
                    "backlinks",
                    "--vault-root",
                    &vault_root_string,
                    "--db-path",
                    &db_path_string,
                    "--path",
                    "notes/projects/project-a.md",
                ],
            ),
            (
                "properties.get",
                vec![
                    "obs",
                    "--json",
                    "properties",
                    "get",
                    "--vault-root",
                    &vault_root_string,
                    "--db-path",
                    &db_path_string,
                    "--path",
                    "notes/projects/project-a.md",
                ],
            ),
            (
                "properties.set",
                vec![
                    "obs",
                    "--json",
                    "properties",
                    "set",
                    "--vault-root",
                    &vault_root_string,
                    "--db-path",
                    &db_path_string,
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
                    "obs",
                    "--json",
                    "bases",
                    "list",
                    "--vault-root",
                    &vault_root_string,
                    "--db-path",
                    &db_path_string,
                ],
            ),
            (
                "bases.view",
                vec![
                    "obs",
                    "--json",
                    "bases",
                    "view",
                    "--vault-root",
                    &vault_root_string,
                    "--db-path",
                    &db_path_string,
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
                    "obs",
                    "--json",
                    "search",
                    "query",
                    "--vault-root",
                    &vault_root_string,
                    "--db-path",
                    &db_path_string,
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
                    "obs",
                    "--json",
                    "vault",
                    "reconcile",
                    "--vault-root",
                    &vault_root_string,
                    "--db-path",
                    &db_path_string,
                ],
            ),
        ];

        for (expected_command, args) in scenarios {
            let cli = Cli::parse_from(args);
            let result = dispatch(cli.command).expect("dispatch json contract scenario");
            let output = render_output(cli.json, &result).expect("render json output");
            let envelope: JsonValue = serde_json::from_str(&output).expect("parse json output");
            assert_json_contract(&envelope, expected_command);
        }
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
}
