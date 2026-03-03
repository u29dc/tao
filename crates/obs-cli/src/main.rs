use std::collections::BTreeMap;

use anyhow::Result;
use clap::{Args, Parser, Subcommand};
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
            placeholder_result("vault.open", "vault open is not implemented yet", args)
        }
        VaultCommands::Stats(args) => {
            placeholder_result("vault.stats", "vault stats is not implemented yet", args)
        }
        VaultCommands::Reindex(args) => placeholder_result(
            "vault.reindex",
            "vault reindex is not implemented yet",
            args,
        ),
        VaultCommands::Reconcile(args) => placeholder_result(
            "vault.reconcile",
            "vault reconcile is not implemented yet",
            args,
        ),
    }
}

fn handle_note(command: NoteCommands) -> Result<CommandResult> {
    match command {
        NoteCommands::Get(args) => {
            placeholder_result("note.get", "note get is not implemented yet", args)
        }
        NoteCommands::Put(args) => {
            placeholder_result("note.put", "note put is not implemented yet", args)
        }
        NoteCommands::List(args) => {
            placeholder_result("note.list", "note list is not implemented yet", args)
        }
    }
}

fn handle_links(command: LinksCommands) -> Result<CommandResult> {
    match command {
        LinksCommands::Outgoing(args) => placeholder_result(
            "links.outgoing",
            "links outgoing is not implemented yet",
            args,
        ),
        LinksCommands::Backlinks(args) => placeholder_result(
            "links.backlinks",
            "links backlinks is not implemented yet",
            args,
        ),
    }
}

fn handle_properties(command: PropertiesCommands) -> Result<CommandResult> {
    match command {
        PropertiesCommands::Get(args) => placeholder_result(
            "properties.get",
            "properties get is not implemented yet",
            args,
        ),
        PropertiesCommands::Set(args) => placeholder_result(
            "properties.set",
            "properties set is not implemented yet",
            args,
        ),
    }
}

fn handle_bases(command: BasesCommands) -> Result<CommandResult> {
    match command {
        BasesCommands::List(args) => {
            placeholder_result("bases.list", "bases list is not implemented yet", args)
        }
        BasesCommands::View(args) => {
            placeholder_result("bases.view", "bases view is not implemented yet", args)
        }
    }
}

fn handle_search(command: SearchCommands) -> Result<CommandResult> {
    match command {
        SearchCommands::Query(args) => {
            placeholder_result("search.query", "search query is not implemented yet", args)
        }
    }
}

fn placeholder_result<A: Serialize>(
    command: &str,
    summary: &str,
    args: A,
) -> Result<CommandResult> {
    Ok(CommandResult {
        command: command.to_string(),
        summary: summary.to_string(),
        args: serde_json::to_value(args)?,
    })
}

#[cfg(test)]
mod tests {
    use super::{Cli, dispatch, render_output};
    use clap::{CommandFactory, Parser};

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
        let cli = Cli::parse_from([
            "obs",
            "--json",
            "vault",
            "open",
            "--vault-root",
            "/tmp/vault",
            "--db-path",
            "/tmp/obs.sqlite",
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
}
