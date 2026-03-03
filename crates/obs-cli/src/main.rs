use anyhow::Result;
use clap::{Args, Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(name = "obs", version, about = "obs cli")]
struct Cli {
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

#[derive(Debug, Clone, Args)]
struct VaultPathArgs {
    /// Absolute vault root path.
    #[arg(long)]
    vault_root: String,
    /// SQLite database file path.
    #[arg(long)]
    db_path: String,
}

#[derive(Debug, Clone, Args)]
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

#[derive(Debug, Clone, Args)]
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

#[derive(Debug, Clone, Args)]
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

#[derive(Debug, Clone, Args)]
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

#[derive(Debug, Clone, Args)]
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

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Vault { command } => handle_vault(command),
        Commands::Note { command } => handle_note(command),
        Commands::Links { command } => handle_links(command),
        Commands::Properties { command } => handle_properties(command),
        Commands::Bases { command } => handle_bases(command),
        Commands::Search { command } => handle_search(command),
    }
}

fn handle_vault(command: VaultCommands) -> Result<()> {
    match command {
        VaultCommands::Open(args) => println!("not implemented: vault open {:?}", args),
        VaultCommands::Stats(args) => println!("not implemented: vault stats {:?}", args),
        VaultCommands::Reindex(args) => println!("not implemented: vault reindex {:?}", args),
        VaultCommands::Reconcile(args) => println!("not implemented: vault reconcile {:?}", args),
    }
    Ok(())
}

fn handle_note(command: NoteCommands) -> Result<()> {
    match command {
        NoteCommands::Get(args) => println!("not implemented: note get {:?}", args),
        NoteCommands::Put(args) => println!("not implemented: note put {:?}", args),
        NoteCommands::List(args) => println!("not implemented: note list {:?}", args),
    }
    Ok(())
}

fn handle_links(command: LinksCommands) -> Result<()> {
    match command {
        LinksCommands::Outgoing(args) => println!("not implemented: links outgoing {:?}", args),
        LinksCommands::Backlinks(args) => println!("not implemented: links backlinks {:?}", args),
    }
    Ok(())
}

fn handle_properties(command: PropertiesCommands) -> Result<()> {
    match command {
        PropertiesCommands::Get(args) => println!("not implemented: properties get {:?}", args),
        PropertiesCommands::Set(args) => println!("not implemented: properties set {:?}", args),
    }
    Ok(())
}

fn handle_bases(command: BasesCommands) -> Result<()> {
    match command {
        BasesCommands::List(args) => println!("not implemented: bases list {:?}", args),
        BasesCommands::View(args) => println!("not implemented: bases view {:?}", args),
    }
    Ok(())
}

fn handle_search(command: SearchCommands) -> Result<()> {
    match command {
        SearchCommands::Query(args) => println!("not implemented: search query {:?}", args),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::Cli;
    use clap::CommandFactory;

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
}
