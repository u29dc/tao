use clap::{Parser, Subcommand};
use obs_sdk_bridge::{
    BRIDGE_ERROR_INIT_FAILED, BRIDGE_ERROR_SERIALIZE_FAILED, BRIDGE_SCHEMA_VERSION, BridgeEnvelope,
    BridgeError, BridgeKernel,
};
use serde_json::Value as JsonValue;

#[derive(Debug, Parser)]
#[command(
    name = "obs-sdk-bridge",
    version,
    about = "Bridge shell for Swift-to-Rust read/write APIs"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Return bridge ping envelope.
    Ping,
    /// Return vault stats envelope.
    VaultStats {
        /// Absolute vault root path.
        #[arg(long)]
        vault_root: String,
        /// SQLite database file path.
        #[arg(long)]
        db_path: String,
    },
    /// Return one note envelope by normalized path.
    NoteGet {
        /// Absolute vault root path.
        #[arg(long)]
        vault_root: String,
        /// SQLite database file path.
        #[arg(long)]
        db_path: String,
        /// Note normalized path.
        #[arg(long)]
        path: String,
    },
    /// Return one paged markdown note list window.
    NotesList {
        /// Absolute vault root path.
        #[arg(long)]
        vault_root: String,
        /// SQLite database file path.
        #[arg(long)]
        db_path: String,
        /// Cursor path for paging.
        #[arg(long)]
        after_path: Option<String>,
        /// Maximum rows to return.
        #[arg(long, default_value_t = 128)]
        limit: u64,
    },
    /// Return outgoing/backlink panels for one note.
    NoteLinks {
        /// Absolute vault root path.
        #[arg(long)]
        vault_root: String,
        /// SQLite database file path.
        #[arg(long)]
        db_path: String,
        /// Note normalized path.
        #[arg(long)]
        path: String,
    },
    /// Create or update one note and return write acknowledgement.
    NotePut {
        /// Absolute vault root path.
        #[arg(long)]
        vault_root: String,
        /// SQLite database file path.
        #[arg(long)]
        db_path: String,
        /// Note normalized path.
        #[arg(long)]
        path: String,
        /// Full markdown content payload.
        #[arg(long)]
        content: String,
    },
    /// Poll bridge events after one cursor for Swift subscription flows.
    EventsPoll {
        /// Absolute vault root path.
        #[arg(long)]
        vault_root: String,
        /// SQLite database file path.
        #[arg(long)]
        db_path: String,
        /// Last seen event id cursor.
        #[arg(long, default_value_t = 0)]
        after_id: u64,
        /// Maximum events to return.
        #[arg(long, default_value_t = 128)]
        limit: u64,
    },
}

fn main() {
    let cli = Cli::parse();

    let output = match cli.command {
        Commands::Ping => serialize_output(&BridgeKernelPing::envelope()),
        Commands::VaultStats {
            vault_root,
            db_path,
        } => with_kernel(vault_root, db_path, |kernel| kernel.vault_stats()),
        Commands::NoteGet {
            vault_root,
            db_path,
            path,
        } => with_kernel(vault_root, db_path, |kernel| kernel.note_get(&path)),
        Commands::NotesList {
            vault_root,
            db_path,
            after_path,
            limit,
        } => with_kernel(vault_root, db_path, |kernel| {
            kernel.notes_list(after_path.as_deref(), limit)
        }),
        Commands::NoteLinks {
            vault_root,
            db_path,
            path,
        } => with_kernel(vault_root, db_path, |kernel| kernel.note_links(&path)),
        Commands::NotePut {
            vault_root,
            db_path,
            path,
            content,
        } => with_kernel(vault_root, db_path, |kernel| {
            kernel.note_put(&path, &content)
        }),
        Commands::EventsPoll {
            vault_root,
            db_path,
            after_id,
            limit,
        } => with_kernel(vault_root, db_path, |kernel| {
            kernel.events_poll(after_id, limit)
        }),
    };

    println!("{output}");
}

struct BridgeKernelPing;

impl BridgeKernelPing {
    fn envelope() -> BridgeEnvelope<serde_json::Value> {
        BridgeEnvelope::success(serde_json::json!({ "message": "ok" }))
    }
}

fn with_kernel<T: serde::Serialize>(
    vault_root: String,
    db_path: String,
    operation: impl FnOnce(&mut BridgeKernel) -> BridgeEnvelope<T>,
) -> String {
    match BridgeKernel::open(vault_root, db_path) {
        Ok(mut kernel) => serialize_output(&operation(&mut kernel)),
        Err(source) => serialize_output(&BridgeEnvelope::<JsonValue>::failure(
            BridgeError::with_code(BRIDGE_ERROR_INIT_FAILED, source.to_string())
                .with_hint("ensure vault and sqlite paths are valid"),
        )),
    }
}

fn serialize_output<T: serde::Serialize>(payload: &T) -> String {
    serde_json::to_string(payload).unwrap_or_else(|source| {
        format!(
            "{{\"schema_version\":\"{}\",\"ok\":false,\"value\":null,\"error\":{{\"code\":\"{}\",\"message\":\"{}\",\"hint\":null,\"context\":{{}}}}}}",
            BRIDGE_SCHEMA_VERSION, BRIDGE_ERROR_SERIALIZE_FAILED, source
        )
    })
}
