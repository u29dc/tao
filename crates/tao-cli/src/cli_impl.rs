use std::collections::{HashMap, HashSet, VecDeque};
use std::ffi::OsString;
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
use clap::{
    ArgAction, Args, CommandFactory, Parser, Subcommand, error::ErrorKind as ClapErrorKind,
};
use rusqlite::Connection;
use serde::ser::{SerializeMap, SerializeSeq};
use serde::{Deserialize, Serialize, Serializer};
use serde_json::Value as JsonValue;
use tao_sdk_bases::{
    BaseDiagnosticSeverity, BaseDocument, BaseTableQueryPlanner, BaseViewRegistry,
    TableQueryPlanRequest, decode_base_config_json, validate_base_config_json,
};
use tao_sdk_bridge::{BridgeEnvelope, BridgeKernel};
use tao_sdk_search::{
    LogicalPlanBuilder, LogicalQueryPlanRequest, PhysicalPlanBuilder, PhysicalPlanOptimizer,
    SearchQueryProjectedItem, SearchQueryProjection, SearchQueryRequest, SearchQueryService,
    SortKey, WhereExpr, apply_sort, apply_where_filter, parse_sort_keys,
    parse_where_expression_opt,
};
use tao_sdk_service::{
    BacklinkGraphService, BaseTableExecutionOptions, BaseTableExecutorService,
    BaseValidationService, CURRENT_LINK_RESOLUTION_VERSION, FullIndexService,
    GraphScopedInboundRequest, GraphWalkDirection, GraphWalkRequest, HealthSnapshotService,
    LINK_RESOLUTION_VERSION_STATE_KEY, ReconciliationScannerService, SdkConfigLoader,
    SdkConfigOverrides, WatcherStatus, ensure_runtime_paths,
};
use tao_sdk_storage::{
    BasesRepository, FilesRepository, IndexStateRepository, LinksRepository, PropertiesRepository,
    TasksRepository, preflight_migrations, run_migrations,
};
use tao_sdk_vault::{CasePolicy, PathCanonicalizationService, validate_relative_vault_path};
use tao_sdk_watch::{VaultChangeMonitor, WatchReconcileService};

mod args;
mod commands;
mod contract;
mod daemon;
mod helpers;
mod query_docs;
mod registry;
mod runtime;

use args::*;
use contract::*;
use daemon::*;
use helpers::*;
use query_docs::*;
use runtime::*;

const DEFAULT_DAEMON_STARTUP_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_DAEMON_SOCKET_DIR: &str = ".tools/tao/daemons";
const QUERY_DOCS_POST_FILTER_PAGE_LIMIT: u64 = 1_000;

pub fn run() -> i32 {
    let result = run_from_args(std::env::args_os().collect());
    if let Some(clap_output) = result.clap_output {
        emit_clap_output(clap_output);
    } else {
        if let Some(stdout) = result.stdout.as_deref() {
            emit_output(stdout, false);
        }
        if let Some(stderr) = result.stderr.as_deref() {
            emit_output(stderr, true);
        }
    }
    result.exit_kind as i32
}

fn run_from_args(raw_args: Vec<OsString>) -> RunResult {
    let json_output = !raw_args.iter().any(|arg| arg == "--text");
    let cli = match Cli::try_parse_from(raw_args) {
        Ok(cli) => cli,
        Err(error) => return handle_parse_error(error, json_output),
    };

    let started_at = Instant::now();
    let tool = tool_name_for_command(&cli.command);
    let run = || -> Result<String> {
        if cli.json_stream && !cli.json {
            return Err(anyhow!("--json-stream cannot be used with --text"));
        }
        if let Some(output) = maybe_forward_to_daemon(&cli)? {
            return Ok(output);
        }
        if cli.json
            && let Some(output) = maybe_render_streaming_output(&cli)?
        {
            return Ok(output);
        }

        let result = dispatch(cli.command.clone(), cli.allow_writes)?;
        render_output_with_elapsed(cli.json, &result, started_at.elapsed())
    };

    match run() {
        Ok(output) => RunResult {
            exit_kind: ExitKind::Success,
            stdout: Some(output),
            stderr: None,
            clap_output: None,
        },
        Err(source) => {
            let classified = classify_cli_error(&source);
            if cli.json {
                let rendered =
                    render_error_output_for_tool(&tool, started_at.elapsed(), &classified)
                        .unwrap_or_else(|render_source| {
                            fallback_json_error(
                                &tool,
                                started_at.elapsed(),
                                &classified.error,
                                &render_source.to_string(),
                            )
                        });
                RunResult {
                    exit_kind: classified.exit_kind,
                    stdout: Some(rendered),
                    stderr: None,
                    clap_output: None,
                }
            } else {
                RunResult {
                    exit_kind: classified.exit_kind,
                    stdout: None,
                    stderr: Some(source.to_string()),
                    clap_output: None,
                }
            }
        }
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
        return Ok(None);
    }

    let columns = parse_query_docs_columns(args.select.as_deref())?;
    let projection = query_docs_projection(&columns);
    let resolved = args.resolve()?;
    let started_at = Instant::now();
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
        elapsed: started_at.elapsed().as_millis(),
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
        Commands::Tools(args) => commands::tools::dispatch(args),
        Commands::Health(args) => commands::health::dispatch(args, runtime),
        Commands::Doc { command } => commands::doc::dispatch(command, allow_writes, runtime),
        Commands::Base { command } => commands::base::dispatch(command, runtime),
        Commands::Graph { command } => commands::graph::dispatch(command, runtime),
        Commands::Meta { command } => commands::meta::dispatch(command, runtime),
        Commands::Task { command } => commands::task::dispatch(command, allow_writes, runtime),
        Commands::Query(args) => commands::query::dispatch(args, runtime),
        Commands::Vault { command } => commands::vault::dispatch(command, runtime),
    }
}

fn handle_base(command: BaseCommands, runtime: &mut RuntimeMode) -> Result<CommandResult> {
    commands::base::handle(command, runtime)
}

fn handle_graph(command: GraphCommands, runtime: &mut RuntimeMode) -> Result<CommandResult> {
    commands::graph::handle(command, runtime)
}

fn handle_meta(command: MetaCommands, runtime: &mut RuntimeMode) -> Result<CommandResult> {
    commands::meta::handle(command, runtime)
}

fn handle_task(
    command: TaskCommands,
    allow_writes: bool,
    runtime: &mut RuntimeMode,
) -> Result<CommandResult> {
    commands::task::handle(command, allow_writes, runtime)
}

#[cfg(test)]
#[path = "cli_impl/tests.rs"]
mod tests;
