use std::collections::{HashMap, HashSet, VecDeque};
use std::ffi::OsString;
use std::fs;
use std::io::{Read, Write};
#[cfg(unix)]
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
#[cfg(unix)]
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::process::{Command as ProcessCommand, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use clap::{Args, CommandFactory, Parser, Subcommand, error::ErrorKind as ClapErrorKind};
use rusqlite::{Connection, OpenFlags};
use serde::ser::{SerializeMap, SerializeSeq};
use serde::{Deserialize, Serialize, Serializer};
use serde_json::Value as JsonValue;
use tao_sdk_bases::{
    BaseDiagnosticSeverity, BaseDocument, BaseTableQueryPlanner, BaseViewRegistry,
    TableQueryPlanRequest, decode_base_config_json, validate_base_config_json, validate_base_yaml,
};
use tao_sdk_bridge::{BridgeEnvelope, BridgeKernel};
use tao_sdk_properties::{FrontMatterStatus, extract_front_matter};
use tao_sdk_search::{
    LogicalPlanBuilder, LogicalQueryPlanRequest, PhysicalPlanBuilder, PhysicalPlanOptimizer,
    SearchQueryProjectedItem, SearchQueryProjection, SearchQueryRequest, SearchQueryService,
    SortKey, WhereExpr, apply_sort, apply_where_filter, parse_sort_keys,
    parse_where_expression_opt,
};
use tao_sdk_service::{
    BacklinkGraphService, BaseTableExecutionOptions, BaseTableExecutorService, GraphPathRequest,
    GraphScopedInboundRequest, GraphWalkDirection, GraphWalkRequest, HealthSnapshotService,
    IndexRefreshMode, IndexRefreshOptions, IndexRefreshService, ReconciliationScanMode,
    SdkConfigInspectionService, SdkConfigLoader, SdkConfigOverrides, SearchKind,
    VaultSearchRequest, VaultSearchService, WatcherStatus, ensure_runtime_paths,
};
#[cfg(test)]
use tao_sdk_service::{CURRENT_LINK_RESOLUTION_VERSION, LINK_RESOLUTION_VERSION_STATE_KEY};
use tao_sdk_storage::{
    BasesRepository, SearchSegmentMatch, SearchSegmentRepository, TasksRepository,
    preflight_migrations, run_migrations,
};
use tao_sdk_vault::{
    CasePolicy, PathCanonicalizationService, VaultScanService, validate_relative_vault_path,
};
use tao_sdk_watch::{VaultChangeMonitor, WatchReconcileService};

mod args;
mod commands;
mod continuation;
mod contract;
mod daemon;
#[cfg(unix)]
mod daemon_server;
#[cfg(unix)]
mod daemon_socket;
mod helpers;
mod query_docs;
mod registry;
mod runtime;
mod runtime_context;

use args::*;
use continuation::*;
use contract::*;
use daemon::*;
use helpers::*;
use query_docs::*;
use runtime::*;
use runtime_context::*;

const DEFAULT_DAEMON_STARTUP_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_DAEMON_SOCKET_DIR: &str = ".tools/tao/daemons";
const QUERY_DOCS_POST_FILTER_PAGE_LIMIT: u64 = 1_000;

pub fn run() -> i32 {
    let result = run_from_args(std::env::args_os().collect());
    if let Some(clap_output) = result.clap_output {
        emit_clap_output(clap_output);
    } else {
        if let Some(stdout) = result.stdout.as_deref()
            && emit_output(stdout, false).is_err()
        {
            return ExitKind::Failure.code();
        }
        if let Some(stderr) = result.stderr.as_deref()
            && emit_output(stderr, true).is_err()
        {
            return ExitKind::Failure.code();
        }
    }
    result.exit_kind.code()
}

fn run_from_args(raw_args: Vec<OsString>) -> RunResult {
    let cli = match Cli::try_parse_from(raw_args.clone()) {
        Ok(cli) => cli,
        Err(error) => return handle_parse_error(error, &raw_args),
    };

    let started_at = Instant::now();
    let tool = tool_name_for_command(&cli.command);
    let output_format = if cli.toon {
        OutputFormat::Toon
    } else {
        OutputFormat::Json
    };
    let run = || -> Result<String> {
        validate_continuation_usage(&cli.command, cli.continuation.as_deref())?;
        if let Commands::Query(args) = &cli.command {
            commands::query::validate_capabilities(args)?;
        }
        if let Commands::Doc { command } = &cli.command {
            commands::doc::validate_args(command)?;
        }
        // Resolve once in the caller before routing. Every subsequent resolver,
        // including fallback execution, sees the same owned settings snapshot.
        let context = resolve_command_vault_paths(&cli.command)?;
        let _routing_scope = RequestScope::new(
            context,
            false,
            started_at + Duration::from_millis(cli.timeout_ms),
            std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        )
        .enter();
        if let Some(output) = maybe_forward_to_daemon(&cli)? {
            return Ok(output);
        }
        execute_direct_cli(&cli, started_at, output_format)
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
                let rendered = render_error_output_for_tool_with_format(
                    &tool,
                    started_at.elapsed(),
                    &classified,
                    output_format,
                )
                .unwrap_or_else(|render_source| {
                    fallback_render_error(
                        &tool,
                        started_at.elapsed(),
                        &classified.error,
                        &render_source.to_string(),
                        output_format,
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

fn execute_direct_cli(
    cli: &Cli,
    started_at: Instant,
    output_format: OutputFormat,
) -> Result<String> {
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    let resolved = resolve_command_vault_paths(&cli.command)?;
    let deadline = started_at + Duration::from_millis(cli.timeout_ms);
    let cancelled = Arc::new(AtomicBool::new(false));
    let policy = daemon_execution_policy(&cli.command);
    let write_scope =
        RequestScope::new(resolved.clone(), false, deadline, Arc::clone(&cancelled)).enter();
    if policy.refreshes_runtime()
        && let Some(resolved) = &resolved
    {
        let mut runtime = RuntimeMode::OneShot;
        with_connection(&mut runtime, resolved, |connection| {
            IndexRefreshService.refresh(
                Path::new(&resolved.vault_root),
                connection,
                resolved.case_policy,
                IndexRefreshOptions::default(),
            )?;
            Ok(())
        })?;
    }
    drop(write_scope);
    let observational = policy != DaemonExecutionPolicy::ExplicitWork;
    let _scope = RequestScope::new(resolved.clone(), observational, deadline, cancelled).enter();
    let mut runtime = if observational {
        RuntimeMode::Snapshot(Box::<RuntimeCache>::default())
    } else {
        RuntimeMode::OneShot
    };
    let continuation = prepare_continuation(
        &mut runtime,
        resolved.as_ref(),
        &cli.command,
        cli.continuation.as_deref(),
        cli.toon,
        cli.json_stream,
    )?;
    let output = if let Some(output) =
        maybe_render_streaming_output_for_command(&cli.command, cli.json_stream, &mut runtime)?
    {
        output
    } else {
        let result = dispatch_with_runtime(cli.command.clone(), &mut runtime)?;
        render_output_with_format(OutputFormat::Json, &result, started_at.elapsed())?
    };
    if !matches!(
        &cli.command,
        Commands::Vault {
            command: VaultCommands::DaemonServe(_)
                | VaultCommands::Daemon {
                    command: DaemonCommands::Start(DaemonStartArgs {
                        foreground: true,
                        ..
                    })
                }
        }
    ) {
        check_request_deadline()?;
    }
    if output.len() as u64 > MAX_DAEMON_RESPONSE_BYTES {
        return Err(runtime_error(
            "response_too_large",
            "response exceeds 16 MiB; request a smaller window",
        ));
    }
    let output = decorate_runtime_output(output, "direct", "bypass", false, &new_request_id())?;
    let output = decorate_continuation(output, continuation.as_ref())?;
    match output_format {
        OutputFormat::Json => Ok(output),
        OutputFormat::Toon => Ok(toon_format::encode_default(&serde_json::from_str::<
            JsonValue,
        >(&output)?)?),
    }
}

#[cfg(test)]
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
    commands::query::validate_capabilities(args)?;
    if args.explain {
        return Ok(None);
    }
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

#[cfg(test)]
fn dispatch(command: Commands) -> Result<CommandResult> {
    let mut runtime = RuntimeMode::OneShot;
    dispatch_with_runtime(command, &mut runtime)
}

fn dispatch_with_runtime(command: Commands, runtime: &mut RuntimeMode) -> Result<CommandResult> {
    match command {
        Commands::Tools(args) => commands::tools::dispatch(args),
        Commands::Health(args) => commands::health::dispatch(args, runtime),
        Commands::Config { command } => commands::config::dispatch(command),
        Commands::Doc { command } => commands::doc::dispatch(command, runtime),
        Commands::Base { command } => commands::base::dispatch(command, runtime),
        Commands::Graph { command } => commands::graph::dispatch(command, runtime),
        Commands::Meta { command } => commands::meta::dispatch(command, runtime),
        Commands::Task { command } => commands::task::dispatch(command, runtime),
        Commands::Validate(args) => commands::validate::dispatch(args, runtime),
        Commands::Search(args) => commands::search::dispatch(args, runtime),
        Commands::Query(args) => commands::query::dispatch(args, runtime),
        Commands::Vault { command } => commands::vault::dispatch(command, runtime),
    }
}

fn handle_graph(command: GraphCommands, runtime: &mut RuntimeMode) -> Result<CommandResult> {
    commands::graph::handle(command, runtime)
}

fn handle_meta(command: MetaCommands, runtime: &mut RuntimeMode) -> Result<CommandResult> {
    commands::meta::handle(command, runtime)
}

fn handle_task(command: TaskCommands, runtime: &mut RuntimeMode) -> Result<CommandResult> {
    commands::task::handle(command, runtime)
}

#[cfg(test)]
#[path = "cli_impl/tests/mod.rs"]
mod tests;
