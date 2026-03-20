use super::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CommandResult {
    pub(crate) command: String,
    pub(crate) summary: String,
    pub(crate) args: JsonValue,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ExtractedTask {
    pub(crate) path: String,
    pub(crate) line: usize,
    pub(crate) state: String,
    pub(crate) text: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct JsonEnvelope<T: Serialize> {
    pub(crate) ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) data: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) error: Option<JsonError>,
    pub(crate) meta: JsonMeta,
}

#[derive(Debug, Serialize)]
pub(crate) struct JsonError {
    pub(crate) code: String,
    pub(crate) message: String,
    pub(crate) hint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) details: Option<JsonValue>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct JsonMeta {
    pub(crate) tool: String,
    pub(crate) elapsed: u128,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) total: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) has_more: Option<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExitKind {
    Success = 0,
    Failure = 1,
    Blocked = 2,
}

#[derive(Debug)]
pub(crate) struct ClassifiedCliError {
    pub(crate) exit_kind: ExitKind,
    pub(crate) error: JsonError,
}

#[derive(Debug)]
pub(crate) struct CliContractError {
    pub(crate) exit_kind: ExitKind,
    pub(crate) code: &'static str,
    pub(crate) message: String,
    pub(crate) hint: Option<String>,
    pub(crate) details: Option<JsonValue>,
}

#[derive(Debug)]
pub(crate) struct RunResult {
    pub(crate) exit_kind: ExitKind,
    pub(crate) stdout: Option<String>,
    pub(crate) stderr: Option<String>,
    pub(crate) clap_output: Option<ClapOutput>,
}

#[derive(Debug)]
pub(crate) enum ClapOutput {
    RootHelp,
    Error(clap::Error),
}

impl<T: Serialize> JsonEnvelope<T> {
    pub(crate) fn success(data: T, meta: JsonMeta) -> Self {
        Self {
            ok: true,
            data: Some(data),
            error: None,
            meta,
        }
    }

    pub(crate) fn failure(error: JsonError, meta: JsonMeta) -> Self {
        Self {
            ok: false,
            data: None,
            error: Some(error),
            meta,
        }
    }
}

impl CliContractError {
    pub(crate) fn blocked(
        code: &'static str,
        message: impl Into<String>,
        hint: impl Into<Option<String>>,
        details: Option<JsonValue>,
    ) -> Self {
        Self {
            exit_kind: ExitKind::Blocked,
            code,
            message: message.into(),
            hint: hint.into(),
            details,
        }
    }
}

impl std::fmt::Display for CliContractError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for CliContractError {}

pub(crate) fn emit_output(output: &str, to_stderr: bool) {
    if to_stderr {
        if output.ends_with('\n') {
            eprint!("{output}");
        } else {
            eprintln!("{output}");
        }
    } else if output.ends_with('\n') {
        print!("{output}");
    } else {
        println!("{output}");
    }
}

pub(crate) fn emit_clap_output(output: ClapOutput) {
    match output {
        ClapOutput::RootHelp => {
            let mut command = Cli::command();
            command.print_help().expect("print tao root help");
            println!();
        }
        ClapOutput::Error(error) => {
            error.print().expect("print tao clap output");
        }
    }
}

pub(crate) fn handle_parse_error(error: clap::Error, json_output: bool) -> RunResult {
    match error.kind() {
        ClapErrorKind::DisplayHelp
        | ClapErrorKind::DisplayVersion
        | ClapErrorKind::MissingSubcommand
        | ClapErrorKind::DisplayHelpOnMissingArgumentOrSubcommand => RunResult {
            exit_kind: ExitKind::Success,
            stdout: None,
            stderr: None,
            clap_output: Some(match error.kind() {
                ClapErrorKind::DisplayHelp | ClapErrorKind::DisplayVersion => {
                    ClapOutput::Error(error)
                }
                _ => ClapOutput::RootHelp,
            }),
        },
        _ if json_output => {
            let classified = classify_parse_error(&error);
            let rendered = render_error_output_for_tool("tao", Duration::ZERO, &classified)
                .unwrap_or_else(|render_source| {
                    fallback_json_error(
                        "tao",
                        Duration::ZERO,
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
        }
        _ => RunResult {
            exit_kind: ExitKind::Failure,
            stdout: None,
            stderr: Some(error.to_string()),
            clap_output: None,
        },
    }
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn render_error_output(error: &anyhow::Error) -> Result<String> {
    let classified = classify_cli_error(error);
    render_error_output_for_tool("tao", Duration::ZERO, &classified)
}

pub(crate) fn render_error_output_for_tool(
    tool: &str,
    elapsed: Duration,
    classified: &ClassifiedCliError,
) -> Result<String> {
    let envelope = JsonEnvelope::<JsonValue>::failure(
        JsonError {
            code: classified.error.code.clone(),
            message: classified.error.message.clone(),
            hint: classified.error.hint.clone(),
            details: classified.error.details.clone(),
        },
        error_meta(tool, elapsed),
    );
    serde_json::to_string(&envelope).context("serialize json error envelope")
}

pub(crate) fn fallback_json_error(
    tool: &str,
    elapsed: Duration,
    classified_error: &JsonError,
    render_message: &str,
) -> String {
    serde_json::json!({
        "ok": false,
        "error": {
            "code": "render_failed",
            "message": format!("failed to serialize cli envelope: {render_message}"),
            "hint": "inspect the original error payload in details and retry",
            "details": {
                "original": classified_error,
            },
        },
        "meta": error_meta(tool, elapsed),
    })
    .to_string()
}

pub(crate) fn classify_parse_error(error: &clap::Error) -> ClassifiedCliError {
    ClassifiedCliError {
        exit_kind: ExitKind::Failure,
        error: JsonError {
            code: "invalid_argument".to_string(),
            message: error.to_string().trim().to_string(),
            hint: Some(
                "rerun with --text --help or use `tao tools` to inspect the public surface"
                    .to_string(),
            ),
            details: Some(serde_json::json!({
                "kind": format!("{:?}", error.kind()),
            })),
        },
    }
}

pub(crate) fn classify_cli_error(error: &anyhow::Error) -> ClassifiedCliError {
    if let Some(contract_error) = error.downcast_ref::<CliContractError>() {
        return ClassifiedCliError {
            exit_kind: contract_error.exit_kind,
            error: JsonError {
                code: contract_error.code.to_string(),
                message: contract_error.message.clone(),
                hint: contract_error.hint.clone(),
                details: contract_error.details.clone(),
            },
        };
    }

    let message = error.to_string();
    let (exit_kind, code, hint) = if message.contains("--allow-writes") {
        (
            ExitKind::Blocked,
            "write_disabled",
            Some("pass --allow-writes to enable write operations".to_string()),
        )
    } else if message.contains("parse --where failed") || message.contains("parse --sort failed") {
        (
            ExitKind::Failure,
            "query_parse_error",
            Some("fix query expression syntax and retry".to_string()),
        )
    } else if message.contains("connect daemon socket") {
        (
            ExitKind::Blocked,
            "daemon_unavailable",
            Some("daemon auto-start failed; check socket path permissions or override --daemon-socket".to_string()),
        )
    } else if message.contains("resolve sdk config failed")
        || message.contains("vault root does not exist")
        || message.contains("vault root is not a directory")
        || message.contains("prepare runtime paths failed")
        || message.contains("open sqlite database")
    {
        (
            ExitKind::Blocked,
            "blocked_prerequisite",
            Some("fix the vault or database configuration and retry".to_string()),
        )
    } else if message.contains("unsupported query scope")
        || message.contains("requires --view-name")
        || message.contains("unknown tool")
        || message.contains("must not")
    {
        (
            ExitKind::Failure,
            "invalid_argument",
            Some("check command arguments and retry".to_string()),
        )
    } else {
        (
            ExitKind::Failure,
            "command_failed",
            Some("inspect message and rerun with corrected inputs".to_string()),
        )
    };

    ClassifiedCliError {
        exit_kind,
        error: JsonError {
            code: code.to_string(),
            message,
            hint,
            details: None,
        },
    }
}

pub(crate) fn tool_name_for_command(command: &Commands) -> String {
    match command {
        Commands::Tools(_) => "tools".to_string(),
        Commands::Health(_) => "health".to_string(),
        Commands::Doc { command } => match command {
            DocCommands::Read(_) => "doc.read".to_string(),
            DocCommands::Write(_) => "doc.write".to_string(),
            DocCommands::List(_) => "doc.list".to_string(),
        },
        Commands::Base { command } => match command {
            BaseCommands::List(_) => "base.list".to_string(),
            BaseCommands::View(_) => "base.view".to_string(),
            BaseCommands::Schema(_) => "base.schema".to_string(),
            BaseCommands::Validate(_) => "base.validate".to_string(),
        },
        Commands::Graph { command } => match command {
            GraphCommands::Outgoing(_) => "graph.outgoing".to_string(),
            GraphCommands::Backlinks(_) => "graph.backlinks".to_string(),
            GraphCommands::InboundScope(_) => "graph.inbound-scope".to_string(),
            GraphCommands::Unresolved(_) => "graph.unresolved".to_string(),
            GraphCommands::Deadends(_) => "graph.deadends".to_string(),
            GraphCommands::Orphans(_) => "graph.orphans".to_string(),
            GraphCommands::Floating(_) => "graph.floating".to_string(),
            GraphCommands::Components(_) => "graph.components".to_string(),
            GraphCommands::Neighbors(_) => "graph.neighbors".to_string(),
            GraphCommands::Path(_) => "graph.path".to_string(),
            GraphCommands::Walk(_) => "graph.walk".to_string(),
        },
        Commands::Meta { command } => match command {
            MetaCommands::Properties(_) => "meta.properties".to_string(),
            MetaCommands::Tags(_) => "meta.tags".to_string(),
            MetaCommands::Aliases(_) => "meta.aliases".to_string(),
            MetaCommands::Tasks(_) => "meta.tasks".to_string(),
        },
        Commands::Task { command } => match command {
            TaskCommands::List(_) => "task.list".to_string(),
            TaskCommands::SetState(_) => "task.set-state".to_string(),
        },
        Commands::Query(_) => "query.run".to_string(),
        Commands::Vault { command } => match command {
            VaultCommands::Open(_) => "vault.open".to_string(),
            VaultCommands::Stats(_) => "vault.stats".to_string(),
            VaultCommands::Preflight(_) => "vault.preflight".to_string(),
            VaultCommands::Reindex(_) => "vault.reindex".to_string(),
            VaultCommands::Reconcile(_) => "vault.reconcile".to_string(),
            VaultCommands::Daemon { command } => match command {
                DaemonCommands::Start(_) => "vault.daemon.start".to_string(),
                DaemonCommands::Status(_) => "vault.daemon.status".to_string(),
                DaemonCommands::Stop(_) => "vault.daemon.stop".to_string(),
                DaemonCommands::StopAll(_) => "vault.daemon.stop_all".to_string(),
            },
            VaultCommands::DaemonServe(_) => "vault.daemon.serve".to_string(),
        },
    }
}

pub(crate) fn success_meta(tool: &str, elapsed: Duration, data: &JsonValue) -> JsonMeta {
    let count = payload_count(data);
    let total = payload_total(data, count);
    JsonMeta {
        tool: tool.to_string(),
        elapsed: elapsed.as_millis(),
        count,
        total,
        has_more: payload_has_more(data, count, total),
    }
}

pub(crate) fn error_meta(tool: &str, elapsed: Duration) -> JsonMeta {
    JsonMeta {
        tool: tool.to_string(),
        elapsed: elapsed.as_millis(),
        count: None,
        total: None,
        has_more: None,
    }
}

pub(crate) fn payload_count(data: &JsonValue) -> Option<u64> {
    for key in ["items", "rows", "tools", "checks"] {
        if let Some(items) = data.get(key).and_then(JsonValue::as_array) {
            return Some(u64::try_from(items.len()).unwrap_or(u64::MAX));
        }
    }
    None
}

pub(crate) fn payload_total(data: &JsonValue, count: Option<u64>) -> Option<u64> {
    data.get("total").and_then(JsonValue::as_u64).or_else(|| {
        if data.get("tools").is_some() || data.get("checks").is_some() {
            count
        } else {
            None
        }
    })
}

pub(crate) fn payload_has_more(
    data: &JsonValue,
    count: Option<u64>,
    total: Option<u64>,
) -> Option<bool> {
    let total = total?;
    let count = count.unwrap_or(0);

    if let Some(offset) = data.get("offset").and_then(JsonValue::as_u64) {
        return Some(offset.saturating_add(count) < total);
    }
    if let (Some(page), Some(page_size)) = (
        data.get("page").and_then(JsonValue::as_u64),
        data.get("page_size").and_then(JsonValue::as_u64),
    ) {
        return Some(page.saturating_mul(page_size) < total);
    }

    Some(count < total)
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn render_output(json: bool, result: &CommandResult) -> Result<String> {
    render_output_with_elapsed(json, result, Duration::ZERO)
}

pub(crate) fn render_output_with_elapsed(
    json: bool,
    result: &CommandResult,
    elapsed: Duration,
) -> Result<String> {
    if json {
        Ok(serde_json::to_string(&JsonEnvelope::success(
            result.args.clone(),
            success_meta(&result.command, elapsed, &result.args),
        ))?)
    } else {
        Ok(result.summary.clone())
    }
}

pub(crate) fn retag_result(
    mut result: CommandResult,
    command: &str,
    summary: &str,
) -> CommandResult {
    result.command = command.to_string();
    result.summary = summary.to_string();
    result
}
