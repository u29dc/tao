use std::env;
use std::fs;
use std::io::Cursor;
#[cfg(unix)]
use std::os::unix::net::UnixListener;
use std::path::Path;
use std::sync::{Mutex, OnceLock};

use super::{
    CURRENT_LINK_RESOLUTION_VERSION, CachedCommandResult, ClapOutput, Cli, CommandResult, Commands,
    DaemonCommands, DaemonExecutionPolicy, DaemonSocketArgs, DaemonStopAllArgs, DocCommands,
    ExitKind, LINK_RESOLUTION_VERSION_STATE_KEY, NotePutArgs, QueryArgs, RuntimeCache, RuntimeMode,
    VaultCommands, VaultPathArgs, classify_cli_error, daemon_execution_policy,
    derive_daemon_socket_for_vault, dispatch, dispatch_with_runtime, handle_daemon,
    maybe_forward_to_daemon, maybe_refresh_daemon_state, maybe_render_streaming_output,
    prepare_daemon_socket_path, read_bounded_bytes, registry, render_error_output, render_output,
    resolve_command_vault_paths, resolve_daemon_socket_for_cli, run_from_args, runtime_cache_key,
    update_daemon_command_cache,
};
use clap::{CommandFactory, Parser, error::ErrorKind as ClapErrorKind};
use rusqlite::Connection;
use serde_json::Value as JsonValue;
use tao_sdk_storage::{
    FilesRepository, IndexStateRecordInput, IndexStateRepository, LinkRecordInput, LinksRepository,
};

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
    assert!(rendered.contains("tools"));
    assert!(rendered.contains("health"));
    assert!(!rendered.contains("note"));
    assert!(!rendered.contains("links"));
    assert!(!rendered.contains("properties"));
    assert!(!rendered.contains("bases"));
    assert!(!rendered.contains("search"));
    assert!(!rendered.contains("hubs"));
}

#[test]
fn bare_invocation_prints_help_instead_of_json() {
    let result = run_from_args(["tao"].into_iter().map(std::ffi::OsString::from).collect());

    assert_eq!(result.exit_kind, ExitKind::Success);
    assert!(result.stdout.is_none());
    assert!(result.stderr.is_none());
    assert!(matches!(result.clap_output, Some(ClapOutput::RootHelp)));
}

#[test]
fn help_flag_uses_native_clap_output_path() {
    let result = run_from_args(
        ["tao", "--help"]
            .into_iter()
            .map(std::ffi::OsString::from)
            .collect(),
    );

    assert_eq!(result.exit_kind, ExitKind::Success);
    assert!(result.stdout.is_none());
    assert!(result.stderr.is_none());
    match result.clap_output {
        Some(ClapOutput::Error(error)) => {
            assert_eq!(error.kind(), ClapErrorKind::DisplayHelp);
        }
        other => panic!("expected clap help output, got {other:?}"),
    }
}

#[test]
fn json_output_is_one_envelope_object() {
    with_temp_cwd(|| {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let vault_root = tempdir.path().to_path_buf();
        let cli = Cli::parse_from([
            "tao".to_string(),
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
                .get("data")
                .and_then(|raw| raw.get("db_ready"))
                .and_then(serde_json::Value::as_bool),
            Some(true)
        );
        assert_eq!(
            value
                .get("meta")
                .and_then(|raw| raw.get("tool"))
                .and_then(serde_json::Value::as_str),
            Some("vault.open")
        );
        assert!(
            value
                .as_object()
                .is_some_and(|envelope| !envelope.contains_key("error"))
        );
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
        fs::write(notes_dir.join("tasks.md"), "- [ ] ship tao cli\n").expect("write tasks note");
        fs::write(
                views_dir.join("projects.base"),
                "views:\n  - name: ActiveProjects\n    type: table\n    source: notes/projects\n    filters:\n      - key: status\n        op: eq\n        value: active\n    sorts:\n      - key: priority\n        direction: desc\n    columns:\n      - title\n      - status\n      - priority\n",
            )
            .expect("write projects base");

        let vault_root_string = vault_root.to_string_lossy().to_string();

        let scenarios = [
            (
                "vault.open",
                vec!["tao", "vault", "open", "--vault-root", &vault_root_string],
            ),
            (
                "vault.stats",
                vec!["tao", "vault", "stats", "--vault-root", &vault_root_string],
            ),
            (
                "vault.preflight",
                vec![
                    "tao",
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
                vec!["tao", "doc", "list", "--vault-root", &vault_root_string],
            ),
            (
                "doc.write",
                vec![
                    "tao",
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
                    "graph",
                    "backlinks",
                    "--vault-root",
                    &vault_root_string,
                    "--path",
                    "notes/projects/project-a.md",
                ],
            ),
            (
                "graph.inbound-scope",
                vec![
                    "tao",
                    "graph",
                    "inbound-scope",
                    "--vault-root",
                    &vault_root_string,
                    "--scope",
                    "notes",
                    "--include-markdown",
                ],
            ),
            (
                "graph.unresolved",
                vec![
                    "tao",
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
                    "graph",
                    "orphans",
                    "--vault-root",
                    &vault_root_string,
                ],
            ),
            (
                "graph.floating",
                vec![
                    "tao",
                    "graph",
                    "floating",
                    "--vault-root",
                    &vault_root_string,
                ],
            ),
            (
                "graph.components",
                vec![
                    "tao",
                    "graph",
                    "components",
                    "--vault-root",
                    &vault_root_string,
                ],
            ),
            (
                "graph.neighbors",
                vec![
                    "tao",
                    "graph",
                    "neighbors",
                    "--vault-root",
                    &vault_root_string,
                    "--path",
                    "notes/alpha.md",
                ],
            ),
            (
                "graph.path",
                vec![
                    "tao",
                    "graph",
                    "path",
                    "--vault-root",
                    &vault_root_string,
                    "--from",
                    "notes/alpha.md",
                    "--to",
                    "notes/projects/project-a.md",
                ],
            ),
            (
                "graph.walk",
                vec![
                    "tao",
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
                vec!["tao", "base", "list", "--vault-root", &vault_root_string],
            ),
            (
                "base.schema",
                vec![
                    "tao",
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
                    "meta",
                    "properties",
                    "--vault-root",
                    &vault_root_string,
                ],
            ),
            (
                "meta.tags",
                vec!["tao", "meta", "tags", "--vault-root", &vault_root_string],
            ),
            (
                "meta.aliases",
                vec!["tao", "meta", "aliases", "--vault-root", &vault_root_string],
            ),
            (
                "meta.tasks",
                vec!["tao", "meta", "tasks", "--vault-root", &vault_root_string],
            ),
            (
                "task.list",
                vec!["tao", "task", "list", "--vault-root", &vault_root_string],
            ),
            (
                "task.set-state",
                vec![
                    "tao",
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
                    "vault",
                    "reconcile",
                    "--vault-root",
                    &vault_root_string,
                ],
            ),
        ];

        for (expected_command, args) in scenarios {
            let cli = Cli::parse_from(args);
            let result =
                dispatch(cli.command, cli.allow_writes).expect("dispatch json contract scenario");
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
    assert!(envelope.contains_key("data"));
    assert!(envelope.contains_key("meta"));
    assert_eq!(
        envelope.get("ok").and_then(JsonValue::as_bool),
        Some(true),
        "expected ok=true for command {expected_command}",
    );
    assert!(!envelope.contains_key("error"));

    let payload = envelope
        .get("data")
        .and_then(JsonValue::as_object)
        .expect("data payload must be object");
    assert!(!payload.is_empty());
    assert_eq!(
        envelope
            .get("meta")
            .and_then(|meta| meta.get("tool"))
            .and_then(JsonValue::as_str),
        Some(expected_command)
    );
    assert!(
        envelope
            .get("meta")
            .and_then(|meta| meta.get("elapsed"))
            .and_then(JsonValue::as_u64)
            .is_some()
    );
}

#[test]
fn json_error_envelope_uses_stable_write_disabled_code() {
    let cli = Cli::parse_from([
        "tao",
        "doc",
        "write",
        "--vault-root",
        "/tmp",
        "--path",
        "notes/test.md",
        "--content",
        "# test",
    ]);
    let error = dispatch(cli.command, cli.allow_writes).expect_err("write must fail");
    let output = render_error_output(&error).expect("render error output");
    let envelope: JsonValue = serde_json::from_str(&output).expect("parse output");
    assert_eq!(envelope.get("ok").and_then(JsonValue::as_bool), Some(false));
    assert!(
        envelope
            .as_object()
            .is_some_and(|object| !object.contains_key("data"))
    );
    let error_payload = envelope
        .get("error")
        .and_then(JsonValue::as_object)
        .expect("error object");
    assert_eq!(
        error_payload.get("code").and_then(JsonValue::as_str),
        Some("write_disabled")
    );
    assert!(
        error_payload
            .get("message")
            .and_then(JsonValue::as_str)
            .is_some_and(|message| !message.contains("panicked at")),
        "error message should not include stack traces"
    );
}

#[test]
fn json_error_envelope_uses_stable_query_parse_error_code() {
    let cli = Cli::parse_from([
        "tao",
        "query",
        "--vault-root",
        "/tmp",
        "--from",
        "docs",
        "--query",
        "project",
        "--where",
        "title = 'alpha'",
    ]);
    let error = dispatch(cli.command, cli.allow_writes).expect_err("parse must fail");
    let output = render_error_output(&error).expect("render error output");
    let envelope: JsonValue = serde_json::from_str(&output).expect("parse output");
    let error_payload = envelope
        .get("error")
        .and_then(JsonValue::as_object)
        .expect("error object");
    assert_eq!(
        error_payload.get("code").and_then(JsonValue::as_str),
        Some("query_parse_error")
    );
    assert!(error_payload.get("hint").is_some_and(JsonValue::is_string));
}

#[test]
fn runtime_json_failures_return_exit_code_one() {
    let result = run_from_args(
        ["tao", "tools", "missing.tool"]
            .into_iter()
            .map(std::ffi::OsString::from)
            .collect(),
    );

    assert_eq!(result.exit_kind, ExitKind::Failure);
    assert!(result.stderr.is_none());
    let stdout = result.stdout.expect("json stdout");
    let envelope: JsonValue = serde_json::from_str(&stdout).expect("parse json failure");
    assert_eq!(
        envelope
            .get("error")
            .and_then(|error| error.get("code"))
            .and_then(JsonValue::as_str),
        Some("invalid_argument")
    );
}

#[test]
fn blocked_json_failures_return_exit_code_two() {
    let result = run_from_args(
        [
            "tao",
            "vault",
            "stats",
            "--vault-root",
            "/definitely/missing-tao-vault",
        ]
        .into_iter()
        .map(std::ffi::OsString::from)
        .collect(),
    );

    assert_eq!(result.exit_kind, ExitKind::Blocked);
    assert!(result.stderr.is_none());
    let stdout = result.stdout.expect("json stdout");
    let envelope: JsonValue = serde_json::from_str(&stdout).expect("parse blocked failure");
    assert_eq!(
        envelope
            .get("error")
            .and_then(|error| error.get("code"))
            .and_then(JsonValue::as_str),
        Some("blocked_prerequisite")
    );
}

#[test]
fn write_disabled_errors_classify_as_blocked_prerequisites() {
    with_temp_cwd(|| {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let vault_root = tempdir.path().join("vault");
        fs::create_dir_all(vault_root.join("notes")).expect("create notes dir");

        let cli = Cli::parse_from([
            "tao",
            "doc",
            "write",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
            "--path",
            "notes/test.md",
            "--content",
            "# test",
        ]);
        let error = dispatch(cli.command, cli.allow_writes).expect_err("write should fail");
        let classified = classify_cli_error(&error);

        assert_eq!(classified.exit_kind, ExitKind::Blocked);
        let envelope: JsonValue =
            serde_json::from_str(&render_error_output(&error).expect("render error"))
                .expect("parse blocked failure");
        assert_eq!(
            envelope
                .get("error")
                .and_then(|error| error.get("code"))
                .and_then(JsonValue::as_str),
            Some("write_disabled")
        );
    });
}

#[test]
fn health_blocked_prerequisites_use_error_envelope_and_exit_code_two() {
    let result = run_from_args(
        [
            "tao",
            "health",
            "--vault-root",
            "/definitely/missing-tao-vault",
        ]
        .into_iter()
        .map(std::ffi::OsString::from)
        .collect(),
    );

    assert_eq!(result.exit_kind, ExitKind::Blocked);
    assert!(result.stderr.is_none());
    let stdout = result.stdout.expect("json stdout");
    let envelope: JsonValue = serde_json::from_str(&stdout).expect("parse blocked failure");
    assert_eq!(envelope.get("ok").and_then(JsonValue::as_bool), Some(false));
    assert_eq!(
        envelope
            .get("meta")
            .and_then(|meta| meta.get("tool"))
            .and_then(JsonValue::as_str),
        Some("health")
    );
    let details = envelope
        .get("error")
        .and_then(|error| error.get("details"))
        .expect("health blocked details");
    assert_eq!(
        details.get("status").and_then(JsonValue::as_str),
        Some("blocked")
    );
}

#[test]
fn tools_catalog_includes_version_and_optional_query_parameters() {
    let cli = Cli::parse_from(["tao", "tools"]);
    let result = dispatch(cli.command, cli.allow_writes).expect("dispatch tools");
    let output = render_output(cli.json, &result).expect("render tools");
    let envelope: JsonValue = serde_json::from_str(&output).expect("parse tools output");

    assert_eq!(
        envelope
            .get("data")
            .and_then(|data| data.get("version"))
            .and_then(JsonValue::as_str),
        Some(env!("CARGO_PKG_VERSION"))
    );

    let tools_entry = registry::tool_detail("tools").expect("tools registry entry");
    assert!(
        tools_entry.output_fields.contains(&"version"),
        "tools registry outputFields should advertise version"
    );

    let query_tool = envelope
        .get("data")
        .and_then(|data| data.get("tools"))
        .and_then(JsonValue::as_array)
        .and_then(|tools| {
            tools
                .iter()
                .find(|tool| tool.get("name").and_then(JsonValue::as_str) == Some("query.run"))
        })
        .expect("query.run tool");
    let parameters = query_tool
        .get("parameters")
        .and_then(JsonValue::as_array)
        .expect("query.run parameters");

    let path = parameters
        .iter()
        .find(|parameter| parameter.get("name").and_then(JsonValue::as_str) == Some("path"))
        .expect("query.run path parameter");
    assert_eq!(
        path.get("required").and_then(JsonValue::as_bool),
        Some(false)
    );

    let view_name = parameters
        .iter()
        .find(|parameter| parameter.get("name").and_then(JsonValue::as_str) == Some("view_name"))
        .expect("query.run view_name parameter");
    assert_eq!(
        view_name.get("required").and_then(JsonValue::as_bool),
        Some(false)
    );
}

#[test]
fn parse_failures_default_to_json_error_envelope() {
    let result = run_from_args(
        ["tao", "note", "read"]
            .into_iter()
            .map(std::ffi::OsString::from)
            .collect(),
    );

    assert_eq!(result.exit_kind, ExitKind::Failure);
    assert!(result.stderr.is_none());
    let stdout = result.stdout.expect("json stdout");
    let envelope: JsonValue = serde_json::from_str(&stdout).expect("parse cli parse error");
    assert_eq!(
        envelope
            .get("error")
            .and_then(|error| error.get("code"))
            .and_then(JsonValue::as_str),
        Some("invalid_argument")
    );
    assert_eq!(
        envelope
            .get("meta")
            .and_then(|meta| meta.get("tool"))
            .and_then(JsonValue::as_str),
        Some("tao")
    );
}

#[test]
fn write_commands_are_blocked_without_allow_writes_flag() {
    with_temp_cwd(|| {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let vault_root = tempdir.path().join("vault");
        let notes_dir = vault_root.join("notes");
        fs::create_dir_all(&vault_root).expect("create vault dir");
        fs::create_dir_all(&notes_dir).expect("create notes dir");
        fs::write(notes_dir.join("tasks.md"), "- [ ] blocked task\n").expect("write task fixture");

        let doc_write = Cli::parse_from([
            "tao",
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
fn task_set_state_rejects_paths_outside_the_vault_boundary() {
    with_temp_cwd(|| {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let vault_root = tempdir.path().join("vault");
        let outside_path = tempdir.path().join("outside.md");
        fs::create_dir_all(vault_root.join("notes")).expect("create notes");
        fs::write(vault_root.join("notes/tasks.md"), "- [ ] inside task\n").expect("write note");
        fs::write(&outside_path, "- [ ] outside task\n").expect("write outside");

        let absolute = Cli::parse_from([
            "tao",
            "--allow-writes",
            "task",
            "set-state",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
            "--path",
            outside_path.to_string_lossy().as_ref(),
            "--line",
            "1",
            "--state",
            "done",
        ]);
        let absolute_error = dispatch(absolute.command, absolute.allow_writes)
            .expect_err("absolute path should be rejected");
        assert!(absolute_error.to_string().contains("vault-relative"));
        assert_eq!(
            fs::read_to_string(&outside_path).expect("read outside after absolute attempt"),
            "- [ ] outside task\n"
        );

        let parent = Cli::parse_from([
            "tao",
            "--allow-writes",
            "task",
            "set-state",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
            "--path",
            "../outside.md",
            "--line",
            "1",
            "--state",
            "done",
        ]);
        let parent_error = dispatch(parent.command, parent.allow_writes)
            .expect_err("parent traversal should be rejected");
        assert!(parent_error.to_string().contains("traverse"));
        assert_eq!(
            fs::read_to_string(&outside_path).expect("read outside after traversal attempt"),
            "- [ ] outside task\n"
        );
    });
}

#[test]
fn vault_commands_use_configured_default_root_when_vault_root_arg_is_omitted() {
    with_temp_cwd(|| {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let vault_root = tempdir.path().join("vault");
        fs::create_dir_all(vault_root.join("notes")).expect("create notes");
        fs::write(vault_root.join("notes/a.md"), "# A\n").expect("write note");

        fs::write(
            Path::new("config.toml"),
            format!(
                r#"[vault]
root = "{}"

[security]
read_only = true
"#,
                vault_root.display()
            ),
        )
        .expect("write root config");

        let cli = Cli::parse_from(["tao", "vault", "stats"]);
        let result = dispatch(cli.command, cli.allow_writes).expect("dispatch");
        let output = render_output(cli.json, &result).expect("render output");
        let envelope: JsonValue = serde_json::from_str(&output).expect("parse output");
        let resolved_root = envelope
            .get("data")
            .and_then(|raw| raw.get("vault_root"))
            .and_then(JsonValue::as_str)
            .expect("resolved vault root");

        assert_eq!(
            Path::new(resolved_root),
            fs::canonicalize(vault_root)
                .expect("canonical vault")
                .as_path()
        );
    });
}

#[test]
fn write_commands_are_enabled_when_read_only_policy_is_disabled_in_config() {
    with_temp_cwd(|| {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let vault_root = tempdir.path().join("vault");
        fs::create_dir_all(vault_root.join("notes")).expect("create notes");
        fs::write(vault_root.join("notes/a.md"), "# A\n").expect("write note");

        fs::write(
            Path::new("config.toml"),
            format!(
                r#"[vault]
root = "{}"

[security]
read_only = false
"#,
                vault_root.display()
            ),
        )
        .expect("write root config");

        let cli = Cli::parse_from([
            "tao",
            "doc",
            "write",
            "--path",
            "notes/policy-write.md",
            "--content",
            "# policy",
        ]);
        let result = dispatch(cli.command, cli.allow_writes).expect("dispatch doc write");
        let output = render_output(cli.json, &result).expect("render output");
        let envelope: JsonValue = serde_json::from_str(&output).expect("parse output");
        assert_eq!(envelope.get("ok").and_then(JsonValue::as_bool), Some(true));
        assert!(
            vault_root.join("notes/policy-write.md").exists(),
            "write should succeed when read_only=false"
        );
    });
}

#[test]
fn query_docs_select_projects_requested_columns_only() {
    with_temp_cwd(|| {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let vault_root = tempdir.path().join("vault");
        fs::create_dir_all(vault_root.join("notes/projects")).expect("create notes");
        fs::write(
            vault_root.join("notes/projects/alpha.md"),
            "# Alpha\nproject roadmap",
        )
        .expect("write alpha");
        fs::write(
            vault_root.join("notes/projects/beta.md"),
            "# Beta\nproject updates",
        )
        .expect("write beta");

        let open = Cli::parse_from([
            "tao",
            "vault",
            "open",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(open.command, open.allow_writes).expect("open vault");
        let reindex = Cli::parse_from([
            "tao",
            "vault",
            "reindex",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(reindex.command, reindex.allow_writes).expect("reindex vault");

        let cli = Cli::parse_from([
            "tao",
            "query",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
            "--from",
            "docs",
            "--query",
            "project",
            "--select",
            "path,title",
            "--limit",
            "10",
            "--offset",
            "0",
        ]);
        let result = dispatch(cli.command, cli.allow_writes).expect("dispatch docs query");
        let output = render_output(cli.json, &result).expect("render output");
        let envelope: JsonValue = serde_json::from_str(&output).expect("parse output");
        let columns = envelope
            .get("data")
            .and_then(|args| args.get("columns"))
            .and_then(JsonValue::as_array)
            .expect("columns array");
        let column_names = columns
            .iter()
            .filter_map(JsonValue::as_str)
            .collect::<Vec<_>>();
        assert_eq!(column_names, vec!["path", "title"]);
        let rows = envelope
            .get("data")
            .and_then(|args| args.get("rows"))
            .and_then(JsonValue::as_array)
            .expect("rows array");
        assert!(!rows.is_empty(), "expected at least one query row");
        for row in rows {
            let object = row.as_object().expect("row object");
            assert!(object.contains_key("path"));
            assert!(object.contains_key("title"));
            assert!(!object.contains_key("file_id"));
            assert!(!object.contains_key("matched_in"));
        }
    });
}

#[test]
fn query_docs_where_uses_unselected_title_field() {
    with_temp_cwd(|| {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let vault_root = tempdir.path().join("vault");
        fs::create_dir_all(vault_root.join("notes/projects")).expect("create notes");
        fs::write(
            vault_root.join("notes/projects/alpha.md"),
            "# Alpha\nproject roadmap",
        )
        .expect("write alpha");
        fs::write(
            vault_root.join("notes/projects/beta.md"),
            "# Beta\nproject roadmap",
        )
        .expect("write beta");

        let open = Cli::parse_from([
            "tao",
            "vault",
            "open",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(open.command, open.allow_writes).expect("open vault");
        let reindex = Cli::parse_from([
            "tao",
            "vault",
            "reindex",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(reindex.command, reindex.allow_writes).expect("reindex vault");

        let cli = Cli::parse_from([
            "tao",
            "query",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
            "--from",
            "docs",
            "--query",
            "project",
            "--where",
            "title starts_with 'a'",
            "--select",
            "path",
            "--limit",
            "10",
            "--offset",
            "0",
        ]);
        let result = dispatch(cli.command, cli.allow_writes).expect("dispatch docs query");
        let output = render_output(cli.json, &result).expect("render output");
        let envelope: JsonValue = serde_json::from_str(&output).expect("parse output");
        let rows = envelope
            .get("data")
            .and_then(|args| args.get("rows"))
            .and_then(JsonValue::as_array)
            .expect("rows array");
        assert_eq!(rows.len(), 1);
        let row = rows[0].as_object().expect("row object");
        assert_eq!(
            row.get("path").and_then(JsonValue::as_str),
            Some("notes/projects/alpha.md")
        );
        assert!(!row.contains_key("title"));
    });
}

#[test]
fn query_docs_sort_uses_unselected_title_field() {
    with_temp_cwd(|| {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let vault_root = tempdir.path().join("vault");
        fs::create_dir_all(vault_root.join("notes/a")).expect("create first dir");
        fs::create_dir_all(vault_root.join("notes/z")).expect("create second dir");
        fs::write(vault_root.join("notes/a/zeta.md"), "# Zeta\nproject").expect("write zeta");
        fs::write(vault_root.join("notes/z/alpha.md"), "# Alpha\nproject").expect("write alpha");

        let open = Cli::parse_from([
            "tao",
            "vault",
            "open",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(open.command, open.allow_writes).expect("open vault");
        let reindex = Cli::parse_from([
            "tao",
            "vault",
            "reindex",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(reindex.command, reindex.allow_writes).expect("reindex vault");

        let cli = Cli::parse_from([
            "tao",
            "query",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
            "--from",
            "docs",
            "--query",
            "project",
            "--sort",
            "title:asc",
            "--select",
            "path",
            "--limit",
            "10",
            "--offset",
            "0",
        ]);
        let result = dispatch(cli.command, cli.allow_writes).expect("dispatch docs query");
        let output = render_output(cli.json, &result).expect("render output");
        let envelope: JsonValue = serde_json::from_str(&output).expect("parse output");
        let rows = envelope
            .get("data")
            .and_then(|args| args.get("rows"))
            .and_then(JsonValue::as_array)
            .expect("rows array");
        let paths = rows
            .iter()
            .map(|row| {
                row.get("path")
                    .and_then(JsonValue::as_str)
                    .expect("path")
                    .to_string()
            })
            .collect::<Vec<_>>();
        assert_eq!(paths, vec!["notes/z/alpha.md", "notes/a/zeta.md"]);
        for row in rows {
            let object = row.as_object().expect("row object");
            assert!(!object.contains_key("title"));
        }
    });
}

#[test]
fn query_docs_where_and_sort_are_applied_deterministically() {
    with_temp_cwd(|| {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let vault_root = tempdir.path().join("vault");
        fs::create_dir_all(vault_root.join("notes/projects")).expect("create notes");
        fs::write(
            vault_root.join("notes/projects/alpha.md"),
            "# Alpha\nproject roadmap",
        )
        .expect("write alpha");
        fs::write(
            vault_root.join("notes/projects/beta.md"),
            "# Beta\nproject roadmap",
        )
        .expect("write beta");
        fs::write(
            vault_root.join("notes/projects/gamma.md"),
            "# Gamma\nproject roadmap",
        )
        .expect("write gamma");

        let open = Cli::parse_from([
            "tao",
            "vault",
            "open",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(open.command, open.allow_writes).expect("open vault");
        let reindex = Cli::parse_from([
            "tao",
            "vault",
            "reindex",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(reindex.command, reindex.allow_writes).expect("reindex vault");

        let cli = Cli::parse_from([
            "tao",
            "query",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
            "--from",
            "docs",
            "--query",
            "project",
            "--where",
            "title starts_with 'a' or title starts_with 'b'",
            "--sort",
            "title:desc,path:asc",
            "--select",
            "path,title",
            "--limit",
            "10",
            "--offset",
            "0",
        ]);
        let result = dispatch(cli.command, cli.allow_writes).expect("dispatch docs query");
        let output = render_output(cli.json, &result).expect("render output");
        let envelope: JsonValue = serde_json::from_str(&output).expect("parse output");
        let rows = envelope
            .get("data")
            .and_then(|args| args.get("rows"))
            .and_then(JsonValue::as_array)
            .expect("rows array");
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows[0]
                .get("title")
                .and_then(JsonValue::as_str)
                .unwrap_or_default(),
            "beta"
        );
        assert_eq!(
            rows[1]
                .get("title")
                .and_then(JsonValue::as_str)
                .unwrap_or_default(),
            "alpha"
        );
    });
}

#[test]
fn query_docs_where_and_sort_use_internal_fields_when_select_omits_them() {
    with_temp_cwd(|| {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let vault_root = tempdir.path().join("vault");
        fs::create_dir_all(vault_root.join("notes/projects")).expect("create notes");
        fs::write(
            vault_root.join("notes/projects/alpha.md"),
            "# Alpha\nproject roadmap",
        )
        .expect("write alpha");
        fs::write(
            vault_root.join("notes/projects/beta.md"),
            "# Beta\nproject roadmap",
        )
        .expect("write beta");
        fs::write(
            vault_root.join("notes/projects/gamma.md"),
            "# Gamma\nproject roadmap",
        )
        .expect("write gamma");

        let open = Cli::parse_from([
            "tao",
            "vault",
            "open",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(open.command, open.allow_writes).expect("open vault");
        let reindex = Cli::parse_from([
            "tao",
            "vault",
            "reindex",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(reindex.command, reindex.allow_writes).expect("reindex vault");

        let cli = Cli::parse_from([
            "tao",
            "query",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
            "--from",
            "docs",
            "--query",
            "project",
            "--where",
            "title starts_with 'a' or title starts_with 'b'",
            "--sort",
            "title:desc,path:asc",
            "--select",
            "path",
            "--limit",
            "10",
            "--offset",
            "0",
        ]);
        let result = dispatch(cli.command, cli.allow_writes).expect("dispatch docs query");
        let output = render_output(cli.json, &result).expect("render output");
        let envelope: JsonValue = serde_json::from_str(&output).expect("parse output");
        let rows = envelope
            .get("data")
            .and_then(|args| args.get("rows"))
            .and_then(JsonValue::as_array)
            .expect("rows array");
        assert_eq!(rows.len(), 2);
        let paths = rows
            .iter()
            .map(|row| {
                let object = row.as_object().expect("row object");
                assert_eq!(object.len(), 1);
                object
                    .get("path")
                    .and_then(JsonValue::as_str)
                    .expect("path")
                    .to_string()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            paths,
            vec![
                "notes/projects/beta.md".to_string(),
                "notes/projects/alpha.md".to_string()
            ]
        );
    });
}

#[test]
fn query_docs_where_scans_full_match_set_before_post_filtering() {
    with_temp_cwd(|| {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let vault_root = tempdir.path().join("vault");
        let notes_dir = vault_root.join("notes");
        fs::create_dir_all(&notes_dir).expect("create notes");

        for index in 0..40_u32 {
            let stem = format!("note-{index:03}");
            fs::write(
                notes_dir.join(format!("{stem}.md")),
                format!("# {stem}\nproject"),
            )
            .expect("write note");
        }

        let open = Cli::parse_from([
            "tao",
            "vault",
            "open",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(open.command, open.allow_writes).expect("open vault");
        let reindex = Cli::parse_from([
            "tao",
            "vault",
            "reindex",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(reindex.command, reindex.allow_writes).expect("reindex vault");

        let cli = Cli::parse_from([
            "tao",
            "query",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
            "--from",
            "docs",
            "--query",
            "project",
            "--where",
            "title == 'note-025'",
            "--select",
            "path,title",
            "--limit",
            "5",
            "--offset",
            "0",
        ]);
        let result = dispatch(cli.command, cli.allow_writes).expect("dispatch docs query");
        let output = render_output(cli.json, &result).expect("render output");
        let envelope: JsonValue = serde_json::from_str(&output).expect("parse output");
        let args = envelope
            .get("data")
            .and_then(JsonValue::as_object)
            .expect("args object");
        assert_eq!(args.get("total").and_then(JsonValue::as_u64), Some(1));
        let rows = args
            .get("rows")
            .and_then(JsonValue::as_array)
            .expect("rows array");
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].get("title").and_then(JsonValue::as_str),
            Some("note-025")
        );
    });
}

#[test]
fn query_docs_sort_scans_full_match_set_before_pagination() {
    with_temp_cwd(|| {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let vault_root = tempdir.path().join("vault");
        let notes_dir = vault_root.join("notes");
        fs::create_dir_all(&notes_dir).expect("create notes");

        for index in 0..1105_u32 {
            let stem = format!("note-{index:04}");
            fs::write(
                notes_dir.join(format!("{stem}.md")),
                format!("# {stem}\nproject"),
            )
            .expect("write note");
        }

        let open = Cli::parse_from([
            "tao",
            "vault",
            "open",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(open.command, open.allow_writes).expect("open vault");
        let reindex = Cli::parse_from([
            "tao",
            "vault",
            "reindex",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(reindex.command, reindex.allow_writes).expect("reindex vault");

        let cli = Cli::parse_from([
            "tao",
            "query",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
            "--from",
            "docs",
            "--query",
            "project",
            "--sort",
            "path:asc",
            "--select",
            "path,title",
            "--limit",
            "5",
            "--offset",
            "1000",
        ]);
        let result = dispatch(cli.command, cli.allow_writes).expect("dispatch docs query");
        let output = render_output(cli.json, &result).expect("render output");
        let envelope: JsonValue = serde_json::from_str(&output).expect("parse output");
        let args = envelope
            .get("data")
            .and_then(JsonValue::as_object)
            .expect("args object");
        assert_eq!(args.get("total").and_then(JsonValue::as_u64), Some(1105));
        let rows = args
            .get("rows")
            .and_then(JsonValue::as_array)
            .expect("rows array");
        let paths = rows
            .iter()
            .map(|row| {
                row.get("path")
                    .and_then(JsonValue::as_str)
                    .expect("path")
                    .to_string()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            paths,
            vec![
                "notes/note-1000.md",
                "notes/note-1001.md",
                "notes/note-1002.md",
                "notes/note-1003.md",
                "notes/note-1004.md",
            ]
        );
    });
}

#[test]
fn query_docs_explain_returns_plan_without_rows_when_not_executing() {
    with_temp_cwd(|| {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let vault_root = tempdir.path().join("vault");
        fs::create_dir_all(vault_root.join("notes")).expect("create notes");
        fs::write(vault_root.join("notes/a.md"), "# Alpha\nproject").expect("write note");

        let open = Cli::parse_from([
            "tao",
            "vault",
            "open",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(open.command, open.allow_writes).expect("open vault");
        let reindex = Cli::parse_from([
            "tao",
            "vault",
            "reindex",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(reindex.command, reindex.allow_writes).expect("reindex vault");

        let cli = Cli::parse_from([
            "tao",
            "query",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
            "--from",
            "docs",
            "--query",
            "project",
            "--where",
            "title contains 'Alpha'",
            "--sort",
            "path:asc",
            "--explain",
        ]);
        let result = dispatch(cli.command, cli.allow_writes).expect("dispatch explain query");
        let output = render_output(cli.json, &result).expect("render output");
        let envelope: JsonValue = serde_json::from_str(&output).expect("parse output");
        let args = envelope
            .get("data")
            .and_then(JsonValue::as_object)
            .expect("args object");
        assert!(args.contains_key("logical_plan"));
        assert!(args.contains_key("physical_plan"));
        assert!(!args.contains_key("rows"));
    });
}

#[test]
fn query_base_where_and_sort_execute_over_base_scope() {
    with_temp_cwd(|| {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let vault_root = tempdir.path().join("vault");
        fs::create_dir_all(vault_root.join("notes/projects")).expect("create notes");
        fs::create_dir_all(vault_root.join("views")).expect("create views");

        fs::write(
            vault_root.join("views/projects.base"),
            r#"
views:
  - name: AllProjects
    type: table
    source: notes/projects
    columns:
      - title
      - status
      - priority
"#,
        )
        .expect("write base");
        fs::write(
            vault_root.join("notes/projects/a.md"),
            r#"---
status: active
priority: 1
---
# A
"#,
        )
        .expect("write a");
        fs::write(
            vault_root.join("notes/projects/b.md"),
            r#"---
status: paused
priority: 3
---
# B
"#,
        )
        .expect("write b");
        fs::write(
            vault_root.join("notes/projects/c.md"),
            r#"---
status: active
priority: 2
---
# C
"#,
        )
        .expect("write c");

        let open = Cli::parse_from([
            "tao",
            "vault",
            "open",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(open.command, open.allow_writes).expect("open vault");
        let reindex = Cli::parse_from([
            "tao",
            "vault",
            "reindex",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(reindex.command, reindex.allow_writes).expect("reindex vault");

        let cli = Cli::parse_from([
            "tao",
            "query",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
            "--from",
            "base:views/projects.base",
            "--view-name",
            "AllProjects",
            "--where",
            "status == 'active'",
            "--sort",
            "priority:desc",
            "--limit",
            "10",
            "--offset",
            "0",
        ]);
        let result = dispatch(cli.command, cli.allow_writes).expect("dispatch base query");
        let output = render_output(cli.json, &result).expect("render output");
        let envelope: JsonValue = serde_json::from_str(&output).expect("parse output");
        let args = envelope
            .get("data")
            .and_then(JsonValue::as_object)
            .expect("args object");
        assert_eq!(args.get("total").and_then(JsonValue::as_u64), Some(2));
        let rows = args
            .get("rows")
            .and_then(JsonValue::as_array)
            .expect("rows array");
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|row| {
            row.get("values")
                .and_then(|value| value.get("status"))
                .and_then(JsonValue::as_str)
                == Some("active")
        }));
    });
}

#[test]
fn query_base_where_and_sort_scan_all_base_pages_before_pagination() {
    with_temp_cwd(|| {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let vault_root = tempdir.path().join("vault");
        fs::create_dir_all(vault_root.join("notes/projects")).expect("create projects");
        fs::create_dir_all(vault_root.join("views")).expect("create views");

        fs::write(
            vault_root.join("views/projects.base"),
            r#"
views:
  - name: AllProjects
    type: table
    source: notes/projects
    columns:
      - title
      - priority
"#,
        )
        .expect("write base");

        for priority in 1..=700_u32 {
            fs::write(
                vault_root.join(format!("notes/projects/p-{priority:04}.md")),
                format!("---\npriority: {priority}\n---\n# P-{priority:04}\n"),
            )
            .expect("write project note");
        }

        let open = Cli::parse_from([
            "tao",
            "vault",
            "open",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(open.command, open.allow_writes).expect("open vault");
        let reindex = Cli::parse_from([
            "tao",
            "vault",
            "reindex",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(reindex.command, reindex.allow_writes).expect("reindex vault");

        let cli = Cli::parse_from([
            "tao",
            "query",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
            "--from",
            "base:views/projects.base",
            "--view-name",
            "AllProjects",
            "--sort",
            "priority:desc",
            "--limit",
            "5",
            "--offset",
            "650",
        ]);
        let result = dispatch(cli.command, cli.allow_writes).expect("dispatch base query");
        let output = render_output(cli.json, &result).expect("render output");
        let envelope: JsonValue = serde_json::from_str(&output).expect("parse output");
        let args = envelope
            .get("data")
            .and_then(JsonValue::as_object)
            .expect("args object");

        assert_eq!(args.get("total").and_then(JsonValue::as_u64), Some(700));
        let rows = args
            .get("rows")
            .and_then(JsonValue::as_array)
            .expect("rows array");
        let priorities = rows
            .iter()
            .map(|row| {
                row.get("values")
                    .and_then(|value| value.get("priority"))
                    .map(|value| {
                        value
                            .as_str()
                            .map(ToString::to_string)
                            .or_else(|| value.as_i64().map(|number| number.to_string()))
                            .or_else(|| value.as_f64().map(|number| format!("{number:.0}")))
                            .expect("priority")
                    })
                    .expect("priority value")
            })
            .collect::<Vec<_>>();
        assert_eq!(priorities, vec!["50", "49", "48", "47", "46"]);
    });
}

#[test]
fn base_view_supports_obsidian_file_ext_root_filter() {
    with_temp_cwd(|| {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let vault_root = tempdir.path().join("vault");
        fs::create_dir_all(vault_root.join("notes/contents")).expect("create contents");
        fs::create_dir_all(vault_root.join("views")).expect("create views");

        fs::write(
            vault_root.join("views/contents.base"),
            r#"
filters:
  and:
    - file.inFolder("notes/contents")
    - file.ext == "md"
    - '!file.name.startsWith("index_")'
views:
  - type: table
    name: Table
    columns:
      - file.name
"#,
        )
        .expect("write base");
        fs::write(vault_root.join("notes/contents/alpha.md"), "# Alpha\n").expect("write alpha");
        fs::write(vault_root.join("notes/contents/index_home.md"), "# Index\n")
            .expect("write index");

        let open = Cli::parse_from([
            "tao",
            "vault",
            "open",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(open.command, open.allow_writes).expect("open vault");
        let reindex = Cli::parse_from([
            "tao",
            "vault",
            "reindex",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(reindex.command, reindex.allow_writes).expect("reindex vault");

        let cli = Cli::parse_from([
            "tao",
            "base",
            "view",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
            "--path-or-id",
            "views/contents.base",
            "--view-name",
            "Table",
        ]);
        let result = dispatch(cli.command, cli.allow_writes).expect("dispatch base view");
        let output = render_output(cli.json, &result).expect("render base view");
        let envelope: JsonValue = serde_json::from_str(&output).expect("parse output");
        let args = envelope.get("data").expect("data");
        assert_eq!(args.get("total").and_then(JsonValue::as_u64), Some(1));
        let rows = args
            .get("rows")
            .and_then(JsonValue::as_array)
            .expect("rows array");
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0]
                .get("values")
                .and_then(|values| values.get("title"))
                .and_then(JsonValue::as_str),
            Some("alpha")
        );
    });
}

#[test]
fn base_list_reports_invalid_entries_without_failing() {
    with_temp_cwd(|| {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let vault_root = tempdir.path().join("vault");
        fs::create_dir_all(vault_root.join("notes")).expect("create notes");
        fs::create_dir_all(vault_root.join("views")).expect("create views");

        fs::write(
            vault_root.join("views/a-valid.base"),
            r#"
views:
  - name: Valid
    type: table
    columns:
      - title
"#,
        )
        .expect("write valid base");
        fs::write(
            vault_root.join("views/z-invalid.base"),
            r#"
filters:
  and:
    - file.name.endsWith("hub_")
views:
  - name: Invalid
    type: table
    columns:
      - title
"#,
        )
        .expect("write invalid base");
        fs::write(vault_root.join("notes/alpha.md"), "# Alpha\n").expect("write note");

        let open = Cli::parse_from([
            "tao",
            "vault",
            "open",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(open.command, open.allow_writes).expect("open vault");
        let reindex = Cli::parse_from([
            "tao",
            "vault",
            "reindex",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(reindex.command, reindex.allow_writes).expect("reindex vault");

        let cli = Cli::parse_from([
            "tao",
            "base",
            "list",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        let result = dispatch(cli.command, cli.allow_writes).expect("dispatch base list");
        let output = render_output(cli.json, &result).expect("render base list");
        let envelope: JsonValue = serde_json::from_str(&output).expect("parse output");
        let args = envelope.get("data").expect("data");

        assert_eq!(args.get("total").and_then(JsonValue::as_u64), Some(2));
        assert_eq!(args.get("valid_total").and_then(JsonValue::as_u64), Some(1));
        assert_eq!(
            args.get("invalid_total").and_then(JsonValue::as_u64),
            Some(1)
        );

        let items = args
            .get("items")
            .and_then(JsonValue::as_array)
            .expect("valid items");
        assert_eq!(items.len(), 1);
        assert_eq!(
            items[0].get("file_path").and_then(JsonValue::as_str),
            Some("views/a-valid.base")
        );

        let invalid = args
            .get("invalid")
            .and_then(JsonValue::as_array)
            .expect("invalid items");
        assert_eq!(invalid.len(), 1);
        assert_eq!(
            invalid[0].get("file_path").and_then(JsonValue::as_str),
            Some("views/z-invalid.base")
        );
        let diagnostics = invalid[0]
            .get("diagnostics")
            .and_then(JsonValue::as_array)
            .expect("diagnostics");
        assert_eq!(diagnostics.len(), 1);
        assert_eq!(
            diagnostics[0].get("code").and_then(JsonValue::as_str),
            Some("bases.parse.invalid_schema")
        );
        assert!(
            diagnostics[0]
                .get("message")
                .and_then(JsonValue::as_str)
                .is_some_and(|message| message.contains("unsupported root filter expression"))
        );
    });
}

#[test]
fn base_validate_returns_diagnostics_for_invalid_base() {
    with_temp_cwd(|| {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let vault_root = tempdir.path().join("vault");
        fs::create_dir_all(vault_root.join("notes")).expect("create notes");
        fs::create_dir_all(vault_root.join("views")).expect("create views");

        fs::write(
            vault_root.join("views/invalid.base"),
            r#"
filters:
  and:
    - file.name.endsWith("hub_")
views:
  - name: Invalid
    type: table
    columns:
      - title
"#,
        )
        .expect("write invalid base");
        fs::write(vault_root.join("notes/alpha.md"), "# Alpha\n").expect("write note");

        let open = Cli::parse_from([
            "tao",
            "vault",
            "open",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(open.command, open.allow_writes).expect("open vault");
        let reindex = Cli::parse_from([
            "tao",
            "vault",
            "reindex",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(reindex.command, reindex.allow_writes).expect("reindex vault");

        let cli = Cli::parse_from([
            "tao",
            "base",
            "validate",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
            "--path-or-id",
            "views/invalid.base",
        ]);
        let result = dispatch(cli.command, cli.allow_writes).expect("dispatch base validate");
        let output = render_output(cli.json, &result).expect("render base validate");
        let envelope: JsonValue = serde_json::from_str(&output).expect("parse output");
        let args = envelope.get("data").expect("data");

        assert_eq!(
            args.get("file_path").and_then(JsonValue::as_str),
            Some("views/invalid.base")
        );
        assert_eq!(args.get("valid").and_then(JsonValue::as_bool), Some(false));
        let diagnostics = args
            .get("diagnostics")
            .and_then(JsonValue::as_array)
            .expect("diagnostics");
        assert_eq!(diagnostics.len(), 1);
        assert_eq!(
            diagnostics[0].get("field").and_then(JsonValue::as_str),
            Some("filters.and")
        );
    });
}

#[test]
fn query_matrix_covers_docs_graph_and_base_relation_cases() {
    with_temp_cwd(|| {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let vault_root = tempdir.path().join("vault");
        fs::create_dir_all(vault_root.join("notes/projects")).expect("create projects");
        fs::create_dir_all(vault_root.join("notes/links")).expect("create links");
        fs::create_dir_all(vault_root.join("views")).expect("create views");

        fs::write(
            vault_root.join("views/projects.base"),
            r#"
views:
  - name: ProjectTable
    type: table
    source: notes/projects
    columns:
      - title
      - status
      - priority
      - related
"#,
        )
        .expect("write base");
        fs::write(
            vault_root.join("notes/projects/alpha.md"),
            r#"---
status: active
priority: 2
related:
  - "[[notes/links/target.md]]"
---
# Alpha
project alpha
"#,
        )
        .expect("write alpha");
        fs::write(
            vault_root.join("notes/projects/beta.md"),
            r#"---
status: paused
priority: 1
related:
  - "[[notes/links/target.md]]"
---
# Beta
project beta
"#,
        )
        .expect("write beta");
        fs::write(
            vault_root.join("notes/links/source.md"),
            r#"---
related:
  - "[[notes/links/target.md]]"
---
# Source
links fixture
"#,
        )
        .expect("write source");
        fs::write(vault_root.join("notes/links/target.md"), "# Target\n").expect("write target");

        let open = Cli::parse_from([
            "tao",
            "vault",
            "open",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(open.command, open.allow_writes).expect("open vault");
        let reindex = Cli::parse_from([
            "tao",
            "vault",
            "reindex",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(reindex.command, reindex.allow_writes).expect("reindex vault");

        let cases = vec![
            (
                "docs",
                Cli::parse_from([
                    "tao",
                    "query",
                    "--vault-root",
                    vault_root.to_string_lossy().as_ref(),
                    "--from",
                    "docs",
                    "--query",
                    "project",
                    "--where",
                    "title starts_with 'a' or title starts_with 'b'",
                    "--sort",
                    "title:desc",
                    "--limit",
                    "10",
                    "--offset",
                    "0",
                ]),
                2_u64,
            ),
            (
                "base",
                Cli::parse_from([
                    "tao",
                    "query",
                    "--vault-root",
                    vault_root.to_string_lossy().as_ref(),
                    "--from",
                    "base:views/projects.base",
                    "--view-name",
                    "ProjectTable",
                    "--where",
                    "related contains 'target' and status == 'active'",
                    "--sort",
                    "priority:desc",
                    "--limit",
                    "10",
                    "--offset",
                    "0",
                ]),
                1_u64,
            ),
            (
                "graph",
                Cli::parse_from([
                    "tao",
                    "query",
                    "--vault-root",
                    vault_root.to_string_lossy().as_ref(),
                    "--from",
                    "graph",
                    "--path",
                    "notes/links/source.md",
                    "--limit",
                    "10",
                    "--offset",
                    "0",
                ]),
                1_u64,
            ),
        ];

        for (scope, cli, expected_total) in cases {
            let result =
                dispatch(cli.command, cli.allow_writes).expect("dispatch matrix query case");
            let output = render_output(cli.json, &result).expect("render matrix output");
            let envelope: JsonValue = serde_json::from_str(&output).expect("parse matrix output");
            let total = envelope
                .get("data")
                .and_then(|args| args.get("total"))
                .and_then(JsonValue::as_u64)
                .unwrap_or(0);
            assert_eq!(
                total, expected_total,
                "matrix mismatch for scope '{}': expected {}, got {}",
                scope, expected_total, total
            );
        }
    });
}

#[test]
fn query_graph_path_returns_outgoing_and_backlinks_panels() {
    with_temp_cwd(|| {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let vault_root = tempdir.path().join("vault");
        fs::create_dir_all(vault_root.join("notes")).expect("create notes");
        fs::write(vault_root.join("notes/a.md"), "# A\n[[b]]\n").expect("write a");
        fs::write(vault_root.join("notes/b.md"), "# B\n[[c]]\n").expect("write b");
        fs::write(vault_root.join("notes/c.md"), "# C\n[[b]]\n").expect("write c");

        let open = Cli::parse_from([
            "tao",
            "vault",
            "open",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(open.command, open.allow_writes).expect("open vault");
        let reindex = Cli::parse_from([
            "tao",
            "vault",
            "reindex",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(reindex.command, reindex.allow_writes).expect("reindex vault");

        let cli = Cli::parse_from([
            "tao",
            "query",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
            "--from",
            "graph",
            "--path",
            "notes/b.md",
            "--limit",
            "10",
            "--offset",
            "0",
        ]);
        let result = dispatch(cli.command, cli.allow_writes).expect("dispatch graph query");
        let output = render_output(cli.json, &result).expect("render graph query");
        let envelope: JsonValue = serde_json::from_str(&output).expect("parse output");
        let args = envelope.get("data").expect("query args");
        assert_eq!(
            args.get("outgoing_total").and_then(JsonValue::as_u64),
            Some(1)
        );
        assert_eq!(
            args.get("backlinks_total").and_then(JsonValue::as_u64),
            Some(2)
        );
        assert_eq!(args.get("total").and_then(JsonValue::as_u64), Some(3));
        assert_eq!(
            args.get("outgoing")
                .and_then(JsonValue::as_array)
                .map(Vec::len),
            Some(1)
        );
        assert_eq!(
            args.get("backlinks")
                .and_then(JsonValue::as_array)
                .map(Vec::len),
            Some(2)
        );
    });
}

#[test]
fn json_stream_docs_query_uses_streaming_envelope() {
    with_temp_cwd(|| {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let vault_root = tempdir.path().join("vault");
        fs::create_dir_all(vault_root.join("notes/projects")).expect("create notes");
        fs::write(
            vault_root.join("notes/projects/alpha.md"),
            "# Alpha\nproject roadmap",
        )
        .expect("write alpha");

        let open = Cli::parse_from([
            "tao",
            "vault",
            "open",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(open.command, open.allow_writes).expect("open vault");
        let reindex = Cli::parse_from([
            "tao",
            "vault",
            "reindex",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(reindex.command, reindex.allow_writes).expect("reindex vault");

        let cli = Cli::parse_from([
            "tao",
            "--json-stream",
            "query",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
            "--from",
            "docs",
            "--query",
            "project",
            "--select",
            "path,title",
            "--limit",
            "10",
            "--offset",
            "0",
        ]);
        let output = maybe_render_streaming_output(&cli)
            .expect("render streaming output")
            .expect("streaming output expected");
        let envelope: JsonValue = serde_json::from_str(&output).expect("parse streaming json");
        assert_eq!(
            envelope
                .get("meta")
                .and_then(|value| value.get("tool"))
                .and_then(JsonValue::as_str),
            Some("query.run")
        );
        let columns = envelope
            .get("data")
            .and_then(|args| args.get("columns"))
            .and_then(JsonValue::as_array)
            .expect("columns");
        assert_eq!(
            columns
                .iter()
                .filter_map(JsonValue::as_str)
                .collect::<Vec<_>>(),
            vec!["path", "title"]
        );
    });
}

#[test]
fn graph_neighbors_supports_direction_filtering() {
    with_temp_cwd(|| {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let vault_root = tempdir.path().join("vault");
        fs::create_dir_all(vault_root.join("notes")).expect("create notes dir");
        fs::write(vault_root.join("notes/a.md"), "# A\n[[notes/b.md]]\n").expect("write a");
        fs::write(vault_root.join("notes/b.md"), "# B\n[[notes/c.md]]\n").expect("write b");
        fs::write(vault_root.join("notes/c.md"), "# C\n").expect("write c");
        fs::write(vault_root.join("notes/d.md"), "# D\n[[notes/a.md]]\n").expect("write d");

        let open = Cli::parse_from([
            "tao",
            "vault",
            "open",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(open.command, open.allow_writes).expect("open vault");
        let reindex = Cli::parse_from([
            "tao",
            "vault",
            "reindex",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(reindex.command, reindex.allow_writes).expect("reindex vault");

        let neighbors = Cli::parse_from([
            "tao",
            "graph",
            "neighbors",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
            "--path",
            "notes/a.md",
            "--direction",
            "incoming",
        ]);
        let output = render_output(
            neighbors.json,
            &dispatch(neighbors.command, neighbors.allow_writes).expect("dispatch neighbors"),
        )
        .expect("render neighbors");
        let envelope: JsonValue = serde_json::from_str(&output).expect("parse neighbors");
        let items = envelope
            .get("data")
            .and_then(|args| args.get("items"))
            .and_then(JsonValue::as_array)
            .expect("neighbors items");
        assert_eq!(items.len(), 1);
        assert_eq!(
            items[0].get("path").and_then(JsonValue::as_str),
            Some("notes/d.md")
        );
        assert_eq!(
            items[0].get("direction").and_then(JsonValue::as_str),
            Some("incoming")
        );
    });
}

#[test]
fn graph_path_reports_found_not_found_and_guardrail_errors() {
    with_temp_cwd(|| {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let vault_root = tempdir.path().join("vault");
        fs::create_dir_all(vault_root.join("notes")).expect("create notes dir");
        fs::write(vault_root.join("notes/a.md"), "# A\n[[notes/b.md]]\n").expect("write a");
        fs::write(vault_root.join("notes/b.md"), "# B\n[[notes/c.md]]\n").expect("write b");
        fs::write(vault_root.join("notes/c.md"), "# C\n").expect("write c");
        fs::write(vault_root.join("notes/e.md"), "# E\n").expect("write e");

        let open = Cli::parse_from([
            "tao",
            "vault",
            "open",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(open.command, open.allow_writes).expect("open vault");
        let reindex = Cli::parse_from([
            "tao",
            "vault",
            "reindex",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(reindex.command, reindex.allow_writes).expect("reindex vault");

        let found = Cli::parse_from([
            "tao",
            "graph",
            "path",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
            "--from",
            "notes/a.md",
            "--to",
            "notes/a.md",
        ]);
        let found_output = render_output(
            found.json,
            &dispatch(found.command, found.allow_writes).expect("dispatch found path"),
        )
        .expect("render found path");
        let found_envelope: JsonValue =
            serde_json::from_str(&found_output).expect("parse found path");
        assert_eq!(
            found_envelope
                .get("data")
                .and_then(|args| args.get("found"))
                .and_then(JsonValue::as_bool),
            Some(true)
        );

        let missing = Cli::parse_from([
            "tao",
            "graph",
            "path",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
            "--from",
            "notes/a.md",
            "--to",
            "notes/e.md",
        ]);
        let missing_output = render_output(
            missing.json,
            &dispatch(missing.command, missing.allow_writes).expect("dispatch missing path"),
        )
        .expect("render missing path");
        let missing_envelope: JsonValue =
            serde_json::from_str(&missing_output).expect("parse missing path");
        assert_eq!(
            missing_envelope
                .get("data")
                .and_then(|args| args.get("found"))
                .and_then(JsonValue::as_bool),
            Some(false)
        );

        let guardrail = Cli::parse_from([
            "tao",
            "graph",
            "path",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
            "--from",
            "notes/a.md",
            "--to",
            "notes/c.md",
            "--max-nodes",
            "0",
        ]);
        let error =
            dispatch(guardrail.command, guardrail.allow_writes).expect_err("guardrail should fail");
        assert!(
            error
                .to_string()
                .contains("--max-nodes must be greater than zero")
        );
    });
}

#[test]
fn graph_components_supports_weak_and_strong_modes() {
    with_temp_cwd(|| {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let vault_root = tempdir.path().join("vault");
        fs::create_dir_all(vault_root.join("notes")).expect("create notes dir");
        fs::write(vault_root.join("notes/a.md"), "# A\n[[b]]\n").expect("write a");
        fs::write(vault_root.join("notes/b.md"), "# B\n[[a]]\n[[c]]\n").expect("write b");
        fs::write(vault_root.join("notes/c.md"), "# C\n").expect("write c");

        let open = Cli::parse_from([
            "tao",
            "vault",
            "open",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(open.command, open.allow_writes).expect("open vault");
        let reindex = Cli::parse_from([
            "tao",
            "vault",
            "reindex",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(reindex.command, reindex.allow_writes).expect("reindex vault");

        let weak = Cli::parse_from([
            "tao",
            "graph",
            "components",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
            "--mode",
            "weak",
            "--include-members",
        ]);
        let weak_output = render_output(
            weak.json,
            &dispatch(weak.command, weak.allow_writes).expect("dispatch weak components"),
        )
        .expect("render weak components");
        let weak_json: JsonValue = serde_json::from_str(&weak_output).expect("parse weak json");
        let weak_items = weak_json
            .get("data")
            .and_then(|args| args.get("items"))
            .and_then(JsonValue::as_array)
            .expect("weak items");
        assert_eq!(weak_items.len(), 1);
        assert_eq!(
            weak_items[0].get("size").and_then(JsonValue::as_u64),
            Some(3)
        );

        let strong = Cli::parse_from([
            "tao",
            "graph",
            "components",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
            "--mode",
            "strong",
            "--include-members",
        ]);
        let strong_output = render_output(
            strong.json,
            &dispatch(strong.command, strong.allow_writes).expect("dispatch strong components"),
        )
        .expect("render strong components");
        let strong_json: JsonValue =
            serde_json::from_str(&strong_output).expect("parse strong json");
        let strong_items = strong_json
            .get("data")
            .and_then(|args| args.get("items"))
            .and_then(JsonValue::as_array)
            .expect("strong items");
        let strong_sizes = strong_items
            .iter()
            .filter_map(|item| item.get("size").and_then(JsonValue::as_u64))
            .collect::<Vec<_>>();
        assert_eq!(strong_sizes, vec![2, 1]);
    });
}

#[test]
fn graph_walk_can_include_folder_overlay_edges() {
    with_temp_cwd(|| {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let vault_root = tempdir.path().join("vault");
        fs::create_dir_all(vault_root.join("notes/projects")).expect("create projects dir");
        fs::create_dir_all(vault_root.join("notes/meetings")).expect("create meetings dir");
        fs::write(vault_root.join("notes/projects/a.md"), "# A\n").expect("write a");
        fs::write(vault_root.join("notes/projects/b.md"), "# B\n").expect("write b");
        fs::write(vault_root.join("notes/meetings/m1.md"), "# M1\n").expect("write m1");

        let open = Cli::parse_from([
            "tao",
            "vault",
            "open",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(open.command, open.allow_writes).expect("open vault");
        let reindex = Cli::parse_from([
            "tao",
            "vault",
            "reindex",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(reindex.command, reindex.allow_writes).expect("reindex vault");

        let plain_walk = Cli::parse_from([
            "tao",
            "graph",
            "walk",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
            "--path",
            "notes/projects/a.md",
            "--depth",
            "2",
        ]);
        let plain_output = render_output(
            plain_walk.json,
            &dispatch(plain_walk.command, plain_walk.allow_writes).expect("dispatch plain walk"),
        )
        .expect("render plain walk");
        let plain_json: JsonValue = serde_json::from_str(&plain_output).expect("parse plain walk");
        let plain_items = plain_json
            .get("data")
            .and_then(|args| args.get("items"))
            .and_then(JsonValue::as_array)
            .expect("plain items");
        assert!(
            plain_items.is_empty(),
            "expected no wikilink steps in plain walk fixture"
        );

        let folder_walk = Cli::parse_from([
            "tao",
            "graph",
            "walk",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
            "--path",
            "notes/projects/a.md",
            "--depth",
            "2",
            "--include-folders",
        ]);
        let folder_output = render_output(
            folder_walk.json,
            &dispatch(folder_walk.command, folder_walk.allow_writes).expect("dispatch folder walk"),
        )
        .expect("render folder walk");
        let folder_json: JsonValue =
            serde_json::from_str(&folder_output).expect("parse folder walk");
        let folder_items = folder_json
            .get("data")
            .and_then(|args| args.get("items"))
            .and_then(JsonValue::as_array)
            .expect("folder walk items");
        assert!(!folder_items.is_empty(), "expected folder overlay edges");
        assert!(folder_items.iter().any(|item| {
            item.get("edge_type").and_then(JsonValue::as_str) == Some("folder-sibling")
        }));
    });
}

#[test]
fn graph_unresolved_includes_reason_and_source_fields() {
    with_temp_cwd(|| {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let vault_root = tempdir.path().join("vault");
        fs::create_dir_all(vault_root.join("notes")).expect("create notes dir");
        fs::write(
            vault_root.join("notes/a.md"),
            "---\nrefs:\n  - \"[[missing-frontmatter]]\"\n---\n# A\n[[missing-body]]\n",
        )
        .expect("write a");

        let open = Cli::parse_from([
            "tao",
            "vault",
            "open",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(open.command, open.allow_writes).expect("open vault");
        let reindex = Cli::parse_from([
            "tao",
            "vault",
            "reindex",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(reindex.command, reindex.allow_writes).expect("reindex vault");

        let unresolved = Cli::parse_from([
            "tao",
            "graph",
            "unresolved",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
            "--limit",
            "20",
            "--offset",
            "0",
        ]);
        let output = render_output(
            unresolved.json,
            &dispatch(unresolved.command, unresolved.allow_writes).expect("dispatch unresolved"),
        )
        .expect("render unresolved");
        let payload: JsonValue = serde_json::from_str(&output).expect("parse unresolved");
        let items = payload
            .get("data")
            .and_then(|args| args.get("items"))
            .and_then(JsonValue::as_array)
            .expect("unresolved items");
        assert!(
            items
                .iter()
                .all(|item| item.get("unresolved_reason").is_some())
        );
        assert!(items.iter().all(|item| item.get("source_field").is_some()));
        assert!(
            items.iter().any(|item| {
                item.get("source_field").and_then(JsonValue::as_str) == Some("body")
            })
        );
        assert!(items.iter().any(|item| {
            item.get("source_field")
                .and_then(JsonValue::as_str)
                .is_some_and(|value| value.starts_with("frontmatter:"))
        }));
    });
}

#[test]
fn graph_snapshot_contracts_match_golden_outputs() {
    with_temp_cwd(|| {
        let fixture_root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../vault/fixtures/graph-parity")
            .canonicalize()
            .expect("canonicalize graph parity fixture");
        let expected_root = fixture_root.join("expected");

        let tempdir = tempfile::tempdir().expect("create tempdir");
        let vault_root = tempdir.path().join("vault");
        copy_dir_recursive(&fixture_root, &vault_root).expect("copy graph parity fixture");
        let _ = fs::remove_dir_all(vault_root.join("expected"));

        let open = Cli::parse_from([
            "tao",
            "vault",
            "open",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(open.command, open.allow_writes).expect("open vault");
        let reindex = Cli::parse_from([
            "tao",
            "vault",
            "reindex",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(reindex.command, reindex.allow_writes).expect("reindex vault");

        let assert_snapshot = |expected_name: &str, cli: Cli| {
            let rendered = render_output(
                cli.json,
                &dispatch(cli.command, cli.allow_writes).expect("dispatch"),
            )
            .expect("render output");
            let actual: JsonValue = serde_json::from_str(&rendered).expect("parse json envelope");
            let actual_args = actual.get("data").expect("data");
            let expected_raw = fs::read_to_string(expected_root.join(expected_name))
                .expect("read expected snapshot");
            let expected: JsonValue =
                serde_json::from_str(&expected_raw).expect("parse expected snapshot");
            assert_eq!(
                actual_args, &expected,
                "snapshot mismatch for {expected_name}"
            );
        };

        assert_snapshot(
            "outgoing.json",
            Cli::parse_from([
                "tao",
                "graph",
                "outgoing",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
                "--path",
                "notes/root.md",
            ]),
        );
        assert_snapshot(
            "backlinks.json",
            Cli::parse_from([
                "tao",
                "graph",
                "backlinks",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
                "--path",
                "notes/beta.md",
            ]),
        );
        assert_snapshot(
            "unresolved.json",
            Cli::parse_from([
                "tao",
                "graph",
                "unresolved",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
                "--limit",
                "100",
                "--offset",
                "0",
            ]),
        );
        assert_snapshot(
            "deadends.json",
            Cli::parse_from([
                "tao",
                "graph",
                "deadends",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
                "--limit",
                "100",
                "--offset",
                "0",
            ]),
        );
        assert_snapshot(
            "orphans.json",
            Cli::parse_from([
                "tao",
                "graph",
                "orphans",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
                "--limit",
                "100",
                "--offset",
                "0",
            ]),
        );
        assert_snapshot(
            "floating.json",
            Cli::parse_from([
                "tao",
                "graph",
                "floating",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
                "--limit",
                "100",
                "--offset",
                "0",
            ]),
        );
        assert_snapshot(
            "walk.json",
            Cli::parse_from([
                "tao",
                "graph",
                "walk",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
                "--path",
                "notes/root.md",
                "--depth",
                "2",
                "--limit",
                "50",
                "--include-unresolved",
            ]),
        );
    });
}

#[test]
fn graph_outgoing_normalizes_note_path_input_before_lookup() {
    with_temp_cwd(|| {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let vault_root = tempdir.path().join("vault");
        fs::create_dir_all(vault_root.join("notes")).expect("create notes dir");
        fs::write(vault_root.join("notes/source.md"), "# Source\n[[target]]")
            .expect("write source");
        fs::write(vault_root.join("notes/target.md"), "# Target").expect("write target");

        let open = Cli::parse_from([
            "tao",
            "vault",
            "open",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(open.command, open.allow_writes).expect("open vault");
        let reindex = Cli::parse_from([
            "tao",
            "vault",
            "reindex",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(reindex.command, reindex.allow_writes).expect("reindex vault");

        let outgoing = Cli::parse_from([
            "tao",
            "graph",
            "outgoing",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
            "--path",
            "/notes\\source.md/",
        ]);
        let output = render_output(
            outgoing.json,
            &dispatch(outgoing.command, outgoing.allow_writes).expect("dispatch outgoing"),
        )
        .expect("render outgoing");
        let payload: JsonValue = serde_json::from_str(&output).expect("parse outgoing");

        assert_eq!(
            payload
                .get("data")
                .and_then(|data| data.get("path"))
                .and_then(JsonValue::as_str),
            Some("notes/source.md")
        );
        assert_eq!(
            payload
                .get("data")
                .and_then(|data| data.get("total"))
                .and_then(JsonValue::as_u64),
            Some(1)
        );
    });
}

#[test]
fn health_and_vault_stats_report_index_lag_from_reconciliation_drift() {
    with_temp_cwd(|| {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let vault_root = tempdir.path().join("vault");
        fs::create_dir_all(vault_root.join("notes")).expect("create notes dir");
        fs::write(vault_root.join("notes/a.md"), "# A").expect("write a");

        let open = Cli::parse_from([
            "tao",
            "vault",
            "open",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(open.command, open.allow_writes).expect("open vault");
        let reindex = Cli::parse_from([
            "tao",
            "vault",
            "reindex",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(reindex.command, reindex.allow_writes).expect("reindex vault");

        fs::write(vault_root.join("notes/b.md"), "# B").expect("write drifted file");

        let health = Cli::parse_from([
            "tao",
            "health",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        let health_output = render_output(
            health.json,
            &dispatch(health.command, health.allow_writes).expect("dispatch health"),
        )
        .expect("render health");
        let health_payload: JsonValue = serde_json::from_str(&health_output).expect("parse health");

        assert_eq!(
            health_payload
                .get("data")
                .and_then(|data| data.get("status"))
                .and_then(JsonValue::as_str),
            Some("degraded")
        );
        assert_eq!(
            health_payload
                .get("data")
                .and_then(|data| data.get("stats"))
                .and_then(|stats| stats.get("index_lag"))
                .and_then(JsonValue::as_u64),
            Some(1)
        );
        assert_eq!(
            health_payload
                .get("data")
                .and_then(|data| data.get("runtime"))
                .and_then(|runtime| runtime.get("backend"))
                .and_then(JsonValue::as_str),
            Some("oneshot")
        );
        assert_eq!(
            health_payload
                .get("data")
                .and_then(|data| data.get("runtime"))
                .and_then(|runtime| runtime.get("daemon_running"))
                .and_then(JsonValue::as_bool),
            Some(false)
        );

        let stats = Cli::parse_from([
            "tao",
            "vault",
            "stats",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        let stats_output = render_output(
            stats.json,
            &dispatch(stats.command, stats.allow_writes).expect("dispatch stats"),
        )
        .expect("render stats");
        let stats_payload: JsonValue = serde_json::from_str(&stats_output).expect("parse stats");

        assert_eq!(
            stats_payload
                .get("data")
                .and_then(|data| data.get("index_lag"))
                .and_then(JsonValue::as_u64),
            Some(1)
        );
        assert_eq!(
            stats_payload
                .get("data")
                .and_then(|data| data.get("runtime"))
                .and_then(|runtime| runtime.get("backend"))
                .and_then(JsonValue::as_str),
            Some("oneshot")
        );
    });
}

#[test]
fn health_and_vault_stats_report_stale_link_resolution_version() {
    with_temp_cwd(|| {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let vault_root = tempdir.path().join("vault");
        fs::create_dir_all(vault_root.join("notes")).expect("create notes dir");
        fs::write(vault_root.join("notes/a.md"), "# A").expect("write a");

        let open = Cli::parse_from([
            "tao",
            "vault",
            "open",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        let open_output = render_output(
            open.json,
            &dispatch(open.command, open.allow_writes).expect("dispatch open"),
        )
        .expect("render open");
        let open_payload: JsonValue = serde_json::from_str(&open_output).expect("parse open");
        let db_path = open_payload
            .get("data")
            .and_then(|data| data.get("db_path"))
            .and_then(JsonValue::as_str)
            .expect("db path")
            .to_string();

        let reindex = Cli::parse_from([
            "tao",
            "vault",
            "reindex",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(reindex.command, reindex.allow_writes).expect("reindex vault");

        let connection = Connection::open(&db_path).expect("open db");
        IndexStateRepository::upsert(
            &connection,
            &IndexStateRecordInput {
                key: LINK_RESOLUTION_VERSION_STATE_KEY.to_string(),
                value_json: "1".to_string(),
            },
        )
        .expect("downgrade link resolution version");

        let health = Cli::parse_from([
            "tao",
            "health",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        let health_output = render_output(
            health.json,
            &dispatch(health.command, health.allow_writes).expect("dispatch health"),
        )
        .expect("render health");
        let health_payload: JsonValue = serde_json::from_str(&health_output).expect("parse health");

        assert_eq!(
            health_payload
                .get("data")
                .and_then(|data| data.get("status"))
                .and_then(JsonValue::as_str),
            Some("degraded")
        );
        assert_eq!(
            health_payload
                .get("data")
                .and_then(|data| data.get("stats"))
                .and_then(|stats| stats.get("index_lag"))
                .and_then(JsonValue::as_u64),
            Some(1)
        );
        assert_eq!(
            health_payload
                .get("data")
                .and_then(|data| data.get("runtime"))
                .and_then(|runtime| runtime.get("backend"))
                .and_then(JsonValue::as_str),
            Some("oneshot")
        );

        let stats = Cli::parse_from([
            "tao",
            "vault",
            "stats",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        let stats_output = render_output(
            stats.json,
            &dispatch(stats.command, stats.allow_writes).expect("dispatch stats"),
        )
        .expect("render stats");
        let stats_payload: JsonValue = serde_json::from_str(&stats_output).expect("parse stats");

        assert_eq!(
            stats_payload
                .get("data")
                .and_then(|data| data.get("index_lag"))
                .and_then(JsonValue::as_u64),
            Some(1)
        );
        assert_eq!(
            stats_payload
                .get("data")
                .and_then(|data| data.get("runtime"))
                .and_then(|runtime| runtime.get("backend"))
                .and_then(JsonValue::as_str),
            Some("oneshot")
        );
    });
}

#[test]
fn vault_reindex_performs_full_rebuild_when_link_resolution_version_is_stale() {
    with_temp_cwd(|| {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let vault_root = tempdir.path().join("vault");
        let contents_root = vault_root.join("WORK/13-RELATIONS/Contents");
        fs::create_dir_all(contents_root.join("Media")).expect("create media dir");
        fs::write(
            contents_root.join("post.md"),
            "# Post\n![[Contents/Media/foo.jpg]]\n",
        )
        .expect("write post");
        fs::write(contents_root.join("Media/foo.jpg"), "jpg").expect("write image");

        let open = Cli::parse_from([
            "tao",
            "vault",
            "open",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        let open_output = render_output(
            open.json,
            &dispatch(open.command, open.allow_writes).expect("dispatch open"),
        )
        .expect("render open");
        let open_payload: JsonValue = serde_json::from_str(&open_output).expect("parse open");
        let db_path = open_payload
            .get("data")
            .and_then(|data| data.get("db_path"))
            .and_then(JsonValue::as_str)
            .expect("db path")
            .to_string();

        let reindex = Cli::parse_from([
            "tao",
            "vault",
            "reindex",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(reindex.command.clone(), reindex.allow_writes).expect("initial reindex");

        let connection = Connection::open(&db_path).expect("open db");
        let source = FilesRepository::get_by_normalized_path(
            &connection,
            "WORK/13-RELATIONS/Contents/post.md",
        )
        .expect("lookup source")
        .expect("source exists");
        connection
            .execute(
                "DELETE FROM links WHERE source_file_id = ?1",
                rusqlite::params![source.file_id],
            )
            .expect("delete source links");
        LinksRepository::insert(
            &connection,
            &LinkRecordInput {
                link_id: "stale-link".to_string(),
                source_file_id: source.file_id.clone(),
                raw_target: "Contents/Media/foo.jpg".to_string(),
                resolved_file_id: None,
                heading_slug: None,
                block_id: None,
                is_unresolved: true,
                unresolved_reason: Some("missing-note".to_string()),
                source_field: "body".to_string(),
            },
        )
        .expect("insert stale unresolved link");
        IndexStateRepository::upsert(
            &connection,
            &IndexStateRecordInput {
                key: LINK_RESOLUTION_VERSION_STATE_KEY.to_string(),
                value_json: "1".to_string(),
            },
        )
        .expect("downgrade link resolution version");

        let reindex_output = render_output(
            reindex.json,
            &dispatch(reindex.command, reindex.allow_writes).expect("dispatch reindex"),
        )
        .expect("render reindex");
        let reindex_payload: JsonValue =
            serde_json::from_str(&reindex_output).expect("parse reindex");

        assert_eq!(
            reindex_payload
                .get("data")
                .and_then(|data| data.get("mode"))
                .and_then(JsonValue::as_str),
            Some("full_rebuild")
        );
        assert_eq!(
            reindex_payload
                .get("data")
                .and_then(|data| data.get("reason"))
                .and_then(JsonValue::as_str),
            Some("link_resolution_version_mismatch")
        );
        assert_eq!(
            reindex_payload
                .get("data")
                .and_then(|data| data.get("unresolved_links"))
                .and_then(JsonValue::as_u64),
            Some(0)
        );

        let refreshed = Connection::open(&db_path).expect("reopen db");
        let version =
            IndexStateRepository::get_by_key(&refreshed, LINK_RESOLUTION_VERSION_STATE_KEY)
                .expect("load version")
                .expect("version exists");
        assert_eq!(
            serde_json::from_str::<u32>(&version.value_json).expect("parse version"),
            CURRENT_LINK_RESOLUTION_VERSION
        );

        let outgoing = Cli::parse_from([
            "tao",
            "graph",
            "outgoing",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
            "--path",
            "WORK/13-RELATIONS/Contents/post.md",
        ]);
        let outgoing_output = render_output(
            outgoing.json,
            &dispatch(outgoing.command, outgoing.allow_writes).expect("dispatch outgoing"),
        )
        .expect("render outgoing");
        let outgoing_payload: JsonValue =
            serde_json::from_str(&outgoing_output).expect("parse outgoing");
        let items = outgoing_payload
            .get("data")
            .and_then(|data| data.get("items"))
            .and_then(JsonValue::as_array)
            .expect("outgoing items");

        assert_eq!(items.len(), 1);
        assert_eq!(
            items[0].get("target_path").and_then(JsonValue::as_str),
            Some("WORK/13-RELATIONS/Contents/Media/foo.jpg")
        );
        assert_eq!(
            items[0].get("resolved").and_then(JsonValue::as_bool),
            Some(true)
        );
    });
}

#[test]
fn vault_reindex_performs_full_rebuild_when_file_paths_are_inconsistent() {
    with_temp_cwd(|| {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let vault_root = tempdir.path().join("vault");
        fs::create_dir_all(vault_root.join("notes")).expect("create notes");
        fs::write(vault_root.join("notes/a.md"), "# A\n[[b]]\n").expect("write a");
        fs::write(vault_root.join("notes/b.md"), "# B\n").expect("write b");

        let open = Cli::parse_from([
            "tao",
            "vault",
            "open",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        let open_output = render_output(
            open.json,
            &dispatch(open.command, open.allow_writes).expect("dispatch open"),
        )
        .expect("render open");
        let open_payload: JsonValue = serde_json::from_str(&open_output).expect("parse open");
        let db_path = open_payload
            .get("data")
            .and_then(|data| data.get("db_path"))
            .and_then(JsonValue::as_str)
            .expect("db path")
            .to_string();

        let reindex = Cli::parse_from([
            "tao",
            "vault",
            "reindex",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(reindex.command.clone(), reindex.allow_writes).expect("initial reindex");

        let connection = Connection::open(&db_path).expect("open db");
        let bogus_absolute = vault_root
            .join("notes/a.md")
            .canonicalize()
            .expect("canonicalize note");
        let bogus_path = bogus_absolute
            .to_string_lossy()
            .trim_start_matches('/')
            .to_string();
        let metadata = fs::metadata(&bogus_absolute).expect("read metadata");

        FilesRepository::insert(
            &connection,
            &tao_sdk_storage::FileRecordInput {
                file_id: "file-bogus-a".to_string(),
                normalized_path: bogus_path.clone(),
                match_key: bogus_path.to_lowercase(),
                absolute_path: bogus_path.clone(),
                size_bytes: metadata.len(),
                modified_unix_ms: metadata
                    .modified()
                    .expect("modified time")
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("modified after epoch")
                    .as_millis()
                    .try_into()
                    .expect("mtime fits"),
                hash_blake3: "hash-bogus".to_string(),
                is_markdown: true,
            },
        )
        .expect("insert bogus row");
        IndexStateRepository::upsert(
            &connection,
            &IndexStateRecordInput {
                key: LINK_RESOLUTION_VERSION_STATE_KEY.to_string(),
                value_json: CURRENT_LINK_RESOLUTION_VERSION.to_string(),
            },
        )
        .expect("keep current link version");

        let health = Cli::parse_from([
            "tao",
            "health",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        let health_output = render_output(
            health.json,
            &dispatch(health.command, health.allow_writes).expect("dispatch health"),
        )
        .expect("render health");
        let health_payload: JsonValue = serde_json::from_str(&health_output).expect("parse health");
        assert_eq!(
            health_payload
                .get("data")
                .and_then(|data| data.get("stats"))
                .and_then(|stats| stats.get("index_lag"))
                .and_then(JsonValue::as_u64),
            Some(1)
        );

        let reindex_output = render_output(
            reindex.json,
            &dispatch(reindex.command, reindex.allow_writes).expect("dispatch reindex"),
        )
        .expect("render reindex");
        let reindex_payload: JsonValue =
            serde_json::from_str(&reindex_output).expect("parse reindex");

        assert_eq!(
            reindex_payload
                .get("data")
                .and_then(|data| data.get("mode"))
                .and_then(JsonValue::as_str),
            Some("full_rebuild")
        );
        assert_eq!(
            reindex_payload
                .get("data")
                .and_then(|data| data.get("reason"))
                .and_then(JsonValue::as_str),
            Some("file_path_mismatch")
        );
    });
}

#[test]
fn daemon_control_commands_bypass_client_forwarding() {
    let cli = Cli::parse_from([
        "tao",
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
fn daemon_socket_resolution_prefers_explicit_override() {
    let cli = Cli::parse_from([
        "tao",
        "--daemon-socket",
        "/tmp/tao-explicit.sock",
        "vault",
        "open",
        "--vault-root",
        "/tmp",
    ]);
    let socket = resolve_daemon_socket_for_cli(&cli)
        .expect("resolve socket")
        .expect("socket should be resolved");
    assert_eq!(socket, "/tmp/tao-explicit.sock");
}

#[test]
fn daemon_socket_resolution_derives_deterministic_per_vault_path() {
    with_temp_cwd(|| {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let vault_root = tempdir.path().join("vault");
        fs::create_dir_all(&vault_root).expect("create vault root");

        let cli = Cli::parse_from([
            "tao",
            "vault",
            "open",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);

        let socket = resolve_daemon_socket_for_cli(&cli)
            .expect("resolve socket")
            .expect("socket should be resolved");
        let resolved = resolve_command_vault_paths(&cli.command)
            .expect("resolve command vault paths")
            .expect("vault path should resolve");
        let expected = derive_daemon_socket_for_vault(&resolved.vault_root).expect("derive socket");
        assert_eq!(socket, expected);
        assert!(socket.ends_with(".sock"));
    });
}

#[test]
fn daemon_status_reports_stale_and_dead_socket_states() {
    let tempdir = tempfile::tempdir().expect("create tempdir");
    let stale_socket = tempdir.path().join("stale.sock");
    #[cfg(unix)]
    {
        let listener = UnixListener::bind(&stale_socket).expect("bind stale socket");
        drop(listener);
    }
    let stale = handle_daemon(DaemonCommands::Status(DaemonSocketArgs {
        socket: Some(stale_socket.to_string_lossy().to_string()),
        vault_root: None,
        db_path: None,
    }))
    .expect("daemon status");
    assert_eq!(
        stale.args.get("state").and_then(JsonValue::as_str),
        Some("stale")
    );
    assert_eq!(
        stale.args.get("running").and_then(JsonValue::as_bool),
        Some(false)
    );

    let dead_path = tempdir.path().join("dead.sock");
    fs::write(&dead_path, "not-a-socket").expect("write dead socket placeholder");
    let dead = handle_daemon(DaemonCommands::Status(DaemonSocketArgs {
        socket: Some(dead_path.to_string_lossy().to_string()),
        vault_root: None,
        db_path: None,
    }))
    .expect("daemon status");
    assert_eq!(
        dead.args.get("state").and_then(JsonValue::as_str),
        Some("dead")
    );
    assert_eq!(
        dead.args.get("running").and_then(JsonValue::as_bool),
        Some(false)
    );
}

#[test]
fn daemon_socket_prepare_removes_stale_entry_before_bind() {
    let tempdir = tempfile::tempdir().expect("create tempdir");
    let stale_path = tempdir.path().join("prepare.sock");
    fs::write(&stale_path, "stale").expect("write stale file");
    assert!(stale_path.exists());
    let prepared =
        prepare_daemon_socket_path(stale_path.to_string_lossy().as_ref()).expect("prepare");
    assert_eq!(prepared, stale_path);
    assert!(!stale_path.exists(), "stale socket path should be removed");
}

#[test]
fn read_bounded_bytes_accepts_exact_limit_payloads() {
    let payload = vec![b'a'; 8];
    let mut cursor = Cursor::new(payload.clone());

    let bytes = read_bounded_bytes(&mut cursor, 8).expect("read exact-limit payload");
    assert_eq!(bytes, payload);
}

#[test]
fn read_bounded_bytes_rejects_payloads_over_limit() {
    let mut cursor = Cursor::new(vec![b'a'; 9]);

    let error = read_bounded_bytes(&mut cursor, 8).expect_err("oversized payload must fail");
    assert!(
        error
            .to_string()
            .contains("request payload exceeds maximum size")
    );
}

#[test]
fn daemon_stop_all_prunes_stale_socket_files() {
    let tempdir = tempfile::tempdir().expect("create tempdir");
    let socket_dir = tempdir.path().join("daemons");
    fs::create_dir_all(&socket_dir).expect("create daemon socket dir");
    let dead_socket = socket_dir.join("dead.sock");
    fs::write(&dead_socket, "stale").expect("write dead socket marker");
    assert!(dead_socket.exists(), "test precondition");

    let result = handle_daemon(DaemonCommands::StopAll(DaemonStopAllArgs {
        socket_dir: Some(socket_dir.to_string_lossy().to_string()),
    }))
    .expect("daemon stop-all");
    assert_eq!(
        result
            .args
            .get("discovered_sockets")
            .and_then(JsonValue::as_u64),
        Some(1)
    );
    assert_eq!(
        result.args.get("pruned_stale").and_then(JsonValue::as_u64),
        Some(1)
    );
    assert!(!dead_socket.exists(), "stale socket should be removed");
}

#[test]
fn daemon_execution_policy_routes_diagnostics_reads_and_mutations() {
    let health = Commands::Health(VaultPathArgs {
        vault_root: Some("/tmp".to_string()),
        db_path: None,
    });
    assert_eq!(
        daemon_execution_policy(&health),
        DaemonExecutionPolicy::ObservationalFresh
    );

    let stats = Commands::Vault {
        command: VaultCommands::Stats(VaultPathArgs {
            vault_root: Some("/tmp".to_string()),
            db_path: None,
        }),
    };
    assert_eq!(
        daemon_execution_policy(&stats),
        DaemonExecutionPolicy::ObservationalFresh
    );

    let preflight = Commands::Vault {
        command: VaultCommands::Preflight(VaultPathArgs {
            vault_root: Some("/tmp".to_string()),
            db_path: None,
        }),
    };
    assert_eq!(
        daemon_execution_policy(&preflight),
        DaemonExecutionPolicy::ObservationalFresh
    );

    let cacheable_query = Commands::Query(QueryArgs {
        vault_root: Some("/tmp".to_string()),
        db_path: None,
        from: "docs".to_string(),
        query: Some("project".to_string()),
        path: None,
        view_name: None,
        select: None,
        where_clause: None,
        sort: None,
        explain: false,
        execute: false,
        limit: 10,
        offset: 0,
    });
    assert_eq!(
        daemon_execution_policy(&cacheable_query),
        DaemonExecutionPolicy::CachedReadWithRefresh
    );

    let doc_write = Commands::Doc {
        command: DocCommands::Write(NotePutArgs {
            vault_root: Some("/tmp".to_string()),
            db_path: None,
            path: "notes/x.md".to_string(),
            content: "# x".to_string(),
        }),
    };
    assert_eq!(
        daemon_execution_policy(&doc_write),
        DaemonExecutionPolicy::ExplicitWork
    );
}

#[test]
fn observational_policy_leaves_existing_daemon_result_cache_untouched() {
    let mut runtime = RuntimeMode::Daemon(Box::<RuntimeCache>::default());
    let runtime_key = "vault-key".to_string();
    let cached = CommandResult {
        command: "query.run".to_string(),
        summary: "cached".to_string(),
        args: serde_json::json!({ "total": 1 }),
    };
    if let RuntimeMode::Daemon(cache) = &mut runtime {
        cache.command_results.insert(
            "cached-key".to_string(),
            CachedCommandResult {
                runtime_key: runtime_key.clone(),
                result: cached,
            },
        );
    }

    let fresh = CommandResult {
        command: "health".to_string(),
        summary: "health completed".to_string(),
        args: serde_json::json!({ "status": "ready" }),
    };
    update_daemon_command_cache(
        &mut runtime,
        DaemonExecutionPolicy::ObservationalFresh,
        Some(&runtime_key),
        None,
        &fresh,
    );

    if let RuntimeMode::Daemon(cache) = &runtime {
        assert_eq!(cache.command_results.len(), 1);
        assert!(cache.command_results.contains_key("cached-key"));
    }
}

#[test]
fn vault_open_creates_default_db_when_db_path_is_omitted() {
    with_temp_cwd(|| {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let vault_root = tempdir.path().join("vault");
        fs::create_dir_all(&vault_root).expect("create vault dir");

        let cli = Cli::parse_from([
            "tao",
            "vault",
            "open",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        let result = dispatch(cli.command, cli.allow_writes).expect("dispatch");
        let output = render_output(cli.json, &result).expect("render output");
        let envelope: JsonValue = serde_json::from_str(&output).expect("parse output");

        let db_path = envelope
            .get("data")
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
            .get("data")
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

#[test]
fn daemon_refresh_uses_filesystem_monitor_to_pick_up_external_note_changes() {
    with_temp_cwd(|| {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let vault_root = tempdir.path().join("vault");
        fs::create_dir_all(vault_root.join("notes")).expect("create notes");
        fs::write(vault_root.join("notes/a.md"), "# A").expect("write a");

        let command = Commands::Doc {
            command: DocCommands::List(VaultPathArgs {
                vault_root: Some(vault_root.to_string_lossy().to_string()),
                db_path: None,
            }),
        };

        let mut runtime = RuntimeMode::Daemon(Box::<RuntimeCache>::default());
        maybe_refresh_daemon_state(&command, &mut runtime).expect("prime daemon refresh");
        let first = dispatch_with_runtime(command.clone(), false, &mut runtime)
            .expect("dispatch first daemon list");
        assert_eq!(first.args.get("total").and_then(JsonValue::as_u64), Some(1));

        let resolved = resolve_command_vault_paths(&command)
            .expect("resolve paths")
            .expect("resolved args");
        let runtime_key = runtime_cache_key(&resolved);
        if let RuntimeMode::Daemon(cache) = &mut runtime {
            let cache_key = serde_json::to_string(&command).expect("cache key");
            cache.command_results.insert(
                cache_key,
                super::CachedCommandResult {
                    runtime_key,
                    result: first,
                },
            );
        }

        fs::write(vault_root.join("notes/b.md"), "# B").expect("write b");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        let mut refreshed = false;
        while std::time::Instant::now() < deadline {
            if maybe_refresh_daemon_state(&command, &mut runtime).expect("refresh daemon state") {
                refreshed = true;
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(25));
        }
        assert!(
            refreshed,
            "expected daemon refresh after external note change"
        );

        if let RuntimeMode::Daemon(cache) = &runtime {
            assert!(
                cache.command_results.is_empty(),
                "stale cached command results should be invalidated"
            );
        }

        let second = dispatch_with_runtime(command, false, &mut runtime)
            .expect("dispatch refreshed daemon list");
        assert_eq!(
            second.args.get("total").and_then(JsonValue::as_u64),
            Some(2)
        );
    });
}

#[test]
fn daemon_first_observation_syncs_existing_stale_index_before_cached_reads() {
    with_temp_cwd(|| {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let vault_root = tempdir.path().join("vault");
        fs::create_dir_all(vault_root.join("notes")).expect("create notes");
        fs::write(vault_root.join("notes/a.md"), "# A").expect("write a");

        let open = Cli::parse_from([
            "tao",
            "vault",
            "open",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(open.command, open.allow_writes).expect("open vault");
        let reindex = Cli::parse_from([
            "tao",
            "vault",
            "reindex",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(reindex.command, reindex.allow_writes).expect("reindex vault");

        fs::write(vault_root.join("notes/b.md"), "# B").expect("write stale change");

        let command = Commands::Doc {
            command: DocCommands::List(VaultPathArgs {
                vault_root: Some(vault_root.to_string_lossy().to_string()),
                db_path: None,
            }),
        };

        let mut runtime = RuntimeMode::Daemon(Box::<RuntimeCache>::default());
        let refreshed =
            maybe_refresh_daemon_state(&command, &mut runtime).expect("initial daemon sync");
        assert!(
            refreshed,
            "first daemon observation should sync stale indexed state"
        );

        let listed = dispatch_with_runtime(command, false, &mut runtime)
            .expect("dispatch synced daemon list");
        assert_eq!(
            listed.args.get("total").and_then(JsonValue::as_u64),
            Some(2)
        );
    });
}

#[test]
fn health_in_daemon_mode_is_observational_and_reports_runtime_state() {
    with_temp_cwd(|| {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let vault_root = tempdir.path().join("vault");
        fs::create_dir_all(vault_root.join("notes")).expect("create notes");
        fs::write(vault_root.join("notes/a.md"), "# A").expect("write a");

        let open = Cli::parse_from([
            "tao",
            "vault",
            "open",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(open.command, open.allow_writes).expect("open vault");

        let reindex = Cli::parse_from([
            "tao",
            "vault",
            "reindex",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(reindex.command, reindex.allow_writes).expect("reindex vault");

        let health_command = Commands::Health(VaultPathArgs {
            vault_root: Some(vault_root.to_string_lossy().to_string()),
            db_path: None,
        });
        let resolved = resolve_command_vault_paths(&health_command)
            .expect("resolve health command")
            .expect("resolved health command");
        let runtime_key = runtime_cache_key(&resolved);

        let mut runtime = RuntimeMode::Daemon(Box::<RuntimeCache>::default());
        if let RuntimeMode::Daemon(cache) = &mut runtime {
            cache.command_results.insert(
                "cached-query".to_string(),
                CachedCommandResult {
                    runtime_key: runtime_key.clone(),
                    result: CommandResult {
                        command: "query.run".to_string(),
                        summary: "cached query".to_string(),
                        args: serde_json::json!({ "total": 1 }),
                    },
                },
            );
        }

        let first = dispatch_with_runtime(health_command.clone(), false, &mut runtime)
            .expect("dispatch daemon health");
        let first_timestamp = first
            .args
            .get("stats")
            .and_then(|stats| stats.get("last_index_updated_at"))
            .and_then(JsonValue::as_str)
            .expect("first timestamp")
            .to_string();

        assert_eq!(
            first
                .args
                .get("runtime")
                .and_then(|runtime| runtime.get("backend"))
                .and_then(JsonValue::as_str),
            Some("daemon")
        );
        assert_eq!(
            first
                .args
                .get("runtime")
                .and_then(|runtime| runtime.get("daemon_running"))
                .and_then(JsonValue::as_bool),
            Some(true)
        );
        assert_eq!(
            first
                .args
                .get("runtime")
                .and_then(|runtime| runtime.get("change_monitor_initialized"))
                .and_then(JsonValue::as_bool),
            Some(false)
        );
        assert_eq!(
            first
                .args
                .get("runtime")
                .and_then(|runtime| runtime.get("cached_connection"))
                .and_then(JsonValue::as_bool),
            Some(false)
        );
        assert_eq!(
            first
                .args
                .get("stats")
                .and_then(|stats| stats.get("watcher_status"))
                .and_then(JsonValue::as_str),
            Some("stopped")
        );

        fs::write(vault_root.join("notes/b.md"), "# B").expect("write b");

        let second = dispatch_with_runtime(health_command, false, &mut runtime)
            .expect("dispatch daemon health after drift");
        assert_eq!(
            second.args.get("status").and_then(JsonValue::as_str),
            Some("degraded")
        );
        assert_eq!(
            second
                .args
                .get("stats")
                .and_then(|stats| stats.get("index_lag"))
                .and_then(JsonValue::as_u64),
            Some(1)
        );
        assert_eq!(
            second
                .args
                .get("stats")
                .and_then(|stats| stats.get("last_index_updated_at"))
                .and_then(JsonValue::as_str),
            Some(first_timestamp.as_str())
        );
        assert_eq!(
            second
                .args
                .get("runtime")
                .and_then(|runtime| runtime.get("cached_connection"))
                .and_then(JsonValue::as_bool),
            Some(true)
        );

        if let RuntimeMode::Daemon(cache) = &runtime {
            assert!(
                cache.command_results.contains_key("cached-query"),
                "observational health should not clear cached query results"
            );
            assert!(
                !cache.change_monitors.contains_key(&runtime_key),
                "observational health should not initialize change monitors"
            );
        }
    });
}

fn copy_dir_recursive(source: &Path, destination: &Path) -> std::io::Result<()> {
    fs::create_dir_all(destination)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        if file_type.is_dir() {
            copy_dir_recursive(&source_path, &destination_path)?;
        } else if file_type.is_file() {
            if let Some(parent) = destination_path.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::copy(&source_path, &destination_path)?;
        }
    }
    Ok(())
}
