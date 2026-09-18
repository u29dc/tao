use super::*;

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
    assert!(rendered.contains("validate"));
    assert!(rendered.contains("query"));
    assert!(rendered.contains("search"));
    assert!(rendered.contains("tools"));
    assert!(rendered.contains("health"));
    assert!(rendered.contains("config"));
    assert!(rendered.contains("--toon"));
    assert!(!rendered.contains("--allow-writes"));
    assert!(!rendered.contains("--text"));
    assert!(!rendered.contains("note"));
    assert!(!rendered.contains("links"));
    assert!(!rendered.contains("properties"));
    assert!(!rendered.contains("bases"));
    assert!(!rendered.contains("hubs"));
}

#[test]
fn doc_and_task_help_hide_removed_write_surfaces() {
    let mut doc = Cli::command()
        .find_subcommand_mut("doc")
        .expect("doc command")
        .clone();
    let mut doc_output = Vec::new();
    doc.write_long_help(&mut doc_output)
        .expect("render doc help");
    let doc_help = String::from_utf8(doc_output).expect("utf8 doc help");
    assert!(doc_help.contains("read"));
    assert!(doc_help.contains("list"));
    assert!(!doc_help.contains("write"));
    assert!(!doc_help.contains("--allow-writes"));
    assert!(!doc_help.contains("--text"));

    let mut task = Cli::command()
        .find_subcommand_mut("task")
        .expect("task command")
        .clone();
    let mut task_output = Vec::new();
    task.write_long_help(&mut task_output)
        .expect("render task help");
    let task_help = String::from_utf8(task_output).expect("utf8 task help");
    assert!(task_help.contains("list"));
    assert!(!task_help.contains("set-state"));
    assert!(!task_help.contains("--allow-writes"));
    assert!(!task_help.contains("--text"));
}

#[test]
fn run_from_args_supports_toon_tools_output() {
    let result = run_from_args(
        ["tao", "--toon", "tools"]
            .into_iter()
            .map(std::ffi::OsString::from)
            .collect(),
    );

    assert_eq!(result.exit_kind, ExitKind::Success);
    let stdout = result.stdout.expect("toon stdout");
    let envelope: JsonValue = toon_format::decode_default(&stdout).expect("parse toon output");
    assert_eq!(envelope.get("ok").and_then(JsonValue::as_bool), Some(true));
    assert_eq!(
        envelope
            .get("data")
            .and_then(|data| data.get("defaultOutputFormat"))
            .and_then(JsonValue::as_str),
        Some("json")
    );
}

#[test]
fn toon_parse_errors_use_native_clap_usage_error() {
    let result = run_from_args(
        ["tao", "--toon", "validate"]
            .into_iter()
            .map(std::ffi::OsString::from)
            .collect(),
    );

    assert_usage_error(result, ClapErrorKind::MissingRequiredArgument);
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
fn group_invocation_prints_group_help_instead_of_root_help() {
    for group in ["graph", "vault"] {
        let result = run_from_args(
            ["tao", group]
                .into_iter()
                .map(std::ffi::OsString::from)
                .collect(),
        );

        assert_eq!(result.exit_kind, ExitKind::Success);
        assert!(result.stdout.is_none());
        assert!(result.stderr.is_none());
        match result.clap_output {
            Some(ClapOutput::SubcommandHelp(path)) => {
                assert_eq!(path, vec![group.to_string()]);
            }
            other => panic!("expected {group} help output, got {other:?}"),
        }
    }
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
        let result = dispatch(cli.command).expect("dispatch");
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
fn toon_output_decodes_to_matching_envelope() {
    let cli = Cli::parse_from(["tao", "tools"]);
    let result = dispatch(cli.command).expect("dispatch tools");
    let json_output =
        render_output_with_format(OutputFormat::Json, &result, std::time::Duration::ZERO)
            .expect("render json output");
    let toon_output =
        render_output_with_format(OutputFormat::Toon, &result, std::time::Duration::ZERO)
            .expect("render toon output");

    let json_value: JsonValue = serde_json::from_str(&json_output).expect("parse json");
    let toon_value: JsonValue = toon_format::decode_default(&toon_output).expect("parse toon");

    assert_eq!(json_value, toon_value);
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
                "config.show",
                vec!["tao", "config", "show", "--vault-root", &vault_root_string],
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
                "search.run",
                vec![
                    "tao",
                    "search",
                    "project",
                    "--vault-root",
                    &vault_root_string,
                    "--context",
                    "--depth",
                    "2",
                ],
            ),
            (
                "graph.links",
                vec![
                    "tao",
                    "graph",
                    "links",
                    "--direction",
                    "outgoing",
                    "--vault-root",
                    &vault_root_string,
                    "--path",
                    "notes/alpha.md",
                ],
            ),
            (
                "graph.links",
                vec![
                    "tao",
                    "graph",
                    "links",
                    "--direction",
                    "incoming",
                    "--vault-root",
                    &vault_root_string,
                    "--path",
                    "notes/projects/project-a.md",
                ],
            ),
            (
                "graph.links",
                vec![
                    "tao",
                    "graph",
                    "links",
                    "--vault-root",
                    &vault_root_string,
                    "--path",
                    "notes/alpha.md",
                ],
            ),
            (
                "graph.audit",
                vec![
                    "tao",
                    "graph",
                    "audit",
                    "--vault-root",
                    &vault_root_string,
                    "--kind",
                    "unresolved",
                ],
            ),
            (
                "graph.audit",
                vec![
                    "tao",
                    "graph",
                    "audit",
                    "--kind",
                    "inbound-scope",
                    "--vault-root",
                    &vault_root_string,
                    "--scope",
                    "notes",
                    "--include-markdown",
                ],
            ),
            (
                "graph.audit",
                vec![
                    "tao",
                    "graph",
                    "audit",
                    "--kind",
                    "unresolved",
                    "--vault-root",
                    &vault_root_string,
                ],
            ),
            (
                "graph.audit",
                vec![
                    "tao",
                    "graph",
                    "audit",
                    "--kind",
                    "deadends",
                    "--vault-root",
                    &vault_root_string,
                ],
            ),
            (
                "graph.audit",
                vec![
                    "tao",
                    "graph",
                    "audit",
                    "--kind",
                    "orphans",
                    "--vault-root",
                    &vault_root_string,
                ],
            ),
            (
                "graph.audit",
                vec![
                    "tao",
                    "graph",
                    "audit",
                    "--kind",
                    "floating",
                    "--vault-root",
                    &vault_root_string,
                ],
            ),
            (
                "graph.audit",
                vec![
                    "tao",
                    "graph",
                    "audit",
                    "--kind",
                    "components",
                    "--vault-root",
                    &vault_root_string,
                ],
            ),
            (
                "graph.links",
                vec![
                    "tao",
                    "graph",
                    "links",
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
                "validate",
                vec![
                    "tao",
                    "validate",
                    "notes/projects/project-a.md",
                    "--vault-root",
                    &vault_root_string,
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
                "task.list",
                vec!["tao", "task", "list", "--vault-root", &vault_root_string],
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
        ];

        for (expected_command, args) in scenarios {
            let cli = Cli::parse_from(args);
            let result = dispatch(cli.command).expect("dispatch json contract scenario");
            let output = render_output(cli.json, &result).expect("render json output");
            let envelope: JsonValue = serde_json::from_str(&output).expect("parse json output");
            assert_json_contract(&envelope, expected_command);
        }
    });
}

#[test]
fn removed_write_surface_parse_failures_use_native_clap_errors() {
    for args in [
        vec![
            "tao",
            "--allow-writes",
            "doc",
            "read",
            "--path",
            "notes/test.md",
        ],
        vec![
            "tao",
            "doc",
            "write",
            "--vault-root",
            "/tmp",
            "--path",
            "notes/test.md",
            "--content",
            "# test",
        ],
        vec![
            "tao",
            "task",
            "set-state",
            "--vault-root",
            "/tmp",
            "--path",
            "notes/tasks.md",
            "--line",
            "1",
            "--state",
            "done",
        ],
        vec!["tao", "--text", "tools"],
        vec![
            "tao",
            "base",
            "validate",
            "--vault-root",
            "/tmp",
            "--path-or-id",
            "views/projects.base",
        ],
    ] {
        let result = run_from_args(args.into_iter().map(std::ffi::OsString::from).collect());

        assert_eq!(result.exit_kind, ExitKind::Usage);
        assert!(result.stdout.is_none());
        assert!(result.stderr.is_none());
        match result.clap_output {
            Some(ClapOutput::Error(error)) => {
                assert!(
                    matches!(
                        error.kind(),
                        ClapErrorKind::UnknownArgument | ClapErrorKind::InvalidSubcommand
                    ),
                    "unexpected clap error kind for removed surface: {:?}",
                    error.kind(),
                );
                assert!(
                    !error.to_string().contains("panicked at"),
                    "error message should not include stack traces"
                );
            }
            other => panic!("expected native clap error output, got {other:?}"),
        }
    }
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
    let error = dispatch(cli.command).expect_err("parse must fail");
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
fn typed_cli_contract_error_classification_does_not_depend_on_message_text() {
    let error = anyhow::Error::new(CliContractError::failure(
        "stable_custom_code",
        "wording without classifier keywords",
        Some("stable hint".to_string()),
        None,
    ));

    let classified = classify_cli_error(&error);

    assert_eq!(classified.exit_kind, ExitKind::Failure);
    assert_eq!(classified.error.code, "stable_custom_code");
    assert_eq!(
        classified.error.message,
        "wording without classifier keywords"
    );
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
            "reindex",
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
    let result = dispatch(cli.command).expect("dispatch tools");
    let output = render_output(cli.json, &result).expect("render tools");
    let envelope: JsonValue = serde_json::from_str(&output).expect("parse tools output");

    assert_eq!(
        envelope
            .get("data")
            .and_then(|data| data.get("version"))
            .and_then(JsonValue::as_str),
        Some(env!("CARGO_PKG_VERSION"))
    );
    assert_eq!(
        envelope
            .get("data")
            .and_then(|data| data.get("defaultOutputFormat"))
            .and_then(JsonValue::as_str),
        Some("json")
    );
    assert_eq!(
        envelope
            .get("data")
            .and_then(|data| data.get("outputFormats")),
        Some(&serde_json::json!(["json", "toon"]))
    );
    let global_flags = envelope
        .get("data")
        .and_then(|data| data.get("globalFlags"))
        .and_then(JsonValue::as_array)
        .expect("global flags");
    assert!(
        global_flags
            .iter()
            .any(|flag| { flag.get("name").and_then(JsonValue::as_str) == Some("--toon") })
    );
    assert!(
        global_flags
            .iter()
            .all(|flag| { flag.get("name").and_then(JsonValue::as_str) != Some("--text") })
    );

    let tools_entry = registry::tool_detail("tools").expect("tools registry entry");
    assert!(
        tools_entry.output_fields.contains(&"version"),
        "tools registry outputFields should advertise version"
    );
    assert!(
        tools_entry.output_fields.contains(&"outputFormats"),
        "tools registry outputFields should advertise outputFormats"
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

    let public_tools = envelope
        .get("data")
        .and_then(|data| data.get("tools"))
        .and_then(JsonValue::as_array)
        .expect("public tools catalog");
    assert!(
        public_tools
            .iter()
            .all(|tool| tool.get("deprecated").is_none()),
        "default tools catalog should only include canonical tools"
    );
    assert!(
        public_tools
            .iter()
            .all(|tool| { tool.get("name").and_then(JsonValue::as_str) != Some("graph.outgoing") }),
        "compatibility wrappers should be available by direct lookup, not default discovery"
    );
    assert!(
        public_tools
            .iter()
            .all(|tool| { tool.get("name").and_then(JsonValue::as_str) != Some("base.validate") }),
        "base.validate should be replaced by top-level validate"
    );
    assert!(
        public_tools
            .iter()
            .any(|tool| tool.get("name").and_then(JsonValue::as_str) == Some("validate")),
        "top-level validate should be discoverable"
    );

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

    assert!(registry::tool_detail("graph.outgoing").is_none());

    let graph_links = envelope
        .get("data")
        .and_then(|data| data.get("tools"))
        .and_then(JsonValue::as_array)
        .and_then(|tools| {
            tools
                .iter()
                .find(|tool| tool.get("name").and_then(JsonValue::as_str) == Some("graph.links"))
        })
        .expect("graph.links tool");
    assert_eq!(
        graph_links.get("stability").and_then(JsonValue::as_str),
        Some("stable")
    );
}

#[test]
fn parse_failures_default_to_native_clap_error() {
    let result = run_from_args(
        ["tao", "note", "read"]
            .into_iter()
            .map(std::ffi::OsString::from)
            .collect(),
    );

    assert_usage_error(result, ClapErrorKind::InvalidSubcommand);
}

#[test]
fn public_surface_never_exposes_vault_write_commands() {
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

"#,
                vault_root.display()
            ),
        )
        .expect("write root config");

        let doc_result = run_from_args(
            [
                "tao",
                "doc",
                "write",
                "--path",
                "notes/policy-write.md",
                "--content",
                "# policy",
            ]
            .into_iter()
            .map(std::ffi::OsString::from)
            .collect(),
        );
        assert_usage_error(doc_result, ClapErrorKind::InvalidSubcommand);
        assert!(
            !vault_root.join("notes/policy-write.md").exists(),
            "removed public write command must not create vault content"
        );

        let task_result = run_from_args(
            [
                "tao",
                "task",
                "set-state",
                "--path",
                "notes/a.md",
                "--line",
                "1",
                "--state",
                "done",
            ]
            .into_iter()
            .map(std::ffi::OsString::from)
            .collect(),
        );
        assert_usage_error(task_result, ClapErrorKind::InvalidSubcommand);

        let tools = Cli::parse_from(["tao", "tools"]);
        let tools_output = render_output(
            tools.json,
            &dispatch(tools.command).expect("dispatch tools"),
        )
        .expect("render tools");
        let envelope: JsonValue = serde_json::from_str(&tools_output).expect("parse tools");
        let names = envelope
            .get("data")
            .and_then(|data| data.get("tools"))
            .and_then(JsonValue::as_array)
            .expect("tools")
            .iter()
            .filter_map(|tool| tool.get("name").and_then(JsonValue::as_str))
            .collect::<Vec<_>>();
        assert!(!names.contains(&"doc.write"));
        assert!(!names.contains(&"task.set-state"));
    });
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
        let result = dispatch(cli.command).expect("dispatch");
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
        let result = dispatch(cli.command).expect("dispatch");
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
