use super::*;

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

"#,
                vault_root.display()
            ),
        )
        .expect("write root config");

        let cli = Cli::parse_from(["tao", "vault", "open"]);
        let result = dispatch(cli.command).expect("dispatch");
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
fn daemon_socket_prepare_preserves_regular_files() {
    let tempdir = tempfile::tempdir().expect("create tempdir");
    let stale_path = tempdir.path().join("prepare.sock");
    fs::write(&stale_path, "stale").expect("write stale file");
    assert!(stale_path.exists());
    prepare_daemon_socket_path(stale_path.to_string_lossy().as_ref())
        .expect_err("refuse regular file");
    assert_eq!(
        fs::read_to_string(&stale_path).expect("preserved file"),
        "stale"
    );
}

#[cfg(unix)]
#[test]
fn daemon_socket_prepare_preserves_existing_parent_permissions() {
    let tempdir = tempfile::tempdir().expect("create tempdir");
    let socket_dir = tempdir.path().join("daemon");
    fs::create_dir_all(&socket_dir).expect("create socket dir");
    let mut permissions = fs::metadata(&socket_dir)
        .expect("stat socket dir")
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&socket_dir, permissions).expect("loosen socket dir permissions");

    let socket_path = socket_dir.join("taod.sock");
    prepare_daemon_socket_path(socket_path.to_string_lossy().as_ref()).expect("prepare socket");

    let mode = fs::metadata(&socket_dir)
        .expect("stat hardened socket dir")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o755);
}

#[test]
fn daemon_stop_all_preserves_non_socket_files() {
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
        Some(0)
    );
    assert_eq!(
        result.args.get("pruned_stale").and_then(JsonValue::as_u64),
        Some(0)
    );
    assert_eq!(
        fs::read_to_string(dead_socket).expect("preserved file"),
        "stale"
    );
}

#[test]
fn daemon_execution_policy_routes_diagnostics_reads_and_mutations() {
    let health = Commands::Health(HealthArgs {
        vault_root: Some("/tmp".to_string()),
        db_path: None,
        deep: false,
    });
    assert_eq!(
        daemon_execution_policy(&health),
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

    let doc_write = Commands::Search(SearchArgs {
        query: Some("project".to_string()),
        path: None,
        kind: "auto".to_string(),
        scope: None,
        ext: Vec::new(),
        context: false,
        depth: 1,
        limit: 10,
        include_content: false,
        no_pii: false,
        vault_root: Some("/tmp".to_string()),
        db_path: None,
    });
    assert_eq!(
        daemon_execution_policy(&doc_write),
        DaemonExecutionPolicy::CachedReadWithRefresh
    );
}

#[test]
fn daemon_result_cache_evicts_old_entries() {
    let mut runtime = RuntimeMode::Daemon(Box::<RuntimeCache>::default());
    let result = CommandResult {
        command: "query.run".to_string(),
        summary: "cached".to_string(),
        args: serde_json::json!({ "total": 1 }),
    };

    for index in 0..300_u32 {
        update_daemon_command_cache(
            &mut runtime,
            DaemonExecutionPolicy::CachedReadWithRefresh,
            Some("runtime-key"),
            Some(format!("cache-{index:03}")),
            &result,
        );
    }

    if let RuntimeMode::Daemon(cache) = &runtime {
        assert_eq!(cache.command_results.len(), 256);
        assert_eq!(cache.command_result_order.len(), 256);
        assert!(!cache.command_results.contains_key("cache-000"));
        assert!(cache.command_results.contains_key("cache-299"));
    }
}

#[test]
fn daemon_refresh_uses_filesystem_monitor_to_pick_up_external_note_changes() {
    with_temp_cwd(|| {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let vault_root = tempdir.path().join("vault");
        fs::create_dir_all(vault_root.join("notes")).expect("create notes");
        fs::write(vault_root.join("notes/a.md"), "# A").expect("write a");

        let command = Commands::Doc {
            command: DocCommands::List(super::DocListArgs {
                vault_root: Some(vault_root.to_string_lossy().to_string()),
                db_path: None,
                limit: 100,
                offset: 0,
            }),
        };

        let mut runtime = RuntimeMode::Daemon(Box::<RuntimeCache>::default());
        maybe_refresh_daemon_state(&command, &mut runtime).expect("prime daemon refresh");
        let first = dispatch_with_runtime(command.clone(), &mut runtime)
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

        let second =
            dispatch_with_runtime(command, &mut runtime).expect("dispatch refreshed daemon list");
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
        dispatch(open.command).expect("open vault");
        let reindex = Cli::parse_from([
            "tao",
            "vault",
            "reindex",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(reindex.command).expect("reindex vault");

        fs::write(vault_root.join("notes/b.md"), "# B").expect("write stale change");

        let command = Commands::Doc {
            command: DocCommands::List(super::DocListArgs {
                vault_root: Some(vault_root.to_string_lossy().to_string()),
                db_path: None,
                limit: 100,
                offset: 0,
            }),
        };

        let mut runtime = RuntimeMode::Daemon(Box::<RuntimeCache>::default());
        let refreshed =
            maybe_refresh_daemon_state(&command, &mut runtime).expect("initial daemon sync");
        assert!(
            refreshed,
            "first daemon observation should sync stale indexed state"
        );

        let listed =
            dispatch_with_runtime(command, &mut runtime).expect("dispatch synced daemon list");
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
        dispatch(open.command).expect("open vault");

        let reindex = Cli::parse_from([
            "tao",
            "vault",
            "reindex",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(reindex.command).expect("reindex vault");

        let health_command = Commands::Health(HealthArgs {
            vault_root: Some(vault_root.to_string_lossy().to_string()),
            db_path: None,
            deep: true,
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

        let first = dispatch_with_runtime(health_command.clone(), &mut runtime)
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

        let second = dispatch_with_runtime(health_command, &mut runtime)
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
                .and_then(|stats| stats.get("scan_mode"))
                .and_then(JsonValue::as_str),
            Some("deep_metadata")
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
