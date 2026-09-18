use super::super::*;

#[test]
fn large_sorted_offset_cannot_disable_query_work_limits() {
    let sort = parse_sort_keys(Some("title:asc")).unwrap();
    let mut accumulator = QueryPostFilterAccumulator::new(u32::MAX, 1, &sort);
    let rows = (0..100_001).map(|_| serde_json::Map::new()).collect();
    let error = accumulator.push_batch(rows).expect_err("bounded fallback");
    assert_eq!(classify_cli_error(&error).error.code, "query_work_limit");
}

#[test]
fn query_work_limit_counts_cell_bytes_not_only_rows() {
    let mut accumulator = QueryPostFilterAccumulator::new(0, 1, &[]);
    let row = serde_json::Map::from_iter([(
        "title".into(),
        JsonValue::String("x".repeat(32 * 1024 * 1024)),
    )]);
    let error = accumulator.push_batch(vec![row]).expect_err("bounded cell");
    assert_eq!(classify_cli_error(&error).error.code, "query_work_limit");
}

#[test]
fn unsupported_query_options_fail_before_any_database_creation() {
    super::with_temp_cwd(|| {
        let directory = tempfile::tempdir().unwrap();
        let db = directory.path().join("not-created/index.sqlite");
        for (scope, flag, value) in [
            ("graph", "--where", "path = 'x'"),
            ("task", "--sort", "path:asc"),
            ("meta:tags", "--query", "x"),
            ("docs", "--path", "x.md"),
        ] {
            let cli = Cli::parse_from([
                "tao",
                "query",
                "--from",
                scope,
                flag,
                value,
                "--vault-root",
                directory.path().to_str().unwrap(),
                "--db-path",
                db.to_str().unwrap(),
            ]);
            let error = dispatch(cli.command).expect_err("unsupported capability");
            assert_eq!(classify_cli_error(&error).error.code, "invalid_argument");
            assert!(!db.exists());
        }
    });
}

#[test]
fn tool_detail_delivers_schemas_defaults_and_scope_capabilities() {
    super::with_temp_cwd(|| {
        for name in [
            "doc.read",
            "doc.list",
            "query.run",
            "vault.reindex",
            "search.run",
        ] {
            let result = dispatch(Cli::parse_from(["tao", "tools", name]).command).unwrap();
            assert_eq!(result.args["schemas"]["input"]["type"], "object");
            assert_eq!(
                result.args["schemas"]["output"]["required"],
                serde_json::json!(["ok", "meta"])
            );
            assert_eq!(
                result.args["schemas"]["input"]["properties"]["execution_mode"]["default"],
                "auto"
            );
        }
        let result = dispatch(Cli::parse_from(["tao", "tools", "doc.read"]).command).unwrap();
        assert_eq!(
            result.args["schemas"]["input"]["properties"]["limit"]["default"],
            100
        );
        let result = dispatch(Cli::parse_from(["tao", "tools", "query.run"]).command).unwrap();
        assert_eq!(
            result.args["schemas"]["capabilities"]["scopes"]["graph"]["where"],
            false
        );
    });
}

#[test]
fn preflight_reports_missing_database_without_creating_paths() {
    super::with_temp_cwd(|| {
        let directory = tempfile::tempdir().unwrap();
        let db = directory.path().join("not-created/index.sqlite");
        let cli = Cli::parse_from([
            "tao",
            "vault",
            "preflight",
            "--vault-root",
            directory.path().to_str().unwrap(),
            "--db-path",
            db.to_str().unwrap(),
        ]);
        let result = dispatch(cli.command).unwrap();
        assert_eq!(result.args["database_exists"], false);
        assert!(!db.parent().unwrap().exists());
    });
}

#[test]
fn generic_query_explain_is_observational_for_every_scope() {
    super::with_temp_cwd(|| {
        let directory = tempfile::tempdir().unwrap();
        let db = directory.path().join("not-created/index.sqlite");
        for scope in [
            "docs",
            "graph",
            "task",
            "meta:tags",
            "meta:aliases",
            "meta:properties",
        ] {
            let cli = Cli::parse_from([
                "tao",
                "query",
                "--from",
                scope,
                "--explain",
                "--vault-root",
                directory.path().to_str().unwrap(),
                "--db-path",
                db.to_str().unwrap(),
            ]);
            let result = dispatch(cli.command).unwrap();
            assert_eq!(result.args["physical_plan"]["execute"], false);
            assert!(!db.parent().unwrap().exists());
        }
    });
}

#[test]
fn executed_window_queries_include_the_requested_explanation() {
    super::with_temp_cwd(|| {
        let directory = tempfile::tempdir().unwrap();
        std::fs::write(
            directory.path().join("a.md"),
            "---\ntags: [sample]\n---\n# A\n- [ ] work\n[[missing]]\n",
        )
        .unwrap();
        let root = directory.path().to_str().unwrap();
        dispatch(Cli::parse_from(["tao", "vault", "reindex", "--vault-root", root]).command)
            .unwrap();
        for scope in [
            "graph",
            "task",
            "meta:tags",
            "meta:aliases",
            "meta:properties",
        ] {
            let result = dispatch(
                Cli::parse_from([
                    "tao",
                    "query",
                    "--from",
                    scope,
                    "--explain",
                    "--execute",
                    "--vault-root",
                    root,
                ])
                .command,
            )
            .unwrap();
            assert_eq!(result.args["explain"]["execute"], true);
            assert_eq!(result.args["from"], scope);
            assert!(result.args.get("items").is_some());
        }
    });
}

#[test]
fn graph_query_limits_occurrences_and_preserves_panel_totals() {
    super::with_temp_cwd(|| {
        let directory = tempfile::tempdir().unwrap();
        std::fs::write(directory.path().join("a.md"), "[[b]]\n[[b]]\n[[missing]]\n").unwrap();
        std::fs::write(directory.path().join("b.md"), "# B\n[[a]]\n").unwrap();
        let root = directory.path().to_str().unwrap();
        dispatch(Cli::parse_from(["tao", "vault", "reindex", "--vault-root", root]).command)
            .unwrap();
        let result = dispatch(
            Cli::parse_from([
                "tao",
                "query",
                "--from",
                "graph",
                "--path",
                "a.md",
                "--limit",
                "2",
                "--offset",
                "1",
                "--vault-root",
                root,
            ])
            .command,
        )
        .unwrap();
        assert_eq!(result.args["total"], 4);
        assert_eq!(result.args["outgoing_total"], 3);
        assert_eq!(result.args["backlinks_total"], 1);
        assert_eq!(result.args["items"].as_array().unwrap().len(), 2);
    });
}
