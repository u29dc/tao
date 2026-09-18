use super::*;

#[test]
fn graph_and_vault_help_hide_compatibility_surfaces() {
    let mut graph = Cli::command()
        .find_subcommand_mut("graph")
        .expect("graph command")
        .clone();
    let mut graph_output = Vec::new();
    graph
        .write_long_help(&mut graph_output)
        .expect("render graph help");
    let graph_help = String::from_utf8(graph_output).expect("utf8 graph help");
    assert!(graph_help.contains("links"));
    assert!(graph_help.contains("audit"));
    assert!(!graph_help.contains("outgoing"));
    assert!(!graph_help.contains("backlinks"));
    assert!(!graph_help.contains("\n  unresolved"));

    let mut vault = Cli::command()
        .find_subcommand_mut("vault")
        .expect("vault command")
        .clone();
    let mut vault_output = Vec::new();
    vault
        .write_long_help(&mut vault_output)
        .expect("render vault help");
    let vault_help = String::from_utf8(vault_output).expect("utf8 vault help");
    assert!(vault_help.contains("open"));
    assert!(vault_help.contains("preflight"));
    assert!(vault_help.contains("reindex"));
    assert!(!vault_help.contains("\n  stats"));
    assert!(!vault_help.contains("\n  reconcile"));
    assert!(!vault_help.contains("\n  daemon"));

    let mut base = Cli::command()
        .find_subcommand_mut("base")
        .expect("base command")
        .clone();
    let mut base_output = Vec::new();
    base.write_long_help(&mut base_output)
        .expect("render base help");
    let base_help = String::from_utf8(base_output).expect("utf8 base help");
    assert!(base_help.contains("list"));
    assert!(base_help.contains("schema"));
    assert!(base_help.contains("view"));
    assert!(!base_help.contains("validate"));
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
        dispatch(open.command).expect("open vault");
        let reindex = Cli::parse_from([
            "tao",
            "vault",
            "reindex",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(reindex.command).expect("reindex vault");

        let neighbors = Cli::parse_from([
            "tao",
            "graph",
            "links",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
            "--path",
            "notes/a.md",
            "--direction",
            "incoming",
        ]);
        let output = render_output(
            neighbors.json,
            &dispatch(neighbors.command).expect("dispatch neighbors"),
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
        dispatch(open.command).expect("open vault");
        let reindex = Cli::parse_from([
            "tao",
            "vault",
            "reindex",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(reindex.command).expect("reindex vault");

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
            &dispatch(found.command).expect("dispatch found path"),
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
            &dispatch(missing.command).expect("dispatch missing path"),
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
        let error = dispatch(guardrail.command).expect_err("guardrail should fail");
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
        dispatch(open.command).expect("open vault");
        let reindex = Cli::parse_from([
            "tao",
            "vault",
            "reindex",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(reindex.command).expect("reindex vault");

        let weak = Cli::parse_from([
            "tao",
            "graph",
            "audit",
            "--kind",
            "components",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
            "--mode",
            "weak",
            "--include-members",
        ]);
        let weak_output = render_output(
            weak.json,
            &dispatch(weak.command).expect("dispatch weak components"),
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
            "audit",
            "--kind",
            "components",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
            "--mode",
            "strong",
            "--include-members",
        ]);
        let strong_output = render_output(
            strong.json,
            &dispatch(strong.command).expect("dispatch strong components"),
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
fn taoignore_removes_ignored_paths_from_graph_audits_after_reindex() {
    with_temp_cwd(|| {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let vault_root = tempdir.path().join("vault");
        fs::create_dir_all(vault_root.join("notes")).expect("create notes dir");
        fs::create_dir_all(vault_root.join("assets")).expect("create assets dir");
        fs::create_dir_all(vault_root.join("_TMP/ctvc")).expect("create scratch dir");
        fs::create_dir_all(vault_root.join(".tmp")).expect("create dot tmp dir");

        fs::write(vault_root.join(".taoignore"), "_TMP/\n.tmp/\n").expect("write taoignore");
        fs::write(vault_root.join("notes/root.md"), "# Root\n[[linked]]\n").expect("write root");
        fs::write(vault_root.join("notes/linked.md"), "# Linked\n").expect("write linked");
        fs::write(vault_root.join("assets/keep.pdf"), "pdf").expect("write durable asset");
        fs::write(vault_root.join("_TMP/ctvc/floating.md"), "# Scratch\n")
            .expect("write scratch note");
        fs::write(vault_root.join(".tmp/transient.md"), "# Transient\n")
            .expect("write transient note");

        let reindex = Cli::parse_from([
            "tao",
            "vault",
            "reindex",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(reindex.command).expect("reindex vault");

        let floating = Cli::parse_from([
            "tao",
            "graph",
            "audit",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
            "--kind",
            "floating",
            "--limit",
            "100",
            "--offset",
            "0",
        ]);
        let floating_output = render_output(
            floating.json,
            &dispatch(floating.command).expect("dispatch floating audit"),
        )
        .expect("render floating audit");
        let floating_json: JsonValue =
            serde_json::from_str(&floating_output).expect("parse floating json");
        let floating_paths = json_item_paths(&floating_json);
        assert_eq!(floating_paths, vec!["assets/keep.pdf"]);
        assert!(!floating_output.contains("_TMP/ctvc"));
        assert!(!floating_output.contains(".tmp/transient"));

        let components = Cli::parse_from([
            "tao",
            "graph",
            "audit",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
            "--kind",
            "components",
            "--include-members",
            "--limit",
            "100",
            "--offset",
            "0",
        ]);
        let components_output = render_output(
            components.json,
            &dispatch(components.command).expect("dispatch components audit"),
        )
        .expect("render components audit");
        let components_json: JsonValue =
            serde_json::from_str(&components_output).expect("parse components json");
        let component_paths = components_json
            .get("data")
            .and_then(|data| data.get("items"))
            .and_then(JsonValue::as_array)
            .expect("component items")
            .iter()
            .flat_map(|item| {
                item.get("paths")
                    .and_then(JsonValue::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(JsonValue::as_str)
            })
            .collect::<Vec<_>>();
        assert_eq!(
            component_paths,
            vec!["notes/linked.md", "notes/root.md", "assets/keep.pdf"]
        );
        assert!(!components_output.contains("_TMP/ctvc"));
        assert!(!components_output.contains(".tmp/transient"));
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
        dispatch(open.command).expect("open vault");
        let reindex = Cli::parse_from([
            "tao",
            "vault",
            "reindex",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(reindex.command).expect("reindex vault");

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
            &dispatch(plain_walk.command).expect("dispatch plain walk"),
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
            &dispatch(folder_walk.command).expect("dispatch folder walk"),
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
        dispatch(open.command).expect("open vault");
        let reindex = Cli::parse_from([
            "tao",
            "vault",
            "reindex",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(reindex.command).expect("reindex vault");

        let unresolved = Cli::parse_from([
            "tao",
            "graph",
            "audit",
            "--kind",
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
            &dispatch(unresolved.command).expect("dispatch unresolved"),
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
fn graph_audit_rejects_irrelevant_options_before_opening_storage() {
    with_temp_cwd(|| {
        let directory = tempfile::tempdir().unwrap();
        let db = directory.path().join("missing/index.sqlite");
        for (flag, value) in [
            ("--scope", "notes"),
            ("--mode", "strong"),
            ("--sample-size", "2"),
        ] {
            let cli = Cli::parse_from([
                "tao",
                "graph",
                "audit",
                "--kind",
                "unresolved",
                flag,
                value,
                "--vault-root",
                directory.path().to_str().unwrap(),
                "--db-path",
                db.to_str().unwrap(),
            ]);
            assert!(dispatch(cli.command).is_err());
            assert!(!db.exists());
        }
        for removed in [
            "outgoing",
            "backlinks",
            "neighbors",
            "unresolved",
            "deadends",
            "orphans",
            "floating",
            "components",
            "inbound-scope",
        ] {
            assert!(Cli::try_parse_from(["tao", "graph", removed]).is_err());
        }
    });
}

#[test]
fn graph_snapshot_contracts_match_golden_outputs() {
    with_temp_cwd(|| {
        let fixture_root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../fixtures/graph/vault")
            .canonicalize()
            .expect("canonicalize graph parity fixture");
        let expected_root = fixture_root.parent().unwrap().join("expected");

        let tempdir = tempfile::tempdir().expect("create tempdir");
        let vault_root = tempdir.path().join("vault");
        copy_dir_recursive(&fixture_root, &vault_root).expect("copy graph parity fixture");

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

        let assert_snapshot = |expected_name: &str, cli: Cli| {
            let rendered = render_output(cli.json, &dispatch(cli.command).expect("dispatch"))
                .expect("render output");
            let actual: JsonValue = serde_json::from_str(&rendered).expect("parse json envelope");
            let actual_args = actual.get("data").expect("data");
            let items = actual_args["items"].as_array().expect("occurrence items");
            match expected_name {
                "links-outgoing.json" => {
                    assert_eq!(
                        items.len(),
                        8,
                        "six body occurrences plus two frontmatter occurrences"
                    );
                    let source = fs::read(vault_root.join("notes/root.md")).unwrap();
                    for item in items.iter().filter(|item| item["source_field"] == "body") {
                        let evidence = &item["evidence"];
                        let start = evidence["source_start"].as_u64().unwrap() as usize;
                        let end = evidence["source_end"].as_u64().unwrap() as usize;
                        assert_eq!(
                            std::str::from_utf8(&source[start..end]).unwrap(),
                            evidence["raw_expression"]
                        );
                    }
                }
                "links-incoming.json" => {
                    assert_eq!(items.len(), 5, "invalid fragments retain their file edge")
                }
                "audit-unresolved.json" => {
                    assert_eq!(
                        items
                            .iter()
                            .filter(|item| item["issue_scope"] == "document")
                            .count(),
                        2
                    );
                    let fragments = items
                        .iter()
                        .filter(|item| item["issue_scope"] == "fragment")
                        .collect::<Vec<_>>();
                    assert_eq!(fragments.len(), 2);
                    for item in fragments {
                        assert_eq!(item["resolved_path"], "notes/beta.md");
                        assert_eq!(item["is_unresolved"], false);
                        assert_eq!(item["issue_status"], "broken");
                    }
                }
                "walk.json" => {
                    assert_eq!(items.len(), 9);
                    let ids = items
                        .iter()
                        .map(|item| item["link_id"].as_str().unwrap())
                        .collect::<std::collections::HashSet<_>>();
                    assert_eq!(ids.len(), items.len(), "each occurrence is emitted once");
                }
                _ => {}
            }
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
            "links-outgoing.json",
            Cli::parse_from([
                "tao",
                "graph",
                "links",
                "--direction",
                "outgoing",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
                "--path",
                "notes/root.md",
            ]),
        );
        assert_snapshot(
            "links-incoming.json",
            Cli::parse_from([
                "tao",
                "graph",
                "links",
                "--direction",
                "incoming",
                "--vault-root",
                vault_root.to_string_lossy().as_ref(),
                "--path",
                "notes/beta.md",
            ]),
        );
        assert_snapshot(
            "audit-unresolved.json",
            Cli::parse_from([
                "tao",
                "graph",
                "audit",
                "--kind",
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
            "audit-deadends.json",
            Cli::parse_from([
                "tao",
                "graph",
                "audit",
                "--kind",
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
            "audit-orphans.json",
            Cli::parse_from([
                "tao",
                "graph",
                "audit",
                "--kind",
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
            "audit-floating.json",
            Cli::parse_from([
                "tao",
                "graph",
                "audit",
                "--kind",
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
fn graph_links_normalizes_note_path_input_before_lookup() {
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
        dispatch(open.command).expect("open vault");
        let reindex = Cli::parse_from([
            "tao",
            "vault",
            "reindex",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(reindex.command).expect("reindex vault");

        let outgoing = Cli::parse_from([
            "tao",
            "graph",
            "links",
            "--direction",
            "outgoing",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
            "--path",
            "/notes\\source.md/",
        ]);
        let output = render_output(
            outgoing.json,
            &dispatch(outgoing.command).expect("dispatch outgoing"),
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
