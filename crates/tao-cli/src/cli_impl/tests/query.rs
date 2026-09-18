use super::*;

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
        dispatch(open.command).expect("open vault");
        let reindex = Cli::parse_from([
            "tao",
            "vault",
            "reindex",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(reindex.command).expect("reindex vault");

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
        let result = dispatch(cli.command).expect("dispatch docs query");
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
        dispatch(open.command).expect("open vault");
        let reindex = Cli::parse_from([
            "tao",
            "vault",
            "reindex",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(reindex.command).expect("reindex vault");

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
            "title starts_with 'A'",
            "--select",
            "path",
            "--limit",
            "10",
            "--offset",
            "0",
        ]);
        let result = dispatch(cli.command).expect("dispatch docs query");
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
fn query_docs_where_only_does_not_require_text_query() {
    with_temp_cwd(|| {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let vault_root = tempdir.path().join("vault");
        fs::create_dir_all(vault_root.join("notes/projects")).expect("create notes");
        fs::write(
            vault_root.join("notes/projects/alpha.md"),
            "# Alpha\nroadmap",
        )
        .expect("write alpha");
        fs::write(vault_root.join("notes/projects/beta.md"), "# Beta\nroadmap")
            .expect("write beta");

        open_and_reindex_fixture(&vault_root);

        let cli = Cli::parse_from([
            "tao",
            "query",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
            "--from",
            "docs",
            "--where",
            "title == 'Alpha'",
            "--select",
            "path,title",
            "--limit",
            "10",
            "--offset",
            "0",
        ]);
        let result = dispatch(cli.command).expect("dispatch docs query");
        let output = render_output(cli.json, &result).expect("render output");
        let envelope: JsonValue = serde_json::from_str(&output).expect("parse output");
        let rows = envelope
            .get("data")
            .and_then(|args| args.get("rows"))
            .and_then(JsonValue::as_array)
            .expect("rows array");
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].get("path").and_then(JsonValue::as_str),
            Some("notes/projects/alpha.md")
        );
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
        dispatch(open.command).expect("open vault");
        let reindex = Cli::parse_from([
            "tao",
            "vault",
            "reindex",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(reindex.command).expect("reindex vault");

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
        let result = dispatch(cli.command).expect("dispatch docs query");
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
        dispatch(open.command).expect("open vault");
        let reindex = Cli::parse_from([
            "tao",
            "vault",
            "reindex",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(reindex.command).expect("reindex vault");

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
            "title starts_with 'A' or title starts_with 'B'",
            "--sort",
            "title:desc,path:asc",
            "--select",
            "path,title",
            "--limit",
            "10",
            "--offset",
            "0",
        ]);
        let result = dispatch(cli.command).expect("dispatch docs query");
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
            "Beta"
        );
        assert_eq!(
            rows[1]
                .get("title")
                .and_then(JsonValue::as_str)
                .unwrap_or_default(),
            "Alpha"
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
        dispatch(open.command).expect("open vault");
        let reindex = Cli::parse_from([
            "tao",
            "vault",
            "reindex",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(reindex.command).expect("reindex vault");

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
            "title starts_with 'A' or title starts_with 'B'",
            "--sort",
            "title:desc,path:asc",
            "--select",
            "path",
            "--limit",
            "10",
            "--offset",
            "0",
        ]);
        let result = dispatch(cli.command).expect("dispatch docs query");
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
        dispatch(open.command).expect("open vault");
        let reindex = Cli::parse_from([
            "tao",
            "vault",
            "reindex",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(reindex.command).expect("reindex vault");

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
        let result = dispatch(cli.command).expect("dispatch docs query");
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
        dispatch(open.command).expect("open vault");
        let reindex = Cli::parse_from([
            "tao",
            "vault",
            "reindex",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(reindex.command).expect("reindex vault");

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
        let result = dispatch(cli.command).expect("dispatch docs query");
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
        dispatch(open.command).expect("open vault");
        let reindex = Cli::parse_from([
            "tao",
            "vault",
            "reindex",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(reindex.command).expect("reindex vault");

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
        let result = dispatch(cli.command).expect("dispatch explain query");
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
        dispatch(open.command).expect("open vault");
        let reindex = Cli::parse_from([
            "tao",
            "vault",
            "reindex",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(reindex.command).expect("reindex vault");

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
        let result = dispatch(cli.command).expect("dispatch base query");
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
fn query_base_text_query_filters_base_row_values() {
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
      - client
"#,
        )
        .expect("write base");
        fs::write(
            vault_root.join("notes/projects/a.md"),
            r#"---
status: active
client: Northstar Transit Lab
---
# A
"#,
        )
        .expect("write a");
        fs::write(
            vault_root.join("notes/projects/b.md"),
            r#"---
status: active
client: Harbor Grid Studio
---
# B
"#,
        )
        .expect("write b");

        open_and_reindex_fixture(&vault_root);

        let cli = Cli::parse_from([
            "tao",
            "query",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
            "--from",
            "base:views/projects.base",
            "--view-name",
            "AllProjects",
            "--query",
            "northstar",
            "--limit",
            "10",
            "--offset",
            "0",
        ]);
        let result = dispatch(cli.command).expect("dispatch base query");
        let output = render_output(cli.json, &result).expect("render output");
        let envelope: JsonValue = serde_json::from_str(&output).expect("parse output");
        let rows = envelope
            .get("data")
            .and_then(|args| args.get("rows"))
            .and_then(JsonValue::as_array)
            .expect("rows array");
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].get("file_path").and_then(JsonValue::as_str),
            Some("notes/projects/a.md")
        );
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
        dispatch(open.command).expect("open vault");
        let reindex = Cli::parse_from([
            "tao",
            "vault",
            "reindex",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(reindex.command).expect("reindex vault");

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
        let result = dispatch(cli.command).expect("dispatch base query");
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
        dispatch(open.command).expect("open vault");
        let reindex = Cli::parse_from([
            "tao",
            "vault",
            "reindex",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(reindex.command).expect("reindex vault");

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
                    "title starts_with 'A' or title starts_with 'B'",
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
            let result = dispatch(cli.command).expect("dispatch matrix query case");
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
        dispatch(open.command).expect("open vault");
        let reindex = Cli::parse_from([
            "tao",
            "vault",
            "reindex",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(reindex.command).expect("reindex vault");

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
        let result = dispatch(cli.command).expect("dispatch graph query");
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
        dispatch(open.command).expect("open vault");
        let reindex = Cli::parse_from([
            "tao",
            "vault",
            "reindex",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(reindex.command).expect("reindex vault");

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
fn json_stream_and_toon_are_conflicting_output_modes() {
    let result = run_from_args(
        [
            "tao",
            "query",
            "--json-stream",
            "--toon",
            "--vault-root",
            "/tmp",
            "--from",
            "docs",
        ]
        .into_iter()
        .map(std::ffi::OsString::from)
        .collect(),
    );

    assert_usage_error(result, ClapErrorKind::ArgumentConflict);
}
