use super::*;

#[test]
fn search_context_ranks_canonical_entity_note_first() {
    with_temp_cwd(|| {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let vault_root = tempdir.path().join("vault");
        seed_search_fixture(&vault_root);
        open_and_reindex_fixture(&vault_root);

        let cli = Cli::parse_from([
            "tao",
            "search",
            "jordan hart",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
            "--context",
            "--depth",
            "2",
            "--limit",
            "10",
        ]);
        let output = render_output(cli.json, &dispatch(cli.command).expect("dispatch search"))
            .expect("render search");
        let envelope: JsonValue = serde_json::from_str(&output).expect("parse search");
        let data = envelope.get("data").expect("data");
        let first_candidate = data
            .get("candidates")
            .and_then(JsonValue::as_array)
            .and_then(|items| items.first())
            .expect("first candidate");
        assert_eq!(
            first_candidate.get("path").and_then(JsonValue::as_str),
            Some("WORK/013-RELATIONS/013-CON-contacts/jordan_hart.md")
        );
        assert_eq!(
            data.get("context")
                .and_then(|context| context.get("root"))
                .and_then(|root| root.get("path"))
                .and_then(JsonValue::as_str),
            Some("WORK/013-RELATIONS/013-CON-contacts/jordan_hart.md")
        );
    });
}

#[test]
fn search_rebuilds_missing_unified_corpus_before_querying() {
    with_temp_cwd(|| {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let vault_root = tempdir.path().join("vault");
        seed_search_fixture(&vault_root);
        open_and_reindex_fixture(&vault_root);

        let db_path = vault_root.join(".tao/index.sqlite");
        let connection = Connection::open(&db_path).expect("open fixture db");
        connection
            .execute("DELETE FROM search_aliases", [])
            .expect("clear aliases");
        connection
            .execute("DELETE FROM search_segments", [])
            .expect("clear search segments");

        let cli = Cli::parse_from([
            "tao",
            "search",
            "jordan hart",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
            "--limit",
            "10",
        ]);
        let output = render_output(cli.json, &dispatch(cli.command).expect("dispatch search"))
            .expect("render search");
        let envelope: JsonValue = serde_json::from_str(&output).expect("parse search");
        let first_candidate = envelope
            .get("data")
            .and_then(|data| data.get("candidates"))
            .and_then(JsonValue::as_array)
            .and_then(|items| items.first())
            .expect("first candidate");
        assert_eq!(
            first_candidate.get("path").and_then(JsonValue::as_str),
            Some("WORK/013-RELATIONS/013-CON-contacts/jordan_hart.md")
        );

        let rebuilt_segments: u64 = connection
            .query_row("SELECT COUNT(*) FROM search_segments", [], |row| row.get(0))
            .expect("count rebuilt segments");
        let rebuilt_aliases: u64 = connection
            .query_row("SELECT COUNT(*) FROM search_aliases", [], |row| row.get(0))
            .expect("count rebuilt aliases");
        assert!(rebuilt_segments > 0);
        assert!(rebuilt_aliases > 0);
    });
}

#[test]
fn search_rebuilds_corpus_when_aliases_are_missing_but_segments_remain() {
    with_temp_cwd(|| {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let vault_root = tempdir.path().join("vault");
        seed_search_fixture(&vault_root);
        open_and_reindex_fixture(&vault_root);

        let db_path = vault_root.join(".tao/index.sqlite");
        let connection = Connection::open(&db_path).expect("open fixture db");
        let segments_before: u64 = connection
            .query_row("SELECT COUNT(*) FROM search_segments", [], |row| row.get(0))
            .expect("count segments before");
        assert!(segments_before > 0);
        connection
            .execute("DELETE FROM search_aliases", [])
            .expect("clear aliases");

        let dry_run = Cli::parse_from([
            "tao",
            "vault",
            "reindex",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
            "--dry-run",
        ]);
        let output = render_output(
            dry_run.json,
            &dispatch(dry_run.command).expect("dispatch dry-run reindex"),
        )
        .expect("render dry-run");
        let envelope: JsonValue = serde_json::from_str(&output).expect("parse dry-run");
        assert_eq!(
            envelope
                .get("data")
                .and_then(|data| data.get("search_index_stale"))
                .and_then(JsonValue::as_bool),
            Some(true)
        );

        let cli = Cli::parse_from([
            "tao",
            "search",
            "jordan hart",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
            "--limit",
            "10",
        ]);
        let output = render_output(cli.json, &dispatch(cli.command).expect("dispatch search"))
            .expect("render search");
        let envelope: JsonValue = serde_json::from_str(&output).expect("parse search");
        let first_candidate = envelope
            .get("data")
            .and_then(|data| data.get("candidates"))
            .and_then(JsonValue::as_array)
            .and_then(|items| items.first())
            .expect("first candidate");
        assert_eq!(
            first_candidate.get("path").and_then(JsonValue::as_str),
            Some("WORK/013-RELATIONS/013-CON-contacts/jordan_hart.md")
        );

        let rebuilt_aliases: u64 = connection
            .query_row("SELECT COUNT(*) FROM search_aliases", [], |row| row.get(0))
            .expect("count rebuilt aliases");
        assert!(rebuilt_aliases > 0);
    });
}

#[test]
fn search_files_kind_finds_non_markdown_inventory_matches() {
    with_temp_cwd(|| {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let vault_root = tempdir.path().join("vault");
        seed_search_fixture(&vault_root);
        open_and_reindex_fixture(&vault_root);

        let cli = Cli::parse_from([
            "tao",
            "search",
            "invoice",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
            "--kind",
            "files",
            "--scope",
            "WORK/012-FINANCE",
            "--limit",
            "10",
        ]);
        let output = render_output(
            cli.json,
            &dispatch(cli.command).expect("dispatch file search"),
        )
        .expect("render search");
        let envelope: JsonValue = serde_json::from_str(&output).expect("parse search");
        let files = envelope
            .get("data")
            .and_then(|data| data.get("files"))
            .and_then(JsonValue::as_array)
            .expect("files");
        let invoice = files
            .iter()
            .find(|file| {
                file.get("path")
                    .and_then(JsonValue::as_str)
                    .is_some_and(|path| path.ends_with("2026-02-15-invoice-jordan-hart.pdf"))
            })
            .expect("invoice pdf match");
        assert_eq!(
            invoice.get("extension").and_then(JsonValue::as_str),
            Some("pdf")
        );
        assert_eq!(
            invoice.get("is_markdown").and_then(JsonValue::as_bool),
            Some(false)
        );
    });
}

#[test]
fn search_path_context_returns_links_walk_timeline_and_attachments() {
    with_temp_cwd(|| {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let vault_root = tempdir.path().join("vault");
        seed_search_fixture(&vault_root);
        open_and_reindex_fixture(&vault_root);

        let cli = Cli::parse_from([
            "tao",
            "search",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
            "--path",
            "WORK/013-RELATIONS/013-CON-contacts/jordan_hart.md",
            "--context",
            "--depth",
            "2",
            "--limit",
            "10",
        ]);
        let output = render_output(
            cli.json,
            &dispatch(cli.command).expect("dispatch path context"),
        )
        .expect("render search");
        let envelope: JsonValue = serde_json::from_str(&output).expect("parse search");
        let context = envelope
            .get("data")
            .and_then(|data| data.get("context"))
            .expect("context");
        assert!(
            context
                .get("links")
                .and_then(|links| links.get("outgoing"))
                .and_then(JsonValue::as_array)
                .is_some_and(|rows| !rows.is_empty())
        );
        assert!(
            context
                .get("links")
                .and_then(|links| links.get("incoming"))
                .and_then(JsonValue::as_array)
                .is_some_and(|rows| !rows.is_empty())
        );
        assert!(
            context
                .get("walk")
                .and_then(JsonValue::as_array)
                .is_some_and(|rows| !rows.is_empty())
        );
        assert!(
            context
                .get("timeline")
                .and_then(JsonValue::as_array)
                .is_some_and(|rows| !rows.is_empty())
        );
        assert!(
            context
                .get("attachments")
                .and_then(JsonValue::as_array)
                .is_some_and(|rows| !rows.is_empty())
        );
        let base_rows = context
            .get("base_rows")
            .and_then(JsonValue::as_array)
            .expect("base rows");
        assert!(
            base_rows.iter().any(|row| {
                row.get("path").and_then(JsonValue::as_str)
                    == Some("WORK/013-RELATIONS/013-CON-contacts/jordan_hart.md")
            }),
            "path context should hydrate base rows for the selected note"
        );
    });
}

#[test]
fn search_bases_scans_beyond_first_base_page() {
    with_temp_cwd(|| {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let vault_root = tempdir.path().join("vault");
        fs::create_dir_all(vault_root.join("notes/projects")).expect("create projects");
        fs::create_dir_all(vault_root.join("views")).expect("create views");
        fs::write(
            vault_root.join("views/projects.base"),
            r#"
views:
  - name: Projects
    type: table
    source: notes/projects
    columns:
      - title
      - marker
"#,
        )
        .expect("write base");
        for index in 0..75_u32 {
            fs::write(
                vault_root.join(format!("notes/projects/a-{index:03}.md")),
                format!("---\nmarker: ordinary-{index}\n---\n# A {index}\n"),
            )
            .expect("write ordinary project");
        }
        fs::write(
            vault_root.join("notes/projects/zz-target.md"),
            "---\nmarker: after-page-one\n---\n# Target\n",
        )
        .expect("write target project");
        open_and_reindex_fixture(&vault_root);

        let cli = Cli::parse_from([
            "tao",
            "search",
            "after-page-one",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
            "--kind",
            "bases",
            "--limit",
            "10",
        ]);
        let output = render_output(cli.json, &dispatch(cli.command).expect("dispatch search"))
            .expect("render search");
        let envelope: JsonValue = serde_json::from_str(&output).expect("parse search");
        let surface_rows = envelope
            .get("data")
            .and_then(|data| data.get("candidates"))
            .and_then(JsonValue::as_array)
            .expect("candidates");
        assert!(surface_rows.iter().any(|row| {
            row.get("path").and_then(JsonValue::as_str) == Some("notes/projects/zz-target.md")
        }));
    });
}

#[test]
fn search_include_content_handles_unicode_excerpt_boundaries() {
    with_temp_cwd(|| {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let vault_root = tempdir.path().join("vault");
        fs::create_dir_all(vault_root.join("notes")).expect("create notes");
        let prefix = format!("é{}", "a".repeat(119));
        fs::write(
            vault_root.join("notes/unicode.md"),
            format!("{prefix} needle appears after unicode boundary"),
        )
        .expect("write unicode note");
        open_and_reindex_fixture(&vault_root);

        let cli = Cli::parse_from([
            "tao",
            "search",
            "needle",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
            "--include-content",
            "--limit",
            "10",
        ]);
        let output = render_output(cli.json, &dispatch(cli.command).expect("dispatch search"))
            .expect("render search");
        let envelope: JsonValue = serde_json::from_str(&output).expect("parse search");
        let docs = envelope
            .get("data")
            .and_then(|data| data.get("docs"))
            .and_then(JsonValue::as_array)
            .expect("docs");
        assert!(docs.iter().any(|doc| {
            doc.get("path").and_then(JsonValue::as_str) == Some("notes/unicode.md")
                && doc
                    .get("excerpt")
                    .and_then(JsonValue::as_str)
                    .is_some_and(|excerpt| excerpt.contains("needle"))
        }));
    });
}
