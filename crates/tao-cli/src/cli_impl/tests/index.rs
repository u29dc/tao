use super::*;

#[test]
fn vault_reindex_dry_run_reports_missing_search_corpus_without_writing() {
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
        let data = envelope.get("data").expect("data");
        assert_eq!(
            data.get("search_index_stale").and_then(JsonValue::as_bool),
            Some(true)
        );
        assert_eq!(
            data.get("would_rebuild_search_index")
                .and_then(JsonValue::as_bool),
            Some(true)
        );
        assert_eq!(
            data.get("search_segments_rebuilt")
                .and_then(JsonValue::as_bool),
            Some(false)
        );
        assert_eq!(
            data.get("search_corpus_refresh")
                .and_then(JsonValue::as_str),
            Some("none")
        );
        assert_eq!(
            data.get("scan_mode").and_then(JsonValue::as_str),
            Some("content_hash")
        );

        let segments_after_dry_run: u64 = connection
            .query_row("SELECT COUNT(*) FROM search_segments", [], |row| row.get(0))
            .expect("count segments after dry-run");
        assert_eq!(segments_after_dry_run, 0);
    });
}

#[test]
fn vault_reindex_reports_partial_search_corpus_refresh_for_note_edit() {
    with_temp_cwd(|| {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let vault_root = tempdir.path().join("vault");
        seed_search_fixture(&vault_root);
        open_and_reindex_fixture(&vault_root);

        let contact_path = vault_root.join("WORK/013-RELATIONS/013-CON-contacts/jordan_hart.md");
        let mut contact = fs::read_to_string(&contact_path).expect("read contact");
        contact.push_str("\nUnique partial refresh token: heliotrope-reindex-check\n");
        fs::write(&contact_path, contact).expect("update contact");

        let reindex = Cli::parse_from([
            "tao",
            "vault",
            "reindex",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        let output = render_output(
            reindex.json,
            &dispatch(reindex.command).expect("dispatch reindex"),
        )
        .expect("render reindex");
        let envelope: JsonValue = serde_json::from_str(&output).expect("parse reindex");
        let data = envelope.get("data").expect("data");
        assert_eq!(
            data.get("search_corpus_refresh")
                .and_then(JsonValue::as_str),
            Some("partial")
        );
        assert_eq!(
            data.get("search_segments_rebuilt")
                .and_then(JsonValue::as_bool),
            Some(true)
        );

        let search = Cli::parse_from([
            "tao",
            "search",
            "heliotrope-reindex-check",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
            "--limit",
            "5",
        ]);
        let output = render_output(
            search.json,
            &dispatch(search.command).expect("dispatch search"),
        )
        .expect("render search");
        let envelope: JsonValue = serde_json::from_str(&output).expect("parse search");
        let candidates = envelope
            .get("data")
            .and_then(|data| data.get("candidates"))
            .and_then(JsonValue::as_array)
            .expect("candidates");
        assert!(candidates.iter().any(|candidate| {
            candidate.get("path").and_then(JsonValue::as_str)
                == Some("WORK/013-RELATIONS/013-CON-contacts/jordan_hart.md")
        }));
    });
}

#[test]
fn deep_health_reports_index_lag_from_reconciliation_drift() {
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
        dispatch(open.command).expect("open vault");
        let reindex = Cli::parse_from([
            "tao",
            "vault",
            "reindex",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        dispatch(reindex.command).expect("reindex vault");

        fs::write(vault_root.join("notes/b.md"), "# B").expect("write drifted file");

        let health = Cli::parse_from([
            "tao",
            "health",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
            "--deep",
        ]);
        let health_output = render_output(
            health.json,
            &dispatch(health.command).expect("dispatch health"),
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
                .and_then(|data| data.get("stats"))
                .and_then(|stats| stats.get("scan_mode"))
                .and_then(JsonValue::as_str),
            Some("deep_metadata")
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
            "health",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        let stats_output = render_output(
            stats.json,
            &dispatch(stats.command).expect("dispatch stats"),
        )
        .expect("render stats");
        let stats_payload: JsonValue = serde_json::from_str(&stats_output).expect("parse stats");

        assert_eq!(
            stats_payload
                .get("data")
                .and_then(|data| data.get("stats"))
                .and_then(|stats| stats.get("index_lag"))
                .and_then(JsonValue::as_u64),
            Some(0)
        );
        assert_eq!(
            stats_payload
                .get("data")
                .and_then(|data| data.get("stats"))
                .and_then(|stats| stats.get("scan_mode"))
                .and_then(JsonValue::as_str),
            Some("cached")
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
fn health_reports_stale_link_resolution_version() {
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
        let open_output = render_output(open.json, &dispatch(open.command).expect("dispatch open"))
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
        dispatch(reindex.command).expect("reindex vault");

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
            &dispatch(health.command).expect("dispatch health"),
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
                .and_then(|data| data.get("stats"))
                .and_then(|stats| stats.get("scan_mode"))
                .and_then(JsonValue::as_str),
            Some("cached")
        );
        assert_eq!(
            health_payload
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
        let open_output = render_output(open.json, &dispatch(open.command).expect("dispatch open"))
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
        dispatch(reindex.command.clone()).expect("initial reindex");

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
            &dispatch(reindex.command).expect("dispatch reindex"),
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
            "links",
            "--direction",
            "outgoing",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
            "--path",
            "WORK/13-RELATIONS/Contents/post.md",
        ]);
        let outgoing_output = render_output(
            outgoing.json,
            &dispatch(outgoing.command).expect("dispatch outgoing"),
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
            items[0].get("resolved_path").and_then(JsonValue::as_str),
            Some("WORK/13-RELATIONS/Contents/Media/foo.jpg")
        );
        assert_eq!(
            items[0].get("is_unresolved").and_then(JsonValue::as_bool),
            Some(false)
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
        let open_output = render_output(open.json, &dispatch(open.command).expect("dispatch open"))
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
        dispatch(reindex.command.clone()).expect("initial reindex");

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
            "--deep",
        ]);
        let health_output = render_output(
            health.json,
            &dispatch(health.command).expect("dispatch health"),
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
        assert_eq!(
            health_payload
                .get("data")
                .and_then(|data| data.get("stats"))
                .and_then(|stats| stats.get("scan_mode"))
                .and_then(JsonValue::as_str),
            Some("deep_metadata")
        );

        let reindex_output = render_output(
            reindex.json,
            &dispatch(reindex.command).expect("dispatch reindex"),
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
