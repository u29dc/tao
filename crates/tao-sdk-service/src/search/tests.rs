use super::*;
use std::path::PathBuf;
use tao_sdk_storage::{
    DocumentRecordInput, DocumentsRepository, FileRecordInput, FilesRepository,
    PropertiesRepository, PropertyRecordInput, TaskRecordInput, TasksRepository, run_migrations,
};

fn database() -> Connection {
    let mut c = Connection::open_in_memory().unwrap();
    run_migrations(&mut c).unwrap();
    c
}
fn doc(c: &Connection, path: &str, raw: &str, body: &str) {
    FilesRepository::insert(
        c,
        &FileRecordInput {
            file_id: path.into(),
            normalized_path: path.into(),
            match_key: path.into(),
            absolute_path: format!("/unavailable/{path}"),
            size_bytes: raw.len() as u64,
            modified_unix_ms: 1,
            hash_blake3: "revision".into(),
            is_markdown: true,
        },
    )
    .unwrap();
    DocumentsRepository::upsert(
        c,
        &DocumentRecordInput {
            file_id: path.into(),
            source_hash: "revision".into(),
            parser_version: 1,
            raw_text: raw.into(),
            body_text: body.into(),
            title: note_title_from_path(path),
            structure_json: "{}".into(),
        },
    )
    .unwrap();
}
fn request(query: Option<&str>, path: Option<&str>, limit: u32) -> VaultSearchRequest {
    VaultSearchRequest {
        vault_root: PathBuf::from("/unavailable"),
        query: query.map(str::to_string),
        path: path.map(str::to_string),
        kind: SearchKind::Auto,
        scope: None,
        extensions: Vec::new(),
        include_context: false,
        depth: 2,
        limit,
        include_content: true,
        include_pii: false,
    }
}
#[test]
fn unicode_dates_are_safe_and_calendar_valid() {
    assert_eq!(infer_date("über-notebook.md"), None);
    assert_eq!(infer_date("中文/2024_02_29.md"), Some("2024-02-29".into()));
    for invalid in [
        "2023-02-29",
        "2024-99-01",
        "2024-01-00",
        "2024-01_01",
        "0000-01-01",
    ] {
        assert_eq!(infer_date(invalid), None);
    }
}
#[test]
fn canonical_context_and_excerpts_never_reopen_the_original_or_leak_suppressed_metadata() {
    let mut c = database();
    doc(
        &c,
        "über-notebook.md",
        "---\nprivate: secretvalue\n---\npublic text",
        "public text",
    );
    PropertiesRepository::upsert(
        &c,
        &PropertyRecordInput {
            property_id: "p".into(),
            file_id: "über-notebook.md".into(),
            key: "private".into(),
            value_type: "string".into(),
            value_json: "\"secretvalue\"".into(),
        },
    )
    .unwrap();
    crate::SearchCorpusService
        .rebuild_atomic(&mut c, CasePolicy::Sensitive)
        .unwrap();
    let out = VaultSearchService
        .search(
            &mut c,
            request(Some("secretvalue"), None, 20),
            CasePolicy::Sensitive,
        )
        .unwrap();
    assert_eq!(out.total, 1);
    assert!(out.docs[0].excerpt.is_none());
    assert_eq!(out.properties[0].value, "redacted");
    let context = VaultSearchService
        .search(
            &mut c,
            request(None, Some("über-notebook.md"), 20),
            CasePolicy::Sensitive,
        )
        .unwrap()
        .context;
    let root = context.root.unwrap();
    assert_eq!(root.body_excerpt, Some("public text".into()));
    assert_eq!(root.front_matter, Some("redacted".into()));
    assert_eq!(
        redact_base_values(
            JsonMap::from_iter([("private".into(), json!("secretvalue"))]),
            false
        )["private"],
        "redacted"
    );
}
#[test]
fn sections_do_not_starve_and_scope_counts_match_rows() {
    let mut c = database();
    for index in 0..220 {
        doc(&c, &format!("notes/item-{index:03}.md"), "common", "common");
    }
    TasksRepository::upsert(
        &c,
        &TaskRecordInput {
            task_id: "task".into(),
            file_id: "notes/item-000.md".into(),
            file_path: "notes/item-000.md".into(),
            file_path_lc: "notes/item-000.md".into(),
            line_number: 1,
            state: "open".into(),
            text: format!("common {}", "noise ".repeat(10_000)),
            text_lc: format!("common {}", "noise ".repeat(10_000)),
        },
    )
    .unwrap();
    crate::SearchCorpusService
        .rebuild_atomic(&mut c, CasePolicy::Sensitive)
        .unwrap();
    let small = VaultSearchService
        .search(
            &mut c,
            request(Some("common"), None, 1),
            CasePolicy::Sensitive,
        )
        .unwrap();
    let large = VaultSearchService
        .search(
            &mut c,
            request(Some("common"), None, 20),
            CasePolicy::Sensitive,
        )
        .unwrap();
    assert_eq!(small.total, 220);
    assert_eq!(small.tasks.len(), 1);
    assert_eq!(small.candidates[0].path, large.candidates[0].path);
    assert!(small.content_truncated);
    assert!(small.tasks[0].text.len() <= 4096);
    let mut req = request(Some("common"), None, 20);
    req.scope = Some("NOTES".into());
    let scoped = VaultSearchService
        .search(&mut c, req, CasePolicy::Sensitive)
        .unwrap();
    assert_eq!(scoped.total, 0);
    assert!(scoped.candidates.is_empty());
}
#[test]
fn partial_derived_loss_is_detected_and_repaired_from_canonical_revisions() {
    let mut c = database();
    doc(&c, "a.md", "alpha", "alpha");
    doc(&c, "b.md", "beta", "beta");
    crate::SearchCorpusService
        .rebuild_atomic(&mut c, CasePolicy::Sensitive)
        .unwrap();
    c.execute(
        "DELETE FROM search_segments WHERE file_id='a.md' AND surface='docs'",
        [],
    )
    .unwrap();
    assert!(
        crate::SearchCorpusService
            .status(&c)
            .unwrap()
            .search_index_stale
    );
    let result = VaultSearchService
        .search(
            &mut c,
            request(Some("alpha"), None, 20),
            CasePolicy::Sensitive,
        )
        .unwrap();
    assert_eq!(result.docs.len(), 1);
    assert!(
        !crate::SearchCorpusService
            .status(&c)
            .unwrap()
            .search_index_stale
    );
}

#[test]
fn token_prefix_semantics_do_not_invent_mid_identifier_matches() {
    let mut c = database();
    let identifier = format!("{}uniquesuffix", "prefix".repeat(20));
    doc(&c, "identifier.md", &identifier, &identifier);
    crate::SearchCorpusService
        .rebuild_atomic(&mut c, CasePolicy::Sensitive)
        .unwrap();
    let prefix = VaultSearchService
        .search(
            &mut c,
            request(Some("prefixprefix"), None, 20),
            CasePolicy::Sensitive,
        )
        .unwrap();
    assert_eq!(prefix.total, 1);
    let suffix = VaultSearchService
        .search(
            &mut c,
            request(Some("uniquesuffix"), None, 20),
            CasePolicy::Sensitive,
        )
        .unwrap();
    assert_eq!(suffix.total, 0);
    for (limit, depth) in [(0, 1), (101, 1), (1, 5)] {
        let mut req = request(Some("prefix"), None, limit);
        req.depth = depth;
        assert!(matches!(
            VaultSearchService.search_current(&c, req),
            Err(VaultSearchError::InvalidRequest(_))
        ));
    }
}

#[test]
fn literal_scope_and_insensitive_root_resolution_share_canonical_paths() {
    let mut c = database();
    for path in ["Notes_%/Über.md", "NotesXX/Other.md"] {
        doc(&c, path, "common", "common");
    }
    crate::SearchCorpusService
        .rebuild_atomic(&mut c, CasePolicy::Sensitive)
        .unwrap();
    let mut req = request(Some("common"), None, 20);
    req.scope = Some("Notes_%".into());
    let result = VaultSearchService.search_current(&c, req).unwrap();
    assert_eq!(result.total, 1);
    assert_eq!(result.docs[0].path, "Notes_%/Über.md");
    c.execute(
        "UPDATE files SET match_key='notes_%/über.md' WHERE normalized_path='Notes_%/Über.md'",
        [],
    )
    .unwrap();
    tao_sdk_storage::IndexStateRepository::upsert(
        &c,
        &tao_sdk_storage::IndexStateRecordInput {
            key: "index_case_policy".into(),
            value_json: "insensitive".into(),
        },
    )
    .unwrap();
    crate::SearchCorpusService
        .rebuild_atomic(&mut c, CasePolicy::Insensitive)
        .unwrap();
    let mut req = request(Some("common"), None, 20);
    req.scope = Some("notes_%".into());
    let result = VaultSearchService.search_current(&c, req).unwrap();
    assert_eq!(result.total, 1);
    let result = VaultSearchService
        .search_current(&c, request(None, Some("NOTES_%/ÜBER.md"), 20))
        .unwrap();
    assert_eq!(result.context.root.unwrap().path, "Notes_%/Über.md");
}

#[test]
fn graph_corpus_follows_source_revisions_and_redacts_metadata_tokens() {
    let mut c = database();
    doc(&c, "source.md", "source", "source");
    doc(&c, "target.md", "target", "target");
    tao_sdk_storage::LinksRepository::insert(
        &c,
        &tao_sdk_storage::LinkRecordInput {
            link_id: "l".into(),
            source_file_id: "source.md".into(),
            raw_target: "privatecode".into(),
            resolved_file_id: Some("target.md".into()),
            heading_slug: None,
            block_id: None,
            is_unresolved: false,
            unresolved_reason: None,
            source_field: "frontmatter:private".into(),
        },
    )
    .unwrap();
    crate::SearchCorpusService
        .rebuild_atomic(&mut c, CasePolicy::Sensitive)
        .unwrap();
    let result = VaultSearchService
        .search_current(&c, request(Some("privatecode"), None, 20))
        .unwrap();
    assert_eq!(result.graph[0].raw_target, "redacted");
    let result = VaultSearchService
        .search_current(&c, request(None, Some("source.md"), 20))
        .unwrap();
    assert_eq!(result.context.links.outgoing[0].raw_target, "redacted");
    assert_eq!(result.context.walk[0].raw_target, "redacted");
    c.execute("DELETE FROM links WHERE link_id='l'", [])
        .unwrap();
    crate::SearchCorpusService
        .refresh_files_atomic(&mut c, &["source.md".into()], CasePolicy::Sensitive)
        .unwrap();
    let result = VaultSearchService
        .search_current(&c, request(Some("privatecode"), None, 20))
        .unwrap();
    assert_eq!(result.total, 0);
    assert!(result.graph.is_empty());
}

#[test]
fn extracted_document_results_preserve_locator_served_revision_and_coverage() {
    let mut c = database();
    c.execute("INSERT INTO files(file_id,normalized_path,match_key,absolute_path,size_bytes,modified_unix_ms,hash_blake3,is_markdown) VALUES('pdf','paper.pdf','paper.pdf','/unavailable/paper.pdf',1,1,'new',0)",[]).unwrap();
    c.execute("INSERT INTO content_documents(file_id,format,file_group,observed_size,observed_modified_ms,desired_revision,served_revision,served_extractor_identity,extractor_identity,coverage) VALUES('pdf','pdf','document',1,1,'new','old','v1','v1','partial')",[]).unwrap();
    c.execute("INSERT INTO content_segments(file_id,ordinal,locator_kind,source_start,source_end,text,method,coverage) VALUES('pdf',1,'page',7,7,'quartz needle source text','embedded','complete')",[]).unwrap();
    crate::SearchCorpusService
        .rebuild_atomic(&mut c, CasePolicy::Sensitive)
        .unwrap();
    let result = VaultSearchService
        .search_current(&c, request(Some("quartz"), None, 20))
        .unwrap();
    assert_eq!(result.docs.len(), 1);
    assert_eq!(result.docs[0].path, "paper.pdf");
    assert_eq!(result.docs[0].locator.as_ref().unwrap()["start"], 7);
    assert_eq!(result.docs[0].revision.as_deref(), Some("old"));
    assert!(result.docs[0].stale);
    assert!(
        tao_sdk_storage::SearchSegmentRepository::list_docs_page(&c, 20, 0)
            .unwrap()
            .is_empty()
    );
    let result = VaultSearchService
        .search_current(&c, request(None, Some("paper.pdf"), 20))
        .unwrap();
    let root = result.context.root.unwrap();
    assert_eq!(root.coverage.as_deref(), Some("partial"));
    assert_eq!(root.revision.as_deref(), Some("old"));
    assert!(root.stale);
    assert!(root.body_excerpt.unwrap().contains("quartz"));
}

#[test]
fn missing_derived_objects_rebuild_from_canonical_without_live_sources() {
    for object in ["search_segments_fts", "search_segments", "search_aliases"] {
        let mut c = database();
        doc(&c, "a.md", "reconstructable", "reconstructable");
        crate::SearchCorpusService
            .rebuild_atomic(&mut c, CasePolicy::Sensitive)
            .unwrap();
        c.execute(&format!("DROP TABLE {object}"), []).unwrap();
        assert!(
            crate::SearchCorpusService
                .status(&c)
                .unwrap()
                .search_index_stale
        );
        let result = VaultSearchService
            .search(
                &mut c,
                request(Some("reconstructable"), None, 20),
                CasePolicy::Sensitive,
            )
            .unwrap();
        assert_eq!(result.docs.len(), 1, "{object}");
        tao_sdk_storage::SearchSegmentRepository::check_integrity(&c).unwrap();
        assert!(
            !crate::SearchCorpusService
                .status(&c)
                .unwrap()
                .search_index_stale
        );
    }
}

#[test]
fn zero_matches_still_reports_pending_scoped_content_coverage() {
    let mut c = database();
    doc(&c, "notes/a.md", "ordinary", "ordinary");
    c.execute("INSERT INTO files(file_id,normalized_path,match_key,absolute_path,size_bytes,modified_unix_ms,hash_blake3,is_markdown) VALUES('pdf','assets/pending.pdf','assets/pending.pdf','/unavailable/pending.pdf',1,1,'revision',0)",[]).unwrap();
    crate::SearchCorpusService
        .rebuild_atomic(&mut c, CasePolicy::Sensitive)
        .unwrap();
    let result = VaultSearchService
        .search_current(&c, request(Some("unmatched"), None, 20))
        .unwrap();
    assert_eq!(result.total, 0);
    assert_eq!(result.content_coverage.pending_files, 1);
    assert!(!result.content_coverage.complete);
    let mut req = request(Some("unmatched"), None, 20);
    req.scope = Some("notes".into());
    let result = VaultSearchService.search_current(&c, req).unwrap();
    assert_eq!(result.content_coverage.total_files, 1);
    assert!(result.content_coverage.complete);
    let mut req = request(Some("unmatched"), None, 20);
    req.extensions = vec!["pdf".into()];
    let result = VaultSearchService.search_current(&c, req).unwrap();
    assert_eq!(result.content_coverage.total_files, 1);
    assert_eq!(result.content_coverage.markdown_files, 0);
}

#[test]
fn unrelated_partial_refresh_cannot_publish_over_derived_damage() {
    let mut c = database();
    doc(&c, "a.md", "alpha", "alpha");
    doc(&c, "b.md", "beta", "beta");
    crate::SearchCorpusService
        .rebuild_atomic(&mut c, CasePolicy::Sensitive)
        .unwrap();
    c.execute(
        "DELETE FROM search_segments WHERE normalized_path='a.md' AND surface='docs'",
        [],
    )
    .unwrap();
    assert!(
        crate::SearchCorpusService
            .status(&c)
            .unwrap()
            .search_index_stale
    );
    crate::SearchCorpusService
        .refresh_files_atomic(&mut c, &["b.md".into()], CasePolicy::Sensitive)
        .unwrap();
    assert!(
        !crate::SearchCorpusService
            .status(&c)
            .unwrap()
            .search_index_stale
    );
    let result = VaultSearchService
        .search_current(&c, request(Some("alpha"), None, 20))
        .unwrap();
    assert_eq!(result.docs.len(), 1);
}

#[test]
fn partial_refresh_repairs_both_owners_after_derived_owner_mutation() {
    for table in ["search_segments", "search_aliases"] {
        let mut c = database();
        doc(&c, "a.md", "alpha", "alpha");
        doc(&c, "b.md", "beta", "beta");
        crate::SearchCorpusService
            .rebuild_atomic(&mut c, CasePolicy::Sensitive)
            .unwrap();
        c.execute(
            &format!(
                "UPDATE {table} SET file_id='b.md' WHERE normalized_path='a.md' AND surface='docs'"
            ),
            [],
        )
        .unwrap();
        let dirty = tao_sdk_storage::IndexGenerationRepository::derived_dirty_files(&c).unwrap();
        assert!(dirty.contains(&"a.md".to_owned()), "{table}: {dirty:?}");
        assert!(dirty.contains(&"b.md".to_owned()), "{table}: {dirty:?}");
        crate::SearchCorpusService
            .refresh_files_atomic(&mut c, &["b.md".into()], CasePolicy::Sensitive)
            .unwrap();
        assert!(
            !crate::SearchCorpusService
                .status(&c)
                .unwrap()
                .search_index_stale
        );
        let result = VaultSearchService
            .search_current(&c, request(Some("alpha"), None, 20))
            .unwrap();
        assert_eq!(result.docs.len(), 1, "{table}");
        assert_eq!(result.docs[0].path, "a.md", "{table}");
        let incorrect_owners: i64 = c.query_row(
            &format!("SELECT COUNT(*) FROM {table} WHERE normalized_path='a.md' AND file_id != 'a.md'"),
            [],
            |row| row.get(0),
        ).unwrap();
        assert_eq!(incorrect_owners, 0, "{table}");
    }
}

#[test]
fn full_ranking_precedes_candidate_limits_and_keeps_title_recall() {
    let mut c = database();
    for index in 0..120 {
        doc(&c, &format!("a{index:03}.md"), "needle", "needle");
    }
    let body = format!("# Needle specification\n{}", "other ".repeat(15_000));
    doc(&c, "zzztarget.md", &body, &body);
    c.execute(
        "UPDATE canonical_documents SET title='Needle specification' WHERE file_id='zzztarget.md'",
        [],
    )
    .unwrap();
    crate::SearchCorpusService
        .rebuild_atomic(&mut c, CasePolicy::Sensitive)
        .unwrap();
    for limit in [1, 20, 100] {
        let result = VaultSearchService
            .search_current(&c, request(Some("needle"), None, limit))
            .unwrap();
        assert_eq!(result.candidates[0].path, "zzztarget.md", "limit={limit}");
        assert_eq!(result.docs[0].path, "zzztarget.md", "limit={limit}");
        assert_eq!(result.total, 121);
    }
}

#[test]
fn base_view_failure_quarantines_projection_and_recovers_without_losing_healthy_notes() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/qa-tmp");
    std::fs::create_dir_all(&root).unwrap();
    let temp = tempfile::tempdir_in(root).unwrap();
    let root = temp.path();
    std::fs::write(root.join("good.md"), "---\nrank: 1\n---\nhealthyneedle").unwrap();
    std::fs::write(root.join("mixed.md"), "---\nrank: later\n---\nother").unwrap();
    std::fs::write(root.join("mixed.base"),"views:\n - name: Test\n   columns: [path, rank]\n   filters: [{key: rank, op: gt, value: 0}]\n - name: Healthy\n   columns: [path]\n").unwrap();
    let mut c = database();
    crate::FullIndexService::default()
        .rebuild(root, &mut c, CasePolicy::Sensitive)
        .unwrap();
    let diagnostics = tao_sdk_storage::DiagnosticsRepository::list_all(&c).unwrap();
    assert!(diagnostics.iter().any(|d| d.path == "mixed.base"
        && d.kind == "base_view_failed"
        && d.message.contains("Test")));
    assert!(
        !crate::SearchCorpusService
            .status(&c)
            .unwrap()
            .search_index_stale
    );
    let result = VaultSearchService
        .search_current(&c, request(Some("healthyneedle"), None, 20))
        .unwrap();
    assert_eq!(result.docs.len(), 1);
    assert_eq!(result.docs[0].path, "good.md");
    assert_eq!(c.query_row("SELECT COUNT(*) FROM search_segments WHERE field='base_row' AND json_extract(payload_json,'$.view_name')='Test'",[],|r|r.get::<_,u64>(0)).unwrap(),0);
    assert!(c.query_row("SELECT COUNT(*) FROM search_segments WHERE field='base_row' AND json_extract(payload_json,'$.view_name')='Healthy'",[],|r|r.get::<_,u64>(0)).unwrap()>0);
    std::fs::write(root.join("mixed.md"), "---\nrank: 2\n---\nother").unwrap();
    crate::FullIndexService::default()
        .rebuild(root, &mut c, CasePolicy::Sensitive)
        .unwrap();
    assert!(
        !tao_sdk_storage::DiagnosticsRepository::list_all(&c)
            .unwrap()
            .iter()
            .any(|d| d.kind == "base_view_failed")
    );
    assert!(
        !crate::SearchCorpusService
            .status(&c)
            .unwrap()
            .search_index_stale
    );
    assert_eq!(c.query_row("SELECT COUNT(*) FROM search_segments WHERE field='base_row' AND json_extract(payload_json,'$.view_name')='Test'",[],|r|r.get::<_,u64>(0)).unwrap(),2);
}

#[test]
fn lexical_rank_retains_density_order_without_overwhelming_title_evidence() {
    for (weak, strong) in [
        (0, 1_000_000),
        (1_000_000, 2_000_000),
        (1_428_909_000_000, 1_520_498_000_000),
        (10_000_000_000_000, 20_000_000_000_000),
    ] {
        assert!(lexical_score(strong) > lexical_score(weak));
    }
    assert!(lexical_score(i64::MAX) < 30 * SEARCH_SCORE_SCALE);
    let mut c = database();
    for index in 0..120 {
        let text = format!(
            "# Scratchpad {index}\nThis unrelated boilerplate contains the arbitrary test string coastal registry without information about the register."
        );
        doc(&c, &format!("notes/filler-{index:03}.md"), &text, &text);
    }
    doc(
        &c,
        "notes/coastal-registry.md",
        "# Coastal Registry\nAuthoritative register of named stations.",
        "Authoritative register of named stations.",
    );
    c.execute("UPDATE canonical_documents SET title='Coastal Registry' WHERE file_id='notes/coastal-registry.md'",[]).unwrap();
    c.execute("INSERT INTO files(file_id,normalized_path,match_key,absolute_path,size_bytes,modified_unix_ms,hash_blake3,is_markdown) VALUES('txt','texts/station-log.txt','texts/station-log.txt','/unavailable/station-log.txt',1,1,'r1',0)",[]).unwrap();
    c.execute("INSERT INTO content_documents(file_id,format,file_group,observed_size,observed_modified_ms,desired_revision,served_revision,served_extractor_identity,extractor_identity,coverage) VALUES('txt','txt','text',1,1,'r1','r1','v1','v1','complete')",[]).unwrap();
    c.execute("INSERT INTO content_segments(file_id,ordinal,locator_kind,source_start,source_end,text,method,coverage) VALUES('txt',1,'line',1,2,'Station log\nA historical note refers to the coastal registry.','utf8','complete')",[]).unwrap();
    crate::SearchCorpusService
        .rebuild_atomic(&mut c, CasePolicy::Sensitive)
        .unwrap();
    for limit in [1, 3, 10, 100] {
        let result = VaultSearchService
            .search_current(&c, request(Some("coastal registry"), None, limit))
            .unwrap();
        assert_eq!(
            result.candidates[0].path, "notes/coastal-registry.md",
            "limit={limit}"
        );
        if limit > 1 {
            assert_eq!(
                result.candidates[1].path, "texts/station-log.txt",
                "limit={limit}"
            );
            assert_eq!(
                result.docs[1].path, "texts/station-log.txt",
                "limit={limit}"
            );
        }
        assert_eq!(result.total, 122);
    }
}
