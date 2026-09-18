use super::*;
use tao_sdk_storage::{FilesRepository, run_migrations};

fn fixture() -> (tempfile::TempDir, Connection) {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/content-tests");
    fs::create_dir_all(&root).expect("repo-local test directory");
    let temporary = tempfile::Builder::new()
        .prefix("content-")
        .tempdir_in(root)
        .expect("fixture");
    let mut connection = Connection::open_in_memory().expect("db");
    connection
        .execute_batch("PRAGMA foreign_keys=ON")
        .expect("foreign keys");
    run_migrations(&mut connection).expect("migrations");
    (temporary, connection)
}
fn record(root: &Path, path: &str, bytes: &[u8]) -> FileRecordInput {
    let absolute = root.join(path);
    if let Some(parent) = absolute.parent() {
        fs::create_dir_all(parent).unwrap();
    }
    fs::write(&absolute, bytes).unwrap();
    let metadata = fs::metadata(&absolute).unwrap();
    FileRecordInput {
        file_id: blake3::hash(path.as_bytes()).to_hex().to_string(),
        normalized_path: path.to_string(),
        match_key: path.to_string(),
        absolute_path: absolute.to_string_lossy().into_owned(),
        size_bytes: bytes.len() as u64,
        modified_unix_ms: metadata
            .modified()
            .unwrap()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64,
        hash_blake3: String::new(),
        is_markdown: false,
    }
}
fn publish(connection: &Connection, root: &Path, record: &FileRecordInput) {
    let batch = ContentIndexService
        .prepare(
            connection,
            root,
            &root.join(".tao/content-spool"),
            std::slice::from_ref(record),
        )
        .expect("prepare");
    FilesRepository::upsert(connection, record).expect("inventory");
    ContentIndexService
        .publish(connection, &batch)
        .expect("publish");
}

#[test]
fn text_encodings_preserve_exact_lines_and_reject_invalid_data() {
    for (bytes, expected) in [
        (b"\xef\xbb\xbfalpha\r\nbeta".to_vec(), "alpha\r\nbeta"),
        (vec![0xff, 0xfe, b'a', 0, 10, 0, 0xbb, 3], "a\nλ"),
        (vec![0xfe, 0xff, 0, b'a', 0, 10, 3, 0xbb], "a\nλ"),
        (Vec::new(), ""),
    ] {
        let (text, _) = decode_text(&bytes).unwrap();
        assert_eq!(text, expected);
    }
    for invalid in [
        vec![0xff],
        vec![0xff, 0xfe, 0],
        vec![0xff, 0xfe, 0, 0xd8],
        vec![b'a', 0, b'b'],
    ] {
        assert!(decode_text(&invalid).is_err());
    }
    let segments = text_segments("file", "one\r\ntwo\nlast").unwrap();
    assert_eq!(
        segments
            .iter()
            .map(|segment| segment.text.as_str())
            .collect::<Vec<_>>(),
        vec!["one\r\n", "two\n", "last"]
    );
    assert_eq!(segments[2].source_start, 3);
}

#[test]
fn txt_index_search_and_revision_bound_retrieval_use_captured_text() {
    let (temporary, mut connection) = fixture();
    let root = temporary.path();
    let source = record(
        root,
        "Unicode λ.TXT",
        b"---\nstatus: active\n---\n[[not-a-link]]\n- [ ] not a task\nuniquequasar evidence\nfinal",
    );
    publish(&connection, root, &source);
    SearchCorpusService
        .rebuild_atomic(&mut connection, CasePolicy::Sensitive)
        .unwrap();
    let matches:u64=connection.query_row("SELECT COUNT(*) FROM search_segments_fts WHERE search_segments_fts MATCH 'uniquequasar'",[],|row|row.get(0)).unwrap();
    assert!(matches > 0);
    let first = read_content(&connection, root, &source.normalized_path, 0, 2, None).unwrap();
    assert_eq!(first.coverage, "complete");
    assert_eq!(first.next_offset, Some(2));
    assert_eq!(first.segments[1].locator.start, 2);
    assert_eq!(first.original.current_revision_matches_served, Some(true));
    assert!(read_content(&connection, root, &source.normalized_path, 2, 2, None).is_err());
    let next = read_content(
        &connection,
        root,
        &source.normalized_path,
        2,
        2,
        first.continuation_revision.as_deref(),
    )
    .unwrap();
    assert_eq!(next.segments[1].text, "[[not-a-link]]\n");
    for table in ["properties", "tasks", "links"] {
        let count: u64 = connection
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, 0);
    }
    let updated = record(root, &source.normalized_path, b"new revision");
    publish(&connection, root, &updated);
    assert!(
        read_content(
            &connection,
            root,
            &source.normalized_path,
            2,
            2,
            first.continuation_revision.as_deref()
        )
        .is_err()
    );
}

#[test]
fn maximum_offset_is_rejected_instead_of_wrapping_sql_pagination() {
    let (temporary, connection) = fixture();
    let root = temporary.path();
    let source = record(root, "offset.txt", b"one\ntwo");
    publish(&connection, root, &source);
    let first = read_content(&connection, root, "offset.txt", 0, 1, None).unwrap();
    assert!(
        read_content(
            &connection,
            root,
            "offset.txt",
            usize::MAX,
            1,
            first.continuation_revision.as_deref()
        )
        .is_err()
    );
    assert!(ContentRepository::segments(&connection, &source.file_id, usize::MAX, 1).is_err());
    assert!(
        read_content(
            &connection,
            root,
            "offset.txt",
            i64::MAX as usize,
            1,
            first.continuation_revision.as_deref()
        )
        .unwrap()
        .segments
        .is_empty()
    );
}

#[test]
fn invalid_replacement_preserves_last_good_text_with_stale_coverage() {
    let (temporary, connection) = fixture();
    let root = temporary.path();
    let source = record(root, "note.txt", b"last good evidence");
    publish(&connection, root, &source);
    let invalid = record(root, "note.txt", &[0xff]);
    publish(&connection, root, &invalid);
    let read = read_content(&connection, root, "note.txt", 0, 100, None).unwrap();
    assert!(read.stale);
    assert_eq!(read.coverage, "failed");
    assert_eq!(read.segments[0].text, "last good evidence");
    assert_eq!(read.original.current_revision_matches_served, Some(false));
}

#[test]
fn oversized_replacement_line_never_labels_last_good_text_as_the_new_revision() {
    let (temporary, connection) = fixture();
    let root = temporary.path();
    let old = record(root, "bounded.txt", b"original reliable quote");
    publish(&connection, root, &old);
    let first = read_content(&connection, root, "bounded.txt", 0, 100, None).unwrap();
    let replacement = record(root, "bounded.txt", &vec![b'a'; MAX_SEGMENT_BYTES + 1]);
    publish(&connection, root, &replacement);
    let read = read_content(&connection, root, "bounded.txt", 0, 100, None).unwrap();
    assert_eq!(read.served_revision, first.served_revision);
    assert_ne!(Some(read.desired_revision.clone()), read.served_revision);
    assert!(read.stale);
    assert_eq!(read.original.current_revision_matches_served, Some(false));
    assert_eq!(read.segments[0].text, "original reliable quote");
    assert_eq!(read.coverage, "failed");
}

#[test]
fn unchanged_deferred_capture_recovers_after_spool_space_is_reclaimed() {
    if !content_capabilities().native_available {
        return;
    }
    let (temporary, mut connection) = fixture();
    let root = temporary.path();
    let spool = root.join(".tao/content-spool");
    ensure_spool(&spool).unwrap();
    let filler = spool.join("quota-fixture");
    File::create(&filler)
        .unwrap()
        .set_len(MAX_SPOOL_BYTES)
        .unwrap();
    let source = record(
        root,
        "deferred.pdf",
        &pdf(&[Page::Text("Deferred comet evidence")]),
    );
    publish(&connection, root, &source);
    let status = ContentIndexService.status(&connection).unwrap();
    assert_eq!(status.deferred_captures, 1);
    assert_eq!(status.queued, 1);
    assert!(!status.extraction_complete);
    assert!(
        !ContentIndexService
            .needs_refresh(&connection, &source.file_id, &source.normalized_path)
            .unwrap()
    );
    fs::remove_file(filler).unwrap();
    let report = ContentIndexService
        .process_pending(
            &mut connection,
            root,
            &spool,
            Duration::from_secs(3),
            CasePolicy::Sensitive,
        )
        .unwrap();
    assert_eq!(report.published, 1);
    assert_eq!(report.deferred_captures, 0);
    assert!(report.extraction_complete);
    let read = read_content(&connection, root, "deferred.pdf", 0, 100, None).unwrap();
    assert!(read.segments[0].text.contains("Deferred comet evidence"));
    assert!(
        !spool.read_dir().unwrap().any(|entry| entry
            .unwrap()
            .path()
            .extension()
            .is_some_and(|extension| extension == "pdf")),
        "finished captures must promptly release quota"
    );
}

#[test]
fn inventory_assets_never_read_bodies_and_survive_missing_content() {
    let (temporary, connection) = fixture();
    let root = temporary.path();
    let mut asset = record(root, "movie.MOV", b"small placeholder");
    fs::remove_file(&asset.absolute_path).unwrap();
    asset.size_bytes = 153_000_000_000;
    publish(&connection, root, &asset);
    let doc = ContentRepository::get(&connection, &asset.file_id)
        .unwrap()
        .unwrap();
    assert_eq!(doc.file_group, "video");
    assert_eq!(doc.coverage, "unsupported");
    assert!(doc.desired_revision.is_empty());
}

#[test]
fn pdf_queue_deduplicates_recovers_leases_and_fences_superseded_workers() {
    let (temporary, connection) = fixture();
    let root = temporary.path();
    let source = record(root, "source.pdf", &pdf(&[Page::Text("Queue evidence")]));
    publish(&connection, root, &source);
    publish(&connection, root, &source);
    assert_eq!(ContentRepository::counts(&connection).unwrap().queued, 1);
    let first = ContentRepository::claim(&connection, 100, 200, "first")
        .unwrap()
        .unwrap();
    assert!(
        ContentRepository::claim(&connection, 101, 201, "other")
            .unwrap()
            .is_none()
    );
    let recovered = ContentRepository::claim(&connection, 201, 301, "second")
        .unwrap()
        .unwrap();
    assert_eq!(recovered.attempts, 2);
    assert!(!ContentRepository::is_current(&connection, &first, 202).unwrap());
    assert!(ContentRepository::is_current(&connection, &recovered, 202).unwrap());
    let page = ContentSegmentRecord {
        file_id: source.file_id.clone(),
        ordinal: 1,
        locator_kind: "page".to_string(),
        source_start: 1,
        source_end: 1,
        text: "Replacement worker staged page".to_string(),
        method: "native".to_string(),
        coverage: "complete".to_string(),
    };
    assert!(ContentRepository::stage_page(&connection, &recovered, 202, &page).unwrap());
    ContentRepository::finish(&connection, &first, "failed", "stale worker", 0).unwrap();
    assert_eq!(
        ContentRepository::staged_pages(&connection, &recovered)
            .unwrap()
            .len(),
        1
    );
    let updated = record(
        root,
        "source.pdf",
        &pdf(&[Page::Text("Replacement evidence longer")]),
    );
    publish(&connection, root, &updated);
    assert!(!ContentRepository::is_current(&connection, &recovered, 202).unwrap());
    FilesRepository::delete_by_id(&connection, &source.file_id).unwrap();
    assert_eq!(ContentRepository::counts(&connection).unwrap().queued, 0);
    assert!(
        ContentRepository::get(&connection, &source.file_id)
            .unwrap()
            .is_none()
    );
}

#[test]
fn content_windows_stop_at_byte_budget_before_materializing_remaining_rows() {
    let (temporary, connection) = fixture();
    let root = temporary.path();
    let line = format!("{}\n", "x".repeat(256 * 1024 - 1));
    let source = record(root, "bounded.txt", line.repeat(12).as_bytes());
    publish(&connection, root, &source);
    let first = read_content(&connection, root, "bounded.txt", 0, 1000, None).unwrap();
    assert_eq!(first.segments.len(), 4);
    assert_eq!(first.total_segments, 12);
    assert_eq!(first.next_offset, Some(4));
    assert_eq!(
        first
            .segments
            .iter()
            .map(|row| row.text.len())
            .sum::<usize>(),
        MAX_RESPONSE_BYTES
    );
    let next = read_content(
        &connection,
        root,
        "bounded.txt",
        4,
        1000,
        first.continuation_revision.as_deref(),
    )
    .unwrap();
    assert_eq!(next.segments.first().unwrap().ordinal, 5);
    assert_eq!(next.next_offset, Some(8));
    assert!(
        ContentRepository::segments_bounded(&connection, &source.file_id, 0, 1000, 10).is_err()
    );
}

#[test]
fn superseded_captures_are_reclaimed_without_waiting_for_orphan_grace() {
    let (temporary, connection) = fixture();
    let root = temporary.path();
    let original = pdf(&[Page::Text("Original queued page")]);
    let source = record(root, "replace.pdf", &original);
    publish(&connection, root, &source);
    let spool = root.join(".tao/content-spool");
    let old_name = ContentRepository::retained_spools(&connection)
        .unwrap()
        .remove(0);
    let changed = record(
        root,
        "replace.pdf",
        &pdf(&[Page::Text("Changed queued page")]),
    );
    publish(&connection, root, &changed);
    assert!(spool.join(&old_name).exists());
    collect_spool_garbage(&connection, &spool).unwrap();
    assert!(!spool.join(&old_name).exists());
    assert_eq!(ContentRepository::counts(&connection).unwrap().queued, 1);
    assert!(
        ContentRepository::retired_spools(&connection)
            .unwrap()
            .is_empty()
    );
    // Reverting an as-yet-uncollected superseded revision must reactivate that job.
    let reverted = record(root, "replace.pdf", &original);
    publish(&connection, root, &reverted);
    let changed_again = record(
        root,
        "replace.pdf",
        &pdf(&[Page::Text("Changed queued page")]),
    );
    publish(&connection, root, &changed_again);
    let job = ContentRepository::claim(&connection, 100, 200, "reverted")
        .unwrap()
        .unwrap();
    assert_eq!(
        job.desired_revision,
        blake3::hash(&fs::read(&changed_again.absolute_path).unwrap())
            .to_hex()
            .as_str()
    );
}

#[test]
fn terminal_expired_lease_reclaims_staged_quota_and_marks_coverage_failed() {
    let (temporary, mut connection) = fixture();
    let root = temporary.path();
    let source = record(root, "expired.pdf", &pdf(&[Page::Text("Expired job")]));
    publish(&connection, root, &source);
    let job = ContentRepository::claim(&connection, 100, 200, "crashed-worker")
        .unwrap()
        .unwrap();
    let page = ContentSegmentRecord {
        file_id: source.file_id.clone(),
        ordinal: 1,
        locator_kind: "page".to_string(),
        source_start: 1,
        source_end: 1,
        text: "Staged uncommitted text".to_string(),
        method: "native".to_string(),
        coverage: "complete".to_string(),
    };
    assert!(ContentRepository::stage_page(&connection, &job, 101, &page).unwrap());
    connection
        .execute(
            "UPDATE extraction_jobs SET attempts=3 WHERE job_id=?1",
            [&job.job_id],
        )
        .unwrap();
    let report = ContentIndexService
        .process_pending(
            &mut connection,
            root,
            &root.join(".tao/content-spool"),
            Duration::from_millis(50),
            CasePolicy::Sensitive,
        )
        .unwrap();
    assert_eq!(report.failed, 1);
    assert_eq!(report.queued, 0);
    assert!(!report.extraction_complete);
    let bytes: u64 = connection
        .query_row(
            "SELECT bytes_total FROM extraction_stage_usage",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(bytes, 0);
    assert!(
        ContentRepository::staged_pages(&connection, &job)
            .unwrap()
            .is_empty()
    );
    let read = read_content(&connection, root, "expired.pdf", 0, 100, None).unwrap();
    assert_eq!(read.coverage, "failed");
    assert!(read.diagnostics.to_string().contains("3 attempts"));
}

#[test]
fn native_scanned_mixed_and_blank_pdf_pages_are_searchable_with_physical_locators() {
    let capabilities = content_capabilities();
    if !capabilities.native_available || !capabilities.ocr_available {
        eprintln!(
            "PDF/OCR qualification unavailable: {:?}",
            capabilities.diagnostics
        );
        return;
    }
    let (temporary, mut connection) = fixture();
    let root = temporary.path();
    let scan = raster_fixture(root, "Scanned nebula evidence");
    let bytes = pdf(&[
        Page::Text("Native quasar evidence"),
        Page::Image(&scan),
        Page::Mixed("Mixed heading", &scan),
        Page::Blank,
    ]);
    let source = record(root, "evidence.pdf", &bytes);
    publish(&connection, root, &source);
    let result = ContentIndexService
        .process_pending(
            &mut connection,
            root,
            &root.join(".tao/content-spool"),
            Duration::from_secs(60),
            CasePolicy::Sensitive,
        )
        .unwrap();
    assert_eq!(result.published, 1, "{result:?}");
    assert_eq!(result.failed, 0);
    let read = read_content(&connection, root, "evidence.pdf", 0, 100, None).unwrap();
    assert_eq!(read.metadata["page_count"], 4);
    assert_eq!(read.segments.len(), 4);
    assert!(read.all_indexed_text_returned);
    assert_eq!(read.original.current_revision_matches_served, Some(true));
    assert!(read.segments[0].text.contains("Native quasar evidence"));
    assert_eq!(read.segments[0].locator.start, 1);
    assert!(
        read.segments[1].text.contains("Scanned nebula evidence"),
        "{:?}",
        read.segments[1]
    );
    assert_eq!(read.segments[1].locator.start, 2);
    assert_eq!(read.segments[1].method, "ocr");
    assert!(read.segments[2].text.contains("Mixed heading"));
    assert!(read.segments[2].text.contains("Scanned nebula evidence"));
    assert_eq!(read.segments[2].locator.start, 3);
    assert_eq!(read.segments[3].method, "rendered_blank");
    assert_eq!(read.segments[3].coverage, "complete");
    assert_eq!(fs::read(&source.absolute_path).unwrap(), bytes);
    let matches: u64 = connection
        .query_row(
            "SELECT COUNT(*) FROM search_segments_fts WHERE search_segments_fts MATCH 'nebula'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(matches >= 2);
    let no_work = ContentIndexService
        .process_pending(
            &mut connection,
            root,
            &root.join(".tao/content-spool"),
            Duration::from_secs(1),
            CasePolicy::Sensitive,
        )
        .unwrap();
    assert_eq!(no_work.published, 0);
}

#[test]
fn malformed_pdf_failure_is_contained_and_preserves_source() {
    if !content_capabilities().native_available {
        return;
    }
    let (temporary, mut connection) = fixture();
    let root = temporary.path();
    let source = record(root, "broken.pdf", b"%PDF-1.7\ninvalid broken document");
    publish(&connection, root, &source);
    let result = ContentIndexService
        .process_pending(
            &mut connection,
            root,
            &root.join(".tao/content-spool"),
            Duration::from_secs(3),
            CasePolicy::Sensitive,
        )
        .unwrap();
    assert_eq!(result.failed, 1);
    let read = read_content(&connection, root, "broken.pdf", 0, 100, None).unwrap();
    assert_eq!(read.coverage, "failed");
    assert_eq!(read.total_segments, 0);
}

#[test]
fn superseded_pdf_is_not_published_and_oversized_capture_is_rejected() {
    if !content_capabilities().native_available {
        return;
    }
    let (temporary, mut connection) = fixture();
    let root = temporary.path();
    let source = record(root, "old.pdf", &pdf(&[Page::Text("Old evidence")]));
    publish(&connection, root, &source);
    fs::write(&source.absolute_path, pdf(&[Page::Text("New evidence")])).unwrap();
    let report = ContentIndexService
        .process_pending(
            &mut connection,
            root,
            &root.join(".tao/content-spool"),
            Duration::from_secs(3),
            CasePolicy::Sensitive,
        )
        .unwrap();
    assert_eq!(report.discarded, 1);
    assert_eq!(
        ContentRepository::segment_count(&connection, &source.file_id).unwrap(),
        0
    );
    let mut large = record(root, "large.pdf", b"%PDF-");
    File::options()
        .write(true)
        .open(&large.absolute_path)
        .unwrap()
        .set_len(MAX_PDF_BYTES + 1)
        .unwrap();
    large.size_bytes = MAX_PDF_BYTES + 1;
    publish(&connection, root, &large);
    assert_eq!(
        ContentRepository::get(&connection, &large.file_id)
            .unwrap()
            .unwrap()
            .coverage,
        "failed"
    );
}

#[cfg(unix)]
#[test]
fn symlink_sources_and_spools_cannot_escape_the_vault() {
    let (temporary, connection) = fixture();
    let root = temporary.path();
    let outside = root.join("other");
    fs::create_dir(&outside).unwrap();
    let vault = root.join("vault");
    fs::create_dir(&vault).unwrap();
    let mut source = record(&outside, "secret.txt", b"not indexed");
    source.normalized_path = "escape.txt".to_string();
    source.absolute_path = vault.join("escape.txt").to_string_lossy().to_string();
    std::os::unix::fs::symlink(outside.join("secret.txt"), &source.absolute_path).unwrap();
    publish(&connection, &vault, &source);
    assert_eq!(
        ContentRepository::get(&connection, &source.file_id)
            .unwrap()
            .unwrap()
            .coverage,
        "failed"
    );
    std::os::unix::fs::symlink(&outside, vault.join("spool")).unwrap();
    assert!(ensure_spool(&vault.join("spool")).is_err());
}

#[test]
fn encrypted_pdf_has_explicit_coverage_without_exposing_placeholder_text() {
    if !content_capabilities().native_available {
        return;
    }
    let (temporary, mut connection) = fixture();
    let root = temporary.path();
    let mut bytes = pdf(&[Page::Text("Protected evidence")]);
    let previous = String::from_utf8_lossy(&bytes)
        .split("startxref\n")
        .last()
        .unwrap()
        .lines()
        .next()
        .unwrap()
        .to_string();
    let object_offset = bytes.len();
    bytes.extend_from_slice(
        format!(
            "6 0 obj\n<< /Filter /Standard /V 1 /R 2 /Length 40 /O <{}> /U <{}> /P -4 >>\nendobj\n",
            "00".repeat(32),
            "00".repeat(32)
        )
        .as_bytes(),
    );
    let xref = bytes.len();
    bytes.extend_from_slice(format!("xref\n6 1\n{object_offset:010} 00000 n \ntrailer\n<< /Size 7 /Root 1 0 R /Encrypt 6 0 R /ID [<00112233445566778899aabbccddeeff><00112233445566778899aabbccddeeff>] /Prev {previous} >>\nstartxref\n{xref}\n%%EOF\n").as_bytes());
    let source = record(root, "encrypted.pdf", &bytes);
    publish(&connection, root, &source);
    ContentIndexService
        .process_pending(
            &mut connection,
            root,
            &root.join(".tao/content-spool"),
            Duration::from_secs(3),
            CasePolicy::Sensitive,
        )
        .unwrap();
    let read = read_content(&connection, root, "encrypted.pdf", 0, 100, None).unwrap();
    assert_eq!(read.coverage, "encrypted", "{read:?}");
    assert!(read.segments.is_empty());
}

#[test]
fn native_rotated_table_columns_and_encoded_font_keep_designated_evidence() {
    if !content_capabilities().native_available {
        return;
    }
    let (temporary, mut connection) = fixture();
    let root = temporary.path();
    for (name, text, expected) in [
        (
            "unicode.pdf",
            "Caf\\351 na\\357ve evidence",
            "Café naïve evidence",
        ),
        ("table.pdf", "ColumnA   ColumnB   Row1  120.50", "Row1"),
        (
            "rotated.pdf",
            "Rotated satellite evidence",
            "Rotated satellite evidence",
        ),
    ] {
        let mut bytes = pdf(&[Page::Text(text)]);
        if name == "rotated.pdf" {
            // Add rotation in an incremental page update, preserving original stream offsets.
            let previous = String::from_utf8_lossy(&bytes)
                .split("startxref\n")
                .last()
                .unwrap()
                .lines()
                .next()
                .unwrap()
                .to_string();
            let offset = bytes.len();
            bytes.extend_from_slice(b"3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Rotate 90 /Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>\nendobj\n");
            let xref = bytes.len();
            bytes.extend_from_slice(format!("xref\n3 1\n{offset:010} 00000 n \ntrailer\n<< /Size 6 /Root 1 0 R /Prev {previous} >>\nstartxref\n{xref}\n%%EOF\n").as_bytes());
        }
        let source = record(root, name, &bytes);
        publish(&connection, root, &source);
        ContentIndexService
            .process_pending(
                &mut connection,
                root,
                &root.join(".tao/content-spool"),
                Duration::from_secs(3),
                CasePolicy::Sensitive,
            )
            .unwrap();
        let read = read_content(&connection, root, name, 0, 100, None).unwrap();
        assert!(
            read.segments[0].text.contains(expected),
            "{name}: {:?}",
            read.segments
        );
        assert_eq!(read.segments[0].locator.start, 1);
    }
}

#[test]
fn cancelled_work_is_durable_and_orphan_cleanup_preserves_active_sources() {
    let (temporary, mut connection) = fixture();
    let root = temporary.path();
    let source = record(root, "queued.pdf", &pdf(&[Page::Text("Queue remains")]));
    publish(&connection, root, &source);
    let result = ContentIndexService
        .process_pending_cancellable(
            &mut connection,
            root,
            &root.join(".tao/content-spool"),
            Duration::from_secs(3),
            CasePolicy::Sensitive,
            &AtomicBool::new(true),
        )
        .unwrap();
    assert_eq!(result.queued, 1);
    let retained = ContentRepository::retained_spools(&connection).unwrap();
    collect_spool_garbage(&connection, &root.join(".tao/content-spool")).unwrap();
    assert!(root.join(".tao/content-spool").join(&retained[0]).is_file());
}

#[test]
fn staged_pages_resume_after_restart_without_becoming_visible_early() {
    if !content_capabilities().native_available {
        return;
    }
    let (temporary, mut connection) = fixture();
    let root = temporary.path();
    let source = record(
        root,
        "resume.pdf",
        &pdf(&[
            Page::Text("First native page"),
            Page::Text("Second native page"),
        ]),
    );
    publish(&connection, root, &source);
    let job = ContentRepository::claim(&connection, now_ms(), now_ms() + 60000, "old-worker")
        .unwrap()
        .unwrap();
    let page = ContentSegmentRecord {
        file_id: source.file_id.clone(),
        ordinal: 1,
        locator_kind: "page".to_string(),
        source_start: 1,
        source_end: 1,
        text: "First native page\n".to_string(),
        method: "native".to_string(),
        coverage: "complete".to_string(),
    };
    assert!(ContentRepository::stage_page(&connection, &job, now_ms(), &page).unwrap());
    assert_eq!(
        read_content(&connection, root, "resume.pdf", 0, 100, None)
            .unwrap()
            .total_segments,
        0
    );
    ContentRepository::release(&connection, &job, "restart", 0).unwrap();
    let report = ContentIndexService
        .process_pending(
            &mut connection,
            root,
            &root.join(".tao/content-spool"),
            Duration::from_secs(3),
            CasePolicy::Sensitive,
        )
        .unwrap();
    assert_eq!(report.published, 1);
    assert!(report.extraction_complete);
    let read = read_content(&connection, root, "resume.pdf", 0, 100, None).unwrap();
    assert_eq!(read.segments[0].text, "First native page\n");
    assert!(read.segments[1].text.contains("Second native page"));
    let stage_bytes: u64 = connection
        .query_row(
            "SELECT bytes_total FROM extraction_stage_usage",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(stage_bytes, 0);
}

#[test]
fn multiple_columns_preserve_critical_strings_without_claiming_layout_certainty() {
    if !content_capabilities().native_available {
        return;
    }
    let (temporary, mut connection) = fixture();
    let root = temporary.path();
    let source = record(
        root,
        "columns.pdf",
        &pdf(&[Page::Raw(
            "BT /F1 18 Tf 60 720 Td (North Alpha) Tj ET\nBT /F1 18 Tf 360 720 Td (South Beta) Tj ET\nBT /F1 18 Tf 60 680 Td (North Gamma) Tj ET\nBT /F1 18 Tf 360 680 Td (South Delta) Tj ET\n",
        )]),
    );
    publish(&connection, root, &source);
    ContentIndexService
        .process_pending(
            &mut connection,
            root,
            &root.join(".tao/content-spool"),
            Duration::from_secs(3),
            CasePolicy::Sensitive,
        )
        .unwrap();
    let read = read_content(&connection, root, "columns.pdf", 0, 100, None).unwrap();
    let words = read.segments[0].text.split_whitespace().collect::<Vec<_>>();
    assert_eq!(
        words,
        vec![
            "North", "Alpha", "South", "Beta", "North", "Gamma", "South", "Delta"
        ]
    );
    assert_eq!(read.coverage, "partial");
}

#[test]
fn page_count_bound_rejects_large_document_without_unbounded_work() {
    if !content_capabilities().native_available {
        return;
    }
    let (temporary, mut connection) = fixture();
    let root = temporary.path();
    let pages = (0..1001).map(|_| Page::Blank).collect::<Vec<_>>();
    let source = record(root, "many-pages.pdf", &pdf(&pages));
    publish(&connection, root, &source);
    ContentIndexService
        .process_pending(
            &mut connection,
            root,
            &root.join(".tao/content-spool"),
            Duration::from_secs(3),
            CasePolicy::Sensitive,
        )
        .unwrap();
    let read = read_content(&connection, root, "many-pages.pdf", 0, 100, None).unwrap();
    assert_eq!(read.coverage, "failed");
    assert!(read.diagnostics.to_string().contains("1000-page"));
}

struct Raster {
    width: usize,
    height: usize,
    pixels: Vec<u8>,
}
enum Page<'a> {
    Text(&'a str),
    Raw(&'a str),
    Image(&'a Raster),
    Mixed(&'a str, &'a Raster),
    Blank,
}
fn pdf(pages: &[Page<'_>]) -> Vec<u8> {
    let mut objects = vec![b"<< /Type /Catalog /Pages 2 0 R >>".to_vec(), Vec::new()];
    let mut kids = Vec::new();
    for page in pages {
        let page_id = objects.len() + 1;
        let stream_id = page_id + 1;
        let font_id = page_id + 2;
        let image_id = page_id + 3;
        kids.push(format!("{page_id} 0 R"));
        let image = match page {
            Page::Image(image) | Page::Mixed(_, image) => Some(*image),
            _ => None,
        };
        let resources = if image.is_some() {
            format!("/Font << /F1 {font_id} 0 R >> /XObject << /Img {image_id} 0 R >>")
        } else {
            format!("/Font << /F1 {font_id} 0 R >>")
        };
        objects.push(format!("<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << {resources} >> /Contents {stream_id} 0 R >>").into_bytes());
        let mut content = String::new();
        if image.is_some() {
            content.push_str("q 612 0 0 660 0 0 cm /Img Do Q\n");
        }
        if let Page::Text(text) | Page::Mixed(text, _) = page {
            content.push_str(&format!("BT /F1 24 Tf 60 730 Td ({text}) Tj ET\n"));
        }
        if let Page::Raw(raw) = page {
            content.push_str(raw);
        }
        objects.push(stream("", content.as_bytes()));
        objects.push(
            b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica /Encoding /WinAnsiEncoding >>"
                .to_vec(),
        );
        if let Some(image) = image {
            objects.push(stream(&format!("/Type /XObject /Subtype /Image /Width {} /Height {} /ColorSpace /DeviceGray /BitsPerComponent 8",image.width,image.height),&image.pixels));
        }
    }
    objects[1] = format!(
        "<< /Type /Pages /Kids [{}] /Count {} >>",
        kids.join(" "),
        pages.len()
    )
    .into_bytes();
    let mut bytes = b"%PDF-1.4\n".to_vec();
    let mut offsets = vec![0];
    for (index, object) in objects.iter().enumerate() {
        offsets.push(bytes.len());
        bytes.extend_from_slice(format!("{} 0 obj\n", index + 1).as_bytes());
        bytes.extend_from_slice(object);
        bytes.extend_from_slice(b"\nendobj\n");
    }
    let xref = bytes.len();
    bytes.extend_from_slice(format!("xref\n0 {}\n0000000000 65535 f \n", offsets.len()).as_bytes());
    for offset in offsets.iter().skip(1) {
        bytes.extend_from_slice(format!("{offset:010} 00000 n \n").as_bytes());
    }
    bytes.extend_from_slice(
        format!(
            "trailer\n<< /Size {} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n",
            offsets.len()
        )
        .as_bytes(),
    );
    bytes
}
fn stream(dictionary: &str, bytes: &[u8]) -> Vec<u8> {
    let mut result = format!("<< {dictionary} /Length {} >>\nstream\n", bytes.len()).into_bytes();
    result.extend_from_slice(bytes);
    result.extend_from_slice(b"\nendstream");
    result
}
fn raster_fixture(root: &Path, text: &str) -> Raster {
    let input = root.join("render-fixture.pdf");
    fs::write(&input, pdf(&[Page::Text(text)])).unwrap();
    let prefix = root.join("render-fixture");
    let renderer = content_capabilities().pdftoppm.unwrap();
    let output = std::process::Command::new(renderer)
        .args(["-singlefile", "-scale-to", "1200", "-gray"])
        .arg(input)
        .arg(&prefix)
        .output()
        .unwrap();
    assert!(output.status.success(), "{:?}", output);
    let bytes = fs::read(prefix.with_extension("pgm")).unwrap();
    let mut at = 0;
    let mut tokens = Vec::new();
    while tokens.len() < 4 {
        while bytes[at].is_ascii_whitespace() {
            at += 1;
        }
        if bytes[at] == b'#' {
            while bytes[at] != b'\n' {
                at += 1;
            }
            continue;
        }
        let start = at;
        while !bytes[at].is_ascii_whitespace() {
            at += 1;
        }
        tokens.push(std::str::from_utf8(&bytes[start..at]).unwrap().to_string());
    }
    at += 1;
    Raster {
        width: tokens[1].parse().unwrap(),
        height: tokens[2].parse().unwrap(),
        pixels: bytes[at..].to_vec(),
    }
}

#[test]
fn deferred_capture_cannot_resurrect_or_overwrite_concurrent_refresh() {
    for removed in [true, false] {
        let (temporary, mut connection) = fixture();
        let root = temporary.path();
        let input = record(root, "race.pdf", &pdf(&[Page::Text("Old captured source")]));
        publish(&connection, root, &input);
        connection
            .execute("UPDATE content_documents SET availability='deferred'", [])
            .unwrap();
        let expected_file = FilesRepository::get_by_id(&connection, &input.file_id)
            .unwrap()
            .unwrap();
        let expected_document = ContentRepository::get(&connection, &input.file_id)
            .unwrap()
            .unwrap();
        let batch = ContentIndexService
            .prepare(
                &connection,
                root,
                &root.join(".tao/content-spool"),
                std::slice::from_ref(&input),
            )
            .unwrap();
        if removed {
            FilesRepository::delete_by_id(&connection, &input.file_id).unwrap();
        } else {
            let replacement = record(
                root,
                "race.pdf",
                &pdf(&[Page::Text("Newer source revision evidence")]),
            );
            publish(&connection, root, &replacement);
        }
        let current_file = FilesRepository::get_by_id(&connection, &input.file_id).unwrap();
        let current_document = ContentRepository::get(&connection, &input.file_id).unwrap();
        ContentIndexService
            .publish_deferred_capture(
                &mut connection,
                &expected_file,
                &expected_document,
                &input,
                &batch,
                CasePolicy::Sensitive,
            )
            .unwrap();
        assert_eq!(
            FilesRepository::get_by_id(&connection, &input.file_id).unwrap(),
            current_file
        );
        assert_eq!(
            ContentRepository::get(&connection, &input.file_id).unwrap(),
            current_document
        );
    }
}

#[test]
fn page_staging_waits_for_writer_then_rechecks_superseded_lease() {
    let (temporary, _) = fixture();
    let root = temporary.path();
    let database = root.join("race.sqlite");
    let mut writer = Connection::open(&database).unwrap();
    run_migrations(&mut writer).unwrap();
    let source = record(root, "race.pdf", &pdf(&[Page::Text("Staging race")]));
    publish(&writer, root, &source);
    let job = ContentRepository::claim(&writer, 100, 10000, "first")
        .unwrap()
        .unwrap();
    let page = ContentSegmentRecord {
        file_id: source.file_id,
        ordinal: 1,
        locator_kind: "page".to_string(),
        source_start: 1,
        source_end: 1,
        text: "must not stage obsolete bytes".to_string(),
        method: "native".to_string(),
        coverage: "complete".to_string(),
    };
    let transaction = writer
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    transaction
        .execute("UPDATE extraction_jobs SET lease_token='replacement'", [])
        .unwrap();
    let (ready, started) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let connection = Connection::open(database).unwrap();
        connection.busy_timeout(Duration::from_secs(2)).unwrap();
        ready.send(()).unwrap();
        ContentRepository::stage_page(&connection, &job, 200, &page)
    });
    started.recv().unwrap();
    std::thread::sleep(Duration::from_millis(50));
    transaction.commit().unwrap();
    assert!(!worker.join().unwrap().unwrap());
    assert_eq!(
        writer
            .query_row("SELECT COUNT(*) FROM extraction_staged_pages", [], |row| {
                row.get::<_, u64>(0)
            })
            .unwrap(),
        0
    );
}

#[test]
#[cfg(unix)]
fn concurrent_pdf_captures_share_one_quota_excluding_worker_scratch() {
    let (temporary, _) = fixture();
    let root = temporary.path();
    let spool = root.join("spool");
    ensure_spool(&spool).unwrap();
    let bytes = pdf(&[Page::Text("Quota race")]);
    let source = root.join("source.pdf");
    fs::write(&source, &bytes).unwrap();
    let scratch = spool.join("job-active");
    fs::create_dir(&scratch).unwrap();
    File::create(scratch.join("rendered"))
        .unwrap()
        .set_len(MAX_SPOOL_BYTES)
        .unwrap();
    let available = MAX_SPOOL_BYTES - source_spool_bytes(&spool).unwrap() - bytes.len() as u64;
    File::create(spool.join("quota-fixture"))
        .unwrap()
        .set_len(available)
        .unwrap();
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let workers = (0..2)
        .map(|_| {
            let source = source.clone();
            let spool = spool.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                capture_pdf(&source, &spool)
            })
        })
        .collect::<Vec<_>>();
    let results = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, Err(ContentError::Quota(_))))
            .count(),
        1
    );
    assert_eq!(source_spool_bytes(&spool).unwrap(), MAX_SPOOL_BYTES);
}

#[test]
fn bounded_drain_waits_for_an_existing_worker_until_its_deadline() {
    let (temporary, mut connection) = fixture();
    let root = temporary.path();
    let source = record(
        root,
        "leased.pdf",
        &pdf(&[Page::Text("Owned by another worker")]),
    );
    publish(&connection, root, &source);
    let now = now_ms();
    ContentRepository::claim(&connection, now, now + 10_000, "other-process")
        .unwrap()
        .unwrap();
    let started = Instant::now();
    let status = ContentIndexService
        .process_pending(
            &mut connection,
            root,
            &root.join(".tao/content-spool"),
            Duration::from_millis(150),
            CasePolicy::Sensitive,
        )
        .unwrap();
    assert!(started.elapsed() >= Duration::from_millis(140));
    assert!(status.deadline_reached);
    assert!(status.worker_active);
    assert!(!status.extraction_complete);
    assert_eq!(status.failed, 0);
}

#[test]
fn file_backed_workers_extract_distinct_jobs_concurrently_and_preserve_pages() {
    if !content_capabilities().native_available {
        return;
    }
    let (temporary, _) = fixture();
    let root = temporary.path();
    fs::create_dir_all(root.join(".tao")).unwrap();
    let database = root.join(".tao/index.sqlite");
    let mut connection = Connection::open(&database).unwrap();
    run_migrations(&mut connection).unwrap();
    for n in 0..12 {
        let source = record(
            root,
            &format!("parallel-{n}.pdf"),
            &pdf(&[
                Page::Text("First physical page"),
                Page::Blank,
                Page::Text("Third physical page"),
            ]),
        );
        publish(&connection, root, &source);
    }
    let finished = std::sync::Arc::new(AtomicBool::new(false));
    let monitor_finished = finished.clone();
    let monitor = std::thread::spawn(move || {
        let db = Connection::open(database).unwrap();
        let mut peak = 0;
        while !monitor_finished.load(Ordering::Relaxed) {
            let running = ContentRepository::counts(&db).unwrap().running;
            peak = peak.max(running);
            std::thread::sleep(Duration::from_millis(2));
        }
        peak
    });
    let result = ContentIndexService.process_pending(
        &mut connection,
        root,
        &root.join(".tao/content-spool"),
        Duration::from_secs(15),
        CasePolicy::Sensitive,
    );
    finished.store(true, Ordering::Relaxed);
    let peak = monitor.join().unwrap();
    let report = result.unwrap();
    assert_eq!(report.published, 12);
    assert_eq!(report.failed, 0);
    assert_eq!(report.running, 0);
    assert_eq!(report.queued, 0);
    assert!(peak <= tao_sdk_storage::MAX_EXTRACTION_WORKERS as u64);
    if pool::worker_count() > 1 {
        assert!(peak > 1, "expected concurrent leased jobs, peak={peak}");
    }
    for n in 0..12 {
        let read = read_content(
            &connection,
            root,
            &format!("parallel-{n}.pdf"),
            0,
            100,
            None,
        )
        .unwrap();
        assert_eq!(read.total_segments, 3);
        assert!(read.segments[0].text.contains("First physical page"));
        assert!(read.segments[1].text.trim().is_empty());
        assert!(read.segments[2].text.contains("Third physical page"));
    }
    assert_eq!(
        connection
            .query_row("SELECT MAX(attempts) FROM extraction_jobs", [], |row| row
                .get::<_, u32>(
                0
            ))
            .unwrap(),
        1
    );
}

#[test]
fn job_admission_is_bounded_across_connections() {
    let (temporary, _) = fixture();
    let root = temporary.path();
    let database = root.join("admission.sqlite");
    let mut connection = Connection::open(&database).unwrap();
    run_migrations(&mut connection).unwrap();
    let other = Connection::open(database).unwrap();
    let mut claims = std::collections::HashSet::new();
    for n in 0..=tao_sdk_storage::MAX_EXTRACTION_WORKERS {
        let source = record(
            root,
            &format!("claim-{n}.pdf"),
            &pdf(&[Page::Text("Admission evidence")]),
        );
        publish(&connection, root, &source);
    }
    for n in 0..tao_sdk_storage::MAX_EXTRACTION_WORKERS {
        let writer = if n % 2 == 0 { &connection } else { &other };
        let job = ContentRepository::claim(writer, 100, 1000, &n.to_string())
            .unwrap()
            .unwrap();
        assert!(claims.insert(job.job_id));
    }
    assert!(
        ContentRepository::claim(&connection, 100, 1000, "overflow")
            .unwrap()
            .is_none()
    );
}

#[test]
fn content_publication_retries_writer_contention_and_obeys_deadline() {
    let (temporary, _) = fixture();
    let database = temporary.path().join("publication.sqlite");
    let writer = Connection::open(&database).unwrap();
    writer
        .execute_batch("CREATE TABLE evidence(value INTEGER); BEGIN IMMEDIATE;")
        .unwrap();
    let (ready, started) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let connection = Connection::open(database).unwrap();
        connection.busy_timeout(Duration::from_millis(5)).unwrap();
        ready.send(()).unwrap();
        let transaction = begin_content_publication(
            &connection,
            Instant::now() + Duration::from_secs(2),
            &AtomicBool::new(false),
        )
        .unwrap()
        .unwrap();
        transaction
            .execute("INSERT INTO evidence VALUES (1)", [])
            .unwrap();
        transaction.commit().unwrap();
    });
    started.recv().unwrap();
    std::thread::sleep(Duration::from_millis(100));
    writer.execute_batch("COMMIT").unwrap();
    worker.join().unwrap();
    assert_eq!(
        writer
            .query_row("SELECT COUNT(*) FROM evidence", [], |row| row
                .get::<_, u64>(0))
            .unwrap(),
        1
    );

    let blocked = Connection::open(temporary.path().join("publication.sqlite")).unwrap();
    blocked.busy_timeout(Duration::from_millis(5)).unwrap();
    writer.execute_batch("BEGIN IMMEDIATE").unwrap();
    let started = Instant::now();
    assert!(
        begin_content_publication(
            &blocked,
            started + Duration::from_millis(50),
            &AtomicBool::new(false),
        )
        .unwrap()
        .is_none()
    );
    assert!(started.elapsed() >= Duration::from_millis(50));
    writer.execute_batch("ROLLBACK").unwrap();
}
