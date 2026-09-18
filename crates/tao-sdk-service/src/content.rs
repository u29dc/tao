//! Source-addressed TXT/PDF content, persistent extraction, and bounded retrieval.

mod pool;
#[cfg(test)]
mod tests;
mod worker;

use std::borrow::Cow;
use std::collections::{BTreeSet, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, TransactionBehavior};
use serde::Serialize;
use serde_json::{Value, json};
use tao_sdk_storage::{
    ContentDocumentRecord, ContentRepository, ContentSegmentRecord, DocumentsRepository,
    ExtractionJobRecord, FileRecordInput, FilesRepository,
};
use tao_sdk_vault::{CapturedFile, CasePolicy};
use thiserror::Error;

use crate::SearchCorpusService;
pub use worker::{ContentCapabilities, content_capabilities};

const MAX_TEXT_BYTES: u64 = 32 * 1024 * 1024;
const MAX_PDF_BYTES: u64 = 64 * 1024 * 1024;
const MAX_SPOOL_BYTES: u64 = 256 * 1024 * 1024;
const MAX_RESPONSE_BYTES: usize = 1024 * 1024;
const MAX_SEGMENT_BYTES: usize = 256 * 1024;
const TXT_EXTRACTOR: &str = "tao-txt-v1:utf8-bom:utf16-bom:lines";

/// Content errors are isolated from unrelated files by preparation.
#[derive(Debug, Error)]
pub enum ContentError {
    #[error("content I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("content storage: {0}")]
    Storage(#[from] rusqlite::Error),
    #[error("content serialization: {0}")]
    Json(#[from] serde_json::Error),
    #[error("{0}")]
    Invalid(String),
    #[error("{0}")]
    Quota(String),
}

/// Queue/publication totals; index completion and content completion are distinct.
#[derive(Debug, Default, Clone, Serialize)]
pub struct ContentRefreshReport {
    pub queued: u64,
    pub running: u64,
    pub failed: u64,
    pub complete: u64,
    pub published: u64,
    pub discarded: u64,
    pub worker_active: bool,
    pub deadline_reached: bool,
    pub extraction_complete: bool,
    pub incomplete_documents: u64,
    pub deferred_captures: u64,
}

#[derive(Debug)]
struct PreparedContent {
    document: ContentDocumentRecord,
    segments: Option<Vec<ContentSegmentRecord>>,
    job: Option<ExtractionJobRecord>,
}

/// Captured revisions prepared before the writer transaction.
#[derive(Debug, Default)]
pub struct PreparedContentBatch {
    entries: Vec<PreparedContent>,
}
impl PreparedContentBatch {
    /// Retained allocation capacities, excluding transient decoder/worker scratch.
    pub(crate) fn retained_bytes(&self) -> usize {
        let strings = |values: &[&String]| values.iter().map(|v| v.capacity()).sum::<usize>();
        let optional = |value: &Option<String>| value.as_ref().map_or(0, String::capacity);
        let mut bytes = self.entries.capacity() * size_of::<PreparedContent>();
        for entry in &self.entries {
            let doc = &entry.document;
            bytes += strings(&[
                &doc.file_id,
                &doc.format,
                &doc.file_group,
                &doc.desired_revision,
                &doc.extractor_identity,
                &doc.coverage,
                &doc.availability,
                &doc.metadata_json,
                &doc.diagnostics_json,
            ]);
            bytes += optional(&doc.served_revision) + optional(&doc.served_extractor_identity);
            if let Some(segments) = &entry.segments {
                bytes += segments.capacity() * size_of::<ContentSegmentRecord>();
                for segment in segments {
                    bytes += strings(&[
                        &segment.file_id,
                        &segment.locator_kind,
                        &segment.text,
                        &segment.method,
                        &segment.coverage,
                    ]);
                }
            }
            if let Some(job) = &entry.job {
                bytes += strings(&[
                    &job.job_id,
                    &job.file_id,
                    &job.desired_revision,
                    &job.extractor_identity,
                    &job.spool_name,
                    &job.state,
                    &job.diagnostic,
                ]);
                bytes += optional(&job.lease_token);
            }
        }
        bytes
    }
    /// Aggregate prepared small publications while releasing source capture buffers.
    pub fn extend(&mut self, other: Self) {
        self.entries.extend(other.entries);
    }

    /// Exact source metadata from the captured revision, when prepared.
    pub fn metadata_for(&self, file_id: &str) -> Option<(u64, i64)> {
        self.entries
            .iter()
            .find(|entry| entry.document.file_id == file_id)
            .map(|entry| {
                (
                    entry.document.observed_size,
                    entry.document.observed_modified_ms,
                )
            })
    }
    /// Captured content hash to persist with the same inventory revision.
    pub fn revision_for(&self, file_id: &str) -> Option<&str> {
        self.entries
            .iter()
            .find(|entry| entry.document.file_id == file_id)
            .map(|entry| entry.document.desired_revision.as_str())
            .filter(|revision| !revision.is_empty())
    }
}

/// Extension-derived inventory classification; never opens inventory-only assets.
pub fn classify_content(path: &str) -> (String, String) {
    let format = Path::new(path)
        .extension()
        .and_then(|v| v.to_str())
        .unwrap_or("")
        .to_lowercase();
    let group = match format.as_str() {
        "md" | "markdown" | "txt" | "pdf" | "doc" | "docx" | "rtf" | "odt" => "document",
        "png" | "jpg" | "jpeg" | "gif" | "webp" | "tif" | "tiff" | "svg" | "heic" => "image",
        "mp3" | "wav" | "m4a" | "flac" | "ogg" => "audio",
        "mp4" | "mov" | "mkv" | "webm" => "video",
        "zip" | "gz" | "tar" | "7z" => "archive",
        "csv" | "tsv" | "xls" | "xlsx" => "spreadsheet",
        "base" => "base",
        _ => "other",
    };
    (format, group.to_string())
}

#[derive(Debug, Default, Clone, Copy)]
pub struct ContentIndexService;
impl ContentIndexService {
    /// Detect extractor configuration changes without opening source asset bytes.
    pub fn needs_refresh(
        &self,
        connection: &Connection,
        file_id: &str,
        path: &str,
    ) -> Result<bool, ContentError> {
        let (format, _) = classify_content(path);
        if format != "txt" && format != "pdf" {
            return Ok(false);
        }
        let Some(document) = ContentRepository::get(connection, file_id)? else {
            return Ok(true);
        };
        let expected = if format == "txt" {
            TXT_EXTRACTOR.to_string()
        } else {
            worker::extractor_identity()
        };
        Ok(document.extractor_identity != expected
            || document.coverage == "stale"
            || document.availability == "unavailable")
    }

    /// Capture only changed supported content. PDF bytes stream directly to bounded disk.
    pub fn prepare(
        &self,
        connection: &Connection,
        vault_root: &Path,
        spool_root: &Path,
        files: &[FileRecordInput],
    ) -> Result<PreparedContentBatch, ContentError> {
        self.prepare_with_captures(connection, vault_root, spool_root, files, &HashMap::new())
    }

    /// Reuse source captures supplied by verified reconciliation.
    pub fn prepare_with_captures(
        &self,
        connection: &Connection,
        vault_root: &Path,
        spool_root: &Path,
        files: &[FileRecordInput],
        captures: &HashMap<String, CapturedFile>,
    ) -> Result<PreparedContentBatch, ContentError> {
        let mut batch = PreparedContentBatch::default();
        for file in files {
            check_content_cancellation()?;
            if file.is_markdown {
                continue;
            }
            let (format, group) = classify_content(&file.normalized_path);
            if format == "base" {
                continue;
            }
            let previous = ContentRepository::get(connection, &file.file_id)?;
            let extractor = if format == "txt" {
                TXT_EXTRACTOR.to_string()
            } else if format == "pdf" {
                worker::extractor_identity()
            } else {
                "inventory-v1".to_string()
            };
            if previous.as_ref().is_some_and(|old| {
                old.extractor_identity == extractor
                    && old.observed_size == file.size_bytes
                    && old.observed_modified_ms == file.modified_unix_ms
                    && (file.hash_blake3.is_empty() || old.desired_revision == file.hash_blake3)
                    && old.availability == "accessible"
                    && old.coverage != "stale"
            }) {
                continue;
            }
            let mut document = ContentDocumentRecord {
                file_id: file.file_id.clone(),
                format: format.clone(),
                file_group: group,
                observed_size: file.size_bytes,
                observed_modified_ms: file.modified_unix_ms,
                desired_revision: String::new(),
                served_revision: previous
                    .as_ref()
                    .and_then(|old| old.served_revision.clone()),
                served_extractor_identity: previous
                    .as_ref()
                    .and_then(|old| old.served_extractor_identity.clone()),
                extractor_identity: extractor,
                coverage: "unsupported".to_string(),
                availability: "accessible".to_string(),
                metadata_json: previous
                    .as_ref()
                    .map_or_else(|| "{}".to_string(), |old| old.metadata_json.clone()),
                diagnostics_json: "[]".to_string(),
            };
            if format != "txt" && format != "pdf" {
                document.metadata_json =
                    json!({"classification":"extension","content_digest_computed":false})
                        .to_string();
                batch.entries.push(PreparedContent {
                    document,
                    segments: None,
                    job: None,
                });
                continue;
            }
            let prepared = (|| -> Result<PreparedContent, ContentError> {
                let path = fs::canonicalize(&file.absolute_path)?;
                let root = fs::canonicalize(vault_root)?;
                if !path.starts_with(&root) {
                    return Err(ContentError::Invalid(
                        "source escapes vault boundary".to_string(),
                    ));
                }
                if format == "txt" {
                    let (bytes, revision, size, modified) =
                        if let Some(capture) = captures.get(&file.normalized_path) {
                            if capture.bytes.len() as u64 > MAX_TEXT_BYTES {
                                return Err(ContentError::Invalid(
                                    "TXT capture exceeds input limit".to_string(),
                                ));
                            }
                            (
                                Cow::Borrowed(capture.bytes.as_slice()),
                                capture.fingerprint.hash_blake3.clone(),
                                capture.fingerprint.size_bytes,
                                capture.fingerprint.modified_unix_ms.min(i64::MAX as u128) as i64,
                            )
                        } else {
                            let (bytes, revision, size, modified) =
                                capture_text(&path, MAX_TEXT_BYTES)?;
                            (Cow::Owned(bytes), revision, size, modified)
                        };
                    document.observed_size = size;
                    document.observed_modified_ms = modified;
                    document.desired_revision = revision.clone();
                    let (text, encoding) = decode_text(&bytes)?;
                    let segments = text_segments(&file.file_id, &text)?;
                    document.served_revision = Some(revision);
                    document.served_extractor_identity = Some(document.extractor_identity.clone());
                    document.coverage = "complete".to_string();
                    document.metadata_json=json!({"encoding":encoding,"classification":"extension","line_count":text.split_inclusive('\n').count()}).to_string();
                    Ok(PreparedContent {
                        document: document.clone(),
                        segments: Some(segments),
                        job: None,
                    })
                } else {
                    ensure_spool(spool_root)?;
                    let (revision, spool_name, size, modified) =
                        if let Some(capture) = captures.get(&file.normalized_path) {
                            let (revision, spool_name) = spool_captured_pdf(capture, spool_root)?;
                            (
                                revision,
                                spool_name,
                                capture.fingerprint.size_bytes,
                                capture.fingerprint.modified_unix_ms.min(i64::MAX as u128) as i64,
                            )
                        } else {
                            capture_pdf(&path, spool_root)?
                        };
                    document.observed_size = size;
                    document.observed_modified_ms = modified;
                    document.desired_revision = revision.clone();
                    document.coverage = "pending".to_string();
                    let identity = serde_json::to_vec(&(
                        &file.file_id,
                        &revision,
                        &document.extractor_identity,
                    ))?;
                    let job = ExtractionJobRecord {
                        job_id: blake3::hash(&identity).to_hex().to_string(),
                        file_id: file.file_id.clone(),
                        desired_revision: revision,
                        extractor_identity: document.extractor_identity.clone(),
                        spool_name,
                        state: "queued".to_string(),
                        lease_token: None,
                        lease_until_ms: 0,
                        attempts: 0,
                        next_attempt_ms: 0,
                        diagnostic: String::new(),
                    };
                    Ok(PreparedContent {
                        document: document.clone(),
                        segments: None,
                        job: Some(job),
                    })
                }
            })();
            match prepared {
                Ok(entry) => batch.entries.push(entry),
                Err(error) => {
                    document.coverage = if matches!(&error, ContentError::Quota(_)) {
                        "pending"
                    } else {
                        "failed"
                    }
                    .to_string();
                    document.availability = match &error {
                        ContentError::Quota(_) => "deferred",
                        ContentError::Io(_) => "unavailable",
                        _ => "accessible",
                    }
                    .to_string();
                    document.diagnostics_json = json!([error.to_string()]).to_string();
                    // Failed capture has no asserted new digest. Last-good segments remain labelled stale.
                    batch.entries.push(PreparedContent {
                        document,
                        segments: None,
                        job: None,
                    });
                }
            }
        }
        Ok(batch)
    }

    /// Publish prepared revisions within the caller's inventory/search transaction.
    pub fn publish(
        &self,
        connection: &Connection,
        batch: &PreparedContentBatch,
    ) -> Result<ContentRefreshReport, ContentError> {
        for entry in &batch.entries {
            ContentRepository::upsert(connection, &entry.document)?;
            if entry.document.format == "pdf" && entry.document.coverage != "complete" {
                crate::revalidate_pdf_page_links(connection, &entry.document.file_id, None)?;
            }
            if let Some(segments) = &entry.segments {
                ContentRepository::replace_segments(connection, &entry.document.file_id, segments)?;
            }
            if let Some(job) = &entry.job {
                ContentRepository::enqueue(connection, job)?;
            }
        }
        let mut report = self.status(connection)?;
        report.published = batch.entries.len() as u64;
        Ok(report)
    }

    pub fn status(&self, connection: &Connection) -> Result<ContentRefreshReport, ContentError> {
        let counts = ContentRepository::counts(connection)?;
        let incomplete_documents = ContentRepository::coverage_stats(connection)?
            .iter()
            .filter(|(coverage, _)| coverage != "complete" && coverage != "unsupported")
            .map(|(_, count)| count)
            .sum();
        let deferred_captures = ContentRepository::deferred_count(connection)?;
        Ok(ContentRefreshReport {
            deferred_captures,
            extraction_complete: incomplete_documents == 0,
            incomplete_documents,
            queued: counts.queued + deferred_captures,
            running: counts.running,
            failed: counts.failed,
            complete: counts.complete,
            worker_active: counts.running > 0,
            ..ContentRefreshReport::default()
        })
    }

    fn retry_deferred_capture(
        &self,
        connection: &mut Connection,
        vault_root: &Path,
        spool_root: &Path,
        case_policy: CasePolicy,
    ) -> Result<(), ContentError> {
        let _publication = crate::publication_lock::PublicationGuard::acquire(connection)?;
        let Some(file_id) = ContentRepository::deferred_file(connection)? else {
            return Ok(());
        };
        let Some(file) = FilesRepository::get_by_id(connection, &file_id)
            .map_err(|error| ContentError::Invalid(error.to_string()))?
        else {
            return Ok(());
        };
        if source_spool_bytes(spool_root)?.saturating_add(file.size_bytes) > MAX_SPOOL_BYTES {
            return Ok(());
        }
        let expected_file = file.clone();
        let Some(expected_document) = ContentRepository::get(connection, &file_id)?
            .filter(|document| document.availability == "deferred")
        else {
            return Ok(());
        };
        let mut input = FileRecordInput {
            file_id: file.file_id,
            normalized_path: file.normalized_path,
            match_key: file.match_key,
            absolute_path: file.absolute_path,
            size_bytes: file.size_bytes,
            modified_unix_ms: file.modified_unix_ms,
            hash_blake3: file.hash_blake3,
            is_markdown: file.is_markdown,
        };
        let batch = self.prepare(
            connection,
            vault_root,
            spool_root,
            std::slice::from_ref(&input),
        )?;
        if let Some(hash) = batch.revision_for(&file_id) {
            input.hash_blake3 = hash.to_string();
        }
        if let Some((size, modified)) = batch.metadata_for(&file_id) {
            input.size_bytes = size;
            input.modified_unix_ms = modified;
        }
        self.publish_deferred_capture(
            connection,
            &expected_file,
            &expected_document,
            &input,
            &batch,
            case_policy,
        )
    }

    fn publish_deferred_capture(
        &self,
        connection: &mut Connection,
        expected_file: &tao_sdk_storage::FileRecord,
        expected_document: &ContentDocumentRecord,
        input: &FileRecordInput,
        batch: &PreparedContentBatch,
        case_policy: CasePolicy,
    ) -> Result<(), ContentError> {
        let _publication = crate::publication_lock::PublicationGuard::acquire(connection)?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        // Preparation runs outside the writer transaction. Never resurrect a removed
        // file or replace a newer desired revision after a concurrent refresh.
        if FilesRepository::get_by_id(&transaction, &expected_file.file_id)
            .map_err(|error| ContentError::Invalid(error.to_string()))?
            .as_ref()
            != Some(expected_file)
            || ContentRepository::get(&transaction, &expected_file.file_id)?.as_ref()
                != Some(expected_document)
        {
            return Ok(());
        }
        FilesRepository::upsert(&transaction, input)
            .map_err(|error| ContentError::Invalid(error.to_string()))?;
        self.publish(&transaction, batch)?;
        SearchCorpusService
            .refresh_files_in_transaction(
                &transaction,
                std::slice::from_ref(&input.file_id),
                case_policy,
            )
            .map_err(|error| ContentError::Invalid(error.to_string()))?;
        transaction.commit()?;
        Ok(())
    }

    /// Drain bounded local work using CPU-sized, cross-process leased workers.
    pub fn process_pending(
        &self,
        connection: &mut Connection,
        vault_root: &Path,
        spool_root: &Path,
        budget: Duration,
        case_policy: CasePolicy,
    ) -> Result<ContentRefreshReport, ContentError> {
        self.process_pending_cancellable(
            connection,
            vault_root,
            spool_root,
            budget,
            case_policy,
            &AtomicBool::new(false),
        )
    }

    /// Drain jobs with cooperative cancellation propagated to each isolated subprocess.
    pub fn process_pending_cancellable(
        &self,
        connection: &mut Connection,
        vault_root: &Path,
        spool_root: &Path,
        budget: Duration,
        case_policy: CasePolicy,
        cancelled: &AtomicBool,
    ) -> Result<ContentRefreshReport, ContentError> {
        pool::drain(pool::DrainRequest {
            connection,
            vault_root,
            spool_root,
            budget,
            case_policy,
            cancelled,
        })
    }

    fn process_worker_cancellable(
        &self,
        connection: &mut Connection,
        vault_root: &Path,
        spool_root: &Path,
        budget: Duration,
        case_policy: CasePolicy,
        cancelled: &AtomicBool,
    ) -> Result<ContentRefreshReport, ContentError> {
        if budget.is_zero() || cancelled.load(Ordering::Relaxed) {
            return self.status(connection);
        }
        ensure_spool(spool_root)?;
        let deadline = Instant::now() + budget.min(Duration::from_secs(600));
        let publication = crate::publication_lock::PublicationGuard::acquire_until(
            connection,
            deadline,
            Some(cancelled),
        )?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let recovered = ContentRepository::recover_expired(&transaction, now_ms())?;
        if !recovered.is_empty() {
            SearchCorpusService
                .refresh_files_in_transaction(&transaction, &recovered, case_policy)
                .map_err(|error| ContentError::Invalid(error.to_string()))?;
        }
        transaction.commit()?;
        drop(publication);
        collect_spool_garbage(connection, spool_root)?;
        let mut published = 0;
        let mut discarded = 0;
        while Instant::now() < deadline && !cancelled.load(Ordering::Relaxed) {
            self.retry_deferred_capture(connection, vault_root, spool_root, case_policy)?;
            let now = now_ms();
            let token = uuid::Uuid::new_v4().to_string();
            let lease = now
                + (deadline
                    .saturating_duration_since(Instant::now())
                    .as_millis() as i64)
                + 10_000;
            let job = {
                let _publication = crate::publication_lock::PublicationGuard::acquire_until(
                    connection,
                    deadline,
                    Some(cancelled),
                )?;
                ContentRepository::claim(connection, now, lease, &token)?
            };
            let Some(job) = job else {
                let status = self.status(connection)?;
                if status.running == 0 && status.queued == 0 {
                    break;
                }
                // Another worker can own the lease, or a retry can be delayed.
                // A bounded drain waits for that work instead of claiming completion.
                if check_content_cancellation().is_err() {
                    break;
                }
                std::thread::sleep(
                    Duration::from_millis(50)
                        .min(deadline.saturating_duration_since(Instant::now())),
                );
                continue;
            };
            let scratch = spool_root.join(format!("job-{token}"));
            fs::create_dir(&scratch)?;
            let staged = ContentRepository::staged_pages(connection, &job)?;
            let mut on_page = |page: &ContentSegmentRecord| -> Result<(), ContentError> {
                loop {
                    check_content_cancellation()?;
                    match ContentRepository::stage_page(connection, &job, now_ms(), page) {
                        Ok(true) => return Ok(()),
                        Ok(false) => {
                            return Err(ContentError::Invalid(
                                "extraction lease superseded while staging page".to_string(),
                            ));
                        }
                        Err(error) => {
                            let error = ContentError::Storage(error);
                            if !is_storage_busy(&error)
                                || Instant::now() >= deadline
                                || cancelled.load(Ordering::Relaxed)
                            {
                                return Err(error);
                            }
                            // Retain the computed page while an index publication
                            // holds SQLite's writer slot; never repeat OCR for contention.
                            std::thread::sleep(Duration::from_millis(10));
                        }
                    }
                }
            };
            let result = worker::extract_pdf(worker::PdfWorkRequest {
                job: &job,
                spool_root,
                scratch: &scratch,
                deadline,
                cancelled,
                staged: &staged,
                on_page: &mut on_page,
            });
            let _ = fs::remove_dir_all(&scratch);
            if result.as_ref().is_err_and(is_storage_busy) {
                ContentRepository::release(
                    connection,
                    &job,
                    "publication busy; retrying staged extraction",
                    now_ms() + 1000,
                )?;
                break;
            }
            if cancelled.load(Ordering::Relaxed)
                || Instant::now() >= deadline
                || check_content_cancellation().is_err()
            {
                ContentRepository::release(
                    connection,
                    &job,
                    "worker cancelled or deadline reached",
                    now_ms() + 1000,
                )?;
                break;
            }
            let _publication = match crate::publication_lock::PublicationGuard::acquire_until(
                connection,
                deadline,
                Some(cancelled),
            ) {
                Ok(guard) => guard,
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::Interrupted | std::io::ErrorKind::TimedOut
                    ) =>
                {
                    ContentRepository::release(
                        connection,
                        &job,
                        "publication deferred; completed pages retained",
                        now_ms() + 1000,
                    )?;
                    break;
                }
                Err(error) => return Err(error.into()),
            };
            if !ContentRepository::is_current(connection, &job, now_ms())? {
                discarded += 1;
                continue;
            }
            let Some(file) = FilesRepository::get_by_id(connection, &job.file_id)
                .map_err(|error| ContentError::Invalid(error.to_string()))?
            else {
                discarded += 1;
                continue;
            };
            // Files may change on disk while extraction is in flight. Never assert current content
            // against a replacement source, even when its timestamps were restored.
            let original = fs::canonicalize(&file.absolute_path);
            let root = fs::canonicalize(vault_root)?;
            let matches = original
                .as_ref()
                .ok()
                .filter(|path| path.starts_with(&root) && path.is_file())
                .and_then(|path| hash_bounded(path, MAX_PDF_BYTES).ok())
                .is_some_and(|hash| hash == job.desired_revision);
            if cancelled.load(Ordering::Relaxed)
                || Instant::now() >= deadline
                || check_content_cancellation().is_err()
            {
                ContentRepository::release(
                    connection,
                    &job,
                    "worker cancelled before publication",
                    now_ms() + 1000,
                )?;
                break;
            }
            let Some(transaction) = begin_content_publication(connection, deadline, cancelled)?
            else {
                ContentRepository::release(
                    connection,
                    &job,
                    "worker cancelled or deadline reached before publication",
                    now_ms() + 1000,
                )?;
                break;
            };
            if !ContentRepository::is_current(&transaction, &job, now_ms())? {
                discarded += 1;
                transaction.commit()?;
                continue;
            }
            if !matches {
                ContentRepository::finish(
                    &transaction,
                    &job,
                    "superseded",
                    "source changed or disappeared during extraction",
                    0,
                )?;
                if let Some(mut document) = ContentRepository::get(&transaction, &job.file_id)? {
                    document.coverage = "stale".to_string();
                    document.diagnostics_json =
                        json!(["source changed during extraction; refresh required"]).to_string();
                    ContentRepository::upsert(&transaction, &document)?;
                }
                SearchCorpusService
                    .refresh_files_in_transaction(
                        &transaction,
                        std::slice::from_ref(&job.file_id),
                        case_policy,
                    )
                    .map_err(|error| ContentError::Invalid(error.to_string()))?;
                discarded += 1;
                transaction.commit()?;
                continue;
            }
            if let Some(mut document) = ContentRepository::get(&transaction, &job.file_id)? {
                match result {
                    Ok(extracted) => {
                        document.served_revision = Some(job.desired_revision.clone());
                        document.served_extractor_identity = Some(job.extractor_identity.clone());
                        document.coverage = extracted.coverage;
                        document.metadata_json = extracted.metadata.to_string();
                        document.diagnostics_json = json!(extracted.diagnostics).to_string();
                        ContentRepository::upsert(&transaction, &document)?;
                        crate::revalidate_pdf_page_links(
                            &transaction,
                            &job.file_id,
                            extracted
                                .metadata
                                .get("page_count")
                                .and_then(Value::as_u64)
                                .and_then(|value| u32::try_from(value).ok()),
                        )?;
                        ContentRepository::replace_segments(
                            &transaction,
                            &job.file_id,
                            &extracted.segments,
                        )?;
                        ContentRepository::finish(&transaction, &job, "done", "", 0)?;
                        published += 1;
                    }
                    Err(error) => {
                        let message = error.to_string();
                        let retry = job.attempts < 3 && matches!(error, ContentError::Io(_));
                        document.coverage = if message.to_lowercase().contains("password")
                            || message.to_lowercase().contains("encrypted")
                        {
                            "encrypted"
                        } else {
                            "failed"
                        }
                        .to_string();
                        document.diagnostics_json = json!([message]).to_string();
                        ContentRepository::upsert(&transaction, &document)?;
                        ContentRepository::finish(
                            &transaction,
                            &job,
                            if retry { "retry" } else { "failed" },
                            &message,
                            now_ms() + i64::from(job.attempts) * 5_000,
                        )?;
                    }
                }
                SearchCorpusService
                    .refresh_files_in_transaction(
                        &transaction,
                        std::slice::from_ref(&job.file_id),
                        case_policy,
                    )
                    .map_err(|error| ContentError::Invalid(error.to_string()))?;
            }
            transaction.commit()?;
            if !ContentRepository::retained_spools(connection)?.contains(&job.spool_name) {
                let captured = spool_root.join(&job.spool_name);
                if captured.is_file() {
                    remove_spool_file(&captured)?;
                }
            }
        }
        collect_spool_garbage(connection, spool_root)?;
        let mut report = self.status(connection)?;
        report.published = published;
        report.discarded = discarded;
        report.deadline_reached =
            Instant::now() >= deadline && (report.queued > 0 || report.running > 0);
        Ok(report)
    }
}

fn begin_content_publication<'connection>(
    connection: &'connection Connection,
    deadline: Instant,
    cancelled: &AtomicBool,
) -> Result<Option<rusqlite::Transaction<'connection>>, ContentError> {
    loop {
        if Instant::now() >= deadline
            || cancelled.load(Ordering::Relaxed)
            || check_content_cancellation().is_err()
        {
            return Ok(None);
        }
        match rusqlite::Transaction::new_unchecked(connection, TransactionBehavior::Immediate) {
            Ok(transaction) => return Ok(Some(transaction)),
            Err(error) => {
                let error = ContentError::Storage(error);
                if !is_storage_busy(&error) {
                    return Err(error);
                }
                // Retain the completed extraction while another publisher owns
                // SQLite's writer slot, without leaving its job lease stranded.
                std::thread::sleep(
                    Duration::from_millis(10)
                        .min(deadline.saturating_duration_since(Instant::now())),
                );
            }
        }
    }
}

fn is_storage_busy(error: &ContentError) -> bool {
    matches!(error, ContentError::Storage(rusqlite::Error::SqliteFailure(error, _))
        if matches!(error.code, rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked))
}

/// Revision-bound window of indexed text. Location indices are always physical and one-based.
#[derive(Debug, Clone, Serialize)]
pub struct ContentReadResult {
    pub path: String,
    pub format: String,
    pub file_group: String,
    pub desired_revision: String,
    pub served_revision: Option<String>,
    pub continuation_revision: Option<String>,
    pub stale: bool,
    pub coverage: String,
    pub availability: String,
    pub extractor_identity: String,
    pub desired_extractor_identity: String,
    pub metadata: Value,
    pub diagnostics: Value,
    pub original: OriginalContentReference,
    pub total_segments: u64,
    pub offset: usize,
    pub next_offset: Option<usize>,
    pub all_indexed_text_returned: bool,
    pub segments: Vec<ContentReadSegment>,
}
#[derive(Debug, Clone, Serialize)]
pub struct OriginalContentReference {
    pub path: String,
    pub current_revision_matches_served: Option<bool>,
    pub retained_original_revision: bool,
}
#[derive(Debug, Clone, Serialize)]
pub struct ContentReadSegment {
    pub ordinal: u32,
    pub locator: ContentLocator,
    pub text: String,
    pub method: String,
    pub coverage: String,
}
#[derive(Debug, Clone, Serialize)]
pub struct ContentLocator {
    pub kind: String,
    pub start: u32,
    pub end: u32,
}

/// Read a bounded immutable publication, never the current unindexed source text.
pub fn read_content(
    connection: &Connection,
    vault_root: &Path,
    normalized_path: &str,
    offset: usize,
    limit: usize,
    revision: Option<&str>,
) -> Result<ContentReadResult, ContentError> {
    let transaction = if connection.is_autocommit() {
        Some(connection.unchecked_transaction()?)
    } else {
        None
    };
    let connection = transaction.as_deref().unwrap_or(connection);
    if !(1..=1000).contains(&limit) {
        return Err(ContentError::Invalid(
            "content limit must be 1..=1000".to_string(),
        ));
    }
    if offset > i64::MAX as usize {
        return Err(ContentError::Invalid(
            "content offset must fit a nonnegative SQLite integer".to_string(),
        ));
    }
    let file = FilesRepository::get_by_normalized_path(connection, normalized_path)
        .map_err(|error| ContentError::Invalid(error.to_string()))?
        .ok_or_else(|| ContentError::Invalid("file is not included in the index".to_string()))?;
    if file.is_markdown {
        return read_markdown_content(connection, vault_root, &file, offset, limit, revision);
    }
    let document = ContentRepository::get(connection, &file.file_id)?.ok_or_else(|| {
        ContentError::Invalid(
            "indexed extracted content is not available; refresh the index".to_string(),
        )
    })?;
    let continuation_revision = document
        .served_revision
        .as_ref()
        .zip(document.served_extractor_identity.as_ref())
        .map(|(source, extractor)| continuation_token(source, extractor));
    if let Some(revision) = revision
        && continuation_revision.as_deref() != Some(revision)
    {
        return Err(ContentError::Invalid(
            "content revision changed; restart pagination".to_string(),
        ));
    }
    if offset > 0 && revision.is_none() {
        return Err(ContentError::Invalid(
            "continuation requires --revision from the first window".to_string(),
        ));
    }
    let total = ContentRepository::segment_count(connection, &file.file_id)?;
    let records = ContentRepository::segments_bounded(
        connection,
        &file.file_id,
        offset,
        limit,
        MAX_RESPONSE_BYTES,
    )?;
    let mut segments = Vec::new();
    for record in records {
        segments.push(ContentReadSegment {
            ordinal: record.ordinal,
            locator: ContentLocator {
                kind: record.locator_kind,
                start: record.source_start,
                end: record.source_end,
            },
            text: record.text,
            method: record.method,
            coverage: record.coverage,
        });
    }
    let next = (offset + segments.len() < total as usize).then_some(offset + segments.len());
    let revision_stale = document.served_revision.as_deref() != Some(&document.desired_revision)
        || document.served_extractor_identity.as_deref() != Some(&document.extractor_identity);
    // This source check is explicit and bounded. A hash failure is unknown, never a match.
    let matches = document.served_revision.as_ref().and_then(|served| {
        verify_original(
            vault_root,
            Path::new(&file.absolute_path),
            if document.format == "pdf" {
                MAX_PDF_BYTES
            } else {
                MAX_TEXT_BYTES
            },
        )
        .ok()
        .map(|hash| hash == *served)
    });
    let stale = revision_stale || matches == Some(false);
    Ok(ContentReadResult {
        path: normalized_path.to_string(),
        format: document.format,
        file_group: document.file_group,
        desired_revision: document.desired_revision,
        served_revision: document.served_revision,
        continuation_revision,
        stale,
        coverage: document.coverage,
        availability: document.availability,
        extractor_identity: document
            .served_extractor_identity
            .unwrap_or_else(|| document.extractor_identity.clone()),
        desired_extractor_identity: document.extractor_identity,
        metadata: serde_json::from_str(&document.metadata_json)?,
        diagnostics: serde_json::from_str(&document.diagnostics_json)?,
        original: OriginalContentReference {
            path: file.absolute_path,
            current_revision_matches_served: matches,
            retained_original_revision: false,
        },
        total_segments: total,
        offset,
        next_offset: next,
        all_indexed_text_returned: offset == 0 && next.is_none(),
        segments,
    })
}

fn read_markdown_content(
    connection: &Connection,
    vault_root: &Path,
    file: &tao_sdk_storage::FileRecord,
    offset: usize,
    limit: usize,
    revision: Option<&str>,
) -> Result<ContentReadResult, ContentError> {
    let document =
        DocumentsRepository::get_by_file_id(connection, &file.file_id)?.ok_or_else(|| {
            ContentError::Invalid("Markdown revision is not indexed; refresh the index".to_string())
        })?;
    let continuation_revision = continuation_token(
        &document.source_hash,
        &format!("markdown-parser-{}", document.parser_version),
    );
    if revision.is_some_and(|expected| expected != continuation_revision)
        || (offset > 0 && revision.is_none())
    {
        return Err(ContentError::Invalid(
            "content revision missing or changed; restart pagination".to_string(),
        ));
    }
    let total = document.raw_text.split_inclusive('\n').count() as u64;
    let mut bytes = 0;
    let mut segments = Vec::new();
    for (index, line) in document
        .raw_text
        .split_inclusive('\n')
        .enumerate()
        .skip(offset)
        .take(limit)
    {
        if bytes + line.len() > MAX_RESPONSE_BYTES {
            if segments.is_empty() {
                return Err(ContentError::Invalid(
                    "Markdown line exceeds bounded content response".to_string(),
                ));
            }
            break;
        }
        bytes += line.len();
        segments.push(ContentReadSegment {
            ordinal: index as u32 + 1,
            locator: ContentLocator {
                kind: "line".to_string(),
                start: index as u32 + 1,
                end: index as u32 + 1,
            },
            text: line.to_string(),
            method: "markdown".to_string(),
            coverage: "complete".to_string(),
        });
    }
    let next = (offset + segments.len() < total as usize).then_some(offset + segments.len());
    let matches = verify_original(vault_root, Path::new(&file.absolute_path), MAX_TEXT_BYTES)
        .ok()
        .map(|hash| hash == document.source_hash);
    Ok(ContentReadResult {
        path: file.normalized_path.clone(),
        format: "md".to_string(),
        file_group: "document".to_string(),
        desired_revision: file.hash_blake3.clone(),
        served_revision: Some(document.source_hash.clone()),
        continuation_revision: Some(continuation_revision),
        stale: matches != Some(true),
        coverage: "complete".to_string(),
        availability: if matches.is_some() {
            "accessible"
        } else {
            "unavailable"
        }
        .to_string(),
        extractor_identity: format!("markdown-parser-{}", document.parser_version),
        desired_extractor_identity: format!("markdown-parser-{}", document.parser_version),
        metadata: json!({"title":document.title}),
        diagnostics: json!([]),
        original: OriginalContentReference {
            path: file.absolute_path.clone(),
            current_revision_matches_served: matches,
            retained_original_revision: false,
        },
        total_segments: total,
        offset,
        next_offset: next,
        all_indexed_text_returned: offset == 0 && next.is_none(),
        segments,
    })
}

fn continuation_token(source: &str, extractor: &str) -> String {
    let identity = json!([source, extractor]);
    blake3::hash(identity.to_string().as_bytes())
        .to_hex()
        .to_string()
}

fn decode_text(bytes: &[u8]) -> Result<(String, &'static str), ContentError> {
    if bytes.starts_with(&[0xff, 0xfe, 0, 0]) || bytes.starts_with(&[0, 0, 0xfe, 0xff]) {
        return Err(ContentError::Invalid(
            "UTF-32 is not a supported text encoding".to_string(),
        ));
    }
    let (data, encoding) = if bytes.starts_with(&[0xef, 0xbb, 0xbf]) {
        (&bytes[3..], "utf-8-bom")
    } else {
        (bytes, "utf-8")
    };
    if bytes.starts_with(&[0xff, 0xfe]) || bytes.starts_with(&[0xfe, 0xff]) {
        if !bytes.len().is_multiple_of(2) {
            return Err(ContentError::Invalid(
                "invalid UTF-16: odd byte length".to_string(),
            ));
        }
        let little = bytes[0] == 0xff;
        let words = bytes[2..]
            .chunks_exact(2)
            .map(|pair| {
                if little {
                    u16::from_le_bytes([pair[0], pair[1]])
                } else {
                    u16::from_be_bytes([pair[0], pair[1]])
                }
            })
            .collect::<Vec<_>>();
        return String::from_utf16(&words)
            .map(|text| {
                (
                    text,
                    if little {
                        "utf-16le-bom"
                    } else {
                        "utf-16be-bom"
                    },
                )
            })
            .map_err(|_| ContentError::Invalid("invalid UTF-16 surrogate sequence".to_string()));
    }
    if data.contains(&0) {
        return Err(ContentError::Invalid(
            "unsupported text encoding: NUL bytes require a supported BOM-marked encoding"
                .to_string(),
        ));
    }
    String::from_utf8(data.to_vec())
        .map(|text| (text, encoding))
        .map_err(|_| {
            ContentError::Invalid(
                "invalid UTF-8; supported encodings are UTF-8 and BOM-marked UTF-16 LE/BE"
                    .to_string(),
            )
        })
}

fn text_segments(file_id: &str, text: &str) -> Result<Vec<ContentSegmentRecord>, ContentError> {
    let mut result = Vec::new();
    for (index, line) in text.split_inclusive('\n').enumerate() {
        if line.len() > MAX_SEGMENT_BYTES {
            return Err(ContentError::Invalid(
                "text line exceeds 256 KiB extraction limit".to_string(),
            ));
        }
        result.push(ContentSegmentRecord {
            file_id: file_id.to_string(),
            ordinal: index as u32 + 1,
            locator_kind: "line".to_string(),
            source_start: index as u32 + 1,
            source_end: index as u32 + 1,
            text: line.to_string(),
            method: "decode".to_string(),
            coverage: "complete".to_string(),
        });
    }
    Ok(result)
}

fn capture_text(path: &Path, limit: u64) -> Result<(Vec<u8>, String, u64, i64), ContentError> {
    let before = fs::metadata(path)?;
    if before.len() > limit {
        return Err(ContentError::Invalid(format!(
            "source exceeds {limit}-byte content limit"
        )));
    }
    let mut bytes = Vec::new();
    let mut input = open_source(path)?;
    let mut buffer = [0u8; 64 * 1024];
    loop {
        check_content_cancellation()?;
        let count = input.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        if bytes.len() as u64 + count as u64 > limit {
            return Err(ContentError::Invalid(
                "source grew beyond capture limit".to_string(),
            ));
        }
        bytes.extend_from_slice(&buffer[..count]);
    }
    let after = fs::metadata(path)?;
    if bytes.len() as u64 > limit
        || before.len() != after.len()
        || before.modified()? != after.modified()?
        || after.len() != bytes.len() as u64
    {
        return Err(ContentError::Invalid(
            "source changed during capture or exceeded limit".to_string(),
        ));
    }
    let revision = blake3::hash(&bytes).to_hex().to_string();
    Ok((bytes, revision, after.len(), modified_ms(&after)?))
}

fn verify_original(vault_root: &Path, path: &Path, limit: u64) -> Result<String, ContentError> {
    let root = fs::canonicalize(vault_root)?;
    let physical = fs::canonicalize(path)?;
    if !physical.starts_with(root) || !physical.is_file() {
        return Err(ContentError::Invalid(
            "original source is unavailable within the vault".to_string(),
        ));
    }
    hash_bounded(&physical, limit)
}

fn hash_bounded(path: &Path, limit: u64) -> Result<String, ContentError> {
    let before = fs::metadata(path)?;
    if before.len() > limit {
        return Err(ContentError::Invalid(
            "source exceeds digest verification limit".to_string(),
        ));
    }
    let mut file = open_source(path)?;
    let mut buffer = [0u8; 64 * 1024];
    let mut hasher = blake3::Hasher::new();
    let mut total = 0;
    loop {
        check_content_cancellation()?;
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        total += count as u64;
        if total > limit {
            return Err(ContentError::Invalid(
                "source grew beyond digest limit".to_string(),
            ));
        }
        hasher.update(&buffer[..count]);
    }
    let after = fs::metadata(path)?;
    if before.len() != after.len()
        || before.modified()? != after.modified()?
        || total != after.len()
    {
        return Err(ContentError::Invalid(
            "source changed during digest verification".to_string(),
        ));
    }
    Ok(hasher.finalize().to_hex().to_string())
}

fn capture_pdf(path: &Path, spool_root: &Path) -> Result<(String, String, u64, i64), ContentError> {
    let _capture_lock = lock_capture_spool(spool_root)?;
    let before = fs::metadata(path)?;
    if before.len() > MAX_PDF_BYTES {
        return Err(ContentError::Invalid(
            "PDF exceeds 64 MiB input limit".to_string(),
        ));
    }
    if source_spool_bytes(spool_root)?.saturating_add(before.len()) > MAX_SPOOL_BYTES {
        return Err(ContentError::Quota(
            "PDF spool quota exhausted (256 MiB); capture deferred until space is reclaimed"
                .to_string(),
        ));
    }
    let temporary = spool_root.join(format!("capture-{}", uuid::Uuid::new_v4()));
    let result = (|| -> Result<(String, String, u64, i64), ContentError> {
        let mut output = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        let mut input = open_source(path)?;
        let mut buffer = [0u8; 64 * 1024];
        let mut total = 0;
        let mut hasher = blake3::Hasher::new();
        loop {
            check_content_cancellation()?;
            let count = input.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            if total == 0 && !buffer[..count].starts_with(b"%PDF-") {
                return Err(ContentError::Invalid(
                    ".pdf signature does not identify a PDF".to_string(),
                ));
            }
            total += count as u64;
            if total > MAX_PDF_BYTES {
                return Err(ContentError::Invalid(
                    "PDF grew beyond input limit".to_string(),
                ));
            }
            hasher.update(&buffer[..count]);
            output.write_all(&buffer[..count])?;
        }
        output.sync_all()?;
        let after = fs::metadata(path)?;
        if total == 0
            || total != after.len()
            || before.len() != after.len()
            || before.modified()? != after.modified()?
        {
            return Err(ContentError::Invalid(
                "PDF changed during capture".to_string(),
            ));
        }
        let revision = hasher.finalize().to_hex().to_string();
        let name = format!("{revision}-{}.pdf", uuid::Uuid::new_v4());
        let destination = spool_root.join(&name);
        if destination.exists() {
            if fs::symlink_metadata(&destination)?.file_type().is_symlink() {
                return Err(ContentError::Invalid(
                    "symlink in extraction spool".to_string(),
                ));
            }
        } else {
            fs::rename(&temporary, &destination)?;
        }
        Ok((revision, name, after.len(), modified_ms(&after)?))
    })();
    let _ = fs::remove_file(&temporary);
    result
}

/// Runtime state follows the selected database, with separate spool ownership per DB.
pub fn content_spool_root(connection: &Connection, vault_root: &Path) -> PathBuf {
    match connection.path().filter(|path| !path.is_empty()) {
        Some(path) => {
            let path = Path::new(path);
            let canonical = fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
            let identity = blake3::hash(canonical.to_string_lossy().as_bytes()).to_hex();
            canonical
                .parent()
                .unwrap_or(vault_root)
                .join("content-spool")
                .join(&identity.as_str()[..16])
        }
        None => vault_root.join(".tao/content-spool"),
    }
}
fn modified_ms(metadata: &fs::Metadata) -> Result<i64, ContentError> {
    Ok(metadata
        .modified()?
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64)
}
fn spool_captured_pdf(
    capture: &CapturedFile,
    root: &Path,
) -> Result<(String, String), ContentError> {
    let _capture_lock = lock_capture_spool(root)?;
    if capture.bytes.len() as u64 > MAX_PDF_BYTES || !capture.bytes.starts_with(b"%PDF-") {
        return Err(ContentError::Invalid(
            "invalid or oversized PDF capture".to_string(),
        ));
    }
    if source_spool_bytes(root)?.saturating_add(capture.bytes.len() as u64) > MAX_SPOOL_BYTES {
        return Err(ContentError::Quota("PDF spool quota exhausted".to_string()));
    }
    let revision = capture.fingerprint.hash_blake3.clone();
    let name = format!("{revision}-{}.pdf", uuid::Uuid::new_v4());
    let destination = root.join(&name);
    if destination.exists() {
        if fs::symlink_metadata(&destination)?.file_type().is_symlink() {
            return Err(ContentError::Invalid(
                "symlink in extraction spool".to_string(),
            ));
        }
    } else {
        let temporary = root.join(format!("capture-{}", uuid::Uuid::new_v4()));
        let result = (|| -> Result<(), ContentError> {
            let mut output = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&temporary)?;
            output.write_all(&capture.bytes)?;
            output.sync_all()?;
            fs::rename(&temporary, &destination)?;
            Ok(())
        })();
        let _ = fs::remove_file(&temporary);
        result?;
    }
    Ok((revision, name))
}

fn ensure_spool(root: &Path) -> Result<(), ContentError> {
    let mut cursor = Some(root);
    while let Some(path) = cursor {
        if let Ok(metadata) = fs::symlink_metadata(path)
            && metadata.file_type().is_symlink()
        {
            return Err(ContentError::Invalid(
                "extraction spool cannot contain symlink ancestors".to_string(),
            ));
        }
        cursor = path.parent();
    }
    fs::create_dir_all(root)?;
    let marker = root.join(".tao-content-spool-v1");
    if !marker.exists() {
        let result = OpenOptions::new().write(true).create_new(true).open(marker);
        match result {
            Ok(mut file) => file.write_all(b"tao-owned extraction staging v1\n")?,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

// Capture quota belongs to captured originals; extractor scratch has its own cap.
fn source_spool_bytes(root: &Path) -> Result<u64, ContentError> {
    let mut total = 0u64;
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let metadata = match fs::symlink_metadata(entry.path()) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        if metadata.file_type().is_symlink() {
            return Err(ContentError::Invalid(
                "symlink in extraction staging".to_string(),
            ));
        }
        if metadata.is_file() {
            total = total.saturating_add(metadata.len());
        }
    }
    Ok(total)
}

#[cfg(unix)]
fn lock_capture_spool(root: &Path) -> Result<File, ContentError> {
    use std::os::unix::fs::OpenOptionsExt;
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(root.join(".capture.lock"))?;
    if !lock.metadata()?.is_file() {
        return Err(ContentError::Invalid(
            "capture lock is not a regular file".to_string(),
        ));
    }
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        check_content_cancellation()?;
        match rustix::fs::flock(&lock, rustix::fs::FlockOperation::NonBlockingLockExclusive) {
            Ok(()) => return Ok(lock),
            Err(rustix::io::Errno::WOULDBLOCK) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(rustix::io::Errno::WOULDBLOCK) => {
                return Err(ContentError::Quota(
                    "capture is busy; retry deferred".to_string(),
                ));
            }
            Err(error) => return Err(std::io::Error::from(error).into()),
        }
    }
}

#[cfg(not(unix))]
fn lock_capture_spool(_root: &Path) -> Result<File, ContentError> {
    Err(ContentError::Invalid(
        "PDF capture requires Unix file locking".to_string(),
    ))
}

fn directory_bytes(root: &Path) -> Result<u64, ContentError> {
    let mut total = 0u64;
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let metadata = entry.file_type()?;
        if metadata.is_symlink() {
            return Err(ContentError::Invalid(
                "symlink in extraction staging".to_string(),
            ));
        }
        total = total.saturating_add(if metadata.is_dir() {
            directory_bytes(&entry.path())?
        } else {
            entry.metadata()?.len()
        });
    }
    Ok(total)
}

fn remove_spool_file(path: &Path) -> Result<(), ContentError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn collect_spool_garbage(connection: &Connection, root: &Path) -> Result<(), ContentError> {
    let retained = ContentRepository::retained_spools(connection)?
        .into_iter()
        .collect::<BTreeSet<_>>();
    let retired = ContentRepository::retired_spools(connection)?
        .into_iter()
        .collect::<BTreeSet<_>>();
    let active = ContentRepository::active_tokens(connection, now_ms())?
        .into_iter()
        .map(|token| format!("job-{token}"))
        .collect::<BTreeSet<_>>();
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        let metadata = match entry.metadata() {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        if entry.file_type()?.is_symlink() {
            continue;
        }
        let age = metadata.modified()?.elapsed().unwrap_or_default();
        // Fresh unpublished captures remain protected across the prepare/commit boundary.
        if name.ends_with(".pdf")
            && (name.len() == 68 || name.len() == 105)
            && !retained.contains(&name)
            && (retired.contains(&name) || age > Duration::from_secs(3600))
        {
            remove_spool_file(&entry.path())?;
        }
        if name.starts_with("capture-") && age > Duration::from_secs(3600) {
            remove_spool_file(&entry.path())?;
        }
        if name.starts_with("job-")
            && !active.contains(&name)
            && age > Duration::from_secs(600)
            && metadata.is_dir()
        {
            match fs::remove_dir_all(entry.path()) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
    }
    for name in retired {
        if !root.join(&name).exists() {
            ContentRepository::prune_superseded_spool(connection, &name)?;
        }
    }
    Ok(())
}
fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

/// Bounded Markdown-only listing. `offset` is a fresh window; content continuation uses revisions.
#[derive(Debug, Serialize)]
pub struct DocumentListResult {
    pub total: u64,
    pub offset: usize,
    pub next_offset: Option<usize>,
    pub items: Vec<DocumentListItem>,
}
#[derive(Debug, Serialize)]
pub struct DocumentListItem {
    pub file_id: String,
    pub path: String,
    pub title: String,
    pub updated_at: String,
}
pub fn list_documents(
    connection: &Connection,
    offset: usize,
    limit: u32,
) -> Result<DocumentListResult, ContentError> {
    if !(1..=1000).contains(&limit) || offset > i64::MAX as usize {
        return Err(ContentError::Invalid(
            "doc list limit must be1..=1000 and offset fit SQLite integer".to_string(),
        ));
    }
    let transaction = if connection.is_autocommit() {
        Some(connection.unchecked_transaction()?)
    } else {
        None
    };
    let connection = transaction.as_deref().unwrap_or(connection);
    let total = ContentRepository::markdown_count(connection)?;
    let items = ContentRepository::markdown_page(connection, offset, limit)?
        .into_iter()
        .map(|record| DocumentListItem {
            file_id: record.file_id,
            title: if record.title.is_empty() {
                tao_sdk_core::note_title_from_path(&record.path)
            } else {
                record.title
            },
            path: record.path,
            updated_at: record.updated_at,
        })
        .collect::<Vec<_>>();
    let next_offset = (offset + items.len() < total as usize).then_some(offset + items.len());
    Ok(DocumentListResult {
        total,
        offset,
        next_offset,
        items,
    })
}

fn open_source(path: &Path) -> Result<File, ContentError> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NONBLOCK | libc::O_NOFOLLOW);
    }
    let file = options.open(path)?;
    if !file.metadata()?.is_file() {
        return Err(ContentError::Invalid(
            "content source must remain a regular file".to_string(),
        ));
    }
    Ok(file)
}

fn check_content_cancellation() -> Result<(), ContentError> {
    crate::check_index_cancellation().map_err(|error| ContentError::Invalid(error.to_string()))
}
