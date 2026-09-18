//! Persistent extracted content and leased work. All SQL stays in the storage layer.

use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};

/// Cross-process admission limit for PDF jobs sharing one database.
pub const MAX_EXTRACTION_WORKERS: usize = 8;

/// Current desired/served content revision and its explicit coverage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContentDocumentRecord {
    pub file_id: String,
    pub format: String,
    pub file_group: String,
    pub observed_size: u64,
    pub observed_modified_ms: i64,
    pub desired_revision: String,
    pub served_revision: Option<String>,
    pub served_extractor_identity: Option<String>,
    pub extractor_identity: String,
    pub coverage: String,
    pub availability: String,
    pub metadata_json: String,
    pub diagnostics_json: String,
}

/// One indexed physical source location, including empty/failed pages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContentSegmentRecord {
    pub file_id: String,
    pub ordinal: u32,
    pub locator_kind: String,
    pub source_start: u32,
    pub source_end: u32,
    pub text: String,
    pub method: String,
    pub coverage: String,
}

/// Deduplicated extraction job; a lease token fences obsolete workers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtractionJobRecord {
    pub job_id: String,
    pub file_id: String,
    pub desired_revision: String,
    pub extractor_identity: String,
    pub spool_name: String,
    pub state: String,
    pub lease_token: Option<String>,
    pub lease_until_ms: i64,
    pub attempts: u32,
    pub next_attempt_ms: i64,
    pub diagnostic: String,
}

/// Persistent extraction queue totals.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ContentQueueCounts {
    pub queued: u64,
    pub running: u64,
    pub failed: u64,
    pub complete: u64,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct ContentRepository;

impl ContentRepository {
    /// Bounded Markdown-only list; source metadata and document titles share the snapshot.
    pub fn markdown_page(
        connection: &Connection,
        offset: usize,
        limit: u32,
    ) -> rusqlite::Result<Vec<IndexedDocumentListRecord>> {
        let offset = i64::try_from(offset).map_err(|_| {
            rusqlite::Error::InvalidParameterName("offset exceeds i64::MAX".to_string())
        })?;
        let mut statement=connection.prepare("SELECT f.file_id,f.normalized_path,COALESCE(d.title,''),f.indexed_at FROM files f LEFT JOIN canonical_documents d ON d.file_id=f.file_id WHERE f.is_markdown=1 ORDER BY f.normalized_path LIMIT ?1 OFFSET ?2")?;
        statement
            .query_map(params![limit, offset], |row| {
                Ok(IndexedDocumentListRecord {
                    file_id: row.get(0)?,
                    path: row.get(1)?,
                    title: row.get(2)?,
                    updated_at: row.get(3)?,
                })
            })?
            .collect()
    }
    pub fn markdown_count(connection: &Connection) -> rusqlite::Result<u64> {
        connection.query_row(
            "SELECT COUNT(*) FROM files WHERE is_markdown=1",
            [],
            |row| row.get(0),
        )
    }

    pub fn get(
        connection: &Connection,
        file_id: &str,
    ) -> rusqlite::Result<Option<ContentDocumentRecord>> {
        connection.query_row("SELECT file_id,format,file_group,observed_size,observed_modified_ms,desired_revision,served_revision,served_extractor_identity,extractor_identity,coverage,availability,metadata_json,diagnostics_json FROM content_documents WHERE file_id=?1", [file_id], document_row).optional()
    }

    pub fn upsert(
        connection: &Connection,
        document: &ContentDocumentRecord,
    ) -> rusqlite::Result<()> {
        connection.execute("INSERT INTO content_documents(file_id,format,file_group,observed_size,observed_modified_ms,desired_revision,served_revision,served_extractor_identity,extractor_identity,coverage,availability,metadata_json,diagnostics_json) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13) ON CONFLICT(file_id) DO UPDATE SET format=excluded.format,file_group=excluded.file_group,observed_size=excluded.observed_size,observed_modified_ms=excluded.observed_modified_ms,desired_revision=excluded.desired_revision,served_revision=excluded.served_revision,served_extractor_identity=excluded.served_extractor_identity,extractor_identity=excluded.extractor_identity,coverage=excluded.coverage,availability=excluded.availability,metadata_json=excluded.metadata_json,diagnostics_json=excluded.diagnostics_json", params![document.file_id,document.format,document.file_group,document.observed_size,document.observed_modified_ms,document.desired_revision,document.served_revision,document.served_extractor_identity,document.extractor_identity,document.coverage,document.availability,document.metadata_json,document.diagnostics_json])?;
        connection.execute("UPDATE extraction_jobs SET state='superseded',lease_token=NULL WHERE file_id=?1 AND (desired_revision<>?2 OR extractor_identity<>?3) AND state<>'superseded'", params![document.file_id,document.desired_revision,document.extractor_identity])?;
        connection.execute(
            "DELETE FROM extraction_staged_pages WHERE job_id IN (SELECT job_id FROM extraction_jobs WHERE file_id=?1 AND state='superseded')",
            [&document.file_id],
        )?;
        Ok(())
    }

    pub fn replace_segments(
        connection: &Connection,
        file_id: &str,
        segments: &[ContentSegmentRecord],
    ) -> rusqlite::Result<()> {
        connection.execute("DELETE FROM content_segments WHERE file_id=?1", [file_id])?;
        let mut statement = connection.prepare_cached("INSERT INTO content_segments(file_id,ordinal,locator_kind,source_start,source_end,text,method,coverage) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)")?;
        for segment in segments {
            statement.execute(params![
                file_id,
                segment.ordinal,
                segment.locator_kind,
                segment.source_start,
                segment.source_end,
                segment.text,
                segment.method,
                segment.coverage
            ])?;
        }
        Ok(())
    }

    pub fn segments(
        connection: &Connection,
        file_id: &str,
        offset: usize,
        limit: usize,
    ) -> rusqlite::Result<Vec<ContentSegmentRecord>> {
        Self::segments_bounded(connection, file_id, offset, limit, usize::MAX)
    }

    /// Stop before allocating text beyond the requested response window.
    pub fn segments_bounded(
        connection: &Connection,
        file_id: &str,
        offset: usize,
        limit: usize,
        max_text_bytes: usize,
    ) -> rusqlite::Result<Vec<ContentSegmentRecord>> {
        let offset = i64::try_from(offset).map_err(|_| {
            rusqlite::Error::InvalidParameterName("offset exceeds i64::MAX".to_string())
        })?;
        let limit = i64::try_from(limit).map_err(|_| {
            rusqlite::Error::InvalidParameterName("limit exceeds i64::MAX".to_string())
        })?;
        let mut statement=connection.prepare("SELECT file_id,ordinal,locator_kind,source_start,source_end,text,method,coverage,length(CAST(text AS BLOB)) FROM content_segments WHERE file_id=?1 ORDER BY ordinal LIMIT ?2 OFFSET ?3")?;
        let mut rows = statement.query(params![file_id, limit, offset])?;
        let mut records = Vec::new();
        let mut bytes = 0usize;
        while let Some(row) = rows.next()? {
            let text_bytes: usize = row.get(8)?;
            if text_bytes > max_text_bytes.saturating_sub(bytes) {
                if records.is_empty() {
                    return Err(rusqlite::Error::InvalidParameterName(
                        "content segment exceeds requested byte window".to_string(),
                    ));
                }
                break;
            }
            records.push(segment_row(row)?);
            bytes += text_bytes;
        }
        Ok(records)
    }

    pub fn segment_count(connection: &Connection, file_id: &str) -> rusqlite::Result<u64> {
        connection.query_row(
            "SELECT COUNT(*) FROM content_segments WHERE file_id=?1",
            [file_id],
            |row| row.get(0),
        )
    }

    pub fn enqueue(connection: &Connection, job: &ExtractionJobRecord) -> rusqlite::Result<()> {
        connection.execute("INSERT INTO extraction_jobs(job_id,file_id,desired_revision,extractor_identity,spool_name,state) VALUES(?1,?2,?3,?4,?5,'queued') ON CONFLICT(file_id,desired_revision,extractor_identity) DO UPDATE SET spool_name=excluded.spool_name,state='queued',attempts=0,next_attempt_ms=0,lease_token=NULL,lease_until_ms=0,diagnostic='' WHERE extraction_jobs.state='superseded'", params![job.job_id,job.file_id,job.desired_revision,job.extractor_identity,job.spool_name])?;
        Ok(())
    }

    /// Atomically claim a distinct job within the cross-process worker limit.
    pub fn claim(
        connection: &Connection,
        now_ms: i64,
        lease_until_ms: i64,
        token: &str,
    ) -> rusqlite::Result<Option<ExtractionJobRecord>> {
        Self::recover_expired(connection, now_ms)?;
        connection.query_row("UPDATE extraction_jobs SET state='running',lease_token=?1,lease_until_ms=?2,attempts=attempts+1 WHERE job_id=(SELECT j.job_id FROM extraction_jobs j JOIN content_documents d ON d.file_id=j.file_id WHERE j.state IN ('queued','retry') AND j.next_attempt_ms<=?3 AND j.attempts<3 AND j.desired_revision=d.desired_revision AND j.extractor_identity=d.extractor_identity ORDER BY j.next_attempt_ms,j.job_id LIMIT 1) AND (SELECT COUNT(*) FROM extraction_jobs WHERE state='running' AND lease_until_ms>?3) < ?4 RETURNING job_id,file_id,desired_revision,extractor_identity,spool_name,state,lease_token,lease_until_ms,attempts,next_attempt_ms,diagnostic",params![token,lease_until_ms,now_ms,MAX_EXTRACTION_WORKERS],job_row).optional()
    }

    /// Expired terminal leases release staging and publish a precise failed document state.
    /// The caller can enclose this and derived-corpus refresh in one writer transaction.
    pub fn recover_expired(connection: &Connection, now_ms: i64) -> rusqlite::Result<Vec<String>> {
        let transaction = if connection.is_autocommit() {
            Some(Transaction::new_unchecked(
                connection,
                TransactionBehavior::Immediate,
            )?)
        } else {
            None
        };
        let affected=connection.prepare("SELECT file_id FROM extraction_jobs WHERE state='running' AND lease_until_ms<=?1 AND attempts>=3")?.query_map([now_ms],|row|row.get(0))?.collect::<rusqlite::Result<Vec<String>>>()?;
        connection.execute("UPDATE content_documents SET coverage='failed',diagnostics_json=json_array('worker lease expired after 3 attempts') WHERE file_id IN (SELECT file_id FROM extraction_jobs WHERE state='running' AND lease_until_ms<=?1 AND attempts>=3)",[now_ms])?;
        connection.execute("DELETE FROM extraction_staged_pages WHERE job_id IN (SELECT job_id FROM extraction_jobs WHERE state='running' AND lease_until_ms<=?1 AND attempts>=3)",[now_ms])?;
        connection.execute("UPDATE extraction_jobs SET state=CASE WHEN attempts>=3 THEN 'failed' ELSE 'retry' END,lease_token=NULL,lease_until_ms=0,diagnostic='worker lease expired' WHERE state='running' AND lease_until_ms<=?1", [now_ms])?;
        if let Some(transaction) = transaction {
            transaction.commit()?;
        }
        Ok(affected)
    }

    pub fn is_current(
        connection: &Connection,
        job: &ExtractionJobRecord,
        now_ms: i64,
    ) -> rusqlite::Result<bool> {
        connection.query_row("SELECT EXISTS(SELECT 1 FROM extraction_jobs j JOIN content_documents d ON d.file_id=j.file_id WHERE j.job_id=?1 AND j.state='running' AND j.lease_token=?2 AND j.lease_until_ms>?3 AND j.desired_revision=d.desired_revision AND j.extractor_identity=d.extractor_identity)",params![job.job_id,job.lease_token,now_ms],|row|row.get(0))
    }

    /// Resume already extracted physical pages without publishing staging to readers.
    pub fn staged_pages(
        connection: &Connection,
        job: &ExtractionJobRecord,
    ) -> rusqlite::Result<Vec<ContentSegmentRecord>> {
        let mut statement=connection.prepare("SELECT ordinal,text,method,coverage FROM extraction_staged_pages WHERE job_id=?1 ORDER BY ordinal")?;
        statement
            .query_map([&job.job_id], |row| {
                let ordinal: u32 = row.get(0)?;
                Ok(ContentSegmentRecord {
                    file_id: job.file_id.clone(),
                    ordinal,
                    locator_kind: "page".to_string(),
                    source_start: ordinal,
                    source_end: ordinal,
                    text: row.get(1)?,
                    method: row.get(2)?,
                    coverage: row.get(3)?,
                })
            })?
            .collect()
    }
    pub fn stage_page(
        connection: &Connection,
        job: &ExtractionJobRecord,
        now_ms: i64,
        page: &ContentSegmentRecord,
    ) -> rusqlite::Result<bool> {
        let transaction = if connection.is_autocommit() {
            Some(Transaction::new_unchecked(
                connection,
                TransactionBehavior::Immediate,
            )?)
        } else {
            None
        };
        if !Self::is_current(connection, job, now_ms)? {
            return Ok(false);
        }
        let bytes: u64 = connection.query_row(
            "SELECT bytes_total FROM extraction_stage_usage WHERE singleton=1",
            [],
            |row| row.get(0),
        )?;
        let previous:u64=connection.query_row("SELECT length(CAST(text AS BLOB)) FROM extraction_staged_pages WHERE job_id=?1 AND ordinal=?2",params![job.job_id,page.ordinal],|row|row.get(0)).optional()?.unwrap_or(0);
        if bytes
            .saturating_sub(previous)
            .saturating_add(page.text.len() as u64)
            > 64 * 1024 * 1024
        {
            return Err(rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_FULL),
                Some("extraction text staging exceeds 64 MiB quota".to_string()),
            ));
        }
        connection.execute("INSERT INTO extraction_staged_pages(job_id,ordinal,text,method,coverage) VALUES(?1,?2,?3,?4,?5) ON CONFLICT(job_id,ordinal) DO UPDATE SET text=excluded.text,method=excluded.method,coverage=excluded.coverage",params![job.job_id,page.ordinal,page.text,page.method,page.coverage])?;
        if let Some(transaction) = transaction {
            transaction.commit()?;
        }
        Ok(true)
    }

    /// Cancellation does not consume a retry attempt or strand a lease.
    pub fn release(
        connection: &Connection,
        job: &ExtractionJobRecord,
        diagnostic: &str,
        retry_at: i64,
    ) -> rusqlite::Result<()> {
        connection.execute("UPDATE extraction_jobs SET state='retry',attempts=MAX(0,attempts-1),lease_token=NULL,lease_until_ms=0,diagnostic=?1,next_attempt_ms=?2 WHERE job_id=?3 AND lease_token=?4 AND state='running'",params![diagnostic,retry_at,job.job_id,job.lease_token])?;
        Ok(())
    }

    pub fn finish(
        connection: &Connection,
        job: &ExtractionJobRecord,
        state: &str,
        diagnostic: &str,
        retry_at: i64,
    ) -> rusqlite::Result<()> {
        let transaction = if connection.is_autocommit() {
            Some(Transaction::new_unchecked(
                connection,
                TransactionBehavior::Immediate,
            )?)
        } else {
            None
        };
        let changed = connection.execute("UPDATE extraction_jobs SET state=?1,diagnostic=?2,next_attempt_ms=?3,lease_token=NULL,lease_until_ms=0 WHERE job_id=?4 AND lease_token=?5 AND state='running'",params![state,diagnostic,retry_at,job.job_id,job.lease_token])?;
        if changed > 0 && (state == "done" || state == "failed" || state == "superseded") {
            connection.execute(
                "DELETE FROM extraction_staged_pages WHERE job_id=?1",
                [&job.job_id],
            )?;
        }
        if let Some(transaction) = transaction {
            transaction.commit()?;
        }
        Ok(())
    }

    /// Coverage counts grouped without materializing content or source bytes.
    pub fn coverage_stats(connection: &Connection) -> rusqlite::Result<Vec<(String, u64)>> {
        let mut statement = connection.prepare(
            "SELECT coverage,COUNT(*) FROM content_documents GROUP BY coverage ORDER BY coverage",
        )?;
        statement
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect()
    }

    pub fn counts(connection: &Connection) -> rusqlite::Result<ContentQueueCounts> {
        connection.query_row("SELECT COALESCE(SUM(state IN ('queued','retry')),0),COALESCE(SUM(state='running'),0),COALESCE(SUM(state='failed'),0),COALESCE(SUM(state='done'),0) FROM extraction_jobs",[],|row|Ok(ContentQueueCounts{queued:row.get(0)?,running:row.get(1)?,failed:row.get(2)?,complete:row.get(3)?}))
    }

    pub fn deferred_count(connection: &Connection) -> rusqlite::Result<u64> {
        connection.query_row(
            "SELECT COUNT(*) FROM content_documents WHERE availability='deferred'",
            [],
            |row| row.get(0),
        )
    }
    pub fn deferred_file(connection: &Connection) -> rusqlite::Result<Option<String>> {
        connection.query_row("SELECT file_id FROM content_documents WHERE availability='deferred' ORDER BY observed_size,file_id LIMIT 1",[],|row|row.get(0)).optional()
    }
    pub fn retired_spools(connection: &Connection) -> rusqlite::Result<Vec<String>> {
        connection.prepare("SELECT spool_name FROM extraction_jobs WHERE state IN ('done','failed','superseded')")?.query_map([],|row|row.get(0))?.collect()
    }
    /// Keep superseded source names until their capture has actually been reclaimed.
    pub fn prune_superseded_spool(
        connection: &Connection,
        spool_name: &str,
    ) -> rusqlite::Result<()> {
        connection.execute(
            "DELETE FROM extraction_jobs WHERE spool_name=?1 AND state='superseded'",
            [spool_name],
        )?;
        Ok(())
    }

    /// Sources and scratch directories owned by still-recoverable jobs.
    pub fn retained_spools(connection: &Connection) -> rusqlite::Result<Vec<String>> {
        let mut statement=connection.prepare("SELECT DISTINCT spool_name FROM extraction_jobs WHERE state IN ('queued','retry','running')")?;
        statement.query_map([], |row| row.get(0))?.collect()
    }

    pub fn active_tokens(connection: &Connection, now_ms: i64) -> rusqlite::Result<Vec<String>> {
        let mut statement=connection.prepare("SELECT lease_token FROM extraction_jobs WHERE state='running' AND lease_until_ms>?1 AND lease_token IS NOT NULL")?;
        statement.query_map([now_ms], |row| row.get(0))?.collect()
    }
}

fn document_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ContentDocumentRecord> {
    Ok(ContentDocumentRecord {
        file_id: row.get(0)?,
        format: row.get(1)?,
        file_group: row.get(2)?,
        observed_size: row.get(3)?,
        observed_modified_ms: row.get(4)?,
        desired_revision: row.get(5)?,
        served_revision: row.get(6)?,
        served_extractor_identity: row.get(7)?,
        extractor_identity: row.get(8)?,
        coverage: row.get(9)?,
        availability: row.get(10)?,
        metadata_json: row.get(11)?,
        diagnostics_json: row.get(12)?,
    })
}
fn segment_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ContentSegmentRecord> {
    Ok(ContentSegmentRecord {
        file_id: row.get(0)?,
        ordinal: row.get(1)?,
        locator_kind: row.get(2)?,
        source_start: row.get(3)?,
        source_end: row.get(4)?,
        text: row.get(5)?,
        method: row.get(6)?,
        coverage: row.get(7)?,
    })
}
fn job_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ExtractionJobRecord> {
    Ok(ExtractionJobRecord {
        job_id: row.get(0)?,
        file_id: row.get(1)?,
        desired_revision: row.get(2)?,
        extractor_identity: row.get(3)?,
        spool_name: row.get(4)?,
        state: row.get(5)?,
        lease_token: row.get(6)?,
        lease_until_ms: row.get(7)?,
        attempts: row.get(8)?,
        next_attempt_ms: row.get(9)?,
        diagnostic: row.get(10)?,
    })
}

/// Bounded list projection, separate from full captured text.
#[derive(Debug, Clone)]
pub struct IndexedDocumentListRecord {
    pub file_id: String,
    pub path: String,
    pub title: String,
    pub updated_at: String,
}
