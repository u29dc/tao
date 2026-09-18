//! Captured source revisions. All derived projections use these committed bytes.
use rusqlite::{Connection, OptionalExtension, params, params_from_iter};

/// Captured Markdown source and parsed structural facts for one revision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocumentRecordInput {
    /// Owning inventory identifier.
    pub file_id: String,
    /// Hash of the captured original bytes.
    pub source_hash: String,
    /// Version of the parser/structural encoding.
    pub parser_version: u32,
    /// Original text, including frontmatter.
    pub raw_text: String,
    /// Body with frontmatter excluded.
    pub body_text: String,
    /// Display title derived from this revision.
    pub title: String,
    /// Versioned serialized links, headings and blocks.
    pub structure_json: String,
}
/// Persisted captured document.
pub type DocumentRecord = DocumentRecordInput;
/// Lightweight structure projection without document text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocumentStructureRecord {
    /// Owning file.
    pub file_id: String,
    /// Captured source hash.
    pub source_hash: String,
    /// Structural format version.
    pub parser_version: u32,
    /// Parsed structures.
    pub structure_json: String,
}
/// Canonical document repository.
#[derive(Debug, Default, Clone, Copy)]
pub struct DocumentsRepository;
impl DocumentsRepository {
    /// Store the source revision inside its caller's publication transaction.
    pub fn upsert(c: &Connection, r: &DocumentRecordInput) -> rusqlite::Result<()> {
        c.prepare_cached("INSERT INTO canonical_documents(file_id,source_hash,parser_version,raw_text,body_text,title,structure_json) VALUES(?1,?2,?3,?4,?5,?6,?7) ON CONFLICT(file_id) DO UPDATE SET source_hash=excluded.source_hash,parser_version=excluded.parser_version,raw_text=excluded.raw_text,body_text=excluded.body_text,title=excluded.title,structure_json=excluded.structure_json")?.execute(params![r.file_id,r.source_hash,r.parser_version,r.raw_text,r.body_text,r.title,r.structure_json])?;
        Ok(())
    }
    /// Read one source revision.
    pub fn get_by_file_id(c: &Connection, id: &str) -> rusqlite::Result<Option<DocumentRecord>> {
        c.query_row(
            "SELECT * FROM canonical_documents WHERE file_id=?1",
            [id],
            read,
        )
        .optional()
    }
    /// Read all captured documents in stable identifier order.
    pub fn list_all(c: &Connection) -> rusqlite::Result<Vec<DocumentRecord>> {
        c.prepare("SELECT * FROM canonical_documents ORDER BY file_id")?
            .query_map([], read)?
            .collect()
    }
    /// Read selected documents with a fixed parameter bound.
    pub fn list_by_file_ids(
        c: &Connection,
        ids: &[String],
    ) -> rusqlite::Result<Vec<DocumentRecord>> {
        let mut rows = Vec::new();
        for chunk in ids.chunks(crate::SQL_PARAMETER_CHUNK) {
            let sql = format!(
                "SELECT * FROM canonical_documents WHERE file_id IN ({})",
                vec!["?"; chunk.len()].join(",")
            );
            rows.extend(
                c.prepare(&sql)?
                    .query_map(params_from_iter(chunk), read)?
                    .collect::<rusqlite::Result<Vec<_>>>()?,
            );
        }
        rows.sort_by(|a, b| a.file_id.cmp(&b.file_id));
        rows.dedup_by(|a, b| a.file_id == b.file_id);
        Ok(rows)
    }
    /// Read only persisted structure for unchanged-note link resolution.
    pub fn list_structures(c: &Connection) -> rusqlite::Result<Vec<DocumentStructureRecord>> {
        c.prepare("SELECT file_id,source_hash,parser_version,structure_json FROM canonical_documents ORDER BY file_id")?.query_map([],|r|Ok(DocumentStructureRecord{file_id:r.get(0)?,source_hash:r.get(1)?,parser_version:r.get(2)?,structure_json:r.get(3)?}))?.collect()
    }
}
fn read(r: &rusqlite::Row<'_>) -> rusqlite::Result<DocumentRecord> {
    Ok(DocumentRecord {
        file_id: r.get("file_id")?,
        source_hash: r.get("source_hash")?,
        parser_version: r.get("parser_version")?,
        raw_text: r.get("raw_text")?,
        body_text: r.get("body_text")?,
        title: r.get("title")?,
        structure_json: r.get("structure_json")?,
    })
}
/// One source failure, separate from the last served valid revision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileDiagnosticInput {
    /// Vault-relative source path.
    pub path: String,
    /// Known inventory identifier, if present.
    pub file_id: Option<String>,
    /// Stable failure category.
    pub kind: String,
    /// Human-readable failure context.
    pub message: String,
}
/// Durable per-file diagnostic operations.
#[derive(Debug, Default, Clone, Copy)]
pub struct DiagnosticsRepository;
impl DiagnosticsRepository {
    /// Upsert the latest observed failure.
    pub fn upsert(c: &Connection, r: &FileDiagnosticInput) -> rusqlite::Result<()> {
        c.execute("INSERT INTO file_diagnostics(path,file_id,kind,message) VALUES(?1,?2,?3,?4) ON CONFLICT(path,kind) DO UPDATE SET file_id=excluded.file_id,kind=excluded.kind,message=excluded.message,observed_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')",params![r.path,r.file_id,r.kind,r.message])?;
        Ok(())
    }
    /// Remove a recovered path's diagnostic.
    pub fn clear_for_path(c: &Connection, path: &str) -> rusqlite::Result<()> {
        c.execute("DELETE FROM file_diagnostics WHERE path=?1", [path])?;
        Ok(())
    }
    /// Read all outstanding diagnostics in stable order.
    pub fn list_all(c: &Connection) -> rusqlite::Result<Vec<FileDiagnosticInput>> {
        c.prepare("SELECT path,file_id,kind,message FROM file_diagnostics ORDER BY path,kind")?
            .query_map([], |r| {
                Ok(FileDiagnosticInput {
                    path: r.get(0)?,
                    file_id: r.get(1)?,
                    kind: r.get(2)?,
                    message: r.get(3)?,
                })
            })?
            .collect()
    }
}
