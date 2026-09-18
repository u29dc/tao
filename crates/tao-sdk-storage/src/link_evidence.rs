//! Occurrence provenance independent of compatibility link row contracts.
use rusqlite::{Connection, OptionalExtension, params};
/// Resolution evidence for one original source occurrence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinkEvidenceInput {
    /// Stable link occurrence identifier.
    pub link_id: String,
    /// Original UTF-8 start byte offset.
    pub source_start: u64,
    /// Original UTF-8 end byte offset.
    pub source_end: u64,
    /// One-based source line.
    pub line: u64,
    /// One-based final source line.
    pub end_line: u64,
    /// Complete original source expression, including label/embed syntax.
    pub raw_expression: String,
    /// Original link syntax.
    pub syntax: String,
    /// Typed fragment JSON.
    pub fragment_json: String,
    /// Independent fragment validation outcome.
    pub fragment_status: String,
    /// Deterministic document resolution rule.
    pub resolution_rule: String,
    /// All candidate document paths when ambiguous.
    pub candidates_json: String,
}
/// Typed occurrence evidence persistence.
#[derive(Debug, Default, Clone, Copy)]
pub struct LinkEvidenceRepository;
impl LinkEvidenceRepository {
    /// Persist an occurrence in the canonical publication transaction.
    pub fn upsert(c: &Connection, r: &LinkEvidenceInput) -> rusqlite::Result<()> {
        c.execute("INSERT INTO link_evidence(link_id,source_start,source_end,line,end_line,raw_expression,syntax,fragment_json,fragment_status,resolution_rule,candidates_json) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11) ON CONFLICT(link_id) DO UPDATE SET source_start=excluded.source_start,source_end=excluded.source_end,line=excluded.line,end_line=excluded.end_line,raw_expression=excluded.raw_expression,syntax=excluded.syntax,fragment_json=excluded.fragment_json,fragment_status=excluded.fragment_status,resolution_rule=excluded.resolution_rule,candidates_json=excluded.candidates_json",params![r.link_id,r.source_start,r.source_end,r.line,r.end_line,r.raw_expression,r.syntax,r.fragment_json,r.fragment_status,r.resolution_rule,r.candidates_json])?;
        Ok(())
    }
    /// Read persisted evidence where the source revision supplied it.
    pub fn get_by_link_id(c: &Connection, id: &str) -> rusqlite::Result<Option<LinkEvidenceInput>> {
        c.query_row("SELECT * FROM link_evidence WHERE link_id=?1", [id], |r| {
            Ok(LinkEvidenceInput {
                link_id: r.get("link_id")?,
                source_start: r.get("source_start")?,
                source_end: r.get("source_end")?,
                line: r.get("line")?,
                end_line: r.get("end_line")?,
                raw_expression: r.get("raw_expression")?,
                syntax: r.get("syntax")?,
                fragment_json: r.get("fragment_json")?,
                fragment_status: r.get("fragment_status")?,
                resolution_rule: r.get("resolution_rule")?,
                candidates_json: r.get("candidates_json")?,
            })
        })
        .optional()
    }
}
