//! Capacity accounting for the retained, pre-publication source work set.
//!
//! This is an admission limit, not a process RSS promise: SQLite, parser scratch,
//! allocator overhead and one bounded current source have independent lifetimes.
use super::*;

pub(super) const MAX_PREPARATION_BYTES: usize = 256 * 1024 * 1024;

#[derive(Default, Serialize)]
pub(super) struct IndexWork {
    pub markdown_parses: u64,
    pub source_captures: u64,
    pub source_bytes_captured: u64,
    pub canonical_structures_loaded: u64,
    pub canonical_structures_reparsed: u64,
    pub graph_sources_resolved: u64,
    pub publication_transactions: u64,
    /// Peak retained preparation charge, including captures and projections.
    pub prepared_bytes: usize,
    pub peak_retained_capture_bytes: usize,
}

pub(super) fn admit(
    bytes: usize,
    limit: usize,
    work: &mut IndexWork,
) -> Result<(), FullIndexError> {
    work.prepared_bytes = work.prepared_bytes.max(bytes);
    if bytes > limit {
        return Err(FullIndexError::PreparationBudgetExceeded {
            required_bytes: bytes,
            limit_bytes: limit,
        });
    }
    Ok(())
}

fn strings<const N: usize>(values: [&String; N]) -> usize {
    values.into_iter().map(String::capacity).sum()
}
fn optional(value: &Option<String>) -> usize {
    value.as_ref().map_or(0, String::capacity)
}

pub(super) fn capture_bytes(capture: &CapturedFile) -> usize {
    let value = &capture.fingerprint;
    capture.bytes.capacity()
        + value.absolute.capacity()
        + value.relative.capacity()
        + strings([&value.normalized, &value.match_key, &value.hash_blake3])
}

pub(super) fn change_bytes(change: &IndexChange) -> usize {
    match change {
        IndexChange::Remove { normalized_path } => normalized_path.capacity(),
        IndexChange::Upsert { entry, captured } => {
            size_of::<VaultManifestEntry>()
                + entry.absolute.capacity()
                + entry.relative.capacity()
                + strings([&entry.normalized, &entry.match_key])
                + captured.as_ref().map_or(0, capture_bytes)
        }
    }
}

pub(super) fn diagnostic_bytes(value: &FileDiagnosticInput) -> usize {
    size_of::<FileDiagnosticInput>() * 4
        + strings([&value.path, &value.kind, &value.message])
        + optional(&value.file_id)
}

pub(super) fn prepared_bytes(value: &PreparedIndexEntry) -> usize {
    // Four slots cover initial minimum capacity and amortized Vec growth, without
    // rewalking all earlier entries. This intentionally overcharges spare slots.
    let file = &value.file_record;
    let mut bytes = size_of::<PreparedIndexEntry>() * 4
        + strings([
            &file.file_id,
            &file.normalized_path,
            &file.match_key,
            &file.absolute_path,
            &file.hash_blake3,
        ]);
    if let Some(doc) = &value.document_record {
        bytes += strings([
            &doc.file_id,
            &doc.source_hash,
            &doc.raw_text,
            &doc.body_text,
            &doc.title,
            &doc.structure_json,
        ]);
    }
    if let Some(base) = &value.base_record {
        bytes += strings([&base.base_id, &base.file_id, &base.config_json]);
    }
    if let Some(diagnostic) = &value.diagnostic {
        bytes += diagnostic_bytes(diagnostic);
    }
    if let Some(doc) = &value.markdown_doc {
        bytes += strings([&doc.file_id, &doc.source_path]);
        bytes += doc.links.capacity() * size_of::<IndexedWikiLink>();
        for link in &doc.links {
            bytes += strings([
                &link.link.raw,
                &link.link.target,
                &link.source,
                &link.target.path,
            ]);
            bytes += optional(&link.link.display)
                + optional(&link.link.heading)
                + optional(&link.link.block)
                + optional(&link.target.invalid_reason);
            bytes += match &link.target.fragment {
                Some(
                    LinkFragment::Heading(value)
                    | LinkFragment::Block(value)
                    | LinkFragment::InvalidPage(value),
                ) => value.capacity(),
                _ => 0,
            };
        }
        bytes += doc.properties.capacity() * size_of::<PropertyRecordInput>();
        for property in &doc.properties {
            bytes += strings([
                &property.property_id,
                &property.file_id,
                &property.key,
                &property.value_type,
                &property.value_json,
            ]);
        }
        bytes += doc.tasks.capacity() * size_of::<TaskRecordInput>();
        for task in &doc.tasks {
            bytes += strings([
                &task.task_id,
                &task.file_id,
                &task.file_path,
                &task.file_path_lc,
                &task.state,
                &task.text,
                &task.text_lc,
            ]);
        }
    }
    bytes
}
