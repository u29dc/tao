//! Transaction-owned canonical/derived publication generations.
use rusqlite::Connection;
/// Constant-time publication metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexGenerations {
    /// Canonical table mutation generation.
    pub canonical_generation: i64,
    /// Generation used to publish search.
    pub search_generation: i64,
    /// Generation of every derived row mutation, including updates with unchanged counts.
    pub derived_generation: i64,
    /// Derived generation validated at publication.
    pub published_derived_generation: i64,
    /// Current inventory size, maintained by triggers.
    pub files_total: u64,
    /// Current segment count, maintained by triggers.
    pub segments_total: u64,
    /// Current alias count, maintained by triggers.
    pub aliases_total: u64,
    /// Segment count at last publication.
    pub published_segments: u64,
    /// Alias count at last publication.
    pub published_aliases: u64,
}
/// Publication generation operations.
#[derive(Debug, Default, Clone, Copy)]
pub struct IndexGenerationRepository;
impl IndexGenerationRepository {
    /// Inspect one singleton row; does not scan source tables.
    pub fn get(c: &Connection) -> rusqlite::Result<IndexGenerations> {
        c.query_row("SELECT canonical_generation,search_generation,files_total,segments_total,aliases_total,published_segments,published_aliases,derived_generation,published_derived_generation FROM index_generations WHERE singleton=1",[],|r|Ok(IndexGenerations{canonical_generation:r.get(0)?,search_generation:r.get(1)?,files_total:r.get(2)?,segments_total:r.get(3)?,aliases_total:r.get(4)?,published_segments:r.get(5)?,published_aliases:r.get(6)?,derived_generation:r.get(7)?,published_derived_generation:r.get(8)?}))
    }
    /// Mark a fully rebuilt or dependency-complete search corpus ready in this transaction.
    pub fn publish_search(c: &Connection) -> rusqlite::Result<()> {
        c.execute("UPDATE index_generations SET search_generation=canonical_generation,published_derived_generation=derived_generation,published_segments=segments_total,published_aliases=aliases_total WHERE singleton=1",[])?;
        c.execute("DELETE FROM index_dirty_files", [])?;
        c.execute("DELETE FROM derived_dirty_files", [])?;
        Ok(())
    }
    /// Derived row owners mutated since publication; used to distinguish source cascades from damage.
    pub fn derived_dirty_files(c: &Connection) -> rusqlite::Result<Vec<String>> {
        c.prepare("SELECT file_id FROM derived_dirty_files ORDER BY file_id")?
            .query_map([], |row| row.get(0))?
            .collect()
    }
    /// Read persistent canonical dirty file identities.
    pub fn dirty_files(c: &Connection) -> rusqlite::Result<Vec<String>> {
        c.prepare("SELECT file_id FROM index_dirty_files ORDER BY file_id")?
            .query_map([], |r| r.get(0))?
            .collect()
    }
}
