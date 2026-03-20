use super::*;

#[derive(Debug, Default)]
struct BaseTableCacheState {
    metadata_digest: Option<String>,
    entries: HashMap<String, BaseTablePage>,
}

/// Cached base table query service with automatic invalidation on metadata changes.
#[derive(Debug, Default)]
pub struct BaseTableCachedQueryService {
    executor: BaseTableExecutorService,
    state: Mutex<BaseTableCacheState>,
}

impl BaseTableCachedQueryService {
    /// Execute one table plan using cache when the metadata digest is unchanged.
    pub fn execute(
        &self,
        connection: &Connection,
        plan: &TableQueryPlan,
    ) -> Result<BaseTablePage, BaseTableCacheError> {
        let metadata_digest = compute_base_table_metadata_digest(connection)?;
        let cache_key = serde_json::to_string(plan)
            .map_err(|source| BaseTableCacheError::SerializePlan { source })?;

        {
            let mut state = self
                .state
                .lock()
                .map_err(|_| BaseTableCacheError::LockPoisoned)?;
            if state.metadata_digest.as_deref() != Some(&metadata_digest) {
                state.entries.clear();
                state.metadata_digest = Some(metadata_digest.clone());
            }
            if let Some(cached) = state.entries.get(&cache_key) {
                return Ok(cached.clone());
            }
        }

        let computed = self.executor.execute(connection, plan).map_err(|source| {
            BaseTableCacheError::Execute {
                source: Box::new(source),
            }
        })?;

        let mut state = self
            .state
            .lock()
            .map_err(|_| BaseTableCacheError::LockPoisoned)?;
        if state.metadata_digest.as_deref() != Some(&metadata_digest) {
            state.entries.clear();
            state.metadata_digest = Some(metadata_digest);
        }
        state.entries.insert(cache_key, computed.clone());
        Ok(computed)
    }

    /// Explicitly clear all cached table pages.
    pub fn invalidate_all(&self) -> Result<(), BaseTableCacheError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| BaseTableCacheError::LockPoisoned)?;
        state.entries.clear();
        state.metadata_digest = None;
        Ok(())
    }
}

fn compute_base_table_metadata_digest(
    connection: &Connection,
) -> Result<String, BaseTableCacheError> {
    let mut hasher = blake3::Hasher::new();
    hash_table_rows_into_digest(
        connection,
        &mut hasher,
        "files",
        r#"
SELECT file_id, normalized_path, indexed_at, hash_blake3
FROM files
ORDER BY file_id ASC
"#,
    )?;
    hash_table_rows_into_digest(
        connection,
        &mut hasher,
        "properties",
        r#"
SELECT property_id, file_id, key, value_json, updated_at
FROM properties
ORDER BY property_id ASC
"#,
    )?;
    hash_table_rows_into_digest(
        connection,
        &mut hasher,
        "bases",
        r#"
SELECT base_id, file_id, config_json, updated_at
FROM bases
ORDER BY base_id ASC
"#,
    )?;

    Ok(hasher.finalize().to_hex().to_string())
}

fn hash_table_rows_into_digest(
    connection: &Connection,
    hasher: &mut blake3::Hasher,
    table_name: &'static str,
    query: &'static str,
) -> Result<(), BaseTableCacheError> {
    hasher.update(table_name.as_bytes());
    hasher.update(&[0x1d]);

    let mut statement =
        connection
            .prepare(query)
            .map_err(|source| BaseTableCacheError::DigestQuery {
                operation: "prepare_digest_query",
                source,
            })?;
    let mut rows = statement
        .query([])
        .map_err(|source| BaseTableCacheError::DigestQuery {
            operation: "run_digest_query",
            source,
        })?;

    while let Some(row) = rows
        .next()
        .map_err(|source| BaseTableCacheError::DigestQuery {
            operation: "iterate_digest_rows",
            source,
        })?
    {
        for column_index in 0..row.as_ref().column_count() {
            let value =
                row.get_ref(column_index)
                    .map_err(|source| BaseTableCacheError::DigestQuery {
                        operation: "read_digest_row_value",
                        source,
                    })?;
            match value {
                rusqlite::types::ValueRef::Null => {
                    hasher.update(b"<null>");
                }
                rusqlite::types::ValueRef::Integer(value) => {
                    hasher.update(value.to_string().as_bytes());
                }
                rusqlite::types::ValueRef::Real(value) => {
                    hasher.update(value.to_string().as_bytes());
                }
                rusqlite::types::ValueRef::Text(bytes) => {
                    hasher.update(bytes);
                }
                rusqlite::types::ValueRef::Blob(bytes) => {
                    hasher.update(bytes);
                }
            }
            hasher.update(&[0x1f]);
        }
        hasher.update(&[0x1e]);
    }

    Ok(())
}

/// Cached base table query failures.
#[derive(Debug, Error)]
pub enum BaseTableCacheError {
    /// Cache lock was poisoned.
    #[error("base table cache lock poisoned")]
    LockPoisoned,
    /// Plan serialization failed while creating cache key.
    #[error("failed to serialize table plan for cache key: {source}")]
    SerializePlan {
        /// JSON serialization error.
        #[source]
        source: serde_json::Error,
    },
    /// Metadata digest query failed.
    #[error("failed to compute base metadata digest during '{operation}': {source}")]
    DigestQuery {
        /// Operation name.
        operation: &'static str,
        /// SQLite error.
        #[source]
        source: rusqlite::Error,
    },
    /// Underlying table execution failed.
    #[error("failed to execute table plan while populating cache: {source}")]
    Execute {
        /// Execution error.
        #[source]
        source: Box<BaseTableExecutorError>,
    },
}
