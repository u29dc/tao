use rusqlite::{Connection, OptionalExtension, params};
use thiserror::Error;

/// Persisted row model for `properties` table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PropertyRecord {
    /// Stable property identifier.
    pub property_id: String,
    /// Owning file id.
    pub file_id: String,
    /// Property key.
    pub key: String,
    /// Property value type.
    pub value_type: String,
    /// Property value payload encoded as JSON.
    pub value_json: String,
    /// Updated timestamp.
    pub updated_at: String,
}

/// Input payload for property upserts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PropertyRecordInput {
    /// Stable property identifier.
    pub property_id: String,
    /// Owning file id.
    pub file_id: String,
    /// Property key.
    pub key: String,
    /// Property value type.
    pub value_type: String,
    /// Property value payload encoded as JSON.
    pub value_json: String,
}

/// Property row enriched with file normalized path from join queries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PropertyWithPath {
    /// Stable property identifier.
    pub property_id: String,
    /// Owning file id.
    pub file_id: String,
    /// Owning file normalized path.
    pub file_path: String,
    /// Property key.
    pub key: String,
    /// Property value type.
    pub value_type: String,
    /// Property value payload encoded as JSON.
    pub value_json: String,
    /// Updated timestamp.
    pub updated_at: String,
}

/// Repository operations over `properties` table.
#[derive(Debug, Default, Clone, Copy)]
pub struct PropertiesRepository;

impl PropertiesRepository {
    /// Insert or update one property row.
    pub fn upsert(
        connection: &Connection,
        property: &PropertyRecordInput,
    ) -> Result<(), PropertiesRepositoryError> {
        connection
            .execute(
                r#"
INSERT INTO properties (
  property_id,
  file_id,
  key,
  value_type,
  value_json
)
VALUES (?1, ?2, ?3, ?4, ?5)
ON CONFLICT(file_id, key)
DO UPDATE SET
  value_type = excluded.value_type,
  value_json = excluded.value_json,
  updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
"#,
                params![
                    property.property_id,
                    property.file_id,
                    property.key,
                    property.value_type,
                    property.value_json
                ],
            )
            .map_err(|source| PropertiesRepositoryError::Sql {
                operation: "upsert",
                source,
            })?;

        Ok(())
    }

    /// Fetch one property by `(file_id, key)`.
    pub fn get_by_file_and_key(
        connection: &Connection,
        file_id: &str,
        key: &str,
    ) -> Result<Option<PropertyRecord>, PropertiesRepositoryError> {
        let mut statement = connection
            .prepare(
                r#"
SELECT
  property_id,
  file_id,
  key,
  value_type,
  value_json,
  updated_at
FROM properties
WHERE file_id = ?1 AND key = ?2
"#,
            )
            .map_err(|source| PropertiesRepositoryError::Sql {
                operation: "prepare_get_by_file_and_key",
                source,
            })?;

        statement
            .query_row(params![file_id, key], row_to_property_record)
            .optional()
            .map_err(|source| PropertiesRepositoryError::Sql {
                operation: "get_by_file_and_key",
                source,
            })
    }

    /// List all properties for one file with joined file path.
    pub fn list_for_file_with_path(
        connection: &Connection,
        file_id: &str,
    ) -> Result<Vec<PropertyWithPath>, PropertiesRepositoryError> {
        let mut statement = connection
            .prepare(
                r#"
SELECT
  p.property_id,
  p.file_id,
  f.normalized_path AS file_path,
  p.key,
  p.value_type,
  p.value_json,
  p.updated_at
FROM properties p
JOIN files f ON f.file_id = p.file_id
WHERE p.file_id = ?1
ORDER BY p.key ASC
"#,
            )
            .map_err(|source| PropertiesRepositoryError::Sql {
                operation: "prepare_list_for_file_with_path",
                source,
            })?;

        let rows = statement
            .query_map(params![file_id], row_to_property_with_path)
            .map_err(|source| PropertiesRepositoryError::Sql {
                operation: "list_for_file_with_path",
                source,
            })?;

        rows.map(|row| {
            row.map_err(|source| PropertiesRepositoryError::Sql {
                operation: "list_for_file_with_path_row",
                source,
            })
        })
        .collect()
    }

    /// List properties by key across files with joined file paths.
    pub fn list_by_key_with_paths(
        connection: &Connection,
        key: &str,
    ) -> Result<Vec<PropertyWithPath>, PropertiesRepositoryError> {
        let mut statement = connection
            .prepare(
                r#"
SELECT
  p.property_id,
  p.file_id,
  f.normalized_path AS file_path,
  p.key,
  p.value_type,
  p.value_json,
  p.updated_at
FROM properties p
JOIN files f ON f.file_id = p.file_id
WHERE p.key = ?1
ORDER BY f.normalized_path ASC
"#,
            )
            .map_err(|source| PropertiesRepositoryError::Sql {
                operation: "prepare_list_by_key_with_paths",
                source,
            })?;

        let rows = statement
            .query_map(params![key], row_to_property_with_path)
            .map_err(|source| PropertiesRepositoryError::Sql {
                operation: "list_by_key_with_paths",
                source,
            })?;

        rows.map(|row| {
            row.map_err(|source| PropertiesRepositoryError::Sql {
                operation: "list_by_key_with_paths_row",
                source,
            })
        })
        .collect()
    }
}

fn row_to_property_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<PropertyRecord> {
    Ok(PropertyRecord {
        property_id: row.get("property_id")?,
        file_id: row.get("file_id")?,
        key: row.get("key")?,
        value_type: row.get("value_type")?,
        value_json: row.get("value_json")?,
        updated_at: row.get("updated_at")?,
    })
}

fn row_to_property_with_path(row: &rusqlite::Row<'_>) -> rusqlite::Result<PropertyWithPath> {
    Ok(PropertyWithPath {
        property_id: row.get("property_id")?,
        file_id: row.get("file_id")?,
        file_path: row.get("file_path")?,
        key: row.get("key")?,
        value_type: row.get("value_type")?,
        value_json: row.get("value_json")?,
        updated_at: row.get("updated_at")?,
    })
}

/// Properties repository operation failures.
#[derive(Debug, Error)]
pub enum PropertiesRepositoryError {
    /// SQL error with operation context.
    #[error("properties repository operation '{operation}' failed: {source}")]
    Sql {
        /// Repository operation name.
        operation: &'static str,
        /// SQLite error.
        #[source]
        source: rusqlite::Error,
    },
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;

    use crate::{
        FileRecordInput, FilesRepository, PropertiesRepository, PropertyRecordInput, run_migrations,
    };

    fn file(file_id: &str, path: &str) -> FileRecordInput {
        FileRecordInput {
            file_id: file_id.to_string(),
            normalized_path: path.to_string(),
            match_key: path.to_lowercase(),
            absolute_path: format!("/vault/{path}"),
            size_bytes: 10,
            modified_unix_ms: 1_700_000_000_000,
            hash_blake3: format!("hash-{file_id}"),
            is_markdown: path.ends_with(".md"),
        }
    }

    #[test]
    fn property_join_queries_return_file_paths() {
        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");

        FilesRepository::insert(&connection, &file("f1", "notes/a.md")).expect("insert file a");
        FilesRepository::insert(&connection, &file("f2", "notes/b.md")).expect("insert file b");

        let p1 = PropertyRecordInput {
            property_id: "p1".to_string(),
            file_id: "f1".to_string(),
            key: "status".to_string(),
            value_type: "string".to_string(),
            value_json: "\"draft\"".to_string(),
        };
        let p2 = PropertyRecordInput {
            property_id: "p2".to_string(),
            file_id: "f2".to_string(),
            key: "status".to_string(),
            value_type: "string".to_string(),
            value_json: "\"published\"".to_string(),
        };

        PropertiesRepository::upsert(&connection, &p1).expect("upsert property p1");
        PropertiesRepository::upsert(&connection, &p2).expect("upsert property p2");

        let by_file = PropertiesRepository::list_for_file_with_path(&connection, "f1")
            .expect("list properties for file");
        assert_eq!(by_file.len(), 1);
        assert_eq!(by_file[0].file_path, "notes/a.md");
        assert_eq!(by_file[0].value_json, "\"draft\"");

        let by_key = PropertiesRepository::list_by_key_with_paths(&connection, "status")
            .expect("list by key");
        assert_eq!(by_key.len(), 2);
        assert_eq!(by_key[0].file_path, "notes/a.md");
        assert_eq!(by_key[1].file_path, "notes/b.md");
    }

    #[test]
    fn upsert_updates_existing_property_value() {
        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");

        FilesRepository::insert(&connection, &file("f1", "notes/a.md")).expect("insert file");

        let initial = PropertyRecordInput {
            property_id: "p1".to_string(),
            file_id: "f1".to_string(),
            key: "status".to_string(),
            value_type: "string".to_string(),
            value_json: "\"draft\"".to_string(),
        };
        let updated = PropertyRecordInput {
            property_id: "p2".to_string(),
            file_id: "f1".to_string(),
            key: "status".to_string(),
            value_type: "string".to_string(),
            value_json: "\"published\"".to_string(),
        };

        PropertiesRepository::upsert(&connection, &initial).expect("upsert initial");
        PropertiesRepository::upsert(&connection, &updated).expect("upsert updated");

        let fetched = PropertiesRepository::get_by_file_and_key(&connection, "f1", "status")
            .expect("get property")
            .expect("property should exist");
        assert_eq!(fetched.value_json, "\"published\"");
    }
}
