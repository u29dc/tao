use rusqlite::{Connection, OptionalExtension, params, params_from_iter, types::Value};
use thiserror::Error;

/// Persisted row model for `tasks` table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskRecord {
    /// Stable task identifier.
    pub task_id: String,
    /// Owning file id.
    pub file_id: String,
    /// Owning file normalized path.
    pub file_path: String,
    /// Lower-cased file path projection.
    pub file_path_lc: String,
    /// One-based line number inside markdown source.
    pub line_number: i64,
    /// Task state (`open`, `done`, `cancelled`).
    pub state: String,
    /// Task text.
    pub text: String,
    /// Lower-cased task text projection.
    pub text_lc: String,
    /// Updated timestamp.
    pub updated_at: String,
}

/// Input payload for inserting or updating task rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskRecordInput {
    /// Stable task identifier.
    pub task_id: String,
    /// Owning file id.
    pub file_id: String,
    /// Owning file normalized path.
    pub file_path: String,
    /// Lower-cased file path projection.
    pub file_path_lc: String,
    /// One-based line number inside markdown source.
    pub line_number: i64,
    /// Task state (`open`, `done`, `cancelled`).
    pub state: String,
    /// Task text.
    pub text: String,
    /// Lower-cased task text projection.
    pub text_lc: String,
}

/// Task row exposed by list queries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskWithPath {
    /// Stable task identifier.
    pub task_id: String,
    /// Owning file id.
    pub file_id: String,
    /// Owning file normalized path.
    pub file_path: String,
    /// One-based line number inside markdown source.
    pub line_number: i64,
    /// Task state (`open`, `done`, `cancelled`).
    pub state: String,
    /// Task text.
    pub text: String,
    /// Updated timestamp.
    pub updated_at: String,
}

/// Repository operations over `tasks` table.
#[derive(Debug, Default, Clone, Copy)]
pub struct TasksRepository;

impl TasksRepository {
    /// Insert or update one task row keyed by `(file_id, line_number)`.
    pub fn upsert(
        connection: &Connection,
        task: &TaskRecordInput,
    ) -> Result<(), TasksRepositoryError> {
        connection
            .execute(
                r#"
INSERT INTO tasks (
  task_id,
  file_id,
  file_path,
  file_path_lc,
  line_number,
  state,
  text,
  text_lc
)
VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
ON CONFLICT(file_id, line_number)
DO UPDATE SET
  file_path = excluded.file_path,
  file_path_lc = excluded.file_path_lc,
  state = excluded.state,
  text = excluded.text,
  text_lc = excluded.text_lc,
  updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
"#,
                params![
                    task.task_id,
                    task.file_id,
                    task.file_path,
                    task.file_path_lc,
                    task.line_number,
                    task.state,
                    task.text,
                    task.text_lc
                ],
            )
            .map_err(|source| TasksRepositoryError::Sql {
                operation: "upsert",
                source,
            })?;

        Ok(())
    }

    /// Read one task row by `(file_id, line_number)`.
    pub fn get_by_file_and_line(
        connection: &Connection,
        file_id: &str,
        line_number: i64,
    ) -> Result<Option<TaskRecord>, TasksRepositoryError> {
        let mut statement = connection
            .prepare(
                r#"
SELECT
  task_id,
  file_id,
  file_path,
  file_path_lc,
  line_number,
  state,
  text,
  text_lc,
  updated_at
FROM tasks
WHERE file_id = ?1 AND line_number = ?2
"#,
            )
            .map_err(|source| TasksRepositoryError::Sql {
                operation: "prepare_get_by_file_and_line",
                source,
            })?;

        statement
            .query_row(params![file_id, line_number], row_to_task_record)
            .optional()
            .map_err(|source| TasksRepositoryError::Sql {
                operation: "get_by_file_and_line",
                source,
            })
    }

    /// Delete all task rows for one file id.
    pub fn delete_by_file_id(
        connection: &Connection,
        file_id: &str,
    ) -> Result<u64, TasksRepositoryError> {
        let removed = connection
            .execute("DELETE FROM tasks WHERE file_id = ?1", params![file_id])
            .map_err(|source| TasksRepositoryError::Sql {
                operation: "delete_by_file_id",
                source,
            })?;
        Ok(removed as u64)
    }

    /// List task rows with optional state/query/path filters.
    pub fn list_with_paths(
        connection: &Connection,
        state: Option<&str>,
        query: Option<&str>,
        path: Option<&str>,
        limit: u32,
        offset: u32,
    ) -> Result<Vec<TaskWithPath>, TasksRepositoryError> {
        let mut params = Vec::<Value>::new();
        let mut sql = String::from(
            r#"
SELECT
  task_id,
  file_id,
  file_path,
  line_number,
  state,
  text,
  updated_at
FROM tasks
"#,
        );
        append_filters(&mut sql, &mut params, state, query, path);
        sql.push_str("\nORDER BY file_path ASC, line_number ASC\nLIMIT ? OFFSET ?\n");
        params.push(Value::Integer(i64::from(limit)));
        params.push(Value::Integer(i64::from(offset)));

        let mut statement =
            connection
                .prepare(&sql)
                .map_err(|source| TasksRepositoryError::Sql {
                    operation: "prepare_list_with_paths",
                    source,
                })?;

        let rows = statement
            .query_map(params_from_iter(params.iter()), row_to_task_with_path)
            .map_err(|source| TasksRepositoryError::Sql {
                operation: "list_with_paths",
                source,
            })?;

        rows.map(|row| {
            row.map_err(|source| TasksRepositoryError::Sql {
                operation: "list_with_paths_row",
                source,
            })
        })
        .collect()
    }

    /// Count task rows for one optional filter set.
    pub fn count_with_paths(
        connection: &Connection,
        state: Option<&str>,
        query: Option<&str>,
        path: Option<&str>,
    ) -> Result<u64, TasksRepositoryError> {
        let mut params = Vec::<Value>::new();
        let mut sql = String::from("SELECT COUNT(*) FROM tasks");
        append_filters(&mut sql, &mut params, state, query, path);

        connection
            .query_row(&sql, params_from_iter(params.iter()), |row| {
                row.get::<_, u64>(0)
            })
            .map_err(|source| TasksRepositoryError::Sql {
                operation: "count_with_paths",
                source,
            })
    }
}

fn append_filters(
    sql: &mut String,
    params: &mut Vec<Value>,
    state: Option<&str>,
    query: Option<&str>,
    path: Option<&str>,
) {
    let mut clauses = Vec::<String>::new();

    if let Some(state) = state
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_ascii_lowercase)
    {
        clauses.push("state = ?".to_string());
        params.push(Value::Text(state));
    }

    if let Some(query) = query
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_ascii_lowercase)
    {
        clauses.push("(text_lc LIKE ? OR file_path_lc LIKE ?)".to_string());
        let pattern = format!("%{query}%");
        params.push(Value::Text(pattern.clone()));
        params.push(Value::Text(pattern));
    }

    if let Some(path) = path
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_ascii_lowercase)
    {
        clauses.push("file_path_lc LIKE ?".to_string());
        params.push(Value::Text(format!("%{path}%")));
    }

    if !clauses.is_empty() {
        sql.push_str("\nWHERE ");
        sql.push_str(&clauses.join(" AND "));
    }
}

fn row_to_task_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<TaskRecord> {
    Ok(TaskRecord {
        task_id: row.get("task_id")?,
        file_id: row.get("file_id")?,
        file_path: row.get("file_path")?,
        file_path_lc: row.get("file_path_lc")?,
        line_number: row.get("line_number")?,
        state: row.get("state")?,
        text: row.get("text")?,
        text_lc: row.get("text_lc")?,
        updated_at: row.get("updated_at")?,
    })
}

fn row_to_task_with_path(row: &rusqlite::Row<'_>) -> rusqlite::Result<TaskWithPath> {
    Ok(TaskWithPath {
        task_id: row.get("task_id")?,
        file_id: row.get("file_id")?,
        file_path: row.get("file_path")?,
        line_number: row.get("line_number")?,
        state: row.get("state")?,
        text: row.get("text")?,
        updated_at: row.get("updated_at")?,
    })
}

/// Repository operation failures.
#[derive(Debug, Error)]
pub enum TasksRepositoryError {
    /// SQL error with operation context.
    #[error("tasks repository operation '{operation}' failed: {source}")]
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
        FileRecordInput, FilesRepository, TaskRecordInput, TasksRepository, run_migrations,
    };

    fn file(file_id: &str, path: &str) -> FileRecordInput {
        FileRecordInput {
            file_id: file_id.to_string(),
            normalized_path: path.to_string(),
            match_key: path.to_ascii_lowercase(),
            absolute_path: format!("/vault/{path}"),
            size_bytes: 10,
            modified_unix_ms: 1_700_000_000_000,
            hash_blake3: format!("hash-{file_id}"),
            is_markdown: path.ends_with(".md"),
        }
    }

    #[test]
    fn task_repository_supports_upsert_filter_count_and_delete() {
        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");

        FilesRepository::insert(&connection, &file("f1", "notes/a.md")).expect("insert f1");
        FilesRepository::insert(&connection, &file("f2", "projects/b.md")).expect("insert f2");

        TasksRepository::upsert(
            &connection,
            &TaskRecordInput {
                task_id: "t1".to_string(),
                file_id: "f1".to_string(),
                file_path: "notes/a.md".to_string(),
                file_path_lc: "notes/a.md".to_string(),
                line_number: 2,
                state: "open".to_string(),
                text: "Write spec".to_string(),
                text_lc: "write spec".to_string(),
            },
        )
        .expect("upsert t1");

        TasksRepository::upsert(
            &connection,
            &TaskRecordInput {
                task_id: "t2".to_string(),
                file_id: "f2".to_string(),
                file_path: "projects/b.md".to_string(),
                file_path_lc: "projects/b.md".to_string(),
                line_number: 5,
                state: "done".to_string(),
                text: "Ship release".to_string(),
                text_lc: "ship release".to_string(),
            },
        )
        .expect("upsert t2");

        let fetched = TasksRepository::get_by_file_and_line(&connection, "f1", 2)
            .expect("get by file and line")
            .expect("task exists");
        assert_eq!(fetched.state, "open");

        let listed = TasksRepository::list_with_paths(&connection, None, Some("ship"), None, 10, 0)
            .expect("list with filters");
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].file_path, "projects/b.md");

        let total_done = TasksRepository::count_with_paths(&connection, Some("done"), None, None)
            .expect("count done");
        assert_eq!(total_done, 1);

        let removed =
            TasksRepository::delete_by_file_id(&connection, "f1").expect("delete by file");
        assert_eq!(removed, 1);
    }

    #[test]
    fn task_rows_cascade_on_file_delete() {
        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");

        FilesRepository::insert(&connection, &file("f1", "notes/a.md")).expect("insert file");
        TasksRepository::upsert(
            &connection,
            &TaskRecordInput {
                task_id: "t1".to_string(),
                file_id: "f1".to_string(),
                file_path: "notes/a.md".to_string(),
                file_path_lc: "notes/a.md".to_string(),
                line_number: 3,
                state: "open".to_string(),
                text: "Task".to_string(),
                text_lc: "task".to_string(),
            },
        )
        .expect("upsert task");

        FilesRepository::delete_by_id(&connection, "f1").expect("delete file");

        assert!(
            TasksRepository::get_by_file_and_line(&connection, "f1", 3)
                .expect("fetch deleted task")
                .is_none()
        );
    }
}
