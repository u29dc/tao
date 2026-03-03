use rusqlite::Connection;
use thiserror::Error;

use crate::{FileRecord, FileRecordInput, FilesRepository, FilesRepositoryError};

/// Typed transaction wrapper over storage repositories.
#[derive(Debug)]
pub struct StorageTransaction<'tx> {
    transaction: rusqlite::Transaction<'tx>,
}

impl<'tx> StorageTransaction<'tx> {
    fn new(transaction: rusqlite::Transaction<'tx>) -> Self {
        Self { transaction }
    }

    /// Insert one file record inside the active transaction.
    pub fn files_insert(&self, record: &FileRecordInput) -> Result<(), StorageTransactionError> {
        FilesRepository::insert(&self.transaction, record).map_err(StorageTransactionError::from)
    }

    /// Insert or update one file record inside the active transaction.
    pub fn files_upsert(&self, record: &FileRecordInput) -> Result<(), StorageTransactionError> {
        FilesRepository::upsert(&self.transaction, record).map_err(StorageTransactionError::from)
    }

    /// Lookup file by id inside the active transaction.
    pub fn files_get_by_id(
        &self,
        file_id: &str,
    ) -> Result<Option<FileRecord>, StorageTransactionError> {
        FilesRepository::get_by_id(&self.transaction, file_id)
            .map_err(StorageTransactionError::from)
    }

    /// Delete file by id inside the active transaction.
    pub fn files_delete_by_id(&self, file_id: &str) -> Result<bool, StorageTransactionError> {
        FilesRepository::delete_by_id(&self.transaction, file_id)
            .map_err(StorageTransactionError::from)
    }

    /// List all files inside the active transaction.
    pub fn files_list_all(&self) -> Result<Vec<FileRecord>, StorageTransactionError> {
        FilesRepository::list_all(&self.transaction).map_err(StorageTransactionError::from)
    }

    fn commit(self) -> Result<(), StorageTransactionError> {
        self.transaction
            .commit()
            .map_err(|source| StorageTransactionError::Commit { source })
    }
}

/// Execute one typed storage transaction.
pub fn with_transaction<T, F>(
    connection: &mut Connection,
    operation: F,
) -> Result<T, StorageTransactionError>
where
    F: FnOnce(&StorageTransaction<'_>) -> Result<T, StorageTransactionError>,
{
    let transaction = connection
        .transaction()
        .map_err(|source| StorageTransactionError::Begin { source })?;
    let typed_transaction = StorageTransaction::new(transaction);

    let value = operation(&typed_transaction)?;
    typed_transaction.commit()?;
    Ok(value)
}

/// Storage transaction failures.
#[derive(Debug, Error)]
pub enum StorageTransactionError {
    /// Starting transaction failed.
    #[error("failed to begin storage transaction: {source}")]
    Begin {
        /// SQLite error.
        #[source]
        source: rusqlite::Error,
    },
    /// Committing transaction failed.
    #[error("failed to commit storage transaction: {source}")]
    Commit {
        /// SQLite error.
        #[source]
        source: rusqlite::Error,
    },
    /// Repository operation failed inside transaction.
    #[error("repository operation failed in transaction: {source}")]
    Repository {
        /// Repository error.
        #[from]
        source: FilesRepositoryError,
    },
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;

    use crate::{FileRecordInput, FilesRepository, run_migrations, with_transaction};

    fn sample_record(file_id: &str, path: &str) -> FileRecordInput {
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
    fn with_transaction_commits_successful_operations() {
        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");

        let record = sample_record("f1", "notes/one.md");
        with_transaction(&mut connection, |tx| {
            tx.files_insert(&record)?;
            Ok(())
        })
        .expect("transaction should commit");

        let persisted = FilesRepository::get_by_id(&connection, "f1")
            .expect("get by id")
            .expect("persisted row");
        assert_eq!(persisted.normalized_path, "notes/one.md");
    }

    #[test]
    fn with_transaction_rolls_back_when_operation_fails() {
        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");

        let first = sample_record("f1", "notes/one.md");
        let duplicate = sample_record("f1", "notes/one.md");

        let result = with_transaction(&mut connection, |tx| {
            tx.files_insert(&first)?;
            tx.files_insert(&duplicate)?;
            Ok(())
        });

        assert!(result.is_err());
        let all = FilesRepository::list_all(&connection).expect("list all after rollback");
        assert!(all.is_empty());
    }
}
