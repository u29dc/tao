//! Typed storage transaction coordination helpers.

use rusqlite::Connection;
use tao_sdk_storage::{FileRecordInput, StorageTransactionError, with_transaction};
use thiserror::Error;

/// Coordinates write operations through typed SDK storage transactions.
#[derive(Debug, Default, Clone, Copy)]
pub struct SdkTransactionCoordinator;

impl SdkTransactionCoordinator {
    /// Insert one file metadata row in a typed transaction.
    pub fn insert_file_metadata(
        &self,
        connection: &mut Connection,
        record: &FileRecordInput,
    ) -> Result<(), SdkTransactionError> {
        with_transaction(connection, |transaction| {
            transaction.files_insert(record)?;
            Ok(())
        })
        .map_err(|source| SdkTransactionError::Transaction { source })
    }

    /// Delete one file metadata row in a typed transaction.
    pub fn delete_file_metadata(
        &self,
        connection: &mut Connection,
        file_id: &str,
    ) -> Result<bool, SdkTransactionError> {
        with_transaction(connection, |transaction| {
            transaction.files_delete_by_id(file_id)
        })
        .map_err(|source| SdkTransactionError::Transaction { source })
    }

    /// Replace one file metadata row atomically in a typed transaction.
    pub fn replace_file_metadata(
        &self,
        connection: &mut Connection,
        _file_id: &str,
        replacement: &FileRecordInput,
    ) -> Result<(), SdkTransactionError> {
        with_transaction(connection, |transaction| {
            transaction.files_upsert(replacement)?;
            Ok(())
        })
        .map_err(|source| SdkTransactionError::Transaction { source })
    }
}

/// Errors returned by SDK transaction coordination.
#[derive(Debug, Error)]
pub enum SdkTransactionError {
    /// Typed storage transaction failed.
    #[error("sdk transaction coordination failed: {source}")]
    Transaction {
        /// Transaction error details.
        #[source]
        source: StorageTransactionError,
    },
}

/// Service wrapper for storage writes executed inside typed transactions.
#[derive(Debug, Default, Clone, Copy)]
pub struct StorageWriteService;

impl StorageWriteService {
    /// Insert one file record using the typed storage transaction API.
    pub fn create_file_record(
        &self,
        connection: &mut Connection,
        record: &FileRecordInput,
    ) -> Result<(), StorageWriteError> {
        SdkTransactionCoordinator
            .insert_file_metadata(connection, record)
            .map_err(|source| StorageWriteError::Coordinator { source })
    }
}

/// Errors returned by service-layer storage writes.
#[derive(Debug, Error)]
pub enum StorageWriteError {
    /// SDK transaction coordinator failed.
    #[error("storage write coordinator failed: {source}")]
    Coordinator {
        /// Coordinator error details.
        #[source]
        source: SdkTransactionError,
    },
}
