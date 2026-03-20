use super::*;

/// Column persistence result for one base view update.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BaseColumnConfigPersistResult {
    /// Base identifier.
    pub base_id: String,
    /// View name that was updated.
    pub view_name: String,
    /// Number of persisted columns.
    pub columns_total: u64,
}

/// Persistence service for updating base view column order/visibility config.
#[derive(Debug, Default, Clone, Copy)]
pub struct BaseColumnConfigPersistenceService;

impl BaseColumnConfigPersistenceService {
    /// Persist the full ordered column configuration for one base view.
    pub fn persist_view_columns(
        &self,
        connection: &Connection,
        base_id: &str,
        view_name: &str,
        columns: Vec<BaseColumnConfig>,
    ) -> Result<BaseColumnConfigPersistResult, BaseColumnConfigPersistError> {
        if base_id.trim().is_empty() {
            return Err(BaseColumnConfigPersistError::InvalidInput {
                field: "base_id".to_string(),
            });
        }
        if view_name.trim().is_empty() {
            return Err(BaseColumnConfigPersistError::InvalidInput {
                field: "view_name".to_string(),
            });
        }

        let Some(base) = BasesRepository::get_by_id(connection, base_id)
            .map_err(|source| BaseColumnConfigPersistError::Repository { source })?
        else {
            return Err(BaseColumnConfigPersistError::BaseNotFound {
                base_id: base_id.to_string(),
            });
        };

        let mut document =
            serde_json::from_str::<BaseDocument>(&base.config_json).map_err(|source| {
                BaseColumnConfigPersistError::DeserializeConfig {
                    base_id: base.base_id.clone(),
                    source,
                }
            })?;

        let (resolved_view_name, columns_total) = {
            let Some(view) = document
                .views
                .iter_mut()
                .find(|view| view.name.eq_ignore_ascii_case(view_name))
            else {
                return Err(BaseColumnConfigPersistError::ViewNotFound {
                    base_id: base.base_id.clone(),
                    view_name: view_name.to_string(),
                });
            };
            view.columns = columns;
            (view.name.clone(), view.columns.len() as u64)
        };

        let config_json = serde_json::to_string(&document).map_err(|source| {
            BaseColumnConfigPersistError::SerializeConfig {
                base_id: base.base_id.clone(),
                source,
            }
        })?;
        BasesRepository::upsert(
            connection,
            &BaseRecordInput {
                base_id: base.base_id.clone(),
                file_id: base.file_id.clone(),
                config_json,
            },
        )
        .map_err(|source| BaseColumnConfigPersistError::Repository { source })?;

        Ok(BaseColumnConfigPersistResult {
            base_id: base.base_id,
            view_name: resolved_view_name,
            columns_total,
        })
    }
}

/// Base column configuration persistence failures.
#[derive(Debug, Error)]
pub enum BaseColumnConfigPersistError {
    /// Required input field was empty.
    #[error("base column persistence input '{field}' must not be empty")]
    InvalidInput {
        /// Field name.
        field: String,
    },
    /// Requested base row was not found.
    #[error("base row '{base_id}' not found")]
    BaseNotFound {
        /// Base id.
        base_id: String,
    },
    /// Stored config JSON failed to decode into a base document.
    #[error("failed to decode base config json for '{base_id}': {source}")]
    DeserializeConfig {
        /// Base id.
        base_id: String,
        /// JSON parse error.
        #[source]
        source: serde_json::Error,
    },
    /// Requested view was not present in base config.
    #[error("view '{view_name}' not found in base '{base_id}'")]
    ViewNotFound {
        /// Base id.
        base_id: String,
        /// View name.
        view_name: String,
    },
    /// Updated base config failed to serialize.
    #[error("failed to serialize updated base config for '{base_id}': {source}")]
    SerializeConfig {
        /// Base id.
        base_id: String,
        /// JSON serialization error.
        #[source]
        source: serde_json::Error,
    },
    /// Repository operation failed.
    #[error("base repository operation failed while persisting columns: {source}")]
    Repository {
        /// Repository error.
        #[source]
        source: tao_sdk_storage::BasesRepositoryError,
    },
}
