use super::*;

/// Base validation API result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BaseValidationResult {
    /// Base identifier.
    pub base_id: String,
    /// Owning file id.
    pub file_id: String,
    /// Owning normalized file path.
    pub file_path: String,
    /// Validation diagnostics.
    pub diagnostics: Vec<BaseDiagnostic>,
}

/// Validation service for base config diagnostics.
#[derive(Debug, Default, Clone, Copy)]
pub struct BaseValidationService;

impl BaseValidationService {
    /// Validate one base config by base id or normalized base file path.
    pub fn validate(
        &self,
        connection: &Connection,
        path_or_id: &str,
    ) -> Result<BaseValidationResult, BaseValidationError> {
        let path_or_id = path_or_id.trim();
        if path_or_id.is_empty() {
            return Err(BaseValidationError::InvalidInput);
        }

        if let Some(base) = BasesRepository::get_by_id(connection, path_or_id)
            .map_err(|source| BaseValidationError::Repository { source })?
        {
            let file = FilesRepository::get_by_id(connection, &base.file_id)
                .map_err(|source| BaseValidationError::FilesRepository { source })?;
            let file_path = file.map(|file| file.normalized_path).unwrap_or_default();

            return Ok(BaseValidationResult {
                base_id: base.base_id,
                file_id: base.file_id,
                file_path,
                diagnostics: validate_base_config_json(&base.config_json),
            });
        }

        let Some(base) = BasesRepository::list_with_paths(connection)
            .map_err(|source| BaseValidationError::Repository { source })?
            .into_iter()
            .find(|base| base.file_path == path_or_id)
        else {
            return Err(BaseValidationError::BaseNotFound {
                path_or_id: path_or_id.to_string(),
            });
        };

        Ok(BaseValidationResult {
            base_id: base.base_id,
            file_id: base.file_id,
            file_path: base.file_path,
            diagnostics: validate_base_config_json(&base.config_json),
        })
    }
}

/// Base validation API failures.
#[derive(Debug, Error)]
pub enum BaseValidationError {
    /// Input was empty.
    #[error("base validation input must not be empty")]
    InvalidInput,
    /// Base id/path lookup failed.
    #[error("base '{path_or_id}' not found for validation")]
    BaseNotFound {
        /// Input value used for lookup.
        path_or_id: String,
    },
    /// Bases repository operation failed.
    #[error("base repository operation failed during validation: {source}")]
    Repository {
        /// Repository error.
        #[source]
        source: tao_sdk_storage::BasesRepositoryError,
    },
    /// Files repository operation failed.
    #[error("files repository operation failed while resolving base path: {source}")]
    FilesRepository {
        /// Repository error.
        #[source]
        source: tao_sdk_storage::FilesRepositoryError,
    },
}
