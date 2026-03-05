use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::ast::{BaseDocument, BaseViewKind};
use crate::parser::{BaseParseError, parse_base_document};

/// Diagnostic severity for base validation messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BaseDiagnosticSeverity {
    /// Validation error that should block execution.
    Error,
    /// Non-blocking validation warning.
    Warning,
}

/// One base validation diagnostic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BaseDiagnostic {
    /// Stable machine-readable diagnostic code.
    pub code: String,
    /// Severity level.
    pub severity: BaseDiagnosticSeverity,
    /// Human-readable message.
    pub message: String,
    /// Optional field path for pinpointing issue location.
    pub field: Option<String>,
}

/// Validate raw `.base` YAML content and return diagnostics.
#[must_use]
pub fn validate_base_yaml(input: &str) -> Vec<BaseDiagnostic> {
    match parse_base_document(input) {
        Ok(document) => validate_base_document(&document),
        Err(error) => vec![parse_error_diagnostic(error)],
    }
}

/// Validate persisted base config JSON payload and return diagnostics.
#[must_use]
pub fn validate_base_config_json(config_json: &str) -> Vec<BaseDiagnostic> {
    match serde_json::from_str::<BaseDocument>(config_json) {
        Ok(document) => validate_base_document(&document),
        Err(source) => vec![BaseDiagnostic {
            code: "bases.parse.invalid_schema".to_string(),
            severity: BaseDiagnosticSeverity::Error,
            message: format!("failed to decode base config json: {source}"),
            field: None,
        }],
    }
}

/// Validate a parsed base document and return normalized diagnostics.
#[must_use]
pub fn validate_base_document(document: &BaseDocument) -> Vec<BaseDiagnostic> {
    let mut diagnostics = Vec::new();
    let mut seen_view_names = HashSet::new();

    for (view_index, view) in document.views.iter().enumerate() {
        let view_name_key = view.name.to_ascii_lowercase();
        if !seen_view_names.insert(view_name_key) {
            diagnostics.push(BaseDiagnostic {
                code: "bases.view.duplicate_name".to_string(),
                severity: BaseDiagnosticSeverity::Error,
                message: format!("duplicate view name '{}'", view.name),
                field: Some(format!("views[{view_index}].name")),
            });
        }

        if matches!(view.kind, BaseViewKind::Table) {
            if view.columns.is_empty() {
                diagnostics.push(BaseDiagnostic {
                    code: "bases.table.missing_columns".to_string(),
                    severity: BaseDiagnosticSeverity::Warning,
                    message: format!("table view '{}' has no configured columns", view.name),
                    field: Some(format!("views[{view_index}].columns")),
                });
            }

            let mut seen_columns = HashSet::new();
            for (column_index, column) in view.columns.iter().enumerate() {
                let column_key = column.key.to_ascii_lowercase();
                if !seen_columns.insert(column_key) {
                    diagnostics.push(BaseDiagnostic {
                        code: "bases.column.duplicate_key".to_string(),
                        severity: BaseDiagnosticSeverity::Warning,
                        message: format!(
                            "table view '{}' has duplicate column key '{}'",
                            view.name, column.key
                        ),
                        field: Some(format!("views[{view_index}].columns[{column_index}].key")),
                    });
                }
            }

            if !view.columns.is_empty() && view.columns.iter().all(|column| column.hidden) {
                diagnostics.push(BaseDiagnostic {
                    code: "bases.table.all_columns_hidden".to_string(),
                    severity: BaseDiagnosticSeverity::Warning,
                    message: format!("table view '{}' hides all configured columns", view.name),
                    field: Some(format!("views[{view_index}].columns")),
                });
            }
        }
    }

    diagnostics.sort_by(|left, right| {
        severity_rank(left.severity)
            .cmp(&severity_rank(right.severity))
            .then_with(|| left.code.cmp(&right.code))
            .then_with(|| left.field.cmp(&right.field))
            .then_with(|| left.message.cmp(&right.message))
    });
    diagnostics
}

fn parse_error_diagnostic(error: BaseParseError) -> BaseDiagnostic {
    BaseDiagnostic {
        code: "bases.parse.invalid_schema".to_string(),
        severity: BaseDiagnosticSeverity::Error,
        message: error.to_string(),
        field: parse_error_field(&error),
    }
}

fn parse_error_field(error: &BaseParseError) -> Option<String> {
    match error {
        BaseParseError::InvalidRootFieldType { field, .. } => Some(field.clone()),
        BaseParseError::MissingField { field, .. }
        | BaseParseError::InvalidFieldType { field, .. }
        | BaseParseError::UnsupportedValue { field, .. }
        | BaseParseError::EmptyField { field, .. }
        | BaseParseError::JsonConversion { field, .. } => Some(field.clone()),
        BaseParseError::MissingViews => Some("views".to_string()),
        BaseParseError::InvalidViewEntry { view_index } => Some(format!("views[{view_index}]")),
        BaseParseError::UnsupportedRootFilter { .. } => Some("filters.and".to_string()),
        BaseParseError::EmptyInput
        | BaseParseError::DeserializeYaml { .. }
        | BaseParseError::RootMustBeMapping => None,
    }
}

fn severity_rank(severity: BaseDiagnosticSeverity) -> u8 {
    match severity {
        BaseDiagnosticSeverity::Error => 0,
        BaseDiagnosticSeverity::Warning => 1,
    }
}
