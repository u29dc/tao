use std::collections::HashSet;

use serde::Deserialize;
use serde_json::Map as JsonMap;
use thiserror::Error;

use crate::ast::{
    BaseAggregateSpec, BaseColumnConfig, BaseDocument, BaseFilterClause, BaseRelationSpec,
    BaseRollupSpec, BaseSortClause, BaseViewRegistryEntry, TableQueryPlan, TableQueryPlanRequest,
};

/// Registry over parsed base views.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BaseViewRegistry {
    views: Vec<BaseViewRegistryEntry>,
}

impl BaseViewRegistry {
    /// Build one registry from a parsed `.base` document.
    pub fn from_document(document: &BaseDocument) -> Result<Self, BaseViewRegistryError> {
        if let Some(diagnostic) = crate::validation::validate_base_document(document)
            .into_iter()
            .find(|diagnostic| {
                matches!(
                    diagnostic.severity,
                    crate::validation::BaseDiagnosticSeverity::Error
                ) && diagnostic.code != "bases.view.duplicate_name"
            })
        {
            return Err(BaseViewRegistryError::InvalidDefinition {
                reason: diagnostic.message,
            });
        }
        let mut views = Vec::with_capacity(document.views.len());
        let mut seen_names = HashSet::new();

        for view in &document.views {
            let dedupe_key = view.name.to_ascii_lowercase();
            if !seen_names.insert(dedupe_key) {
                return Err(BaseViewRegistryError::DuplicateViewName {
                    name: view.name.clone(),
                });
            }

            let mut config = JsonMap::new();
            if let Some(source) = &view.source {
                config.insert(
                    "source".to_string(),
                    serde_json::Value::String(source.clone()),
                );
            }
            config.insert(
                "filters".to_string(),
                serde_json::to_value(&view.filters).map_err(|source| {
                    BaseViewRegistryError::SerializeConfig {
                        view_name: view.name.clone(),
                        source,
                    }
                })?,
            );
            config.insert(
                "sorts".to_string(),
                serde_json::to_value(&view.sorts).map_err(|source| {
                    BaseViewRegistryError::SerializeConfig {
                        view_name: view.name.clone(),
                        source,
                    }
                })?,
            );
            config.insert(
                "columns".to_string(),
                serde_json::to_value(&view.columns).map_err(|source| {
                    BaseViewRegistryError::SerializeConfig {
                        view_name: view.name.clone(),
                        source,
                    }
                })?,
            );
            config.insert(
                "group_by".to_string(),
                serde_json::to_value(&view.group_by).map_err(|source| {
                    BaseViewRegistryError::SerializeConfig {
                        view_name: view.name.clone(),
                        source,
                    }
                })?,
            );
            config.insert(
                "aggregates".to_string(),
                serde_json::to_value(&view.aggregates).map_err(|source| {
                    BaseViewRegistryError::SerializeConfig {
                        view_name: view.name.clone(),
                        source,
                    }
                })?,
            );
            config.insert(
                "relations".to_string(),
                serde_json::to_value(&view.relations).map_err(|source| {
                    BaseViewRegistryError::SerializeConfig {
                        view_name: view.name.clone(),
                        source,
                    }
                })?,
            );
            config.insert(
                "rollups".to_string(),
                serde_json::to_value(&view.rollups).map_err(|source| {
                    BaseViewRegistryError::SerializeConfig {
                        view_name: view.name.clone(),
                        source,
                    }
                })?,
            );
            if !view.extras.is_empty() {
                config.insert(
                    "extras".to_string(),
                    serde_json::Value::Object(view.extras.clone()),
                );
            }

            views.push(BaseViewRegistryEntry {
                name: view.name.clone(),
                kind: view.kind,
                config: serde_json::Value::Object(config),
            });
        }

        Ok(Self { views })
    }

    /// Return all views in deterministic definition order.
    #[must_use]
    pub fn list(&self) -> &[BaseViewRegistryEntry] {
        &self.views
    }

    /// Lookup one view by name using case-insensitive matching.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&BaseViewRegistryEntry> {
        self.views
            .iter()
            .find(|entry| entry.name.eq_ignore_ascii_case(name))
    }
}

/// Query planner that compiles base table definitions into executable plan metadata.
#[derive(Debug, Default, Clone, Copy)]
pub struct BaseTableQueryPlanner;

impl BaseTableQueryPlanner {
    /// Compile one view query plan from registry metadata and paging settings.
    pub fn compile(
        &self,
        registry: &BaseViewRegistry,
        request: &TableQueryPlanRequest,
    ) -> Result<TableQueryPlan, BaseTableQueryPlanError> {
        if request.view_name.trim().is_empty() {
            return Err(BaseTableQueryPlanError::MissingViewName);
        }
        if request.page == 0 || request.page_size == 0 {
            return Err(BaseTableQueryPlanError::InvalidPagination {
                page: request.page,
                page_size: request.page_size,
            });
        }

        let view = registry.get(&request.view_name).ok_or_else(|| {
            BaseTableQueryPlanError::ViewNotFound {
                view_name: request.view_name.clone(),
            }
        })?;

        let config: RegistryTableConfig =
            serde_json::from_value(view.config.clone()).map_err(|source| {
                BaseTableQueryPlanError::InvalidViewConfig {
                    view_name: view.name.clone(),
                    source,
                }
            })?;

        let source_prefix = config.source.and_then(|source| {
            let normalized = source.trim().trim_matches('/').to_string();
            (!normalized.is_empty()).then_some(normalized)
        });
        let filters = config.filters;
        let sorts = config.sorts;
        let mut columns = config.columns;
        let group_by = config.group_by;
        let aggregates = config.aggregates;
        let relations = config.relations;
        let rollups = config.rollups;

        if columns.is_empty() {
            let mut seen = HashSet::new();
            columns = filters
                .iter()
                .map(|f| f.key.as_str())
                .chain(sorts.iter().map(|s| s.key.as_str()))
                .chain(group_by.iter().map(String::as_str))
                .chain(aggregates.iter().map(|a| a.alias.as_str()))
                .chain(relations.iter().map(|r| r.key.as_str()))
                .chain(rollups.iter().map(|r| r.alias.as_str()))
                .filter(|key| seen.insert((*key).to_string()))
                .map(|key| BaseColumnConfig {
                    key: key.to_string(),
                    label: None,
                    width: None,
                    hidden: false,
                })
                .collect();
        }
        let required_property_keys = collect_required_property_keys(
            &filters,
            &sorts,
            &columns,
            &group_by,
            &aggregates,
            &relations,
            &rollups,
        );

        let limit = request.page_size as usize;
        let page_offset = (request.page - 1) as usize;
        let offset =
            page_offset
                .checked_mul(limit)
                .ok_or(BaseTableQueryPlanError::PaginationOverflow {
                    page: request.page,
                    page_size: request.page_size,
                })?;

        Ok(TableQueryPlan {
            view_name: view.name.clone(),
            source_prefix,
            required_property_keys,
            filters,
            sorts,
            columns,
            group_by,
            aggregates,
            relations,
            rollups,
            limit,
            offset,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct RegistryTableConfig {
    #[serde(default)]
    source: Option<String>,
    #[serde(default)]
    filters: Vec<BaseFilterClause>,
    #[serde(default)]
    sorts: Vec<BaseSortClause>,
    #[serde(default)]
    columns: Vec<BaseColumnConfig>,
    #[serde(default)]
    group_by: Vec<String>,
    #[serde(default)]
    aggregates: Vec<BaseAggregateSpec>,
    #[serde(default)]
    relations: Vec<BaseRelationSpec>,
    #[serde(default)]
    rollups: Vec<BaseRollupSpec>,
}

fn collect_required_property_keys(
    filters: &[BaseFilterClause],
    sorts: &[BaseSortClause],
    columns: &[BaseColumnConfig],
    group_by: &[String],
    aggregates: &[BaseAggregateSpec],
    relations: &[BaseRelationSpec],
    rollups: &[BaseRollupSpec],
) -> Vec<String> {
    let mut keys = Vec::new();
    let mut dedupe = HashSet::new();
    let derived_keys = rollups
        .iter()
        .map(|rollup| rollup.alias.as_str())
        .collect::<HashSet<_>>();

    for key in filters
        .iter()
        .map(|filter| filter.key.as_str())
        .chain(sorts.iter().map(|sort| sort.key.as_str()))
        .chain(columns.iter().map(|column| column.key.as_str()))
        .chain(group_by.iter().map(String::as_str))
        .chain(
            aggregates
                .iter()
                .filter_map(|aggregate| aggregate.key.as_deref()),
        )
        .chain(relations.iter().map(|relation| relation.key.as_str()))
        .chain(rollups.iter().map(|rollup| rollup.relation_key.as_str()))
    {
        if derived_keys.contains(key) {
            continue;
        }
        let Some(normalized) = crate::lexer::property_key(key.trim()) else {
            continue;
        };
        if normalized.is_empty() {
            continue;
        }
        let dedupe_key = normalized.to_string();
        if dedupe.insert(dedupe_key) {
            keys.push(normalized.to_string());
        }
    }

    keys
}

/// View registry construction failures.
#[derive(Debug, Error)]
pub enum BaseViewRegistryError {
    /// Invalid execution semantics in the definition.
    #[error("invalid base definition: {reason}")]
    InvalidDefinition { reason: String },
    /// Duplicate names are not allowed in one `.base` document.
    #[error("duplicate base view name '{name}'")]
    DuplicateViewName {
        /// Duplicated name.
        name: String,
    },
    /// View config serialization failed.
    #[error("failed to serialize config for base view '{view_name}': {source}")]
    SerializeConfig {
        /// View name.
        view_name: String,
        /// JSON serialization error.
        #[source]
        source: serde_json::Error,
    },
}

/// Table query planner failures.
#[derive(Debug, Error)]
pub enum BaseTableQueryPlanError {
    /// Request view name was empty.
    #[error("table query plan requires a non-empty view name")]
    MissingViewName,
    /// Pagination values were invalid.
    #[error("invalid table query pagination page={page} page_size={page_size}")]
    InvalidPagination {
        /// One-based page number.
        page: u32,
        /// Page size.
        page_size: u32,
    },
    /// Pagination overflow occurred during offset computation.
    #[error("table query pagination overflow page={page} page_size={page_size}")]
    PaginationOverflow {
        /// One-based page number.
        page: u32,
        /// Page size.
        page_size: u32,
    },
    /// Requested view does not exist in registry.
    #[error("base view '{view_name}' not found in registry")]
    ViewNotFound {
        /// Requested view name.
        view_name: String,
    },
    /// Registry config payload could not be decoded.
    #[error("invalid config payload for base view '{view_name}': {source}")]
    InvalidViewConfig {
        /// View name.
        view_name: String,
        /// JSON deserialization error.
        #[source]
        source: serde_json::Error,
    },
}
