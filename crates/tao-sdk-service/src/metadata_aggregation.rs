//! Shared bounded metadata exploration over canonical ingested properties.

use rusqlite::Connection;
use tao_sdk_storage::{
    MetadataAggregateWindow, MetadataAggregationRepository, MetadataAggregationRepositoryError,
};

pub use tao_sdk_storage::{MAX_METADATA_AGGREGATION_LIMIT, MetadataAggregateRecord};

/// Supported metadata aggregate domain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataAggregationKind {
    /// Property keys, ordered lexically.
    Properties,
    /// Complete tag tokens, ordered by document count then exact token spelling.
    Tags,
    /// Complete alias values, ordered by document count then exact alias spelling.
    Aliases,
}

/// A validated metadata output window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetadataAggregationRequest {
    kind: MetadataAggregationKind,
    limit: u32,
    offset: u32,
}

impl MetadataAggregationRequest {
    /// Validate bounds before any database/configuration work is needed.
    pub fn new(
        kind: MetadataAggregationKind,
        limit: u32,
        offset: u32,
    ) -> Result<Self, MetadataAggregationRepositoryError> {
        if !(1..=MAX_METADATA_AGGREGATION_LIMIT).contains(&limit) {
            return Err(MetadataAggregationRepositoryError::InvalidLimit { limit });
        }
        Ok(Self {
            kind,
            limit,
            offset,
        })
    }
}

/// Exact metadata aggregate cardinality and a bounded deterministic page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataAggregationResult {
    /// Number of distinct keys/tokens before pagination.
    pub total: u64,
    /// Validated page size.
    pub limit: u32,
    /// Requested offset.
    pub offset: u32,
    /// At most `limit` result entries.
    pub items: Vec<MetadataAggregateRecord>,
}

/// SDK metadata aggregator shared by CLI and other adapters.
#[derive(Debug, Default, Clone, Copy)]
pub struct MetadataAggregationService;

impl MetadataAggregationService {
    /// Execute one coherent aggregate/count/window statement against canonical state.
    /// Exact totals require inspecting the selected key's canonical values; only
    /// bounded result rows are materialized by the SDK.
    pub fn aggregate(
        &self,
        connection: &Connection,
        request: MetadataAggregationRequest,
    ) -> Result<MetadataAggregationResult, MetadataAggregationRepositoryError> {
        let MetadataAggregateWindow { total, items } = match request.kind {
            MetadataAggregationKind::Properties => MetadataAggregationRepository::property_keys(
                connection,
                request.limit,
                request.offset,
            )?,
            MetadataAggregationKind::Tags => MetadataAggregationRepository::tokens(
                connection,
                "tags",
                request.limit,
                request.offset,
            )?,
            MetadataAggregationKind::Aliases => MetadataAggregationRepository::tokens(
                connection,
                "aliases",
                request.limit,
                request.offset,
            )?,
        };
        Ok(MetadataAggregationResult {
            total,
            limit: request.limit,
            offset: request.offset,
            items,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tao_sdk_storage::run_migrations;
    use tao_sdk_vault::CasePolicy;
    use tempfile::tempdir;

    fn aggregate(
        connection: &Connection,
        kind: MetadataAggregationKind,
        limit: u32,
        offset: u32,
    ) -> MetadataAggregationResult {
        MetadataAggregationService
            .aggregate(
                connection,
                MetadataAggregationRequest::new(kind, limit, offset).unwrap(),
            )
            .unwrap()
    }

    #[test]
    fn ingestion_preserves_complete_aliases_unicode_case_and_long_values() {
        let temp = tempdir().unwrap();
        let long_alias = "long Unicode alias 界 ".repeat(256);
        let long_alias = long_alias.trim();
        let a = serde_json::json!([
            "Smith, John",
            "New York",
            "#Literal",
            "Äpfel",
            "Café",
            long_alias
        ]);
        let b = serde_json::json!(["Smith, John", "New York", "äpfel", "Cafe\u{301}"]);
        fs::write(temp.path().join("a.md"), format!("---\naliases: {a}\ntags: [\"#Rust\", \"rust\", \"Äpfel\"]\nstatus: active\n---\n# A\n")).unwrap();
        fs::write(
            temp.path().join("b.md"),
            format!("---\naliases: {b}\ntags: [\"Rust\", \"äpfel\"]\n---\n# B\n"),
        )
        .unwrap();
        let mut connection = Connection::open_in_memory().unwrap();
        run_migrations(&mut connection).unwrap();
        crate::FullIndexService::default()
            .rebuild(temp.path(), &mut connection, CasePolicy::Sensitive)
            .unwrap();
        let aliases = aggregate(&connection, MetadataAggregationKind::Aliases, 100, 0);
        assert_eq!(aliases.total, 8);
        let values = aliases
            .items
            .iter()
            .map(|item| (item.value.as_str(), item.total))
            .collect::<std::collections::BTreeMap<_, _>>();
        assert_eq!(values["Smith, John"], 2);
        assert_eq!(values["New York"], 2);
        assert_eq!(values["#Literal"], 1);
        assert_eq!(values["Äpfel"], 1);
        assert_eq!(values["äpfel"], 1);
        assert_eq!(values["Café"], 1);
        assert_eq!(values["Cafe\u{301}"], 1);
        assert_eq!(values[long_alias], 1);
        assert!(!values.contains_key("Smith"));
        assert!(!values.contains_key("New"));
        let first_page = aggregate(&connection, MetadataAggregationKind::Aliases, 1, 0);
        assert_eq!(first_page.total, 8);
        assert_eq!(first_page.items[0].value, "New York");
        let second_page = aggregate(&connection, MetadataAggregationKind::Aliases, 1, 1);
        assert_eq!(second_page.total, 8);
        assert_eq!(second_page.items[0].value, "Smith, John");
        let beyond = aggregate(&connection, MetadataAggregationKind::Aliases, 1, u32::MAX);
        assert_eq!(beyond.total, 8);
        assert!(beyond.items.is_empty());
        let tags = aggregate(&connection, MetadataAggregationKind::Tags, 100, 0);
        assert_eq!(tags.items[0].value, "Rust");
        assert_eq!(tags.items[0].total, 2);
        assert_eq!(tags.total, 3);
        let keys = aggregate(&connection, MetadataAggregationKind::Properties, 1, 1);
        assert_eq!(keys.total, 3);
        assert_eq!(keys.items[0].value, "status");
        assert_eq!(keys.items[0].total, 1);
    }

    #[test]
    fn windows_are_bounded_and_empty_totals_survive_pagination() {
        let temp = tempdir().unwrap();
        let aliases = (0..1205)
            .map(|number| format!("alias-{number:04}"))
            .collect::<Vec<_>>();
        fs::write(
            temp.path().join("many.md"),
            format!(
                "---\naliases: {}\n---\n# Many\n",
                serde_json::to_string(&aliases).unwrap()
            ),
        )
        .unwrap();
        let mut connection = Connection::open_in_memory().unwrap();
        run_migrations(&mut connection).unwrap();
        let empty = aggregate(&connection, MetadataAggregationKind::Aliases, 1, 15);
        assert_eq!(empty.total, 0);
        assert!(empty.items.is_empty());
        crate::FullIndexService::default()
            .rebuild(temp.path(), &mut connection, CasePolicy::Sensitive)
            .unwrap();
        let page = aggregate(&connection, MetadataAggregationKind::Aliases, 3, 1200);
        assert_eq!(page.total, 1205);
        assert_eq!(page.items.len(), 3);
        assert_eq!(page.items[0].value, "alias-1200");
        assert_eq!(page.items[2].value, "alias-1202");
        for limit in [0, 1001, u32::MAX] {
            assert!(
                MetadataAggregationRequest::new(MetadataAggregationKind::Aliases, limit, 0)
                    .is_err()
            );
        }
        for corrupt in ["not valid JSON", "{}", "[1]", "[null]", "[[\"nested\"]]"] {
            connection
                .execute(
                    "UPDATE properties SET value_json=?1 WHERE key='aliases'",
                    [corrupt],
                )
                .unwrap();
            assert!(
                MetadataAggregationService
                    .aggregate(
                        &connection,
                        MetadataAggregationRequest::new(MetadataAggregationKind::Aliases, 1, 0)
                            .unwrap()
                    )
                    .is_err(),
                "corrupt canonical tokens must fail: {corrupt}"
            );
        }
    }
}
