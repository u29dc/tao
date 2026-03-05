use crate::{
    adapters::QueryAdapter,
    logical_plan::{LogicalQueryPlan, QueryScope},
    parser::SortKey,
};

#[derive(Debug, Clone, PartialEq)]
pub struct PhysicalQueryPlan {
    pub adapter: QueryAdapter,
    pub filter_stages: Vec<String>,
    pub sort_keys: Vec<SortKey>,
    pub projection: Vec<String>,
    pub limit: u64,
    pub offset: u64,
    pub execute: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PhysicalPlanError {
    pub message: String,
}

impl std::fmt::Display for PhysicalPlanError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for PhysicalPlanError {}

#[derive(Debug, Default, Clone, Copy)]
pub struct PhysicalPlanBuilder;

impl PhysicalPlanBuilder {
    pub fn build(
        &self,
        logical: &LogicalQueryPlan,
    ) -> Result<PhysicalQueryPlan, PhysicalPlanError> {
        let adapter = match logical.scope {
            QueryScope::Docs => QueryAdapter::DocsFts,
            QueryScope::Base => QueryAdapter::BaseTable,
            QueryScope::Graph => QueryAdapter::GraphIndex,
            QueryScope::Meta => QueryAdapter::MetaIndex,
            QueryScope::Task => QueryAdapter::TaskIndex,
        };

        let mut filter_stages = Vec::new();
        if logical.query.is_some() {
            filter_stages.push("query_text".to_string());
        }
        if logical.where_expr.is_some() {
            filter_stages.push("where_expr".to_string());
        }
        if !logical.sort_keys.is_empty() {
            filter_stages.push("sort".to_string());
        }

        Ok(PhysicalQueryPlan {
            adapter,
            filter_stages,
            sort_keys: logical.sort_keys.clone(),
            projection: logical.projection.clone(),
            limit: logical.limit,
            offset: logical.offset,
            execute: logical.execute,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        logical_plan::{LogicalPlanBuilder, LogicalQueryPlanRequest},
        parser::parse_sort_keys,
    };

    use super::PhysicalPlanBuilder;

    #[test]
    fn physical_plan_builder_keeps_adapter_and_stage_order_stable() {
        let logical = LogicalPlanBuilder
            .build(LogicalQueryPlanRequest {
                from: "docs".to_string(),
                query: Some("alpha".to_string()),
                where_expr: None,
                sort_keys: parse_sort_keys(Some("path:asc")).expect("sort keys"),
                projection: vec!["path".to_string()],
                limit: 10,
                offset: 0,
                execute: true,
            })
            .expect("logical plan");

        let plan = PhysicalPlanBuilder.build(&logical).expect("physical plan");
        assert_eq!(plan.adapter.label(), "docs_fts");
        assert_eq!(plan.filter_stages, vec!["query_text", "sort"]);
    }
}
