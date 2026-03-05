use crate::physical_plan::PhysicalQueryPlan;

/// Lightweight physical plan optimizer used for deterministic fast-path rewrites.
#[derive(Debug, Default, Clone, Copy)]
pub struct PhysicalPlanOptimizer;

impl PhysicalPlanOptimizer {
    /// Apply deterministic rewrites to one physical query plan.
    #[must_use]
    pub fn optimize(&self, mut plan: PhysicalQueryPlan) -> PhysicalQueryPlan {
        // Keep stage order stable while removing accidental duplicates.
        plan.filter_stages.dedup();
        plan
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        adapters::QueryAdapter,
        optimizer::PhysicalPlanOptimizer,
        parser::{NullOrder, SortDirection, SortKey},
        physical_plan::PhysicalQueryPlan,
    };

    #[test]
    fn optimizer_is_deterministic_for_duplicate_stages() {
        let input = PhysicalQueryPlan {
            adapter: QueryAdapter::DocsFts,
            filter_stages: vec![
                "query_text".to_string(),
                "where_expr".to_string(),
                "where_expr".to_string(),
            ],
            sort_keys: vec![SortKey {
                field: "path".to_string(),
                direction: SortDirection::Asc,
                null_order: NullOrder::First,
            }],
            projection: vec!["path".to_string()],
            limit: 10,
            offset: 0,
            execute: true,
        };

        let first = PhysicalPlanOptimizer.optimize(input.clone());
        let second = PhysicalPlanOptimizer.optimize(input);
        assert_eq!(first, second);
        assert_eq!(first.filter_stages, vec!["query_text", "where_expr"]);
    }
}
