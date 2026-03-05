use crate::parser::{SortKey, WhereExpr};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryScope {
    Docs,
    Base,
    Graph,
    Meta,
    Task,
}

impl QueryScope {
    #[must_use]
    pub fn parse(input: &str) -> Option<Self> {
        let normalized = input.trim().to_ascii_lowercase();
        if normalized == "docs" {
            return Some(Self::Docs);
        }
        if normalized.starts_with("base:") {
            return Some(Self::Base);
        }
        if normalized == "graph" {
            return Some(Self::Graph);
        }
        if normalized.starts_with("meta:") {
            return Some(Self::Meta);
        }
        if normalized == "task" {
            return Some(Self::Task);
        }
        None
    }

    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Docs => "docs",
            Self::Base => "base",
            Self::Graph => "graph",
            Self::Meta => "meta",
            Self::Task => "task",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalQueryPlan {
    pub scope: QueryScope,
    pub source: String,
    pub query: Option<String>,
    pub where_expr: Option<WhereExpr>,
    pub sort_keys: Vec<SortKey>,
    pub projection: Vec<String>,
    pub limit: u64,
    pub offset: u64,
    pub execute: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalQueryPlanRequest {
    pub from: String,
    pub query: Option<String>,
    pub where_expr: Option<WhereExpr>,
    pub sort_keys: Vec<SortKey>,
    pub projection: Vec<String>,
    pub limit: u64,
    pub offset: u64,
    pub execute: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalPlanError {
    pub message: String,
}

impl std::fmt::Display for LogicalPlanError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for LogicalPlanError {}

#[derive(Debug, Default, Clone, Copy)]
pub struct LogicalPlanBuilder;

impl LogicalPlanBuilder {
    pub fn build(
        &self,
        request: LogicalQueryPlanRequest,
    ) -> Result<LogicalQueryPlan, LogicalPlanError> {
        let Some(scope) = QueryScope::parse(&request.from) else {
            return Err(LogicalPlanError {
                message: format!("unsupported query scope '{}'", request.from),
            });
        };

        if request.limit == 0 {
            return Err(LogicalPlanError {
                message: "query limit must be greater than zero".to_string(),
            });
        }

        Ok(LogicalQueryPlan {
            scope,
            source: request.from,
            query: request.query,
            where_expr: request.where_expr,
            sort_keys: request.sort_keys,
            projection: request.projection,
            limit: request.limit,
            offset: request.offset,
            execute: request.execute,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::parser::parse_where_expression;

    use super::{LogicalPlanBuilder, LogicalQueryPlanRequest, QueryScope};

    #[test]
    fn logical_plan_builder_generates_deterministic_plans() {
        let where_expr = parse_where_expression("priority >= 2 and done == false").ok();
        let request = LogicalQueryPlanRequest {
            from: "docs".to_string(),
            query: Some("project".to_string()),
            where_expr,
            sort_keys: Vec::new(),
            projection: vec!["path".to_string()],
            limit: 25,
            offset: 10,
            execute: true,
        };

        let first = LogicalPlanBuilder
            .build(request.clone())
            .expect("build plan");
        let second = LogicalPlanBuilder.build(request).expect("build plan again");
        assert_eq!(first, second);
        assert_eq!(first.scope, QueryScope::Docs);
    }

    #[test]
    fn logical_plan_builder_rejects_invalid_limit() {
        let request = LogicalQueryPlanRequest {
            from: "docs".to_string(),
            query: None,
            where_expr: None,
            sort_keys: Vec::new(),
            projection: Vec::new(),
            limit: 0,
            offset: 0,
            execute: true,
        };

        let error = LogicalPlanBuilder.build(request).expect_err("invalid plan");
        assert!(error.message.contains("limit"));
    }
}
