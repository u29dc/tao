#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryAdapter {
    DocsFts,
    BaseTable,
    GraphIndex,
    MetaIndex,
    TaskIndex,
}

impl QueryAdapter {
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::DocsFts => "docs_fts",
            Self::BaseTable => "base_table",
            Self::GraphIndex => "graph_index",
            Self::MetaIndex => "meta_index",
            Self::TaskIndex => "task_index",
        }
    }
}
