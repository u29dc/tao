//! Service-layer orchestration entrypoints over SDK subsystem crates.

mod base_executor;
mod config;
mod content;
mod graph;
mod health;
mod index_refresh;
mod indexing;
mod ingest;
mod metadata_aggregation;
mod property_query;
mod publication_lock;
mod search;
mod search_corpus;

pub use base_executor::*;
pub use config::{
    SdkBootstrapError, SdkBootstrapService, SdkBootstrapSnapshot, SdkConfig, SdkConfigError,
    SdkConfigFieldSources, SdkConfigFileInspection, SdkConfigInspection,
    SdkConfigInspectionService, SdkConfigLoader, SdkConfigOverrides, ensure_runtime_paths,
};
pub use content::*;
pub use graph::*;
pub use health::*;
pub use index_refresh::*;
pub use indexing::{
    CURRENT_LINK_RESOLUTION_VERSION, CheckpointedIndexError, CheckpointedIndexResult,
    CheckpointedIndexService, CoalescedBatchIndexResult, CoalescedBatchIndexService,
    ConsistencyIssueKind, FullIndexError, FullIndexResult, FullIndexService,
    IncrementalIndexResult, IncrementalIndexService, IndexConsistencyChecker,
    IndexConsistencyError, IndexConsistencyIssue, IndexConsistencyReport, IndexSelfHealError,
    IndexSelfHealResult, IndexSelfHealService, LINK_RESOLUTION_VERSION_STATE_KEY,
    ReconciliationScanError, ReconciliationScanMode, ReconciliationScanResult,
    ReconciliationScannerService, SearchCorpusRefreshMode, StaleCleanupError, StaleCleanupResult,
    StaleCleanupService,
};
pub use ingest::{IngestedMarkdownNote, MarkdownIngestError, MarkdownIngestPipeline};
pub use metadata_aggregation::*;
pub use property_query::*;
pub use search::*;
pub use search_corpus::*;
pub use tao_sdk_vault::{IndexCancellationScope, check_index_cancellation};

#[cfg(test)]
mod tests;
