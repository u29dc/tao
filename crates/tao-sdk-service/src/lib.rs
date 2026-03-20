//! Service-layer orchestration entrypoints over SDK subsystem crates.

mod base_executor;
mod config;
mod feature_flags;
mod graph;
mod health;
mod import_export;
mod indexing;
mod ingest;
mod note_crud;
mod property_query;
mod property_update;
mod reconcile;
mod tracing_hooks;
mod transactions;

pub use base_executor::*;
pub use config::{
    SdkBootstrapError, SdkBootstrapService, SdkBootstrapSnapshot, SdkConfig, SdkConfigError,
    SdkConfigLoader, SdkConfigOverrides, ensure_runtime_paths,
};
pub use feature_flags::{FeatureFlagParseError, FeatureFlagRegistry, SdkFeature};
pub use graph::*;
pub use health::*;
pub use import_export::{
    FilesystemImportExportService, ImportExportBoundaryError, ImportExportServiceBoundary,
    TransferExecutionRequest, TransferExecutionResult, TransferFailure, TransferItem,
    TransferItemKind, TransferJobKind, TransferMode, TransferPlan, TransferSummary,
};
pub use indexing::{
    CURRENT_LINK_RESOLUTION_VERSION, CheckpointedIndexError, CheckpointedIndexResult,
    CheckpointedIndexService, CoalescedBatchIndexResult, CoalescedBatchIndexService,
    ConsistencyIssueKind, FullIndexError, FullIndexResult, FullIndexService,
    IncrementalIndexResult, IncrementalIndexService, IndexConsistencyChecker,
    IndexConsistencyError, IndexConsistencyIssue, IndexConsistencyReport, IndexSelfHealError,
    IndexSelfHealResult, IndexSelfHealService, LINK_RESOLUTION_VERSION_STATE_KEY,
    ReconciliationScanError, ReconciliationScanResult, ReconciliationScannerService,
    StaleCleanupError, StaleCleanupResult, StaleCleanupService,
};
pub use ingest::{IngestedMarkdownNote, MarkdownIngestError, MarkdownIngestPipeline};
pub use note_crud::*;
pub use property_query::*;
pub use property_update::*;
pub use reconcile::*;
pub use tracing_hooks::ServiceTraceContext;
pub use transactions::{
    SdkTransactionCoordinator, SdkTransactionError, StorageWriteError, StorageWriteService,
};

#[cfg(test)]
#[path = "service_tests.rs"]
mod tests;
