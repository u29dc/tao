use super::*;

/// Full and incremental index rebuild failures.
#[derive(Debug, Error)]
pub enum FullIndexError {
    /// Scanner initialization failed.
    #[error("failed to initialize full index scanner: {source}")]
    CreateScanner {
        /// Scanner path error.
        #[source]
        source: Box<PathCanonicalizationError>,
    },
    /// Vault scan failed.
    #[error("failed to scan vault during full index: {source}")]
    Scan {
        /// Scan error.
        #[source]
        source: Box<VaultScanError>,
    },
    /// Fingerprint service initialization failed.
    #[error("failed to initialize fingerprint service for full index: {source}")]
    CreateFingerprintService {
        /// Fingerprint service path error.
        #[source]
        source: Box<PathCanonicalizationError>,
    },
    /// Fingerprinting one file failed.
    #[error("failed to fingerprint file '{path}': {source}")]
    Fingerprint {
        /// Absolute file path.
        path: PathBuf,
        /// Fingerprint error.
        #[source]
        source: Box<FileFingerprintError>,
    },
    /// Reading file contents failed.
    #[error("failed to read file '{path}': {source}")]
    ReadFile {
        /// Absolute file path.
        path: PathBuf,
        /// Filesystem error.
        #[source]
        source: std::io::Error,
    },
    /// Markdown parse failed.
    #[error("failed to parse markdown file '{path}': {source}")]
    ParseMarkdown {
        /// Absolute file path.
        path: PathBuf,
        /// Parse error.
        #[source]
        source: Box<MarkdownParseError>,
    },
    /// Typed property projection failed.
    #[error("failed to project typed properties for '{path}': {source}")]
    ProjectProperties {
        /// Absolute file path.
        path: PathBuf,
        /// Projection error.
        #[source]
        source: Box<PropertyProjectionError>,
    },
    /// Property JSON serialization failed.
    #[error("failed to serialize property json for '{path}': {source}")]
    SerializePropertyJson {
        /// Normalized path.
        path: String,
        /// JSON serialization error.
        #[source]
        source: Box<serde_json::Error>,
    },
    /// Base config serialization failed.
    #[error("failed to serialize base config payload for '{path}': {source}")]
    SerializeBaseConfig {
        /// Absolute base path.
        path: PathBuf,
        /// JSON serialization error.
        #[source]
        source: serde_json::Error,
    },
    /// Fingerprint modified timestamp overflows storage integer type.
    #[error("fingerprint modified timestamp overflows i64: {value}")]
    TimestampOverflow {
        /// Raw timestamp value.
        value: u128,
    },
    /// Changed path input is invalid for incremental indexing.
    #[error("invalid changed path '{path}': {reason}")]
    InvalidChangedPath {
        /// Invalid changed path.
        path: PathBuf,
        /// Validation reason.
        reason: String,
    },
    /// Provided batch size is invalid.
    #[error("invalid coalesced batch size: {value}")]
    InvalidBatchSize {
        /// Invalid batch size value.
        value: usize,
    },
    /// Beginning sqlite transaction failed.
    #[error("failed to begin full index transaction: {source}")]
    BeginTransaction {
        /// SQLite error.
        #[source]
        source: Box<rusqlite::Error>,
    },
    /// Clearing index tables failed.
    #[error("failed to clear index tables before rebuild: {source}")]
    ClearTables {
        /// SQLite error.
        #[source]
        source: Box<rusqlite::Error>,
    },
    /// Executing incremental maintenance SQL failed.
    #[error("failed to execute sql operation '{operation}': {source}")]
    ExecuteSql {
        /// SQL operation identifier.
        operation: &'static str,
        /// SQLite error.
        #[source]
        source: Box<rusqlite::Error>,
    },
    /// Upserting files table rows failed.
    #[error("failed to upsert file metadata during full index: {source}")]
    UpsertFileMetadata {
        /// Repository error.
        #[source]
        source: Box<tao_sdk_storage::FilesRepositoryError>,
    },
    /// Upserting properties rows failed.
    #[error("failed to upsert properties during full index: {source}")]
    UpsertProperty {
        /// Repository error.
        #[source]
        source: Box<tao_sdk_storage::PropertiesRepositoryError>,
    },
    /// Upserting task rows failed.
    #[error("failed to upsert tasks during full index: {source}")]
    UpsertTask {
        /// Repository error.
        #[source]
        source: Box<tao_sdk_storage::TasksRepositoryError>,
    },
    /// Inserting links rows failed.
    #[error("failed to insert links during full index: {source}")]
    InsertLink {
        /// Repository error.
        #[source]
        source: Box<tao_sdk_storage::LinksRepositoryError>,
    },
    /// Upserting bases rows failed.
    #[error("failed to upsert bases during full index: {source}")]
    UpsertBase {
        /// Repository error.
        #[source]
        source: Box<tao_sdk_storage::BasesRepositoryError>,
    },
    /// Upserting search index rows failed.
    #[error("failed to upsert search index rows during indexing: {source}")]
    UpsertSearchIndex {
        /// Repository error.
        #[source]
        source: Box<tao_sdk_storage::SearchIndexRepositoryError>,
    },
    /// Upserting index state failed.
    #[error("failed to upsert index state during full index: {source}")]
    UpsertIndexState {
        /// Repository error.
        #[source]
        source: Box<tao_sdk_storage::IndexStateRepositoryError>,
    },
    /// Serializing index summary state failed.
    #[error("failed to serialize index summary state: {source}")]
    SerializeStateSummary {
        /// JSON serialization error.
        #[source]
        source: Box<serde_json::Error>,
    },
    /// Transaction commit failed.
    #[error("failed to commit full index transaction: {source}")]
    CommitTransaction {
        /// SQLite error.
        #[source]
        source: Box<rusqlite::Error>,
    },
    /// Reading system clock failed.
    #[error("failed to read current time during full index: {source}")]
    Clock {
        /// System time conversion error.
        #[source]
        source: Box<std::time::SystemTimeError>,
    },
}

/// Stale cleanup workflow failures.
#[derive(Debug, Error)]
pub enum StaleCleanupError {
    /// Scanner initialization failed.
    #[error("failed to initialize stale cleanup scanner: {source}")]
    CreateScanner {
        /// Scanner path error.
        #[source]
        source: Box<PathCanonicalizationError>,
    },
    /// Vault scan failed.
    #[error("failed to scan vault for stale cleanup: {source}")]
    Scan {
        /// Scan error.
        #[source]
        source: Box<VaultScanError>,
    },
    /// Listing existing file rows failed.
    #[error("failed to list file rows for stale cleanup: {source}")]
    ListFiles {
        /// Repository error.
        #[source]
        source: Box<tao_sdk_storage::FilesRepositoryError>,
    },
    /// Beginning sqlite transaction failed.
    #[error("failed to begin stale cleanup transaction: {source}")]
    BeginTransaction {
        /// SQLite error.
        #[source]
        source: Box<rusqlite::Error>,
    },
    /// Deleting stale file row failed.
    #[error("failed to delete stale file row: {source}")]
    DeleteFileRow {
        /// Repository error.
        #[source]
        source: Box<tao_sdk_storage::FilesRepositoryError>,
    },
    /// Serializing cleanup summary failed.
    #[error("failed to serialize stale cleanup summary: {source}")]
    SerializeSummary {
        /// JSON serialization error.
        #[source]
        source: Box<serde_json::Error>,
    },
    /// Updating index state failed.
    #[error("failed to persist stale cleanup summary state: {source}")]
    UpsertIndexState {
        /// Repository error.
        #[source]
        source: Box<tao_sdk_storage::IndexStateRepositoryError>,
    },
    /// Transaction commit failed.
    #[error("failed to commit stale cleanup transaction: {source}")]
    CommitTransaction {
        /// SQLite error.
        #[source]
        source: Box<rusqlite::Error>,
    },
    /// Reading system clock failed.
    #[error("failed to read current time during stale cleanup: {source}")]
    Clock {
        /// System time conversion error.
        #[source]
        source: Box<std::time::SystemTimeError>,
    },
}

/// Checkpointed incremental indexing failures.
#[derive(Debug, Error)]
pub enum CheckpointedIndexError {
    /// Provided batch size is invalid.
    #[error("invalid checkpoint batch size: {value}")]
    InvalidBatchSize {
        /// Invalid batch size value.
        value: usize,
    },
    /// Provided per-run batch processing limit is invalid.
    #[error("invalid max-batches-per-run value: {value}")]
    InvalidBatchLimit {
        /// Invalid max-batches-per-run value.
        value: usize,
    },
    /// Changed path input is invalid.
    #[error("invalid changed path while creating checkpoint: {source}")]
    NormalizeChangedPath {
        /// Path normalization error.
        #[source]
        source: Box<FullIndexError>,
    },
    /// Stored checkpoint JSON payload cannot be parsed.
    #[error("failed to deserialize checkpoint state payload: {source}")]
    DeserializeCheckpoint {
        /// JSON deserialization error.
        #[source]
        source: Box<serde_json::Error>,
    },
    /// Checkpoint JSON payload serialization failed.
    #[error("failed to serialize checkpoint state payload: {source}")]
    SerializeCheckpoint {
        /// JSON serialization error.
        #[source]
        source: Box<serde_json::Error>,
    },
    /// Checkpoint summary serialization failed.
    #[error("failed to serialize checkpoint summary payload: {source}")]
    SerializeSummary {
        /// JSON serialization error.
        #[source]
        source: Box<serde_json::Error>,
    },
    /// Checkpoint case policy label is invalid.
    #[error("invalid checkpoint case policy '{value}'")]
    InvalidCheckpointCasePolicy {
        /// Unknown case policy label.
        value: String,
    },
    /// Reading checkpoint state row failed.
    #[error("failed to read checkpoint state row: {source}")]
    GetCheckpointState {
        /// Index state repository error.
        #[source]
        source: Box<tao_sdk_storage::IndexStateRepositoryError>,
    },
    /// Persisting checkpoint state row failed.
    #[error("failed to persist checkpoint state row: {source}")]
    UpsertCheckpointState {
        /// Index state repository error.
        #[source]
        source: Box<tao_sdk_storage::IndexStateRepositoryError>,
    },
    /// Deleting consumed checkpoint state row failed.
    #[error("failed to delete consumed checkpoint state row: {source}")]
    DeleteCheckpointState {
        /// Index state repository error.
        #[source]
        source: Box<tao_sdk_storage::IndexStateRepositoryError>,
    },
    /// Persisting checkpoint summary state row failed.
    #[error("failed to persist checkpoint summary state row: {source}")]
    UpsertIndexState {
        /// Index state repository error.
        #[source]
        source: Box<tao_sdk_storage::IndexStateRepositoryError>,
    },
    /// Applying incremental index batch failed.
    #[error("failed to apply incremental batch from checkpoint: {source}")]
    ApplyIncremental {
        /// Incremental indexing error.
        #[source]
        source: Box<FullIndexError>,
    },
    /// Reading system clock failed.
    #[error("failed to read current time during checkpointed indexing: {source}")]
    Clock {
        /// System time conversion error.
        #[source]
        source: Box<std::time::SystemTimeError>,
    },
}

/// Reconciliation scanner failures.
#[derive(Debug, Error)]
pub enum ReconciliationScanError {
    /// Provided batch size is invalid.
    #[error("invalid reconciliation scan batch size: {value}")]
    InvalidBatchSize {
        /// Invalid batch size value.
        value: usize,
    },
    /// Scanner initialization failed.
    #[error("failed to initialize reconciliation scanner: {source}")]
    CreateScanner {
        /// Scanner path error.
        #[source]
        source: Box<PathCanonicalizationError>,
    },
    /// Vault scan failed.
    #[error("failed to scan vault during reconciliation: {source}")]
    Scan {
        /// Scan error.
        #[source]
        source: Box<VaultScanError>,
    },
    /// Loading current indexed file rows failed.
    #[error("failed to list indexed file rows during reconciliation: {source}")]
    ListIndexedFiles {
        /// Files repository error.
        #[source]
        source: Box<tao_sdk_storage::FilesRepositoryError>,
    },
    /// Applying incremental repair batches failed.
    #[error("failed to repair reconciliation drift via incremental batches: {source}")]
    RepairBatch {
        /// Incremental indexing error.
        #[source]
        source: Box<FullIndexError>,
    },
}

/// Index consistency checker failures.
#[derive(Debug, Error)]
pub enum IndexConsistencyError {
    /// Vault root canonicalization failed.
    #[error("failed to canonicalize vault root '{path}': {source}")]
    CanonicalizeVaultRoot {
        /// Input vault root path.
        path: PathBuf,
        /// Filesystem canonicalization error.
        #[source]
        source: std::io::Error,
    },
    /// Listing indexed file rows failed.
    #[error("failed to list indexed file rows for consistency checks: {source}")]
    ListIndexedFiles {
        /// Files repository error.
        #[source]
        source: Box<tao_sdk_storage::FilesRepositoryError>,
    },
    /// SQL query operation failed.
    #[error("consistency checker sql operation '{operation}' failed: {source}")]
    Sql {
        /// SQL operation identifier.
        operation: &'static str,
        /// SQLite error.
        #[source]
        source: Box<rusqlite::Error>,
    },
    /// Reading system clock failed.
    #[error("failed to read current time during consistency check: {source}")]
    Clock {
        /// System time conversion error.
        #[source]
        source: Box<std::time::SystemTimeError>,
    },
}

/// Index self-heal workflow failures.
#[derive(Debug, Error)]
pub enum IndexSelfHealError {
    /// Running pre-repair consistency check failed.
    #[error("failed to run pre-repair consistency check: {source}")]
    CheckBefore {
        /// Consistency checker error.
        #[source]
        source: Box<IndexConsistencyError>,
    },
    /// Starting self-heal transaction failed.
    #[error("failed to begin index self-heal transaction: {source}")]
    BeginTransaction {
        /// SQLite error.
        #[source]
        source: Box<rusqlite::Error>,
    },
    /// Executing one repair SQL operation failed.
    #[error("failed to execute self-heal sql '{operation}' for record '{record_id}': {source}")]
    ExecuteSql {
        /// SQL operation identifier.
        operation: &'static str,
        /// Record identifier targeted for repair.
        record_id: String,
        /// SQLite error.
        #[source]
        source: Box<rusqlite::Error>,
    },
    /// Committing self-heal transaction failed.
    #[error("failed to commit index self-heal transaction: {source}")]
    CommitTransaction {
        /// SQLite error.
        #[source]
        source: Box<rusqlite::Error>,
    },
    /// Running post-repair consistency check failed.
    #[error("failed to run post-repair consistency check: {source}")]
    CheckAfter {
        /// Consistency checker error.
        #[source]
        source: Box<IndexConsistencyError>,
    },
}
