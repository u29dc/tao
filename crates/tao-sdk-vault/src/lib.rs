//! Vault path handling, canonicalization, and scan utilities.

mod cancellation;
mod fingerprint;
mod kind;
mod path;
mod scan;

pub use cancellation::{IndexCancellationScope, OperationCancelled, check_index_cancellation};
pub use fingerprint::{
    CapturedFile, DEFAULT_CAPTURE_LIMIT_BYTES, FINGERPRINT_POLICY_VERSION, FileFingerprint,
    FileFingerprintError, FileFingerprintService, FileMetadata,
};
pub use kind::{FileKind, file_kind, normalized_extension};
pub use path::{
    CanonicalPath, CasePolicy, PathCanonicalizationError, PathCanonicalizationService,
    RelativeVaultPathError, normalize_relative_path, path_match_key, validate_relative_vault_path,
};
pub use scan::{
    VaultInclusionPolicy, VaultManifest, VaultManifestEntry, VaultScanError, VaultScanService,
};
