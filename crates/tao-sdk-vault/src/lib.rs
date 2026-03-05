//! Vault path handling, canonicalization, and scan utilities.

mod fingerprint;
mod path;
mod scan;

pub use fingerprint::{FileFingerprint, FileFingerprintError, FileFingerprintService};
pub use path::{
    CanonicalPath, CasePolicy, PathCanonicalizationError, PathCanonicalizationService,
    RelativeVaultPathError, validate_relative_vault_path,
};
pub use scan::{VaultManifest, VaultManifestEntry, VaultScanError, VaultScanService};
