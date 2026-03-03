//! Vault path handling, canonicalization, and scan utilities.

mod fingerprint;
mod path;
mod scan;

pub use fingerprint::{FileFingerprint, FileFingerprintError, FileFingerprintService};
pub use path::{CanonicalPath, CasePolicy, PathCanonicalizationError, PathCanonicalizationService};
pub use scan::{VaultManifest, VaultManifestEntry, VaultScanError, VaultScanService};
