//! Vault path handling, canonicalization, and scan utilities.

mod path;
mod scan;

pub use path::{CanonicalPath, CasePolicy, PathCanonicalizationError, PathCanonicalizationService};
pub use scan::{VaultManifest, VaultManifestEntry, VaultScanError, VaultScanService};
