## Crate

`tao-sdk-vault`

## Purpose

Provide vault filesystem primitives: canonical path safety, scanning, and file fingerprinting.

## Public API

- Path canonicalization and vault-boundary checks
- `VaultScanService` and manifest structs
- Fingerprint generation for incremental-change detection

## Internal Design

- Deterministic scanner ordering.
- Unicode normalization and case-policy aware matching.
- Parallel metadata collection for scan throughput.

## Data Flow

Vault root -> scan/path normalization -> manifest/fingerprint -> service ingest/index pipeline.

## Dependencies

- External: `walkdir`, `unicode-normalization`, `rayon`, `blake3`, `thiserror`

## Testing

- `cargo test -p tao-sdk-vault --release`
- Tests cover path boundary enforcement, symlink handling, scan stability, and fingerprint updates.

## Limits

- Does not parse markdown or manage database writes.
