# Versioning Policy

## Scope

This policy defines versioning and compatibility for:

- Rust SDK crates (`crates/tao-sdk-*`)
- CLI binary (`obs`)
- Bridge binary and DTO contract (`tao-sdk-bridge`)
- macOS app scaffold (`apps/obs-macos`)

## SemVer Rules

- Use SemVer for all published artifacts: `MAJOR.MINOR.PATCH`.
- `PATCH`: bug fixes, performance improvements, and internal refactors with no public contract changes.
- `MINOR`: backward-compatible public API additions, new CLI commands, and additive DTO fields.
- `MAJOR`: breaking API/contract changes, incompatible schema/version transitions, or removed commands.

## Component Matrix

| Component | Version Source | Compatibility Contract |
| --- | --- | --- |
| SDK crates | Cargo package version | Public Rust API + documented service behavior |
| CLI (`obs`) | Cargo package version | Command names, flags, JSON envelope fields |
| Bridge (`tao-sdk-bridge`) | Cargo package version + `BRIDGE_SCHEMA_VERSION` | DTO schema major compatibility (`v<major>`) |
| macOS app | Swift package version | Compatible with bridge schema major and CLI behavior it invokes |

## Bridge/DTO Compatibility

- Bridge schema string format: `v<major>[.<minor>]`.
- App/clients MUST accept any `minor` for the same `major`.
- App/clients MUST reject incompatible schema majors.
- DTO changes:
  - Additive optional fields: `MINOR`.
  - Field removal/rename/type change: `MAJOR`.

## Database/Migration Compatibility

- Migrations are forward-only with checksum guards.
- Release artifacts MUST run migration preflight before startup.
- Migration checksum mismatch is a blocking release failure.
- Migration compatibility changes that break prior runtime assumptions require `MAJOR`.

## Release Tagging

- Stable releases: `vX.Y.Z`
- Release candidates: `vX.Y.Z-rc.N`
- Pre-release tags MUST map to a committed release report under `docs/release/`.

## Deprecation Window

- Deprecated CLI flags/commands MUST emit warnings for at least one `MINOR` before removal.
- Deprecated bridge fields MUST remain readable for at least one `MINOR` when possible.

## Required Checks Before Tag

- `bun run util:check`
- Swift build/test on macOS CI
- perf budget gate (`scripts/check-perf-budgets.sh`)
- migration preflight check path green
