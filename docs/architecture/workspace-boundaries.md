# Workspace Boundaries

## Core Principles

- Domain behavior lives in Rust SDK crates.
- Interface layers (Swift bridge, CLI, TUI) are adapters only.
- Crates should depend inward toward domain, not outward toward UI adapters.

## Crate Responsibilities

- `tao-sdk-core`: shared primitives, ids, error envelope, time/path utilities.
- `tao-sdk-vault`: vault path handling, discovery, and boundary checks.
- `tao-sdk-markdown`: markdown parse/render and section extraction utilities.
- `tao-sdk-links`: wikilink parser and deterministic resolution.
- `tao-sdk-properties`: front matter extraction and typed projection.
- `tao-sdk-bases`: `.base` parsing, validation, and table planning.
- `tao-sdk-storage`: sqlite schema, migrations, repositories, and transactions.
- `tao-sdk-search`: lexical search adapters over indexed content.
- `tao-sdk-watch`: filesystem watch normalization and reconcile triggering.
- `tao-sdk-service`: use-case orchestration and public service surface.
- `tao-sdk-bridge`: FFI-safe API boundary for Swift consumption.
- `tao-cli`: thin command adapter over `tao-sdk-service`.
- `tao-tui`: future terminal UI adapter over `tao-sdk-service`.

## Dependency Direction

- `tao-sdk-service` depends on SDK subsystem crates.
- `tao-sdk-bridge`, `tao-cli`, and `tao-tui` depend on `tao-sdk-service`.
- `tao-sdk-storage` must not depend on UI or CLI crates.

## Rules

- No domain logic in adapter crates.
- No direct sqlite access from adapter crates.
- Cross-crate DTOs must be documented in API spec.
