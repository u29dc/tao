# Workspace Boundaries

## Core Principles

- Domain behavior lives in Rust SDK crates.
- Interface layers (Swift bridge, CLI, TUI) are adapters only.
- Crates should depend inward toward domain, not outward toward UI adapters.

## Crate Responsibilities

- `obs-sdk-core`: shared primitives, ids, error envelope, time/path utilities.
- `obs-sdk-vault`: vault path handling, discovery, and boundary checks.
- `obs-sdk-markdown`: markdown parse/render and section extraction utilities.
- `obs-sdk-links`: wikilink parser and deterministic resolution.
- `obs-sdk-properties`: front matter extraction and typed projection.
- `obs-sdk-bases`: `.base` parsing, validation, and table planning.
- `obs-sdk-storage`: sqlite schema, migrations, repositories, and transactions.
- `obs-sdk-search`: lexical search adapters over indexed content.
- `obs-sdk-watch`: filesystem watch normalization and reconcile triggering.
- `obs-sdk-service`: use-case orchestration and public service surface.
- `obs-sdk-bridge`: FFI-safe API boundary for Swift consumption.
- `obs-cli`: thin command adapter over `obs-sdk-service`.
- `obs-tui`: future terminal UI adapter over `obs-sdk-service`.

## Dependency Direction

- `obs-sdk-service` depends on SDK subsystem crates.
- `obs-sdk-bridge`, `obs-cli`, and `obs-tui` depend on `obs-sdk-service`.
- `obs-sdk-storage` must not depend on UI or CLI crates.

## Rules

- No domain logic in adapter crates.
- No direct sqlite access from adapter crates.
- Cross-crate DTOs must be documented in API spec.
