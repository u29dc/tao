# Migration and Versioning Strategy

## Scope

Define database migration policy and versioning rules for SDK, CLI, and Swift app integration.

## Versioning Model

- Rust crates use semantic versioning.
- Workspace ships as coordinated release train for v1.
- Bridge DTO contract version is tracked separately and referenced by SDK version.

## Database Migration Rules

- Forward-only SQL migrations in `crates/tao-sdk-storage/migrations/`.
- Migration files use ordered numeric prefix: `0001_*.sql`, `0002_*.sql`.
- Apply migrations inside transaction when possible.
- Record migration id + checksum in `schema_migrations` table.
- Startup must fail fast on checksum mismatch.

## Compatibility

- SDK minor versions may add tables/columns with defaults or nullable semantics.
- Breaking schema changes require major version bump and migration notes.
- CLI and Swift app must check runtime DB schema compatibility at startup.

## Rollback Policy

- No destructive rollback in automatic runtime path.
- Recovery workflow:
  1. restore DB backup
  2. run preflight checks
  3. reapply migrations deterministically

## Release Gating

Before release candidate:

- migration preflight passes on clean and upgraded DB paths
- checksum verification passes
- compatibility matrix updated with schema version references

## Required Artifacts

- `docs/db/migration-runbook.md`
- migration test fixtures for up/downstream compatibility checks
