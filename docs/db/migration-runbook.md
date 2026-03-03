# Migration Runbook

## Preflight

1. Backup current sqlite database.
2. Verify writable filesystem and sufficient disk.
3. Validate migration checksums against known manifest.

## Apply

1. Open DB in migration mode.
2. Run pending migrations in order.
3. Commit migration state update.
4. Execute post-migration integrity checks.

## Validate

- Confirm `schema_migrations` entries match expected ids and checksums.
- Run minimal read/write smoke tests.
- Run `index_reconcile` dry validation.

## Failure Handling

- On checksum mismatch: stop startup, emit `db.migration.checksum_mismatch`.
- On SQL apply failure: rollback transaction and emit `db.migration.apply_failed`.
- Restore backup when migration cannot continue safely.
