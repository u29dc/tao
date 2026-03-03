# SDK Error Code Catalog

## Contract

- Codes are stable string identifiers.
- Codes are namespaced by subsystem.
- Error messages are human-readable, non-secret, and actionable.
- `context` must be structured and machine-parseable.

## Format

`<subsystem>.<category>.<slug>`

Examples:

- `vault.not_found.root_missing`
- `note.invalid.path_outside_vault`
- `links.resolve.ambiguous_target`
- `properties.parse.invalid_yaml`
- `db.migration.checksum_mismatch`

## Catalog

| Code | Meaning | Hint |
| --- | --- | --- |
| `vault.not_found.root_missing` | Vault root path does not exist | Select a valid folder path |
| `vault.invalid.path_not_directory` | Vault path is not a directory | Provide a folder path |
| `vault.io.read_failed` | Failed to read from vault path | Check permissions and disk status |
| `note.not_found.path_missing` | Note path does not exist in vault | Refresh index and verify path |
| `note.invalid.path_outside_vault` | Requested path escapes vault root | Use in-vault relative path |
| `note.write.conflict_detected` | Concurrent change detected while writing | Re-read note and retry update |
| `links.resolve.ambiguous_target` | Multiple targets match unresolved wiki link | Use explicit path-qualified link |
| `links.resolve.target_missing` | No match found for link target | Create target note or fix link |
| `properties.parse.invalid_yaml` | Front matter YAML failed parsing | Fix YAML syntax in note front matter |
| `properties.type.invalid_value` | Value cannot be coerced to required type | Provide value with correct type |
| `bases.parse.invalid_schema` | `.base` file schema is invalid | Validate keys and structure |
| `bases.view.unsupported_type` | Requested base view type unsupported in v1 | Use table view in v1 |
| `search.query.invalid_input` | Search query string is invalid | Provide non-empty query |
| `index.watch.backend_unavailable` | File watch backend failed to initialize | Retry and verify OS watch limits |
| `index.reconcile.failed` | Reconcile pass failed unexpectedly | Run full reindex and inspect logs |
| `db.open.failed` | SQLite database open failed | Check file permissions and disk |
| `db.migration.checksum_mismatch` | Applied migration checksum differs | Inspect migration history and restore consistency |
| `system.internal.unexpected` | Unexpected internal error | Capture diagnostics and file issue |

## Reserved Ranges

- `system.*` reserved for framework-level failures.
- `db.*` reserved for storage engine failures.
- Application features must use their subsystem prefix.
