## Crate

`tao-cli`

## Purpose

Provide the Tao command-line interface as a thin JSON-first adapter over SDK services.

## Public API

- Binary: `tao`
- Command groups: `config`, `vault`, `doc`, `base`, `graph`, `meta`, `task`, `validate`, `search`, `query`
- Public vault-content operations are read-only. Tao writes internal index/cache state for `vault open`, `vault reindex`, daemon/watch/cache maintenance, and content extraction only.
- `search` is the primary graph-aware exploration entrypoint across indexed Markdown, TXT, PDF pages, file inventory, bases/frontmatter, tasks, and links. Use `rg` for raw grep; use `tao search` when index metadata, normalized aliases, canonical ranking, and relationships matter.
- Common search forms:
  - `tao search "query" --context --depth 2`
  - `tao search invoice --kind files --scope WORK/012-FINANCE`
  - `tao search --path path/to/note.md --context --depth 2`
- `validate <path>` checks markdown frontmatter, `.base` files, or supported files in a folder. Folder validation is non-recursive unless `--recursive` is passed.
- Canonical graph primitives: `graph links` for one-hop link windows, `graph audit` for unresolved links, structural isolates, components, and scoped inbound-link audits.
- The graph surface has four commands: `links`, `audit`, `path`, and `walk`. Diagnostic variants are `audit --kind` values. Obsolete aliases are removed.
- JSON envelope output for automation by default

## Internal Design

- `src/cli_impl/` owns command parsing, dispatch, and envelope serialization.
- Business logic remains in SDK crates; CLI should not reimplement domain rules.

## Data Flow

CLI args -> request mapping -> SDK service call -> envelope serialization -> stdout.

## Runtime Semantics

- Normal vault-facing commands may auto-connect to an existing background daemon and auto-start it when unavailable.
- `vault daemon *` commands are inspection and troubleshooting primitives, not the only way daemon mode is entered.
- `config show` reports effective config values, per-field source labels, source inputs, and precedence without opening or migrating SQLite state.
- `vault reindex` honors a vault-root `.taoignore` for Tao knowledge-indexing exclusions; `.gitignore` is not used because Git tracking and knowledge indexing are separate concerns.
- `vault reindex` also rebuilds the derived unified search corpus used by `tao search`; dry-run output reports `search_index_stale`, `would_rebuild_search_index`, `search_segments_total`, and `search_aliases_total`.
- A one-shot `tao search` repairs a missing or stale derived search corpus before querying. This is an internal index/database write, not a vault-content write.
- `health` and `vault preflight` are fresh observational diagnostics; they do not reconcile or cache command results.
- `watcher_status` in CLI health snapshots reflects change-monitor state, not daemon lifecycle state by itself.

## Dependencies

- Internal: `tao-sdk-service`, `tao-sdk-search`, `tao-sdk-bases`, `tao-sdk-watch`, `tao-sdk-vault`, `tao-sdk-storage`, `tao-sdk-bridge`
- External: `clap`, `serde`, `serde_json`, `rusqlite`

## Testing

- `cargo test -p tao-cli --release`
- Contract tests validate stable JSON envelope shape and command IDs.

## Limits

- No UI responsibilities.
- No direct SQLite schema ownership.

## Content and execution contracts

- `tao tools <name>` returns inline JSON Schemas, parser defaults, and capability information. Query options unsupported by a scope are rejected before refresh.
- `--execution-mode auto|direct|required-daemon` selects transport; `--timeout-ms` includes queued work. Responses disclose the actual execution/cache mode.
- The daemon serves coherent published snapshots while background refresh or PDF extraction runs. `meta.runtime.refreshPending` reports outstanding refresh work; an explicit successful `vault reindex` provides a refresh boundary. A missing index still needs initial preparation. Foreground and background refresh take fair turns, and extraction holds database write access only for short state/publication transactions.
- Paged list/query/graph/metadata reads return `meta.continuation.token`. Supply it as `--continuation` on subsequent windows to bind query, scope, projection and publication; changed state returns a restart-required error. An offset without a token is an independent seek against the current snapshot. Content reads instead use their more precise `continuation_revision` with `--revision`.
- `doc list --limit 100 --offset 0` returns a bounded Markdown-only window. `query --from docs` also preserves Markdown-only semantics; broad `search` includes TXT/PDF content.
- Document query titles use the indexed canonical title, including its original case. Structured string comparisons are exact; full-text search uses normalized token/prefix matching rather than arbitrary substrings. General expression/sort fallbacks stop with `query_work_limit` above 100,000 materialized rows or an estimated 32 MiB, including requests with large offsets.
- `doc read --path notes/a.md` returns bounded indexed revision-bound lines. PDF reads return page segments using the same contract. Inventory-only assets return metadata and the original file reference.
- `doc read --path evidence.pdf --limit 20` returns physical one-based page locators. Follow `next_offset` using the returned `continuation_revision` as `--revision`; a revision change requires restarting pagination.
- `vault reindex` publishes inventory and Markdown/TXT content, then reports PDF queue state separately. Add `--wait-content-ms 120000` for bounded local extraction. A timeout preserves pending work.
- PDF extraction requires local Poppler tools and Tesseract with the English model for OCR. No documents are uploaded and no dependencies are installed automatically. `health` reports available capabilities, coverage and failures.
- File-backed indexes process PDFs with half the available CPU parallelism, capped at eight concurrent jobs across processes. Native extraction batches up to 32 pages per subprocess and falls back to individual pages when page boundaries or output limits cannot be verified. OCR preserves the same coverage checks. Existing revision-bound results are reused.
- Pending PDF work keeps its daemon extraction context alive after idle read caches and watchers expire. The queue continues unattended until drained or the daemon is stopped.
- A per-database publication lock coordinates source preparation with short extraction publications, while readers and OCR run independently. Deferred captures are owned by the extraction queue and do not make unchanged files drift on every reindex. No index reset is needed for these changes.
- `health` observes current published index and runtime state without scanning source content; `health --deep` adds filesystem metadata drift and SQLite/foreign-key/FTS consistency checks. FTS checking uses a private in-memory database snapshot, so its memory cost scales with database size. Neither mode repairs the index or reuses a cached command response.
- `vault preflight`, query explain without execution, and reindex dry-run are observational. Missing state remains explicit.
- `--no-pii` on search suppresses structured metadata values. It does not sanitize arbitrary prose, paths, or original documents.

## Building and installing

`bun run util:check` and `bun run build` do not install or overwrite an existing Tao installation. `bun run release:cli` creates a verified package; `bun run release:install -- --out /explicit/path` installs explicitly with rollback and preserves runtime/configuration state. `bun run release:uninstall -- --out /explicit/path --dry-run` previews managed removal.

On macOS, the build, check, test, and benchmark scripts automatically select installed Command Line Tools when `DEVELOPER_DIR` is unset or empty. Run `bun run util:check` or `bun run build` normally. An explicit `DEVELOPER_DIR` is preserved; Linux behavior is unchanged. This per-process selection does not change the system toolchain selection or accept a licence. Direct Cargo commands can use `./scripts/cargo.sh` for the same behavior.

## Index format and configuration

This release starts a clean index format. Previous databases are refused with a rebuild instruction; Tao never deletes them automatically. Stop the old daemon, archive or remove only the configured Tao index directory, and reindex the unchanged vault with the new binary. Custom database locations are shown by `config show`. Source files are not migration inputs and are never rewritten.

Configuration contains vault root, data/database paths and case policy. Before using the new binary, remove retired settings (`tracing_enabled`, `feature_flags`, `read_only`) and retired sections such as `[security]` from existing configuration files. Unknown keys and sections are rejected so a misspelled option cannot silently do nothing. The complete supported schema is `[vault].root`, `[storage].data_dir`, `[storage].db_path`, and `[runtime].case_policy`. Source operations are always read-only. Runtime extraction and cache state remain internal writes.
