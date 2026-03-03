# SDK API Surface Specification

## Design Goals

- One source of truth for core domain behavior.
- Language-neutral contracts for Rust CLI and Swift bridge.
- Stable DTOs with explicit versioning.

## Namespaces

- `vault`
- `note`
- `links`
- `properties`
- `bases`
- `search`
- `index`
- `watch`
- `system`

## Core DTOs

### Result Envelope

```text
Result<T> = { ok: true, value: T } | { ok: false, error: SdkError }
```

### SdkError

```text
SdkError {
  code: String,
  message: String,
  context: Map<String, JsonValue>,
  hint: Option<String>
}
```

### VaultStats

```text
VaultStats {
  vault_path: String,
  files_total: u64,
  markdown_files: u64,
  bases_files: u64,
  unresolved_links: u64,
  last_index_at: Option<String>
}
```

### NoteView

```text
NoteView {
  path: String,
  title: String,
  markdown_raw: String,
  rendered_html: String,
  properties: Vec<PropertyValue>,
  outgoing_links: Vec<LinkRef>,
  backlinks: Vec<LinkRef>
}
```

## Service Methods

### Vault

- `vault_open(path: String) -> Result<VaultStats>`
- `vault_stats() -> Result<VaultStats>`

### Notes

- `note_get(path: String) -> Result<NoteView>`
- `note_render(path: String) -> Result<String>`
- `note_create(path: String, content: String, frontmatter: Option<String>) -> Result<()>`
- `note_update(path: String, content: String) -> Result<()>`
- `note_rename(from: String, to: String) -> Result<()>`
- `note_delete(path: String, soft: bool) -> Result<()>`

### Links

- `links_outgoing(path: String) -> Result<Vec<LinkRef>>`
- `links_backlinks(path: String) -> Result<Vec<LinkRef>>`
- `links_resolve(raw_target: String, source_path: Option<String>) -> Result<LinkResolution>`

### Properties

- `properties_get(path: String) -> Result<Vec<PropertyValue>>`
- `properties_set(path: String, key: String, value: PropertyInput) -> Result<()>`

### Bases

- `bases_list() -> Result<Vec<BaseRef>>`
- `bases_view(path_or_id: String, view_name: String, page: u32, page_size: u32) -> Result<BaseTablePage>`
- `bases_validate(path_or_id: String) -> Result<Vec<BaseDiagnostic>>`

### Search

- `search_query(query: String, limit: u32, offset: u32) -> Result<SearchPage>`

### Index and Watch

- `index_rebuild(mode: String) -> Result<IndexRunSummary>`
- `index_reconcile() -> Result<IndexRunSummary>`
- `watch_start() -> Result<()>`
- `watch_stop() -> Result<()>`
- `health_snapshot() -> Result<HealthSnapshot>`

## Versioning

- DTOs are versioned by additive schema changes only within major line.
- Breaking DTO changes require new major version and bridge compatibility note.
- CLI JSON and Swift bridge must consume identical DTO versions.
