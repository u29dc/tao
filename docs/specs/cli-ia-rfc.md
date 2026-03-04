# Tao CLI IA RFC (Phase23)

## Goals

- Expose a compact top-level command surface:
  - `vault`, `doc`, `base`, `graph`, `meta`, `task`, `query`
- Keep legacy groups (`note`, `links`, `properties`, `bases`, `search`) as compatibility aliases.
- Preserve one-envelope JSON contract for all commands with `--json`.

## Top-Level Groups

- `vault`: lifecycle and index control (`open`, `stats`, `preflight`, `reindex`, `reconcile`)
- `doc`: note read/write/list (`read`, `write`, `list`)
- `base`: base list/view/schema (`list`, `view`, `schema`)
- `graph`: graph traversal/diagnostics (`outgoing`, `backlinks`, `walk`, `unresolved`, `deadends`, `orphans`, `components`)
- `meta`: metadata aggregations (`properties`, `tags`, `aliases`, `tasks`)
- `task`: task extraction and mutation (`list`, `set-state`)
- `query`: unified read entrypoint (`--from docs|graph|task|meta:*|base:<id-or-path>`)

## JSON Envelope Contract

All `--json` commands return:

```json
{
  "ok": true,
  "value": {
    "command": "<command.id>",
    "summary": "<human summary>",
    "args": { "...": "payload" }
  },
  "error": null
}
```

Errors return `ok=false`, `value=null`, and a structured `error` object.

## Migration Aliases

- `note.get` -> `doc.read`
- `note.put` -> `doc.write`
- `note.list` -> `doc.list`
- `bases.list` -> `base.list`
- `bases.view` -> `base.view`
- `links.outgoing` -> `graph.outgoing`
- `links.backlinks` -> `graph.backlinks`
- `search.query` -> `query --from docs`

## Write Gate Policy

Mutating operations remain blocked by default and require `--allow-writes`.

- gated: `doc.write`, `task.set-state`, `properties.set`, legacy mutating aliases
- read-only paths are never gated.
