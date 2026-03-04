# Unified Query Language v1

## Shape

`tao query --vault-root <path> --from <scope> [options]`

## Scopes

- `docs`: full-text and indexed path/title/content retrieval
- `graph`: unresolved list by default; path-scoped outgoing graph when `--path` is supplied
- `task`: extracted markdown tasks
- `meta:tags`
- `meta:aliases`
- `meta:properties`
- `base:<id-or-path>` (requires `--view-name`)

## Core Options

- `--query <text>`: free-text filter
- `--path <note-path>`: path-scoped graph source
- `--view-name <name>`: base view selector
- `--limit <n>` and `--offset <n>`: deterministic pagination window

## Output

`query.run` returns row-oriented payload in `value.args` with scope-specific `columns`, `rows`, and `total`.

## Examples

```bash
# docs scope
./target/release/tao --json query --vault-root <vault> --from docs --query project --limit 20 --offset 0

# graph scope (unresolved)
./target/release/tao --json query --vault-root <vault> --from graph --limit 50 --offset 0

# graph scope (path-scoped outgoing)
./target/release/tao --json query --vault-root <vault> --from graph --path notes/a.md --limit 50 --offset 0

# base scope
./target/release/tao --json query --vault-root <vault> --from base:views/projects.base --view-name Projects --limit 50 --offset 0

# meta scope
./target/release/tao --json query --vault-root <vault> --from meta:tags --limit 50 --offset 0

# task scope
./target/release/tao --json query --vault-root <vault> --from task --query follow-up --limit 50 --offset 0
```
