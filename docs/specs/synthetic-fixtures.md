# Synthetic Fixture Runbook

## Generator

Use deterministic profile-driven generation:

```bash
./scripts/fixtures.sh --profile all --seed 42 --output vault/generated
```

Supported profiles: `all`, `1k`, `5k`, `10k`, `25k`.

Custom note count:

```bash
./scripts/fixtures.sh --notes 2000 --seed 99 --output vault/generated
```

## Safety and Realism Validation

```bash
./scripts/validate_fixtures.sh vault/generated
```

Validator enforces:

- no `*hub*` files
- required base files (`contacts`, `companies`, `projects`, `meetings`)
- markdown task presence
- tag presence
- body + frontmatter wikilinks
- unresolved-link ratio bounds
- no personal path leakage markers

## Data Model Traits

- realistic note families: projects, contacts, companies, meetings, dailies
- mixed body/frontmatter wikilinks
- unresolved links with deterministic ratio
- `.base` views for core entities
