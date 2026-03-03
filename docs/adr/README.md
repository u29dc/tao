# ADR Index

This folder stores architecture decision records for `tao`.

## Numbering

- Use zero-padded IDs: `ADR-0001`, `ADR-0002`, ...
- Never reuse an ADR ID.
- Superseding decisions must reference the prior ADR explicitly.

## Required Sections

Each ADR must include:

1. Title
2. Status (`proposed|accepted|deprecated|superseded`)
3. Date (UTC)
4. Context
5. Decision
6. Consequences
7. Alternatives considered
8. References

## File Naming

- `ADR-0001-<short-kebab-title>.md`

## Minimal Template

```md
# ADR-0000: Title

- Status: proposed
- Date: YYYY-MM-DD

## Context

## Decision

## Consequences

## Alternatives Considered

## References
```
