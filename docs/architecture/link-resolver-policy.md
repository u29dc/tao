# Deterministic Link Resolver Policy

## Goal

Provide stable wiki link resolution across repeated runs and vault mutations.

## Input Forms

- `[[note]]`
- `[[folder/note]]`
- `[[note#heading]]`
- `[[note#^block]]`
- `[[note|display]]`

## Normalization

- Normalize path separators to `/`.
- Trim surrounding whitespace in targets.
- Compare case-insensitively for match candidate discovery.
- Preserve original target casing in stored raw link.

## Resolution Algorithm

1. If target contains explicit path segments, attempt exact normalized path match.
2. Otherwise match by basename without extension.
3. If no candidates, mark unresolved.
4. If one candidate, resolve.
5. If multiple candidates, apply tie-breakers in order:
   - same-folder preference (source note directory)
   - shortest relative path distance
   - lexical path order (stable final tie-break)

## Heading and Block Resolution

- After file target resolution, apply heading lookup by normalized slug.
- For block refs, lookup by indexed block id.
- If file resolves but heading/block does not, retain partial resolution with unresolved fragment flag.

## Ambiguity Handling

- Persist all ambiguity candidates for diagnostics.
- Emit `links.resolve.ambiguous_target` with candidate list.
- CLI and UI should offer path-qualified replacement suggestions.

## Determinism Rules

- Resolver output must be invariant for identical index state.
- Candidate ordering must be stable and explicit.
- All tie-break comparisons must avoid non-deterministic map iteration order.
