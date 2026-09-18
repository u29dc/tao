## Purpose

`tao-sdk-links` interprets source link occurrences and resolves them against a normalized file inventory. Storage and indexing remain service/storage responsibilities.

## Resolution contract

`parse_link_occurrence` consumes the shared Markdown parser's occurrences without reparsing Markdown. `LinkTarget` retains syntax, rootedness, the decoded path, fragment and parse diagnostics. `LinkResolutionIndex::resolve_link` never reparses a decoded filename as link syntax.

- Markdown destinations resolve exactly relative to the source directory. A leading slash means the vault root. A missing explicit destination remains unresolved; it does not search other directories. `..` cannot escape the vault.
- Percent escapes in Markdown destinations are decoded once, after separating the fragment/query. Encoded literal `#`, `?` and `%` characters remain filename characters. Invalid escapes and invalid UTF-8 are diagnosed without lossy replacement.
- Wikilinks retain deterministic basename/ancestor discovery. Explicit `/`, `./` and `../` paths select exact path semantics. Optional complete alias values are fallback candidates after ordinary file matching; spaces and commas are not split.
- Path comparison uses NFC and the configured case policy. Ambiguous matches preserve all candidates and the matching rule.
- Same-note `[[#Heading]]`, `[[#^block]]` and Markdown `(#Heading)` references retain source-document resolution.
- Fragment validation is independent of document resolution. Missing headings/blocks do not erase a successfully resolved file edge.
- PDF page fragments use physical one-based ordinals: `[source](paper.pdf#page=12)`, `[[paper.pdf#page=12]]`, and `![[paper.pdf#page=12]]`. Page labels, including Roman numerals, are not substituted for physical ordinals. Unknown page counts produce `pending`; zero, invalid or out-of-range pages produce `bad_page`.

`LinkOccurrence` preserves raw source, syntax/kind and physical byte/line span. Repeated identical links remain separate occurrences.

## Compatibility APIs

`parse_wikilink`, `extract_wikilinks`, `extract_markdown_links`, `extract_block_ids`, `resolve_target`, `resolve_heading_target`, `resolve_block_target` and `slugify_heading` remain available. Body-extraction wrappers share the Markdown parser; callers already holding a parsed revision should use its tokens directly.

## Validation

`cargo test -p tao-sdk-links --release` covers exact and wiki resolution, Unicode, ambiguity, source occurrences, same-note fragments, PDF page status and decode-once filename handling.
