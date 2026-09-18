## Purpose

`tao-sdk-markdown` derives one source-aware structural representation from a captured Markdown revision. It never modifies the source.

`MarkdownParser::parse` uses the shared frontmatter extractor and one `pulldown-cmark` event stream. The result contains exact body bytes, the body offset, metadata status, headings, task-list occurrences, link occurrences and block identifiers. The service layer persists these facts for incremental reuse.

## Supported structure

- ATX and Setext headings use visible inline text. Fenced and indented code never create headings or tasks.
- Task items support unordered, ordered, nested and quoted lists. `[ ]`, `[x]`/`[X]` and `[-]` mean open, done and cancelled. Empty task text is retained.
- Markdown links, reference links, wikilinks and embeds retain every occurrence. Escaped wikilinks, inline/fenced code, HTML comments and Obsidian `%%` comments do not create links.
- Terminal `^block-id` markers are recognized outside code and comments. Block IDs permit ASCII letters, digits, hyphens and underscores.
- Structural spans use original UTF-8 byte offsets and physical one-based line numbers, including frontmatter. CRLF and final-newline differences remain intact in the source body.
- Malformed metadata is reported by the shared frontmatter status. It does not turn a whole vault into a parser error.

`parse_body` offers the same structural pass when a caller already owns the frontmatter/body boundary. Normal ingestion should use the existing parsed result, rather than calling extraction wrappers repeatedly.

## Validation

`cargo test -p tao-sdk-markdown --release` covers structure, exclusion contexts, occurrence provenance, physical line references, frontmatter preservation and the optional render-cache interface.
