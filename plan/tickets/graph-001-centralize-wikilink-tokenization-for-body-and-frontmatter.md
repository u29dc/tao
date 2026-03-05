## GRAPH-001 Centralize wikilink tokenization for body and frontmatter

Status: done

### Objective
Use one parser path for wikilink extraction from markdown body and frontmatter string values.

### Scope
Extractor logic and indexing integration only.

### Concrete Steps
1. Create a shared tokenization module in tao-sdk-links or tao-sdk-service.
2. Route body parsing and frontmatter recursive string scanning through that module.
3. Emit source attribution metadata (body, frontmatter:<keypath>) for each edge.
4. Add deduplication by canonical target and source context.

### Required Files and Locations
- crates/tao-sdk-service/src/indexing.rs
- crates/tao-sdk-links/src/lib.rs
- crates/tao-sdk-service/tests/conformance_harness.rs

### Implementation Notes
Do not rescan file text more than once per note.

### Dependencies
- none

### Acceptance Criteria
- [x] A note with links only in frontmatter produces outgoing edges.
- [x] Body and frontmatter duplicates appear once in canonical edge results.
- [x] Source attribution is visible in internal edge metadata/tests.
