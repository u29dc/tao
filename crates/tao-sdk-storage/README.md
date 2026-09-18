## Crate

`tao-sdk-storage` owns the current SQLite index format and typed persistence APIs.

## Format and publication

The canonical/extraction schema in `schema/current.sql` and derived schema in
`schema/search.sql` form one fresh format, identified by `CURRENT_FORMAT_EPOCH`
and a schema checksum. Bootstrap initializes an empty database atomically.
Preflight rejects older, newer, and incompatible populated databases without
changing them. Historical index upgrades are intentionally unsupported: archive
or remove the internal index, then rebuild from the unchanged vault sources.

Canonical document revisions, links and evidence, properties, tasks, Bases,
extracted content, and leased extraction jobs share transaction boundaries.
Generation counters and dirty owner sets track canonical changes and derived
mutations. A complete publication records both generations atomically.

Unified search uses external-content FTS over `search_segments`, with exact
aliases in `search_aliases`. Derived schemas can be recreated from canonical
revisions. Ranked candidate queries accept the search service's deterministic
SQLite scorer so final ranking precedes limits. Parameters are batched, inventory
paging is bounded, and temporary SQLite work uses file storage.

## Validation

`cargo test -p tao-sdk-storage --release` covers fresh-format bootstrap/refusal,
checksums, repositories, cascading foreign keys, transaction rollback, and FTS
result/rank equivalence, update/delete integrity, and storage qualification.
Deep FTS checks on a read-only connection copy a consistent snapshot to memory;
this diagnostic costs memory proportional to database size and never repairs or
writes the inspected database.

Business rules remain in SDK services. Public vault content stays read-only;
these APIs write only internal index and job state.
