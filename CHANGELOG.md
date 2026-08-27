# Changelog

All notable changes to this project are documented here. Format loosely
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versioning
follows [Semantic Versioning](https://semver.org/).

## [0.2.0] — 2026-08-27

First tagged release. `farm.query:vgi-spark` had been developed and used
untagged (`version = "0.1.0"`, no git history published) up to this point —
this release is the whole feature set built so far, not a diff against a
prior tag.

### Added

- Spark DataSource V2 catalog connector for VGI workers: catalog/namespace/
  table discovery, real multi-split scan planning mapped onto Spark's
  `InputPartition` model, vectorized reads (`ArrowColumnVector`, no value
  copying).
- Projection, filter, and limit pushdown; `TableInfo.required_filters`
  enforced fail-closed.
- Catalog scalar and aggregate functions (`FunctionCatalog`).
- Table-function `CALL` (Spark's `ProcedureCatalog`, the substitute for
  DuckDB/Trino's `TABLE(...)` syntax, which Spark has no SQL-level
  equivalent for).
- Multi-branch tables: function branches and CSV-format branches.
- The `launch:` shared-warm-worker scheme, wired as the default transport
  for a bare-command `location` (falls back to a plain per-connection
  subprocess spawn on a JDK <22 runtime, where `launch:`'s Foreign Function
  & Memory API dependency isn't available).
- Custom worker-declared ATTACH options (`attach-option.*` config keys).
- ATTACH-time data-version/implementation-version negotiation
  (`data-version-spec`/`implementation-version` config keys).
- All four transports: subprocess, `unix://`, `tcp://`, `http(s)://`.
- A curated sqllogictest conformance suite plus a full-corpus sweep against
  the real upstream `.test` files, run as part of the normal test suite.
- CI (GitHub Actions): the full test suite runs on every push/PR.

### Fixed

Four real, previously-undiscovered VGI wire-protocol bugs, found while
switching the test suite to a stricter reference worker implementation:

- `BindRequest.schema_name` was never sent on scan-function binds — a
  spec-strict worker (VGI protocol 1.1.0+) refuses an unqualified bind
  outright rather than searching every schema by name.
- `FunctionInfo.function_type` comparisons were case-sensitive against a
  field that's actually lowercase on the wire — `CALL` syntax and aggregate
  function discovery never actually found anything as a result.
- A declarative table whose backing scan function uses different raw
  column names than the table itself declares (e.g. a table column named
  `value` backed by a function whose own output column is `n`) failed with
  a worker-side projection error — the wire schema needs the function's own
  bind-resolved names, not the table's declared ones.
- A scalar function called with an all-literal argument list including a
  NULL (e.g. `SELECT some_fn(NULL::DOUBLE)`) threw an unexplained
  `NoSuchElementException` — the connector now implements DuckDB's own
  "STRICT SQL function" short-circuit contract (`FunctionInfo.null_handling`),
  short-circuiting to a NULL result client-side instead of sending a value
  the worker's own wire schema declares non-nullable.

### Known limitations

See `README.md`'s own "Known gaps" section and `CLAUDE.md`'s "Open bugs"
section for the current list (Arrow `Time`-typed columns have no Spark
mapping; aggregate functions don't yet support `any`-typed/`vgi_const`
arguments; a few sqllogictest categories with no Spark SQL equivalent at
all — DuckDB-only introspection, `TABLE(...)` table-function calls).

[0.2.0]: https://github.com/Query-farm/vgi-spark/releases/tag/v0.2.0
