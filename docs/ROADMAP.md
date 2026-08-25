# VGI Spark connector — conformance roadmap

This is the tracking document for "how much of the real `~/Development/vgi/test/sql/integration/`
sqllogictest suite can this connector pass, and what's left to get there."

It is a **living document** — when you implement something, flip its checkbox and note the file
under "Verification," then rerun the sweep (`./gradlew :connector:test --tests
"*VgiSqlLogicTestSweepTest"`) to confirm the record counts actually moved. When you find a new gap
(from a failing sweep record, a user report, or reading more of the suite), add it here before
fixing it, in the right tier.

## How to use this document

- **Status column**: ✅ Done · 🟡 In progress · ⬜ Not started · 🚫 Won't implement (with reason)
- Every ⬜/🟡 item names the exact `.test` files it would unlock, so progress is falsifiable — not
  "aggregates are done," but "these 11 specific files now pass, rerun the sweep to confirm."
- **Tier** is priority, driven by (files unlocked) × (portable-once-built) ÷ (effort). Work top to
  bottom within a tier; tiers themselves aren't strict — a smaller item you're mid-context-on beats
  context-switching to a nominally-higher one.
- This document tracks **connector features**, not test-suite mechanics. A file blocked purely by
  our sweep harness not setting up a dedicated fixture worker (see "Test-infrastructure gaps" below)
  isn't a connector gap — fix the harness, not the roadmap, for those.

## The corpus, at a glance

327 files total. As of the last full survey (2026-08-25):

| Bucket | Files (~) | Meaning |
|---|---:|---|
| Blocked by the Spark table-function-calling ceiling | ~140 | `FROM catalog.func(...)`/`func((SELECT ...))` — Spark SQL has no `TABLE(...)`-argument call syntax for catalog functions. Not fixable without new Spark SQL grammar. See "Won't implement" below. |
| Blocked by DuckDB-only SQL/introspection (`DESCRIBE`, `duckdb_*()`, `vgi_*()` diagnostics, `CREATE SECRET`, `COPY`, `PRAGMA`, `QUALIFY`, macros, ...) | ~90 | No Spark equivalent exists or would ever exist for these specific constructs, independent of any connector feature. |
| Blocked by needing dedicated test infrastructure (special fixture workers, Docker, network, HTTP-only harness) | ~35 | Not a connector gap — see "Test-infrastructure gaps." Some overlap with the above two buckets. |
| **Real, addressable connector feature gaps** | **~55-65** | Tracked below, this is where roadmap work pays off. |
| Already portable and passing | ~7 files fully, ~400 records across ~191 eligible files | Current baseline — rerun the sweep for the live number. |

The sweep tool (`VgiSqlLogicTestSweepTest`) is the source of truth for **current pass counts** — this
document is the source of truth for **what to build next and why**. Don't hand-update pass counts
here beyond rough estimates; rerun the sweep instead.

---

## Tier 1 — highest payoff, no new test infrastructure needed

### 1. Multi-branch tables (`catalog_table_scan_branches_get`)
**Status:** ✅ Done (function branches only — catalog-table, format, and
`branch_filter`-bearing branches still refused, see below)

The single biggest concentrated win, and the *only* way to get real declarative multi-split test
coverage (see item 2's note) — `splits/multi_branch.test` reads a plain declarative table backed by
a real split-capable branch, no table-function-call syntax needed at all.

**What it is:** VGI's successor to the legacy single-scan-function table binding — a table can be
backed by 2+ "branches" (each its own function call, catalog-table reference, or format
descriptor), unioned by the client. `TableScanFunctionGetResponse`/`catalog_table_scan_function_get`
is what we call today; `catalog_table_scan_branches_get` returns a `ScanBranchesResult{branches,
required_extensions}` instead. vgi-java's worker-side `ScanBranch` record (see
`~/vgi-java/vgi/src/main/java/farm/query/vgi/catalog/ScanBranch.java`) already models function
branches, catalog-table branches, and format branches (P4 from the original protocol design study —
already shipped on the wire, per `~/Development/vgi-spark.md`).

**What's needed:**
- `VgiCatalog.loadTable`: try `catalog_table_scan_branches_get` first, fall back to the legacy
  single-function path on `MethodNotImplemented` (matches the documented client contract: "the C++
  extension caches a per-attach capability and falls back... only when the worker raises
  method-not-implemented").
- `VgiTable`/`VgiScan`: represent N branches, each independently bound/planned/scanned; `Batch
  .planInputPartitions()` becomes the concatenation of every branch's partitions (each
  `VgiInputPartition` needs to carry which branch's `bindCall`/`bindOpaqueData` it belongs to).
  `VgiPartitionReaderFactory` needs the per-partition branch context to init the right scan.
- Function branches and catalog-table branches port directly (per the design study). Format
  branches (`format: "parquet"|"csv"|...`) are a second, separable piece of work — start with
  function + catalog-table branches only; refuse (fail closed, name the branch) on an unsupported
  format branch rather than silently dropping it.
- `branch_filter` (a predicate scoping which rows a branch contributes) needs translating into the
  existing `VgiFilterTranslator` machinery or applying as a post-union `WHERE`.

**What shipped:** `VgiCatalog.resolveBranches` tries `catalog_table_scan_branches_get` first,
catching `MethodNotImplementedError` to fall back to the legacy single-function path (unmodified,
zero regression risk — confirmed by the full existing test suite staying green). Decoding uses a
new `ScanBranchesDecoder` (a first port of this wire shape for any JVM client — neither vgi-java's
own client package nor vgi-trino had one), built field-for-field from the worker-side
`ScanBranchesResultSerializer`'s own encode logic. `VgiTable.branches: List<VgiScanBranch>` replaced
the single `scanFunctionName`/`scanFunctionArguments` fields (a one-element list for the legacy
path); `VgiScan.planInputPartitions()` binds+plans each branch independently and concatenates their
partitions — required **no change at all** to `VgiInputPartition`/`VgiPartitionReaderFactory`/
`VgiPartitionReader`, since each partition already self-describes its own `bindCall`/
`bindOpaqueData`. Catalog-table branches, format branches, and any branch declaring a non-empty
`branch_filter` are refused with a clear, named error rather than silently mis-scanned.

**Unlocks — verified:**
- `splits/multi_branch.test` — **the #1 target, now a passing curated conformance test**
  (`VgiSqlLogicTestConformanceTest.multiBranchSplitMatchesTheRealTestFile`), matching the real
  file's exact expected sums (count=50, sum=625, distinct=30, max=29) including cross-branch filter
  pushdown (`WHERE n >= 20` / `WHERE n < 20`) and a real 6-way split on the split-capable arm mixed
  with the plain arm's sentinel path.
- General sweep: +17 passing records after this change (405 → 422), from `catalog/multi_branch_*`
  files gaining partial credit.

**Still refused (tracked, not silently dropped):**
- Catalog-table branches, format branches — `catalog/multi_branch_format.test`,
  `multi_branch_heterogeneous.test`, etc. See item 11 below.
- Any `branch_filter`-bearing branch — `catalog/multi_branch_filtered_numbers`-backed queries. See
  item 11's own note.
- NOT unlocked regardless of this item: `multi_branch_join_optimizer.test`, `multi_branch_lateral.test`
  (DuckDB C++ optimizer-internal regressions, no Catalyst analogue — see "Won't implement"),
  `multi_branch_iceberg.test` (also needs `VGI_TEST_ICEBERG`), `multi_branch_writes_refused.test`
  (needs write support too).

**Verification:** `connector/src/main/java/farm/query/vgispark/branch/` (`ScanBranchesDecoder`,
`VgiScanBranch`), `VgiCatalog.resolveBranches`, `VgiScan.planBranchPartitions`. Tests:
`VgiSqlLogicTestConformanceTest.multiBranchSplitMatchesTheRealTestFile` (curated, real file) +
`VgiSqlLogicTestSweepTest` (general sweep, rerun for current numbers).

---

### 2. Catalog aggregate functions (`AggregateFunction`)
**Status:** ⬜ Not started

**What it is:** Spark's `org.apache.spark.sql.connector.catalog.functions.AggregateFunction<S
extends Serializable, R>` — `newAggregationState()`, `update(S, InternalRow)`, `merge(S, S)`,
`produceResult(S)`. VGI's own aggregate protocol (`AggregateBindRequest/Response`,
`AggregateUpdateRequest`, `AggregateCombineRequest`, `AggregateFinalizeRequest`,
`AggregateDestructorRequest` — all already in `farm.query.vgi.protocol`) is **group-batched**: one
RPC call updates many groups' state at once via a `group_ids_batch`, matching DuckDB's vectorized
hash-aggregate model. Spark's SPI is the opposite granularity — `update()` is called once per row
within one group's local Catalyst-managed state.

**Design (avoids "one RPC per row," which would be unusable):** don't call the worker from
`update()`/`merge()` at all — buffer raw input rows into `S` (a growing local list/Arrow-batch
buffer; `merge()` is then just list concatenation, safe because we haven't asked the worker to
compute anything yet). Only at `produceResult(S)` — called once per group — do the real RPC calls:
`aggregate_bind` (cacheable across groups within one query, same one-time-bind pattern as
`VgiUnboundScalarFunction`), `aggregate_update` with the whole buffered batch, `aggregate_finalize`
for this one group, `aggregate_destructor` to release it. Cost is one worker round-trip **per
group**, not per row — reasonable for typical `GROUP BY` cardinalities.

**Scope for v1** (mirror the scalar-function v1 scoping): positional, non-const, non-vararg,
non-any-typed args with concrete scalar types; single concrete return type. Explicitly deferred:
vararg aggregates, `any`-typed/dynamic return, windowed-frame callbacks
(`aggregate_window_init/_window/_destructor` — a materially different RPC surface, needs Spark's
`PartitionEvaluator`/window-function SPI, separate follow-up item), the "streaming-partitioned"
protocol (no Catalyst analogue found — likely **Won't implement**, see below), struct-typed
returns/args (same bridge gap as scalar), `ConstParam` folding.

**Unlocks (verify via sweep):**
- `aggregate/basic.test`, `grouped.test`, `high_cardinality_1k.test`, `high_cardinality_10k.test`,
  `large_ungrouped.test`, `parallel.test`, `listagg.test` — plain aggregate, v1 shape. **~7 files.**
- `aggregate/varargs.test`, `any_type.test`, `const_param.test` — need the specific extra
  capabilities named above; do these as fast-follows once the base wiring is proven. **~3 files.**
- NOT unlocked: `window.test`, `window_dynamic.test`, `streaming.test` (need the harder RPC
  surfaces above), `dynamic.test`/`function_registration_dynamic.test` (need
  `VGI_WORKER_SUPPORTS_DYNAMIC_CODE`), `nest_tensor.test` (struct return), `function_registration*.test`/
  `same_name_schemas.test` (assert via `duckdb_functions()` regardless).

**Verification:** rerun the sweep, check `aggregate/*.test` in the report.

---

### 3. Struct-subfield filter pushdown
**Status:** ✅ Done

**What it is:** `VgiFilterTranslator.collectColumns` already resolves a `NamedReference` like `s.a`
to the dotted path `"s.a"` — the piece that's missing is building `FilterPredicate.StructField`
(already exists in `farm.query.vgi.client.FilterPredicate`, unused today) instead of failing to
find a projected column named literally `"s.a"`.

**What shipped:** `VgiFilterTranslator.translate` splits a predicate's dotted column path on `.`,
resolves the TOP-level segment as the `ProjectedColumn` (a subfield has no projected index of its
own — the struct itself is what's projected), and walks the Arrow struct's `children` (by name, not
assumed position) via a new `resolveStructPath` to find each segment's child index/type, wrapping
the leaf comparison in one `FilterPredicate.structField(childIndex, childName, ...)` per path segment
(confirmed working 3-deep, `wrapper.mid.leaf`). `required_filters`' "covered columns" check needed no
change — it already stores the full dotted path, so a required group naming `s.a` is satisfied
directly by a translated `s.a` predicate.

**Two real bugs found and fixed along the way** (both pre-existing, exposed by a query shape nothing
had exercised before — multiple filters on subfields of the *same* struct column):
1. Multiple top-level Spark predicates rooted at the same column (e.g. `bbox.xmin >= 0 AND bbox.ymin
   > 0 AND ...`, 4 distinct subfields of one `bbox` struct) were each sent as their own top-level
   wire filter node, all repeating the same `column_index` — a shape the reference Python worker's
   struct-filter evaluator had never been exercised with and crashed on (`IndexError` in
   `batch.column(self.column_index)`). Fixed by grouping translated predicates by top-level column
   and combining each group into one `FilterPredicate.And(...)` before encoding — mirroring how
   DuckDB's own `TableFilterSet` holds at most one (possibly AND-combined) filter tree per column; a
   strictly better encoding regardless (smaller wire payload), not merely a worker-side workaround.
2. **The real bug, worth remembering for any future pushdown work**: `VgiScanBuilder` used to
   translate filters (baking in `column_index`, relative to the *final projected column list*)
   inside `pushPredicates()`, but Spark does **not** guarantee `pruneColumns()` runs before
   `pushPredicates()` — confirmed empirically: for `SELECT count(*) FROM t WHERE bbox.xmin >= 0`,
   Spark pushes filters while the projection is still unrestricted, then narrows it to just `bbox`
   afterward. The baked-in `column_index` went stale once the projection narrowed, silently pointing
   past the end of the worker's actual (narrower) output batch — the worker's IPC stream read then
   failed outright. Fixed by deferring translation to `build()` (a preliminary translation still runs
   in `pushPredicates()` for `pushedPredicates()`'s informational/EXPLAIN-only contract), which
   executes only after every pushdown callback has completed, so the projection used to encode is
   always the one actually sent.

**Unlocks — verified:**
- `table/required_filters_struct.test` — passing curated conformance test
  (`VgiSqlLogicTestConformanceTest.requiredFiltersStructMatchesTheRealTestFile`, 8 executed / 2
  skipped: both `s.a`/`s.b` and the 3-deep `wrapper.mid.leaf` path).
- `table/required_filters_rowid.test` — passing curated conformance test
  (`requiredFiltersRowidMatchesTheRealTestFile`, 4 executed / 2 skipped) — the file that exposed both
  bugs above (4 subfields of `bbox`, filtered without appearing in the output projection).
- `table/required_filters_complex.test` — partial credit: its struct-subfield records now pass; the
  remaining failures are `CREATE VIEW`/`DROP VIEW` (tier 2 item 5, VGI views — unrelated to this item).
- New live regression tests in `VgiCatalogQueryTest` pin both bugs directly (not just via the sweep):
  `filtersOnAStructSubfield`, `filtersOnA3DeepNestedStructPath`,
  `filtersOnFourSubfieldsOfOneStructColumnNotInTheOutputProjection` (reproduces the push-before-prune
  crash shape exactly).

**NOT unlocked** (confirmed by actually running them, not assumed):
- `required_filters_prefix.test` — every record uses DuckDB's `{a: 1, b: 10}::STRUCT(...)` bracket
  struct-literal syntax, which Spark's parser rejects outright — no struct pushdown work changes
  that. Moved to **Won't implement** below (a Spark SQL syntax ceiling, not a connector gap).
- `required_filters_disjunction_null.test` — needs cross-column `OR` and `IN` pushdown (separate,
  already-tracked `VgiFilterTranslator` gaps, not struct-specific).
- `required_filters_above_get.test` — as predicted, a DuckDB planner-internal invariant with wrapped
  expressions (`COALESCE(s.a, 0) > 0`), no Catalyst analogue.
- `required_filters_native.test` — needs `VGI_TEST_BRANCH_DIR` too.

**Verification:** `VgiFilterTranslator.resolveStructPath`/`translate` (grouped encoding),
`VgiScanBuilder` (deferred translation). Tests: `VgiSqlLogicTestConformanceTest
.requiredFiltersStructMatchesTheRealTestFile` / `.requiredFiltersRowidMatchesTheRealTestFile`
(curated, real files), 3 new `VgiCatalogQueryTest` live regression tests, `VgiSqlLogicTestSweepTest`
(general sweep — note this test's own total pass count has run-to-run noise from unrelated Spark
internal-error flakiness under sustained load, ~10-15 records across totally unconnected queries
including pure local `range(...)` ones with no VGI involvement at all — a pre-existing
sweep-harness characteristic, not something this item introduced; the per-file struct-subfield
results above were confirmed stable across repeated runs).

---

### 4. Time travel (`AS OF` / `VERSION AS OF` / `TIMESTAMP AS OF`)
**Status:** ⬜ Not started — **but likely a quick win**, worth doing early

**What it is:** `VgiTable` already carries `atUnit`/`atValue` fields (currently always `null` —
plumbed through from day one but never populated). VGI's wire protocol already threads `at_unit`/
`at_value` through `BindRequest`/`TableFunctionPlanRequest`. Spark's `TableCatalog` interface
already has the exact right hook: `loadTable(Identifier, String version)` and
`loadTable(Identifier, long timestamp)` overloads (both default methods today, presumably throwing
`UnsupportedOperationException` — need to confirm and override). Spark SQL itself has native
`VERSION AS OF`/`TIMESTAMP AS OF` syntax for exactly this.

**What's needed:** override `VgiCatalog.loadTable(Identifier, String version)` (maps to
`at_unit="version"`) and `loadTable(Identifier, long timestamp)` (maps to `at_unit="timestamp"`),
threading the value into `TableInfo`/`catalog_table_scan_function_get`'s existing `at_unit`/
`at_value` parameters (currently hardcoded `null, null` in `VgiCatalog.loadTable`) and into the
`VgiTable` it builds.

**Unlocks (verify via sweep):**
- `table/time_travel.test`, `time_travel_pushdown.test` — **2 files**, need `VGI_VERSIONED_WORKER`
  or similar per the survey — confirm exact fixture requirement before counting on these; if they
  need a dedicated worker, they move to "real feature, but needs test-infra work too."
- `table/constraints_time_travel.test` — same caveat.

**Verification:** rerun the sweep; also worth a hand-written live test against whichever fixture
worker actually supports `at_unit`/`at_value` for a declarative table (check `versioned_data` on
the standard `example` fixture first — it's already used in `VgiCatalogQueryTest` without an AT
clause).

---

## Tier 2 — real payoff, moderate new work

### 5. VGI views
**Status:** ⬜ Not started

**What it is:** `view/views.test`'s actual read queries (`SELECT COUNT(*) FROM example.first_ten`,
`SELECT n FROM example.even_numbers`) are plain declarative SQL — a VGI-declared view just needs to
show up as a readable `Table` in `VgiCatalog.loadTable`/`listTables`, backed by
`catalog_schema_contents_views`/`ViewInfo` (already in `farm.query.vgi.protocol`, unused today).

**What's needed:** `VgiCatalog.listTables` should also enumerate views (or a separate mechanism);
`loadTable` needs to resolve a view name to its backing query/definition and expose it as a
readable `VgiTable`-like `Table` — the simplest approach is likely "resolve the view's own
underlying scan the same way a table resolves," if VGI views are themselves backed by a scan
function per `ViewInfo`.

**Unlocks:** `view/views.test` (its declarative portion) — **1 file**. Small file count, but a
clean, self-contained catalog feature.

---

### 6. Settings passthrough (`BindRequest.settings`)
**Status:** ⬜ Not started

**What it is:** Spark session config (`SET`/`spark.conf.set(...)`) → `BindRequest.settings` (an
Arrow-encoded batch, already a wire field `VgiUnboundScalarFunction`/`VgiScalarFunction` currently
always pass `null` for).

**What's needed:** decide the Spark-side settings mechanism (candidates: `SQLConf`/session
properties read by name at `bind()` time, or catalog-config-level static settings) and thread
through `SettingsEncoder` (already in `farm.query.vgi.client`, unused today) into
`VgiUnboundScalarFunction.bind`'s `BindRequest`.

**Unlocks:** `settings/multiply_by_setting.test`, `settings_types.test` — **2 files** directly (both
call scalar functions, no table-function-call ceiling). `settings/filter_by_setting.test`,
`settings.test`, `struct_settings.test` are still blocked by the table-function-calling ceiling
regardless — settings passthrough alone doesn't unlock them.

---

### 7. Scalar function scope expansion
**Status:** ⬜ Not started (several independent sub-items, can be done incrementally)

Each of these lifts one specific restriction from `VgiUnboundScalarFunction`'s v1 validation. Pick
off independently as needed — don't do all at once.

- **7a. Struct/list scalar args and returns.** Extends `VgiScalarValueBridge` (currently only
  primitive scalars) to read/write nested Arrow struct/list vectors — real new value-bridging code,
  the same shape of work `VgiTypeMapping`'s table-read path never needed because `ArrowColumnVector`
  handles nesting generically; scalar functions have no such generic wrapper since each argument/
  return is one cell, not a batch. Unlocks `scalar/geo_centroid.test`, `geo_distance.test`,
  `binary_packet.test` (also const, see 7c) — **~3 files** (2 also need varargs, 7d).
- **7b. Decimal / unsigned-int scalar types.** Extends `VgiTypeMapping`/`VgiScalarValueBridge` to
  cover `DecimalType` and unsigned Arrow ints (currently excluded from both the table-read
  `VgiTypeMapping.toSparkType` unsigned-64 caveat and the scalar bridge entirely for decimal).
  Unlocks `scalar/numeric_promotion.test` — **1 file**.
- **7c. Const (bind-time-constant) scalar arguments.** A `vgi_const` argument's *value* needs to be
  read from the call site's literal at `bind()` time (Spark's `UnboundFunction.bind(StructType)`
  only sees types, not values — same limitation `vgi-trino`'s `BindCache` worked around, see its
  own javadoc) — likely needs Spark's magic-method/codegen path or falling back to re-binding
  inside `produceResult` the first time a const value is observed (accepting "no caching benefit if
  it varies," same honest tradeoff vgi-trino documents). Unlocks `scalar/conditional_message.test`,
  contributes to `binary_packet.test`, `overload/scalar_overload.test` (const-count/const-type
  dispatch, tier 2 item 9).
- **7d. Vararg scalar functions.** `UnboundFunction.bind(StructType)` receives the CALL SITE's
  actual arity — nothing stops recognizing "this is more args than the declared fixed signature,
  treat the trailing ones as a repeating vararg group" the way `vgi-trino`'s `effectiveArgs`
  expansion does. Unlocks `scalar/sum_values.test` directly; contributes to `geo_centroid`/
  `geo_distance` (7a) and `overload/scalar_varargs_overload.test` (tier 2 item 9).
- **7e. Nested/complex return types** (e.g. `list<struct<...>>`). Same value-bridging work as 7a,
  return-side. Unlocks `scalar/unnest_tensor.test` — **1 file**, likely low priority (niche shape).

---

### 8. Column-statistics-driven scan pruning
**Status:** ⬜ Not started

**What it is:** VGI's `catalog_table_column_statistics_get` RPC (used by vgi-trino's
`getTableStatistics`, unused by vgi-spark today) surfaces per-column min/max/null-count/distinct-
count. Spark's `SupportsReportStatistics`/`Statistics` SPI on `Scan` lets a connector report
row-count and size estimates the optimizer can use for join reordering, broadcast thresholds, and
(via `SupportsPushDownFilters` + the optimizer's own constant-folding) plan-time pruning when a
predicate provably can't match any row.

**What's needed:** thread `TableInfo.cardinality_estimate` and per-column stats into a
`Statistics` implementation on `VgiScan`. Full "prune the scan away entirely at plan time" parity
with what `table/column_statistics.test`'s `EXPLAIN` assertions check isn't achievable (Spark's own
constant-folding/pruning behavior differs from DuckDB's), but real cardinality reporting is
independently valuable (join planning) and directly addressable.

**Unlocks:** the correctness-bearing (non-`EXPLAIN`) portion of `table/column_statistics.test` —
partial credit only, since several of its records assert DuckDB `EXPLAIN` plan text specifically.

---

### 9. Scalar function overload resolution (ConstParam / any-typed / vararg dispatch)
**Status:** ⬜ Not started — depends on 7c/7d above

**What it is:** today `VgiScalarFunctions.loadFunction` resolves exactly one `FunctionInfo` per
name. VGI allows multiple overloads of the same name distinguished by const-argument count/type,
column-argument type, or vararg element type. Spark's `UnboundFunction.bind(StructType)` already
receives the call site's concrete argument types (and, per 7c, could receive const values too) —
architecturally this is plausible (not a Spark-SPI ceiling, confirmed by the survey), it just needs
`loadFunction`/`bind` to consider every same-named `FunctionInfo` and pick the best match instead of
assuming one.

**Unlocks:** `overload/scalar_overload.test`, `scalar_varargs_overload.test` — **2 files**.

---

### 10. Catalog-DDL mutation refusal path
**Status:** ⬜ Not started — cheap, no new protocol work

**What it is:** `attach/ddl_wire_contract.test` expects `CREATE SCHEMA`/`ALTER TABLE ... ADD/DROP
COLUMN` against a read-only VGI catalog to fail with a specific `catalog is read-only`-shaped error
— not "succeed," just "fail the *right* way instead of whatever Spark's default does today."
`VgiCatalog.createNamespace`/`alterTable` already throw `UnsupportedOperationException` — the gap
is purely whether the message/error shape matches what this file expects, and whether Spark's own
analyzer even reaches our code for these statements or fails earlier with a generic error.

**What's needed:** read the file, run the two statements today, see what actually happens, adjust
error wording/shape to match if it's a simple mismatch. Likely a very small, self-contained fix —
good candidate to just do rather than research further.

**Unlocks:** `attach/ddl_wire_contract.test` — **1 file**.

---

### 11. Multi-branch: format branches
**Status:** ⬜ Not started (split out from item 1 — do function + catalog-table branches first)

Once item 1's function/catalog-table branch plumbing exists, format branches
(`ScanBranch.format`/`locations`/`format_options` — parquet/csv/delta/iceberg descriptors, P4 from
the original protocol design study) are the natural extension: map `format="parquet"` to Spark's
own `spark.read.format("parquet")`/`DataSourceV2` file-source machinery per-branch, refuse
(name the branch, don't silently drop rows) for any format we don't recognize.

**Unlocks:** contributes to `catalog/multi_branch_format.test`, `multi_branch_heterogeneous.test`
(both also need `VGI_TEST_BRANCH_DIR`), `table/required_filters_native.test` (also needs it, plus
struct-subfield pushdown, item 3).

---

## Tier 3 — real gaps, lower payoff or higher uncertainty

- **rowid hidden from `SELECT *`** — already attempted once (`SupportsMetadataColumns`), reverted
  after a physical-plan crash (`INTERNAL_ERROR_ATTRIBUTE_NOT_FOUND`, see `VgiTable.columns()`'s own
  note). Worth a focused retry — diagnose exactly which query shape broke pruning before
  re-attempting, rather than re-trying the same approach blind. Low file-count payoff on its own
  (contributes partial credit to a few `table/rowid*`/`required_filters_rowid.test` files already
  counted elsewhere) but is a real, user-visible correctness/ergonomics gap independent of the
  sqllogictest suite.
- **Generated columns** — currently fails with a confusing generic `VGI stream read failed`.
  Two possible directions, worth investigating before committing to one: (a) detect generated
  columns at discovery time (a `generated_columns` field likely lives in Arrow field metadata on
  `TableInfo.columns`, the same pattern `is_row_id` uses — confirm the wire shape) and simply
  **exclude them from the projected read** while still declaring them in the table's schema via
  Spark's `Column.generationExpression()` (confirmed to exist on Spark's `Column` interface,
  `farm.query.vgispark.VgiTable.columns()` doesn't set it today) — if Spark's own engine evaluates
  `generationExpression()` server-side for a read-only V2 source (**unconfirmed, verify first**),
  this could be near-free; if not, (b) fall back to at least turning the crash into a clear,
  named error ("column X is DuckDB-generated, not readable by this connector") rather than
  attempting and failing confusingly. Do (b) regardless of whether (a) pans out — a clear error is
  a strict improvement even as a stepping stone.
- **`github://` / `oci://` / `docker://` worker sourcing** — real, named gap from the original plan.
  Only the pure ATTACH-string-parsing-validation portions (`github/errors.test`,
  `container/errors.test`) are testable without live network/Docker; full support needs both the
  scheme implementation and (separately) live infrastructure to verify against. Low priority unless
  a real user needs one of these sourcing modes.
- **`launch:` scheme** (pooled shared worker) — real, named gap; both test files need dedicated
  env/transport-gating regardless, so implementing this doesn't move the sweep numbers much on its
  own, but it's a real production feature (perf: "30s vs many minutes" per vgi-java's own docs) —
  worth doing for production readiness independent of test-count payoff.
- **Custom ATTACH options** — 2 files, both need dedicated fixture workers regardless of whether
  the feature exists, so near-zero sweep payoff; real design work needed (Spark catalog config is a
  static property bag, not DuckDB's per-statement typed-option grammar) if ever prioritized for
  production use rather than test-passing.
- **ATTACH-time `data_version_spec`/`implementation_version` negotiation** — new gap found by the
  survey, not in the original plan. All 10 related files need a dedicated versioned-fixture worker
  AND assert via `duckdb_databases()`, so implementing this wouldn't move the sweep needle at all —
  purely a production-readiness item if ever prioritized.
- **Companion catalogs (DuckLake federation)** — new gap found by the survey. All 3 related files
  need `VGI_TEST_COMPANION_TARGET` infrastructure — zero sweep payoff. Note for awareness, not
  action, unless a real user needs DuckLake federation through Spark.
- **Secrets resolution (`BindRequest.secrets`)** — blocks all of `secret/` (10 files), but even with
  full protocol support, DuckDB's `CREATE SECRET` SQL has no Spark equivalent, so the *test files as
  written* wouldn't port regardless — would need a parallel Spark-side secret-provisioning mechanism
  (e.g. a catalog config option, or Spark's own credential-passthrough conventions) invented from
  scratch, then new Spark-native tests, not a port of these files. `secret/secret_function_backed_table.test`
  is the one file that's shape-compatible if such a mechanism existed. Real production feature, but
  disconnect from "makes these tests pass" — treat as its own workstream if prioritized.
- **HTTP cookie-jar session stickiness** — found by the survey (`attach/versioning_http.test`);
  unconfirmed whether vgi-spark's HTTP transport does this today. Worth a quick check (does
  `HttpRpcConnection` handle `Set-Cookie`/`Cookie` round-trips?) independent of whether the
  version-negotiation feature above is ever built, since it's a transport-correctness question, not
  a version-negotiation one.

---

## Won't implement (with reasons — revisit only if the reasoning changes)

- **VGI table-function / table-in-out-function calling via SQL** (`FROM catalog.func(...)`,
  `func((SELECT ...))`, `LATERAL func(...)`). Blocks ~140 files across `accumulate/`,
  `filter_pushdown/` (all 13), most of `table/` (~33), all of `table_in_out/` (43), most of
  `splits/` (20 of 21), most of `secret/`/`settings/`, `macro/`'s table-macro portion, `overload/`'s
  table files, `global_functions/`'s table/aggregate/table_buffering portions. This is a Spark
  SQL-*language* ceiling — Spark has no `TABLE(...)`-with-arguments call syntax reachable from a
  catalog-provided function (its newer Python-UDTF `TABLE` argument syntax is Spark-registered-
  function-only, not extensible to arbitrary DataSource-catalog functions). Revisit only if Spark
  itself ever adds such syntax. A DataFrame-level programmatic call API (`VgiTableFunctions.call(spark,
  "catalog.schema.fn", args...)`) remains a possible **separate**, non-SQL escape hatch — deferred
  per the original plan's decision, not reconsidered here.
- **`COPY ... FROM/TO (FORMAT '...')`** (11 files: `copy_from/` all 3, `copy_to/` all 8). DuckDB's
  COPY statement grammar has no Spark equivalent, and unlike other gaps there's no portable
  remainder even in principle — every file's verification reads a local DuckDB table populated by
  the COPY itself, never a VGI catalog object via plain `SELECT`. Some files (`copy_to/parallel.test`,
  `tmp_file.test`) test DuckDB physical-COPY-operator internals with no conceptual Spark analogue at
  all (different writer-parallelism model, no sink-lifecycle concept to even gap-map).
- **VGI's own client-side result cache** (`cache/`, 52 files — the largest single directory in the
  whole suite). Architecturally a DuckDB-C++-extension-internal subsystem (in-memory/disk tiers,
  packed store format, reaper thread) with no wire-protocol/data-plane counterpart — Spark already
  has its own orthogonal caching (`CACHE TABLE`, `persist()`, AQE reuse) at a completely different
  layer. Even a from-scratch Spark-side cache wouldn't make these specific files portable, since
  their pass/fail signal is DuckDB-only introspection (`vgi_result_cache()`,
  `vgi_result_cache_stats()`, `EXPLAIN ANALYZE` hit-rate annotations) with no Spark equivalent to
  assert against. Would need entirely new Spark-native tests, not a port — treat "should vgi-spark
  have its own result cache" as a completely separate product decision, unrelated to sqllogictest
  conformance.
- **DuckDB bracket struct-literal syntax** (`{a: 1, b: 10}::STRUCT(...)`) — `table/
  required_filters_prefix.test`'s entire content (3 records). Confirmed by actually running it:
  Spark's parser rejects `{...}` map/struct-literal syntax outright, independent of any pushdown
  feature (see tier 1 item 3's own "not unlocked" note). Spark's equivalent is `named_struct(...)`/
  `struct(...)`, a different literal syntax entirely — porting the file would mean rewriting its
  queries, not translating a connector gap.
- **DuckDB SQL macros** (`macro/macros.test`). Confirmed worker-provided (not DuckDB-native), but
  the underlying mechanism — text-substitution/inlining at bind time — has no Spark `FunctionCatalog`
  equivalent (Spark functions are typed/bound, never text-expanded) regardless of who authored the
  macro body.
- **Global (unqualified, cross-catalog) function names** (`global_functions/`, 2 files). No Spark
  SPI hook for "this catalog's function is also callable without any catalog prefix" — would need
  inventing a new mechanism from scratch, not wiring up an existing one.
- **DuckDB-internal-only regression tests** — `catalog/window_self_join.test`,
  `catalog/multi_branch_join_optimizer.test`, `catalog/multi_branch_lateral.test`,
  `table/inlined_cardinality.test`, `table/inlined_scan_function.test`,
  `table/late_materialization.test`. These pin specific DuckDB C++ optimizer/plan-serialization
  behaviors (deep-copy callbacks, bind-time RPC-skipping heuristics, TOP_N-to-semi-join rewrites)
  with no Catalyst analogue — there's nothing to even attempt once you understand what they're
  really testing.
- **DuckDB-only introspection as the sole verification mechanism** — recurring across nearly every
  directory (`duckdb_functions()`, `duckdb_tables()`, `duckdb_databases()`, `duckdb_settings()`,
  `duckdb_logs()`/`enable_logging`, `duckdb_constraints()`, `duckdb_columns()`, `duckdb_views()`,
  `duckdb_secret_types()`, `vgi_catalogs()`, `vgi_result_cache*()`, `vgi_table_branches()`,
  `vgi_function_arguments()`, `vgi_worker_pool()`, `vgi_companion_catalogs()`, `vgi_eager_load_threshold`
  MAP-literal `SET`s, `CALL enable_logging`/`truncate_duckdb_logs`). Where the *underlying* feature is
  otherwise addressable (see tiers 1-3 above), the feature is still worth building for its own
  sake — but these specific files won't port even then, since the assertion mechanism itself doesn't
  exist in Spark.

---

## Test-infrastructure gaps (not connector features — separate from the above)

These files are blocked because the sweep/conformance harness only stands up the plain `example`
fixture worker over `VGI_TEST_WORKER`. Building the underlying connector feature doesn't unlock
them without also extending the harness:

| Needs | Files affected | Notes |
|---|---:|---|
| `VGI_SIMPLE_WRITABLE_WORKER` | 5 (`simple_writable/`) | Separate fixture worker; needed regardless once write support (INSERT/UPDATE/DELETE, `SupportsWrite`) exists. `insert_returning.test` and half of `delete.test`/`update.test` are additionally blocked by Spark SQL having no `RETURNING` clause at all — a language ceiling on top. |
| `VGI_ATTACH_OPTIONS_WORKER`, `VGI_ATTACH_OPTIONS_REQUIRED_WORKER` | 2 (`attach/`) | Custom ATTACH options feature (tier 3) |
| `VGI_VERSIONED_WORKER`, `VGI_VERSIONED_TABLES_WORKER`, `*_HTTP_WORKER` variants | 10 (`attach/`) | Also all assert via `duckdb_databases()` — zero payoff even with the worker |
| `VGI_TEST_BEARER_TOKEN` + protected worker | 1 (`bearer_auth/`) | Static bearer token itself already works; this is purely the dedicated-worker gap |
| `VGI_HTTP_TRANSPORT`, `VGI_HTTP_DISABLE_ZSTD`, `VGI_HTTP_NO_COMPRESSION` | 7 (5 in `http/`, 2 in `cache/`) | `http/` tests are DuckDB C++ client internals via log introspection regardless — not worth porting even with the harness |
| `VGI_TEST_COMPANION_TARGET` (+ a pre-seeded DuckLake) | 3 (`catalog/`) | Companion-catalog feature (tier 3), zero payoff without a real DuckLake fixture |
| `VGI_TEST_BRANCH_DIR` | 6-7 (`catalog/multi_branch_*`, `table/required_filters_native.test`) | Cheap to add to the harness (writes real Parquet files) — do this alongside multi-branch work (tier 1 item 1) |
| `VGI_TEST_ICEBERG` + `INSTALL iceberg` | 1 (`catalog/multi_branch_iceberg.test`) | |
| `VGI_WORKER_SUPPORTS_DYNAMIC_CODE` | 3 (`aggregate/`) | Dynamic Python/JS worker code — niche |
| `VGI_BAD_PROTOCOL_WORKER` | 1 (`protocol_version/`) | Cheap to add; also needs the protocol-version-mismatch handling itself confirmed/implemented |
| `VGI_REQUIRE_LAUNCHER_TRANSPORT` | 1 (`launcher/`) | Needs the `launch:` scheme built first (tier 3) |
| `VGI_DOCKER_IMAGE`/`VGI_DOCKER_TCP_IMAGE` + Docker daemon | 3 (`container/`) | Needs `oci://`/`docker://` sourcing built first (tier 3) |
| `VGI_GITHUB_NETWORK_TESTS` (network) | 1 (`github/download.test`) | Needs `github://` sourcing built first (tier 3) |
| `VGI_RULES_WORKER` (Rust-SDK fixture) | 1 (`table_in_out/`) | Also blocked by the calling-syntax ceiling regardless |
| `VGI_TEST_DEDICATED_WORKER` | 2 (`table_in_out/table_buffering_*`) | Also blocked by the calling-syntax ceiling regardless |

`VGI_TEST_BRANCH_DIR` is the one cheap, high-value addition — it's just a scratch directory the
standard fixture worker writes real Parquet/Hive-partitioned files into, no separate worker
process. Worth adding to `VgiSqlLogicTestSweepTest`'s eligibility gate alongside tier 1 item 1
(multi-branch tables), since several of that item's files need it.

---

## Change log

- **2026-08-25** — initial version, from a full 327-file survey (3 parallel agents covering
  `accumulate/aggregate/attach/bearer_auth/cache/catalog/container/protocol_version`,
  `copy_from/copy_to/filter_pushdown/github/global_functions/http/launcher/macro/overload`, and
  `scalar/secret/settings/simple_writable/splits/table/table_in_out/view`) against the connector
  state as of commit `2c6ecc8` (end of the original 5-phase plan).
- **2026-08-25** — Tier 1 item 1 (multi-branch tables, function branches only) done. `splits/multi_branch.test`
  now passes as a curated conformance test; general sweep +17 records (405 → 422 passing).
- **2026-08-25** — Tier 1 item 3 (struct-subfield filter pushdown) done. Found and fixed two real bugs
  along the way (same-column-repeated-`column_index` crash; a `pushPredicates()`-vs-`pruneColumns()`
  call-order bug that baked in a stale, since-narrowed `column_index`). `table/required_filters_struct.test`
  and `table/required_filters_rowid.test` now pass as curated conformance tests. `required_filters_prefix.test`
  moved to "Won't implement" (DuckDB bracket struct-literal syntax, confirmed unparseable by Spark).
