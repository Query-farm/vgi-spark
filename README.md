# vgi-spark

A Spark DataSource V2 catalog connector for [VGI](https://github.com/Query-farm/vgi)
(Vector Gateway Interface) workers — the same out-of-process, Arrow-IPC-based
worker protocol the `vgi` DuckDB extension and
[`vgi-trino`](https://github.com/Query-farm/vgi-trino) attach to. One Spark
catalog maps to one VGI `ATTACH`, the same granularity DuckDB and Trino use.

```scala
spark.conf.set("spark.sql.catalog.vgi_example", "farm.query.vgispark.VgiCatalog")
spark.conf.set("spark.sql.catalog.vgi_example.location",
  "uv run --project ~/Development/vgi-python vgi-fixture-worker")
spark.conf.set("spark.sql.catalog.vgi_example.catalog-name", "example")

spark.sql("SELECT * FROM vgi_example.data.numbers").show()
```

**Conformance roadmap:** [`docs/ROADMAP.md`](docs/ROADMAP.md) tracks, feature by feature, what's
needed to pass more of the real `~/Development/vgi/test/sql/integration/` sqllogictest suite —
what's done, what's next, what's a real gap vs. a Spark SQL-language ceiling we can't cross, and
exactly which `.test` files each item unlocks.

## Status

All five phases of the original plan are implemented and verified against
live workers (Python fixture worker over every transport, plus a
purpose-built in-process split-capable Java worker for real multi-partition
testing):

- Catalog/namespace/table discovery, real multi-split scan planning
  (`table_function_plan` / `ScanSplit` / `InitRequest.split_tokens`) mapped
  onto Spark's `InputPartition` model, vectorized reads end to end (each
  Arrow batch wrapped directly in `ArrowColumnVector`, no value copying).
- Projection, filter (best-effort, non-authoritative — Spark always
  re-verifies), and limit pushdown; `TableInfo.required_filters` enforced
  fail-closed.
- Catalog scalar functions — a scoped v1: positional, non-const, non-vararg,
  non-`any`-typed arguments with a concrete scalar type; a single,
  statically-typed return. See `VgiUnboundScalarFunction`'s own validation
  for exactly what's refused and why.
- All four transports (subprocess, `unix://`, `tcp://`, `http://`) tested.

**Known gaps**, found via a full sweep of the real `.test` sqllogictest suite
(see below), not chased further because each is genuinely new scope beyond
the original plan:
- Filter pushdown doesn't reach into **struct subfields** (`s.a > 5`) —
  `TableInfo.required_filters` naming a nested path (e.g. `bbox.xmin`) can
  never be satisfied, so those tables' scans correctly (if conservatively)
  refuse rather than silently doing a full scan.
- **DuckDB generated columns** (`generated_columns={"doubled": "n * 2"}`) —
  DuckDB evaluates the expression client-side after reading only the base
  columns; this connector has no such expression evaluator, and currently
  fails with an unhelpfully generic `VGI stream read failed` rather than a
  clear error naming the real cause.
- **Catalog aggregate functions** (`vgi_sum`, `vgi_count`, ...) aren't
  implemented at all — only scalar functions (`FunctionCatalog` covers both
  kinds in principle; this connector's v1 only wires up `ScalarFunction`).

**Deliberately not implemented** (non-goals from the start, matching the
plan): multi-branch tables (`catalog_table_scan_branches_get`), time travel,
writes, and VGI table-function calling (Spark has no SQL-level equivalent to
Trino's `TABLE(...)` syntax for this).

### Sqllogictest conformance

Two files (`table/rowid.test`, `catalog/window_self_join.test` — the same
ones `vgi-trino` found portable) run as a curated regression gate with
known skip/pass counts (`VgiSqlLogicTestConformanceTest`).

A separate, uncurated **full-corpus sweep** (`VgiSqlLogicTestSweepTest`)
answers "how many of the ~327 files actually pass" honestly, with no
per-file marker lists — it only excludes files needing infrastructure this
harness doesn't set up (a non-default `ATTACH` alias/catalog/auth, or
`require-env` beyond `VGI_TEST_WORKER`), then attempts every record in what's
left and reports real pass/fail counts plus a full per-file breakdown to
`build/sqllogictest-sweep-report.txt`. Latest run: 191 of 327 files eligible,
~405 of ~2944 attempted records pass. The large majority of the remainder is
expected, not a bug — categorized breakdown:

| Category | Records | Why |
|---|---|---|
| VGI table-function calls | ~180 | No Spark SQL equivalent (non-goal) |
| Scalar/aggregate functions outside v1 scope, or aggregates at all | ~110 | Const/vararg/any-typed args, settings/secrets, or aggregate functions — not yet implemented |
| DuckDB introspection (`duckdb_functions()`, `vgi_function_arguments()`, ...) | ~20 | No Spark equivalent |
| Multi-branch tables | ~12 | Non-goal |
| `EXPLAIN`/`DESCRIBE` output-format differences | several | Different engine, different plan/describe shape — not a correctness question |
| Struct-subfield required-filters / generated columns | ~10 | Known gaps, see above |
| DuckDB dialect-only SQL (`QUALIFY`, labeled `ref<N>` cross-checked queries our minimal parser doesn't model, window-ordering strictness differences) | several | Real dialect differences, not connector bugs |

No mismatch sampled during triage indicated a silent correctness bug in a
supported code path — every non-`EXPLAIN`/`DESCRIBE` mismatch traced to one
of the categories above.

## Configuration

Set per catalog as `spark.sql.catalog.<name>.<key>`:

| Key | Required | Description |
|---|---|---|
| `location` | yes | The worker to attach: a bare shell command (subprocess transport), `unix:///path/to.sock`, `tcp://host:port`, `http(s)://host:port/path`, or `launch:<argv>`. |
| `catalog-name` | yes | The VGI-side catalog to attach (see `catalog_catalogs()` on the worker) — not the Spark catalog name. |
| `connections` | no (default 4) | Pooled driver-side connections used for catalog/table discovery and each scan's bind+plan. Executors open their own dedicated connection per task, outside this pool. |
| `connection-acquire-timeout-millis` | no (default 30000) | How long a pool borrow waits before failing. |
| `target-split-size-bytes` | no | Passed to `table_function_plan` as `target_split_bytes`. |
| `min-splits` | no | Passed as `min_splits`. |
| `max-splits-per-response` | no (default 1000) | Pagination cap per `table_function_plan` call. |
| `max-plan-pages` | no (default 1024) | Bound on `table_function_plan` pagination — `Batch.planInputPartitions()` runs on the driver and must return a fixed array, so it drains the plan's cursor to completion rather than streaming it; this caps that loop rather than following a misbehaving worker forever. |
| `http-bearer-token` | no | Static bearer token for an `http(s)://` `location` that requires one. |

## Building and testing

```bash
./gradlew build
```

Integration tests need `~/Development/vgi-python` checked out (for the
reference Python fixture worker); they skip gracefully via
`Assumptions.assumeTrue` if it isn't present.

## Architecture

Ported from [`vgi-trino`](https://github.com/Query-farm/vgi-trino)'s design —
the closest existing precedent (same JVM, same VGI wire protocol, same
distributed-retry correctness constraints Trino and Spark both have) — onto
Spark's DataSource V2 SPI in place of Trino's connector SPI. See that repo's
own class-by-class structure for the fuller rationale behind each piece;
`farm.query.vgi` (the [`vgi-java`](https://github.com/Query-farm/vgi-java)
client toolkit — `VgiService`, `RpcConnection`/`HttpRpcConnection`, the
protocol records, and the `client.*` pushdown/projection encoders) is
depended on directly rather than reimplemented.
