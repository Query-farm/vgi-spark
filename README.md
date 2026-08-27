# vgi-spark

A Spark DataSource V2 catalog connector for [VGI](https://github.com/Query-farm/vgi)
(Vector Gateway Interface) workers — the same out-of-process, Arrow-IPC-based
worker protocol the `vgi` DuckDB extension and
[`vgi-trino`](https://github.com/Query-farm/vgi-trino) attach to. One Spark
catalog maps to one VGI `ATTACH`, the same granularity DuckDB and Trino use.

```scala
spark.conf.set("spark.sql.catalog.vgi_example", "farm.query.vgispark.VgiCatalog")
spark.conf.set("spark.sql.catalog.vgi_example.location",
  "~/Development/vgi-rust/target/release/vgi-example-worker")
spark.conf.set("spark.sql.catalog.vgi_example.catalog-name", "example")

spark.sql("SELECT * FROM vgi_example.data.numbers").show()
```

`location` accepts a bare shell command (as above — routed through the shared
`launch:` launcher by default, see Configuration below), an explicit
`launch:<argv>`, `unix:///path/to.sock`, `tcp://host:port`, or
`http(s)://host:port/path`.

**Conformance roadmap:** [`docs/ROADMAP.md`](docs/ROADMAP.md) tracks, feature by feature, what's
needed to pass more of the real `~/Development/vgi/test/sql/integration/` sqllogictest suite —
what's done, what's next, what's a real gap vs. a Spark SQL-language ceiling we can't cross, and
exactly which `.test` files each item unlocks.

## Status

Catalog/namespace/table discovery, real multi-split scan planning, vectorized
reads, projection/filter/limit pushdown, catalog scalar and aggregate
functions, table-function `CALL` (Spark's DataSource V2 `ProcedureCatalog`,
in place of DuckDB/Trino's `TABLE(...)` syntax — see Non-goals below), the
`launch:` shared-warm-worker scheme, custom worker-declared ATTACH options,
and ATTACH-time data-version/implementation-version negotiation are all
implemented and verified against a live worker (all four transports:
subprocess, `unix://`, `tcp://`, `http://`).

**Known gaps**, found via a full sweep of the real `.test` sqllogictest suite
(see below), not chased further because each is genuinely new scope beyond
what's implemented so far:
- Filter pushdown doesn't reach into **struct subfields** (`s.a > 5`) —
  `TableInfo.required_filters` naming a nested path (e.g. `bbox.xmin`) can
  never be satisfied, so those tables' scans correctly (if conservatively)
  refuse rather than silently doing a full scan.
- **DuckDB generated columns** (`generated_columns={"doubled": "n * 2"}`) —
  DuckDB evaluates the expression client-side after reading only the base
  columns; this connector has no such expression evaluator.
- **`any`-typed and `vgi_const` (bind-time constant) arguments** to
  aggregate functions aren't supported yet (scalar functions already handle
  both — see `VgiUnboundScalarFunction`'s own validation for exactly what a
  scalar call site refuses and why; `VgiUnboundAggregateFunction` doesn't
  yet extend the same handling to aggregates).
- **Arrow `Time` columns** (e.g. a worker-declared `TIME` ATTACH option) have
  no Spark mapping — Spark's own `ArrowColumnVector` only supports
  nanosecond-precision Time internally (confirmed by inspecting its
  bytecode), not the microsecond precision VGI workers commonly use; this
  throws a clear `UnsupportedOperationException` rather than silently
  mis-reading the column.

**Non-goals**, all with no Spark SQL surface to hook into: multi-branch
non-function/non-CSV branches (`CATALOG_TABLE`/non-CSV `FORMAT` branches —
plain function and CSV-format branches ARE supported), time travel, writes,
and calling a VGI table function directly as `TABLE(...)` (Spark has no
pluggable-catalog SQL syntax for that the way Trino does — `CALL` is the
substitute where the function takes no `TABLE` argument, see
`VgiUnboundTableProcedure`'s own javadoc for exactly why).

### Sqllogictest conformance

A curated regression gate (`VgiSqlLogicTestConformanceTest`) replays a
double-digit set of real upstream `.test` files individually, each asserting
an exact expected pass/skip/fail count so a real regression fails loudly
with a clear diff, not just "the sweep number went down somewhere."

A separate, uncurated **full-corpus sweep** (`VgiSqlLogicTestSweepTest`)
answers "how many of the ~328 files actually pass" honestly, with no
per-file marker lists — it only excludes files needing infrastructure this
harness doesn't set up (a non-default `ATTACH` alias/catalog/auth, or
`require-env` beyond `VGI_TEST_WORKER`), then attempts every record in what's
left and reports real pass/fail counts plus a full per-file breakdown to
`connector/build/sqllogictest-sweep-report.txt`.

Latest run: 191 of 328 files eligible; 720 of 2952 attempted records pass
(19 files fully passing, 172 with at least one failure). The largest single
category of the remainder — by a wide margin — is VGI table-function `CALL`
syntax used through DuckDB's `TABLE(...)` call shape, which the sweep's
plain-record replay can't express in Spark SQL at all (a non-goal, not a
bug); DuckDB-only introspection (`duckdb_functions()`, `duckdb_views()`, ...)
and the aggregate `any`/`vgi_const` gap noted above account for most of the
rest. See the generated report for the full per-file, per-record breakdown —
it's regenerated by every sweep run, not committed.

## Configuration

Set per catalog as `spark.sql.catalog.<name>.<key>`:

| Key | Required | Description |
|---|---|---|
| `location` | yes | The worker to attach: a bare shell command, `unix:///path/to.sock`, `tcp://host:port`, `http(s)://host:port/path`, or `launch:<argv>`. |
| `catalog-name` | yes | The VGI-side catalog to attach (see `catalog_catalogs()` on the worker) — not the Spark catalog name. |
| `connections` | no (default 4) | Pooled driver-side connections used for catalog/table discovery and each scan's bind+plan. Executors open their own dedicated connection per task, outside this pool. |
| `connection-acquire-timeout-millis` | no (default 30000) | How long a pool borrow waits before failing. |
| `target-split-size-bytes` | no | Passed to `table_function_plan` as `target_split_bytes`. |
| `min-splits` | no | Passed as `min_splits`. |
| `max-splits-per-response` | no (default 1000) | Pagination cap per `table_function_plan` call. |
| `max-plan-pages` | no (default 1024) | Bound on `table_function_plan` pagination — `Batch.planInputPartitions()` runs on the driver and must return a fixed array, so it drains the plan's cursor to completion rather than streaming it; this caps that loop rather than following a misbehaving worker forever. |
| `http-bearer-token` | no | Static bearer token for an `http(s)://` `location` that requires one. |
| `launcher-enabled` | no (default `true`) | Whether a bare-command `location` (no scheme prefix) is routed through the shared `launch:` launcher by default, rather than spawning an unshared subprocess per pooled connection. Set `false` to opt back out. Ignored for an explicit `launch:` prefix, `unix://`/`tcp://`/`http(s)://`, which never launch. |
| `launcher-idle-timeout-seconds` | no | How long the shared launched worker stays warm with no active connections before exiting. |
| `launcher-state-dir` | no | Overrides the launcher's own state directory (worker-election lock file, discovery info). |
| `data-version-spec` | no | Requests a specific `catalog_attach` data version; the worker resolves and reports back what it actually selected (see `VgiWorkerClient#resolvedDataVersion`). |
| `implementation-version` | no | Requests a specific worker implementation version at attach time. |
| `attach-option.<name>` | no | A custom, worker-declared ATTACH option (VGI's `AttachOptionSpec` mechanism). Values travel as plain UTF-8 strings only — Spark's catalog config has no type-disambiguation mechanism the way DuckDB's typed `ATTACH (opt 42, ...)` SQL clause does. |

Note: `launch:` (whether explicit or the bare-command default above) needs a
JDK 22+ runtime — it uses the Foreign Function & Memory API for its
`flock`/`geteuid` calls. On an older JDK, an explicit `launch:` location
fails outright; a bare-command default location gracefully falls back to a
plain per-connection subprocess spawn instead.

## Building and testing

```bash
./gradlew build
```

Integration tests need [`vgi-rust`](https://github.com/Query-farm/vgi-rust)
checked out at `~/Development/vgi-rust` with its `vgi-example-worker` binary
built (`cargo build --release --bin vgi-example-worker` there — prefer
`--release`; the test harness picks whichever of the debug/release builds
has the newer mtime, and the debug build's own allocator-tracking overhead
is real under this suite's concurrent test execution). They skip gracefully
via `Assumptions.assumeTrue` if it isn't present. The sqllogictest suites
additionally need [`vgi`](https://github.com/Query-farm/vgi) checked out at
`~/Development/vgi` for the real `.test` corpus.

The test task runs classes in parallel forks and, for its two largest
classes, concurrent methods within a class too (`@Execution(CONCURRENT)`) —
none of the tests write data, so there's no cross-test isolation hazard, only
host RAM/CPU to budget; both scale to `Runtime.getRuntime().availableProcessors()`.
On a modern multi-core machine the full suite (~76 tests, including the
full sqllogictest sweep) runs in under two minutes.

## Deploying it

This module's own jar carries only its own compiled classes — there's no
shadow/shade plugin, so it doesn't bundle its dependencies the way a
self-contained fat jar would. `./gradlew :connector:assembleDeployDir`
assembles everything a real Spark job needs into `connector/build/deploy/`:
this connector's own jar plus every runtime dependency it doesn't already
get from Spark's own classpath (`farm.query:vgi` and its own transitive
`farm.query:vgirpc`). **Arrow and Netty jars are deliberately excluded**
from that directory — Spark 4.2.0 itself bundles newer versions of both
(Arrow 19.0.0, Netty 4.2.13.Final) than what `farm.query:vgi` pulls in
transitively (Arrow 18.1.0, Netty 4.2.9.Final), and shipping the older
copies too via `--jars` would put two conflicting majors on the same
executor classpath. This isn't a guess: Gradle's own dependency resolution
on this module's test classpath already upgrades both to Spark's versions,
and this whole connector's test suite passes against those newer,
Spark-provided versions on every run — proof they already satisfy whatever
API surface `farm.query:vgi`/`farm.query:vgirpc` actually need.

```bash
./gradlew :connector:assembleDeployDir
spark-submit \
  --jars "$(echo connector/build/deploy/*.jar | tr ' ' ',')" \
  --conf spark.sql.catalog.vgi_example=farm.query.vgispark.VgiCatalog \
  --conf spark.sql.catalog.vgi_example.location='unix:///path/to/worker.sock' \
  --conf spark.sql.catalog.vgi_example.catalog-name=example \
  your_job.py
```

**A real deployment constraint worth planning for**: for a bare-command,
`unix://`, or `launch:` `location`, each executor opens its own connection
to the worker independently (`VgiPartitionReader` calls
`VgiWorkerClient.connect` directly, on whichever executor Spark schedules
a task to — confirmed in source, not assumed) — the driver never proxies
data through itself. That means the worker command must be spawnable, or
the `unix://` socket path reachable, from **every executor node**, not
just wherever the driver runs. This only matters once driver and executors
are genuinely different machines (a real cluster — everything in this
repo's own test suite runs `local[N]`/single-process, where the
distinction doesn't exist). `tcp://`/`http(s)://` locations don't have
this constraint, since those just need the worker reachable over the
network from every node, not spawnable/co-located on each one.

**Spark/Scala compatibility**: built and tested against exactly Spark
`4.2.0` / Scala `2.13`. Other Spark 4.x point releases or Scala `2.12` are
untested — nothing here is deliberately incompatible with them, but don't
assume it works without checking.

## Architecture

Ported from [`vgi-trino`](https://github.com/Query-farm/vgi-trino)'s design —
the closest existing precedent (same JVM, same VGI wire protocol, same
distributed-retry correctness constraints Trino and Spark both have) — onto
Spark's DataSource V2 SPI in place of Trino's connector SPI. See that repo's
own class-by-class structure for the fuller rationale behind each piece;
`farm.query.vgi` (the [`vgi-java`](https://github.com/Query-farm/vgi-java)
client toolkit — `VgiService`, `RpcConnection`/`HttpRpcConnection`, the
protocol records, and the `client.*` pushdown/projection encoders) is
depended on directly rather than reimplemented, and `farm.query.vgirpc`'s
own `launcher` package (from
[`vgi-rpc-java`](https://github.com/Query-farm/vgi-rpc-java)) backs the
`launch:` scheme the same way it does in `vgi-trino`.

## License

[Query Farm Source-Available License, Version 1.0](LICENSE) — the same license
[VGI](https://github.com/Query-farm/vgi) itself is released under. Free for
development, testing, and internal Production Use; see the license for the
narrow set of restricted uses (competing hosted offerings, commercial
marketplaces) that require a separate agreement with Query Farm.

Copyright 2026 Query Farm LLC - https://query.farm
