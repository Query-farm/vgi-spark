# vgi-spark

> **Note:** this file is internal working documentation for AI-assisted
> development sessions. It references machine-specific paths (`~/Development`)
> that exist only on the maintainer's machine. If you're evaluating or using
> the connector, start with [README.md](README.md).

Spark DataSource V2 catalog connector for VGI (Vector Gateway Interface)
workers, ported from [`vgi-trino`](https://github.com/Query-farm/vgi-trino)'s
design onto Spark's DataSource V2 SPI. See that repo's own class-by-class
structure for the fuller rationale behind most of this connector's pieces —
most classes here have a direct Trino-side counterpart, named in each class's
own javadoc.

## Canonical references

- **`vgi-trino`** (`~/Development/vgi-trino`): the closest structural
  precedent — same JVM, same VGI wire protocol, same distributed-retry
  correctness constraints. When something here looks odd, check whether
  `vgi-trino` already solved it (`VgiWorkerClient`, `VgiTableScanFunctions`,
  the filter/pushdown encoders) before inventing a new approach. It also
  shares real bugs with this connector — see "Bugs shared with vgi-trino"
  below.
- **`vgi-java`** (`~/vgi-java`, note: NOT under `~/Development`): the client
  toolkit this connector depends on directly (`farm.query.vgi.*` —
  `VgiService`, `RpcConnection`/`HttpRpcConnection`, protocol records, the
  `client.*` pushdown/projection encoders). Composite-included via
  `settings.gradle.kts`'s `VGI_JAVA_DIR` (defaults to `../../vgi-java`
  relative to this repo, i.e. `~/vgi-java` when this repo lives at
  `~/Development/vgi-spark`).
- **`vgi-rpc-java`** (`~/Development/vgi-rpc-java`): the RPC transport layer
  (`farm.query.vgirpc.*`), composite-included via `vgi-java`'s own
  `settings.gradle.kts` (`VGI_RPC_JAVA_DIR`, defaults to
  `../Development/vgi-rpc-java` relative to `vgi-java`). Its `launcher`
  package backs this connector's `launch:` scheme.
- **`vgi-rust`** (`~/Development/vgi-rust`): the worker this connector's own
  test suite runs against (`vgi-example-worker` binary) — see "Test worker"
  below for why, and for a real naming trap on a fresh machine.
- **`vgi`** (`~/Development/vgi`): the DuckDB C++ extension (the reference
  client) and the sqllogictest corpus this connector's conformance suite
  replays (`~/Development/vgi/test/sql/integration/`). Also the license
  template — this repo's `LICENSE` is `~/Development/vgi/LICENSE` with the
  Licensed-Work name/trademark section adapted to `vgi-spark`.
- **`docs/ROADMAP.md`** (this repo): feature-by-feature tracking against the
  sqllogictest suite — what's done, what's next, what's a real gap vs. a
  Spark SQL-language ceiling.

## Test worker: vgi-rust, not vgi-python

The connector's own reference-worker tests (`VgiWorkerHarness`) run against
`vgi-rust`'s compiled `vgi-example-worker` binary over a `unix://` socket,
**not** `vgi-python`'s `uv run --project ~/Development/vgi-python
vgi-fixture-worker`. Both are wire-compatible reference fixtures, but a
`uv run` invocation pays real per-process Python/venv-resolution startup
cost, multiplied by every worker spawn a test run does — switched to the
compiled Rust binary specifically because that made the suite too slow to
iterate on (2026-08-27).

Build it before running tests:

```bash
cd ~/Development/vgi-rust
cargo build --release --bin vgi-example-worker
```

`VgiWorkerHarness.workerBinary` prefers whichever of `target/debug/` and
`target/release/` has the newer mtime — **prefer `--release`**. The debug
build's own allocator-tracking overhead (see the `arrow.memory.debug.allocator`
landmine below — unrelated flag, same "debug build tax" shape) made this
connector's own conformance suite run 30x slower once concurrent test
execution was enabled; the debug-vs-release Rust build choice is a smaller
version of the same lesson.

**Naming trap on a fresh machine:** `Query-farm/vgi-rpc-rust` (a *different*
repo — the generic RPC transport library, unrelated to VGI) is sometimes
already checked out as `~/Development/vgi-rust` on a shared machine set up
for other Query Farm work. Check `git remote -v` before trusting the
directory name — if it says `vgi-rpc-rust.git`, rename that checkout out of
the way and clone the real `Query-farm/vgi-rust.git` fresh.

`VgiWorkerHarness.unix(vgiRustDir, catalogName)` selects which fixture
catalog the one compiled binary serves via `VGI_WORKER_CATALOG_NAME` (Python
packages the same fixtures as separate entry-point scripts instead — one
real difference the wire-compatibility claim doesn't cover). Confirmed
catalog names in use: `example` (default), `versioned`,
`versioned_tables`, `attach_options` (also registers
`attach_options_required` as a secondary catalog on the same worker
process — see `vgi-example-worker/src/main.rs`).

## How tests run

```bash
./gradlew build                                    # everything
./gradlew :connector:test                           # just the connector's own suite
./gradlew :connector:test --tests "farm.query.vgispark.VgiCatalogQueryTest"   # one class
```

Needs `~/Development/vgi-rust` (built, see above) and `~/Development/vgi`
(the sqllogictest corpus) checked out; both gate via
`Assumptions.assumeTrue` and skip gracefully if absent.

**Parallelism**: `forkEvery = 1` (a fresh JVM per test class, for OS-level
socket/thread isolation) plus `maxParallelForks` scaled to
`availableProcessors() / 4`, so multiple test classes run concurrently, not
one at a time. The two largest classes by test-method count
(`VgiCatalogQueryTest`, 39 methods; `VgiSqlLogicTestConformanceTest`, ~16)
additionally run their own methods concurrently
(`@Execution(ExecutionMode.CONCURRENT)`, opted in via
`connector/src/test/resources/junit-platform.properties` — every other
class stays at the default `same_thread` mode) — safe because neither class
writes any data; every method just reads against one shared, already-attached
SparkSession/worker built once in `@BeforeAll`. `VgiSqlLogicTestSweepTest`'s
own internal file-level concurrency (`FILE_PARALLELISM`) and both classes'
`local[N]` executor width are similarly scaled off `availableProcessors()`,
capped (16–32) rather than using the raw core count, since past some width
more concurrent connections against the one worker process mostly add
contention rather than throughput (VGI's RPC is lockstep per connection).
On a 48-core box this all adds up to the full suite (~76 tests, including
the 328-file sqllogictest sweep) running in under two minutes.

### The `arrow.memory.debug.allocator` landmine

**Do not set `-Darrow.memory.debug.allocator=true` on the test JVM.** It was
set in `build.gradle.kts` for a while during this session's own debugging
and turned out to be catastrophic once concurrent test execution was
enabled: Arrow's debug allocator records a full Java stack trace, under a
synchronized lock, on *every* buffer allocation. Under concurrency this
single flag took `VgiSqlLogicTestConformanceTest` from ~9 seconds to 300+
seconds (three of its methods didn't even finish inside their 180–300s
`@Timeout` — not real hangs, just this). Confirmed via a thread dump showing
multiple threads each burning 50+ seconds of *actual CPU time* stuck inside
`RecordCodec.deserializeFromBytes` → `IpcStreamReader`, all serializing on
the same `HistoricalLog` lock. Nothing in the test suite reads or depends on
this flag — removed 2026-08-27, keep it that way.

### Diagnosing a stuck-looking test

Before assuming a hang is a real deadlock or resource contention: get a
thread dump (`jstack <pid>` on the `Gradle Test Executor` JVM) and check
whether the stuck threads are actually burning CPU (`RUNNABLE`, climbing
`cpu=` time on repeated `ps -o time=` checks) vs genuinely blocked/waiting.
The `arrow.memory.debug.allocator` bug above looked exactly like a hang
(three tests sitting at their exact `@Timeout` ceiling every single run)
until a thread dump showed `RUNNABLE` threads spending real CPU time in a
lock, not `BLOCKED`/`WAITING`. Isolating the suspect class from the rest of
the suite (`--tests "...OneClass"` alone) and comparing timings is the
fastest way to tell "this class is slow on its own" from "this class is
slow only when several heavy classes run at once" — in this session's case,
isolation reproduced identical timings, which is what pointed at a
JVM-flag-level cause rather than cross-class contention.

## Wire-protocol landmines (real bugs found this session, worth knowing about)

These bit this connector specifically; watch for the same shape elsewhere.

- **`FunctionInfo.function_type` is lowercase on the wire** (`"table"`,
  `"aggregate"`, `"scalar"`) — `vgi-rust`'s own
  `vgi-protocol/src/protocol/enums.rs`. A case-sensitive `"TABLE".equals(...)`
  /`"AGGREGATE".equals(...)` comparison against this field is always false —
  unrelated to the RPC *call's* own uppercase category constants
  (`"TABLE_FUNCTION"`, `"AGGREGATE_FUNCTION"`, which the server normalizes
  internally). This meant `CALL` syntax (`VgiTableProcedures`) and aggregate
  function discovery (`VgiAggregateFunctions`) **never actually found
  anything, all session**, until fixed 2026-08-27 — always compare
  case-insensitively (`equalsIgnoreCase`) against a `function_type` value
  read back from the wire.
- **`BindRequest.schema_name` is required as of VGI protocol 1.1.0.** A
  spec-strict worker (`vgi-rust`'s) refuses an unqualified bind outright
  (`"bind for '<fn>' carries no schema_name..."`) — it does NOT search every
  schema by name the way older/more lenient reference workers do. Only a
  COPY handler bind, a genuinely pre-1.1.0 peer, or a function the worker
  deliberately hid from its own catalog listing gets a pass on omitting it.
  `vgi-trino` has the identical gap in its own `VgiSplitManager` (untested
  there — see "Bugs shared with vgi-trino" below).
- **A declarative table's backing scan function can use different raw
  column names than the table itself declares** (e.g. the reference
  fixture's `data.numbers` table declares column `value`, but its backing
  `main.sequence` function's own raw output column is `n`). The *wire*
  schema sent in `InitRequest.output_schema` — which the worker uses to
  narrow/project each emitted batch **by name** — must be the function's
  own bind-resolved schema (`BindResponse.output_schema`), not the table's
  declared one; nothing downstream in this connector's own reading actually
  needs the table's declared names, since `ColumnarBatch` is matched to
  Spark's `StructType` purely by position. `vgi-trino` has the identical
  gap in the same two classes (untested there too).
- **`launch:` needs a JDK 22+ *runtime*, not just a modern *language*
  target.** `PosixLauncherSupport` (`vgi-rpc-java`) ships a JDK-21 baseline
  stub (`available() == false`, every op throws
  `UnsupportedOperationException`) plus a JDK-22+ Foreign-Function-and-Memory
  overlay for the real `flock(2)`/`geteuid()` calls. This repo's own Gradle
  toolchain deliberately stays on JDK 21 (a JDK 25 toolchain bump was tried
  once, reverted — see git history on `build.gradle.kts` — Spark 4.2.0's own
  Netty networking and Arrow's `arrow-memory-netty` allocator want two
  different, mutually incompatible Netty majors under JDK 25 on this
  dependency graph). `VgiLaunchTransportTest`'s launcher-path assertions are
  `Assumptions`-guarded to skip on this repo's own JDK 21 toolchain; they
  exist to run for real once a future Spark/Netty/Arrow alignment lets the
  toolchain move past JDK 21.

## Bugs shared with vgi-trino

`vgi-trino`'s `VgiSplitManager`/`VgiPageSourceProvider` use `TableInfo
.columns()` (the table's own declared schema) directly as the wire-level
output schema, and `VgiSplitManager`'s own comment explicitly documents
sending a `null`/unqualified `schema_name` on scan-function binds — the
exact same two bugs this connector had until 2026-08-27. Likely never
surfaced there because `vgi-trino`'s own test corpus doesn't happen to
exercise a table whose backing function renames columns, or a worker that
enforces schema_name as strictly as `vgi-rust`'s does. Also confirmed
`vgi-trino` never reads `FunctionInfo.null_handling` either (grepped) — the
same NULL-scalar-argument bug documented below, just never surfaced there.
Worth porting all three fixes back if `vgi-trino` is ever run against
`vgi-rust`.

## Fixed: scalar function called with a NULL argument (2026-08-27)

A scalar function called via an all-literal argument list including a NULL
(e.g. `SELECT some_scalar_fn(NULL::DOUBLE)`) threw a bare, unexplained
`NoSuchElementException` from `ClientStreamSession.readNextDataBatch`.
Root-caused via direct worker instrumentation (temporary `eprintln!`/
`std::fs::write` diagnostics added to `vgi-rust`'s `dispatch.rs` and
`vgi-rpc-rust`'s `server.rs`, uncommitted, reverted after diagnosis —
needed a `[patch.crates-io]` override in `vgi-rust`'s `Cargo.toml` plus a
matching version bump in `vgi-rpc-rust`'s own `Cargo.toml` to get a locally
edited `vgi-rpc` picked up over the published crate; also reverted):

`FunctionInfo.null_handling` encodes DuckDB's own "STRICT SQL function"
contract — a `DEFAULT`-handling function (the common case; `SPECIAL` opts
out) is short-circuited to a NULL result by the *calling engine*, for any
NULL argument, without the function ever being invoked at all. DuckDB's own
query planner does this upstream of the RPC entirely — confirmed by reading
its scalar-function corpus (`~/Development/vgi/test/sql/integration/overload/scalar_overload.test`
expects `format_number(NULL::DOUBLE)` etc. to return `NULL`) and its own
`vgi_scalar_function_impl.cpp` (no null-specific short-circuit in the
extension's own callback code — it's handled further upstream, in DuckDB's
own function-registration nullability contract). Spark's `ScalarFunction<T>`
API has no equivalent declarative contract of its own — `produceResult` is
always called regardless of null arguments — and this connector never read
`null_handling` at all, so it always sent the RPC through unconditionally.
Sending an actual null for a `DEFAULT`-handling argument hits a real,
worker-side Arrow schema-validation error (confirmed directly:
`"Column 'value' is declared as non-nullable but contains null values"`)
that the client never sees — `vgi-rpc-rust`'s own lockstep server loop
swallows a `read_next()` failure into a silent stream close (`Err(_) =>
break`, no error frame written at all), which is the reason this surfaced
as a bare `NoSuchElementException` with zero explanation instead of a clear
error. **That silent-swallow is a real, separate bug worth reporting
upstream to `vgi-rpc-rust`** — flagged to the user, not fixed here (not
this connector's code to fix, and not blocking once the real cause was
known).

Fix (`VgiUnboundScalarFunction`/`VgiScalarFunction`): compute
`shortCircuitOnNull` from `info.null_handling()` at bind time (anything but
`"SPECIAL"` → true), and check every row's full arity for a null value
before ever calling `ensureConnection()`/attempting the RPC — matching
DuckDB's own engine-level contract, entirely client-side. Three other
hypotheses were tested and ruled out first (kept as permanent regression
coverage in `VgiScalarNullArgumentTest`, which calls `produceResult`
directly — no Spark SQL involved): Spark's `ConstantFolding` calling
`produceResult` repeatedly on the same instance (a single direct call
reproduced identically — not about repetition), connection reuse (a
bounded retry with a genuinely fresh connection failed identically —
tried and reverted, see git history), and a worker crash (checked its
stderr directly, twice, clean both times).

## Open bugs (not yet fixed)

- **`opt_time` / Arrow `Time(MICROSECOND)` has no Spark mapping.**
  Confirmed by inspecting Spark 4.2.0's own `ArrowColumnVector` bytecode
  (`spark-catalyst_2.13-4.2.0.jar`) — it has an accessor only for
  `TimeNanoVector` (nanosecond precision), not `TimeMicroVector`. A real fix
  needs a custom `ColumnVector` wrapper (Spark's own automatic
  `ArrowColumnVector` support can't be coerced into it); not attempted —
  `VgiTypeMapping.toSparkType` throws a clear `UnsupportedOperationException`
  instead of silently mis-reading the column.
- **Aggregate functions don't support `any`-typed or `vgi_const` arguments**
  (scalar functions already do — `VgiUnboundScalarFunction`;
  `VgiUnboundAggregateFunction` doesn't yet extend the same handling). The
  largest concrete gap in the sqllogictest sweep's `aggregate/*.test`
  failures.

## State of play (as of 2026-08-27)

Session summary: implemented `launch:` (as the bare-command default),
custom ATTACH options, and ATTACH-time version negotiation (roadmap tier 3);
switched the whole test suite from the Python fixture worker to the
compiled `vgi-rust` binary for speed; found and fixed four real,
previously-undiscovered wire-protocol bugs (schema_name omission,
case-sensitive `function_type` comparisons that meant `CALL`/aggregates
never worked at all, the declarative-table column-rename projection
mismatch, and the NULL-scalar-argument short-circuit gap); found and
removed the `arrow.memory.debug.allocator` performance landmine; added
cross-class and intra-class test parallelism; added CI (GitHub Actions,
`.github/workflows/ci.yml`), verified green against the real system. Full
suite: ~76 tests, 2 `@Disabled` (the `opt_time` gap — marked disabled
rather than left red, since it's a known, accepted limitation, not a
regression to chase; re-enable if that gap is ever fixed), ~107 seconds
wall clock on a 48-core box (under 4 minutes on a real 2-core GitHub-hosted
CI runner). Sqllogictest sweep: 191 of 328 files eligible, 720 of 2952
records pass (up from 711 before the NULL-argument fix) — up from a stale
405/2944 baseline that predates this session's fixes.

Work happened partly on a temporary EC2 instance (Amazon Linux 2023,
aarch64/Graviton, Corretto 21) rather than the maintainer's own machine,
specifically to get real parallelism headroom the maintainer's own machine
didn't have available at the time (other concurrent sessions on it pushed
load averages past 100). Not expected to be relevant to a normal
development session, but if you see references to it in git history: the
repo now has a real GitHub remote (`Query-farm/vgi-spark`, created this
session — previously local-only, 23 commits with no remote at all) and
everything from that instance was pushed through it, not copied by hand.

## Out of scope

VGI table-function calling as `TABLE(...)` (Spark has no pluggable-catalog
SQL syntax for this — `CALL` covers what it can, see
`VgiUnboundTableProcedure`'s own javadoc), multi-branch `CATALOG_TABLE`/
non-CSV `FORMAT` branches, time travel by version/timestamp (VGI supports
`at_unit`/`at_value` on binds; nothing in this connector threads Spark's own
`TableCatalog.loadTable(Identifier, String)`/`loadTable(Identifier, long)`
time-travel overloads through to it yet), writes.
