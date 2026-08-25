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

## Status

Early: catalog/namespace/table discovery, and real batch reads via the
legacy single-function scan path (`catalog_table_scan_function_get`),
including VGI's real multi-split scan planning (`table_function_plan` /
`ScanSplit` / `InitRequest.split_tokens`) mapped onto Spark's
`InputPartition` model. Reads are vectorized end to end — each Arrow
`VectorSchemaRoot` batch is wrapped directly in
`org.apache.spark.sql.vectorized.ArrowColumnVector`, no value copying.

Not yet implemented (see the plan): projection/filter/limit pushdown,
catalog scalar functions, multi-branch tables
(`catalog_table_scan_branches_get`), time travel, writes, and VGI table-function
calling (Spark has no SQL-level equivalent to Trino's `TABLE(...)` syntax for
this — deferred rather than bolted on awkwardly).

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
