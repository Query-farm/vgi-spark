// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgispark.branch;

import farm.query.vgi.client.ScanFunctionArguments;

/**
 * One physical source backing a {@link farm.query.vgispark.VgiTable}'s scan —
 * a FUNCTION branch whose {@code function_name} names a well-known reader the
 * CALLING engine should run itself, not a VGI-hosted function to RPC-bind
 * (VGI's <em>native scan-function delegation</em> mechanism: {@code
 * TableScanFunctionGetResponse.function_name()}/a {@code ScanBranchesResult}
 * FUNCTION branch's {@code function_name} can name {@code read_parquet},
 * {@code read_csv}, or {@code iceberg_scan} instead of a function the worker
 * actually hosts — the DuckDB C++ extension resolves this by checking its own
 * function catalog before ever treating it as an RPC target).
 *
 * <p>Confirmed live, not hypothetical: a real public worker
 * ({@code vgi-overture-maps-typescript},
 * {@code https://vgi-overture.rusty-bb6.workers.dev}) ships no data at all —
 * every table delegates to {@code read_parquet} against Overture's public S3
 * GeoParquet — and binding {@code read_parquet} as an ordinary RPC target
 * against it raises {@code FunctionNotFoundError} (its own function registry
 * is deliberately empty; there is nothing to RPC-call).
 *
 * <p>{@code VgiCatalog.resolveBranches} builds this instead of {@link
 * VgiScanBranch} whenever {@code functionName} is one of {@link
 * farm.query.vgispark.scan.VgiNativeScanResolver}'s known targets.
 * {@code VgiCatalog.loadTable} then intercepts a table that resolves to
 * exactly one branch of this kind and hands it to {@code
 * VgiNativeScanResolver} to build Spark's own real Parquet/CSV/Iceberg {@code
 * Table} directly — {@link farm.query.vgispark.VgiTable}/{@code
 * VgiScanBuilder}/{@code VgiScan}/{@code VgiPartitionReader} are never
 * involved for a purely-native-delegating table; Spark's own file/Iceberg
 * source machinery does the real reading, with real pushdown, not anything
 * hand-rolled. A MIXED multi-branch table (this branch kind alongside a real
 * {@link VgiScanBranch}/{@link VgiFormatScanBranch}) is refused — see {@code
 * VgiScan.planInputPartitions}'s own sealed-interface switch — matching this
 * connector's established "refuse rather than silently mis-scan" idiom for
 * every other not-yet-supported branch shape.
 *
 * @param functionName the native-delegation function name the worker named
 *        (e.g. {@code "read_parquet"}) — case-insensitive on the wire, kept
 *        as-received here
 * @param arguments the function's bound arguments, decoded (NOT re-encoded as
 *        {@code BindRequest.arguments} the way {@link VgiScanBranch} needs —
 *        there is no RPC bind for this branch kind at all)
 */
public record VgiNativeScanBranch(String functionName, ScanFunctionArguments.Decoded arguments)
        implements VgiBranch {
}
