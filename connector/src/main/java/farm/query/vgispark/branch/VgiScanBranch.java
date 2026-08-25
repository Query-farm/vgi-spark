// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgispark.branch;

/**
 * One physical source backing a {@link farm.query.vgispark.VgiTable}'s scan —
 * a single-function branch, already resolved to bind-ready arguments.
 *
 * <p>v1 supports only <em>function</em> branches (a table function to call,
 * matching what {@code catalog_table_scan_function_get}'s legacy single-scan
 * path already returns) — a table backed by a single scan resolves to a
 * one-element {@code List<VgiScanBranch>}, and a genuinely multi-branch table
 * (via {@code catalog_table_scan_branches_get}) resolves to one entry per
 * VGI-declared function branch. <em>Catalog-table</em> branches (federating
 * into a companion catalog) and <em>format</em> branches (native
 * parquet/csv/iceberg delegation) are refused at discovery time with a clear
 * error naming the table and branch, rather than silently dropped — see
 * {@code VgiCatalog.loadTable}'s branch-resolution logic. A branch declaring
 * a non-empty {@code branch_filter} (a worker-side predicate that must be
 * AND'd into every scan of that branch) is refused the same way for now —
 * translating VGI's small {@code col OP const [AND/OR ...]} branch-filter
 * grammar into a per-branch pushdown alongside the query's own WHERE clause
 * is tracked as follow-up work (see {@code docs/ROADMAP.md}, "Multi-branch:
 * format branches" and its own note on {@code branch_filter}).
 *
 * @param functionName the table function to call for this branch
 * @param scanFunctionArguments the function's bound arguments, already
 *        re-encoded as {@code BindRequest.arguments} bytes (see {@code
 *        farm.query.vgi.client.ScanFunctionArguments#toBindArguments})
 */
public record VgiScanBranch(String functionName, byte[] scanFunctionArguments) {
}
