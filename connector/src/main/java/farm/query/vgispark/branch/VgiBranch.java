// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgispark.branch;

/**
 * One physical source backing a {@link farm.query.vgispark.VgiTable}'s scan
 * — the common supertype of {@link VgiScanBranch} (a VGI table function to
 * RPC-call), {@link VgiFormatScanBranch} (a {@code format} branch: native
 * file locations read directly, without a VGI RPC round trip), and {@link
 * VgiNativeScanBranch} (a FUNCTION branch whose {@code function_name} names a
 * reader the calling engine should run itself — {@code read_parquet}, {@code
 * read_csv}, {@code iceberg_scan} — rather than a function the worker
 * actually hosts; see that record's own javadoc). A catalog-table branch
 * (federating into a companion catalog) has no implementation yet — see
 * {@code VgiCatalog.resolveBranches}, which still refuses it, and each kind's
 * own javadoc.
 */
public sealed interface VgiBranch permits VgiScanBranch, VgiFormatScanBranch, VgiNativeScanBranch {
}
