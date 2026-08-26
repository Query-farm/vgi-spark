// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgispark.branch;

/**
 * One physical source backing a {@link farm.query.vgispark.VgiTable}'s scan
 * — the common supertype of {@link VgiScanBranch} (a VGI table function to
 * call) and {@link VgiFormatScanBranch} (a {@code format} branch: native
 * file locations read directly, without a VGI RPC round trip). A
 * catalog-table branch (federating into a companion catalog) has no
 * implementation yet — see {@code VgiCatalog.resolveBranches}, which still
 * refuses it, and each kind's own javadoc.
 */
public sealed interface VgiBranch permits VgiScanBranch, VgiFormatScanBranch {
}
