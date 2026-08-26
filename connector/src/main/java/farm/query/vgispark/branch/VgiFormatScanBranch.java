// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgispark.branch;

import java.util.List;
import java.util.Map;

/**
 * One FORMAT branch: the worker names a format and one or more file
 * locations rather than a VGI table function — "these files, this format,"
 * letting a worker delegate cold-tier/lakehouse data to a native reader
 * without knowing that reader's argument spelling (see roadmap tier 2 item
 * 11, "Multi-branch: format branches").
 *
 * <p>v1 supports only {@code formatName.equals("csv")}, read directly (a
 * delimiter-plus-optional-header split, NOT a full RFC 4180 parser — no
 * quote/escape handling) by {@code VgiCsvPartitionReader}, and only local
 * filesystem paths (a plain path or {@code file://} URI) — see {@code
 * VgiCatalog.resolveBranches} for what's refused instead of guessed at
 * (parquet/delta/iceberg formats, and any non-local location scheme).
 *
 * @param formatName the reader format the worker named (e.g. {@code "csv"})
 * @param locations one or more file locations to read, unioned — {@code
 *        VgiScan} plans one {@code VgiFormatInputPartition} per location, so
 *        Spark parallelizes across files the same way it does across VGI
 *        splits
 * @param formatOptions the worker's {@code format_options}, decoded from
 *        their 1-row-IPC-batch wire form by {@link FormatOptionsDecoder} —
 *        raw Java values (e.g. {@link String}, {@link Boolean}), interpreted
 *        by whichever reader {@code formatName} selects (only {@code
 *        "delim"}/{@code "header"}/{@code "nullstr"} are recognized by the
 *        CSV reader; an unrecognized key is ignored, not refused, matching
 *        VGI's own additive-metadata philosophy)
 */
public record VgiFormatScanBranch(
        String formatName, List<String> locations, Map<String, Object> formatOptions) implements VgiBranch {
}
