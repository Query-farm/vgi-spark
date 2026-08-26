// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgispark.scan;

import org.apache.spark.sql.connector.read.InputPartition;

import java.util.Map;

/**
 * One FORMAT branch location, planned as its own partition — one file per
 * partition, so Spark parallelizes across a format branch's files the same
 * way it does across VGI splits (see {@code VgiScan#planFormatBranchPartitions}).
 * Read by {@code VgiCsvPartitionReader}, not {@link VgiPartitionReader} — no
 * VGI RPC involved at all for a format-branch partition.
 *
 * @param formatName the reader format (only {@code "csv"} is supported —
 *        see {@code farm.query.vgispark.branch.VgiFormatScanBranch}'s own javadoc)
 * @param location this partition's one file location (a plain local path or
 *        a {@code file://} URI)
 * @param formatOptions the branch's decoded {@code format_options}, shared
 *        across every partition of this branch
 */
public record VgiFormatInputPartition(
        String formatName, String location, Map<String, Object> formatOptions) implements InputPartition {
}
