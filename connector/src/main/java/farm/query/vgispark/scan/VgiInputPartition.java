// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgispark.scan;

import org.apache.spark.sql.connector.read.InputPartition;

import java.util.List;

/**
 * One Spark task worth of a VGI scan: zero or more real, redeemable splits.
 * An empty {@code tokens} list represents the "whole scan, not split-capable" sentinel VGI's
 * framework returns for functions that never opted into splitting (an empty
 * token list — see {@code VgiServiceImpl}'s own doc comment on this exact
 * convention). {@link VgiPartitionReader} branches on that emptiness: real
 * tokens are redeemed sequentially, one stream at a time; an empty list calls
 * {@code init()} with no split tokens at all, an ordinary primary scan. The
 * sequential form lets the driver honor {@code PlanResponse.max_workers}
 * without discarding work or opening too many concurrent streams.
 *
 * <p>Serialized into the task binary shipped to every executor — Spark warns
 * above 1000 KiB per serialized task (covering the whole task, not just this
 * partition), and today's sealed split tokens are on the order of hundreds of
 * bytes (see {@code TableFunctionPlanRequest}'s own javadoc), so a single
 * split is nowhere near that budget. Ported from {@code vgi-trino}'s
 * {@code VgiSplit}, minus the Trino-specific {@code SplitWeight}/{@code
 * HostAddress} machinery — Spark's {@link InputPartition#preferredLocations()}
 * takes plain hostnames directly.
 *
 * @param bindCall the serialised {@code BindRequest} this split's scan was bound with
 * @param bindOpaqueData the matching {@code BindResponse.opaque_data}, or {@code null}
 * @param outputSchema the complete bind output schema for this branch
 * @param pushdownFilters protocol 3 filters encoded against that output schema
 * @param tokens the split redemption tokens, or empty for the not-split-capable sentinel
 * @param estimatedBytes these splits' estimated size, or 0 if unknown
 * @param addresses hosts where this split is cheap to read, from
 *        {@code ScanSplit.location_ids}/{@code PlanResponse.locations}; empty
 *        when the worker named none
 */
public record VgiInputPartition(
        byte[] bindCall,
        byte[] bindOpaqueData,
        byte[] outputSchema,
        byte[] pushdownFilters,
        List<byte[]> tokens,
        long estimatedBytes,
        String[] addresses) implements InputPartition {

    @Override
    public String[] preferredLocations() {
        return addresses;
    }
}
