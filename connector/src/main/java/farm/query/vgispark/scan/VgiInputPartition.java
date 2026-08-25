// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgispark.scan;

import org.apache.spark.sql.connector.read.InputPartition;

/**
 * One unit of a VGI scan: either a real, redeemable split (non-empty
 * {@code token}) or the "whole scan, not split-capable" sentinel VGI's
 * framework returns for functions that never opted into splitting (empty
 * {@code token} — see {@code VgiServiceImpl}'s own doc comment on this exact
 * convention). {@link VgiPartitionReader} branches on that emptiness: a
 * non-empty token redeems via {@code init()}'s {@code split_tokens}; an empty
 * one calls {@code init()} with no split tokens at all, an ordinary primary
 * scan.
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
 * @param token the split's redemption token, or empty for the not-split-capable sentinel
 * @param estimatedBytes this split's estimated size, or 0 if unknown
 * @param addresses hosts where this split is cheap to read, from
 *        {@code ScanSplit.location_ids}/{@code PlanResponse.locations}; empty
 *        when the worker named none
 */
public record VgiInputPartition(
        byte[] bindCall,
        byte[] bindOpaqueData,
        byte[] token,
        long estimatedBytes,
        String[] addresses) implements InputPartition {

    @Override
    public String[] preferredLocations() {
        return addresses;
    }
}
