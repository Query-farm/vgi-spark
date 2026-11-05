// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgispark.scan;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.HasPartitionKey;

/** A VGI input partition with the exact transformed key Spark requires. */
public record VgiKeyGroupedInputPartition(
        VgiInputPartition delegate,
        InternalRow partitionKey) implements HasPartitionKey {

    @Override
    public String[] preferredLocations() {
        return delegate.preferredLocations();
    }
}
