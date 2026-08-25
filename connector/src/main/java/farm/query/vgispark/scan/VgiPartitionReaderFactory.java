// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgispark.scan;

import farm.query.vgispark.VgiCatalogConfig;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.util.List;

/**
 * Builds one {@link VgiPartitionReader} per {@link VgiInputPartition}, on
 * whichever executor Spark schedules that partition to.
 *
 * <p>Serialized into the task binary along with each {@link
 * VgiInputPartition} (see that class's own javadoc on the resulting size),
 * so it carries only what an executor needs to open its own connection —
 * {@link VgiCatalogConfig} — never a live {@code VgiWorkerClient} or
 * connection, neither of which is serializable or would mean anything in a
 * different JVM.
 */
public final class VgiPartitionReaderFactory implements PartitionReaderFactory {

    private final VgiCatalogConfig config;
    private final byte[] tableOutputSchemaBytes;
    private final List<Integer> projectionIds;
    private final byte[] pushdownFiltersBytes;
    private final Long rowLimit;

    /**
     * @param config this catalog's configuration
     * @param tableOutputSchemaBytes the table's full (bind-time) Arrow schema, IPC-encoded
     * @param projectionIds the columns to project, as ordinals into the full
     *        schema, or {@code null} for all of them
     * @param pushdownFiltersBytes the encoded {@code InitRequest.pushdown_filters}
     *        batch, or {@code null} if nothing was pushed — the same bytes
     *        every split's {@code init()} receives, informational/best-effort
     *        only (see {@code VgiScanBuilder}'s own javadoc)
     * @param rowLimit a fetch-limit hint pushed to every split's {@code init()},
     *        or {@code null}
     */
    public VgiPartitionReaderFactory(VgiCatalogConfig config, byte[] tableOutputSchemaBytes,
            List<Integer> projectionIds, byte[] pushdownFiltersBytes, Long rowLimit) {
        this.config = config;
        this.tableOutputSchemaBytes = tableOutputSchemaBytes;
        this.projectionIds = projectionIds;
        this.pushdownFiltersBytes = pushdownFiltersBytes;
        this.rowLimit = rowLimit;
    }

    @Override
    public boolean supportColumnarReads(InputPartition partition) {
        return true;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        // supportColumnarReads always returns true above, so Spark's engine
        // never actually calls this — fail loudly rather than silently
        // returning something wrong if that assumption ever stops holding.
        throw new IllegalStateException(
                "VgiPartitionReaderFactory only supports columnar reads (see supportColumnarReads)");
    }

    @Override
    public PartitionReader<ColumnarBatch> createColumnarReader(InputPartition partition) {
        return new VgiPartitionReader(config, (VgiInputPartition) partition, tableOutputSchemaBytes,
                projectionIds, pushdownFiltersBytes, rowLimit);
    }
}
