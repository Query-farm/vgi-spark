// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgispark.scan;

import farm.query.vgi.protocol.BindRequest;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.protocol.PlanResponse;
import farm.query.vgi.protocol.ScanSplit;
import farm.query.vgi.protocol.TableFunctionPlanRequest;
import farm.query.vgirpc.marshal.RecordCodec;
import farm.query.vgispark.VgiCatalogConfig;
import farm.query.vgispark.VgiTable;
import farm.query.vgispark.client.VgiWorkerClient;
import org.apache.spark.sql.connector.catalog.Column;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * A bound, planned VGI table scan.
 *
 * <p>{@code toBatch()} returns {@code this} (batch is the only supported
 * mode — no streaming, see the plan's non-goals) and {@link
 * #planInputPartitions} does the real work: bind the scan function once,
 * then paginate {@code table_function_plan} to completion.
 *
 * <p>Ported from {@code vgi-trino}'s {@code VgiSplitManager}/{@code
 * VgiSplitSource} pair, collapsed into one class and one synchronous call —
 * the "honest caveat" from the design study
 * (<code>~/Development/vgi-spark.md</code>): {@link Batch#planInputPartitions()}
 * runs on the driver and must return a fixed array, so the cursor pagination
 * VGI's protocol supports for lazy/streaming enumeration is drained to
 * completion here rather than streamed the way Trino's {@code
 * ConnectorSplitSource.getNextBatch} allows. Bounded by {@link
 * VgiCatalogConfig#maxPlanPages()} — see that field's own javadoc for why
 * exceeding it throws rather than silently returning a partial plan.
 */
public final class VgiScan implements Scan, Batch {

    private final VgiWorkerClient client;
    private final VgiCatalogConfig config;
    private final VgiTable table;
    private final List<Integer> projectionIds;
    private final byte[] pushdownFiltersBytes;
    private final Long rowLimit;
    private final StructType readSchema;

    /**
     * @param client the pooled connection to this catalog's VGI worker
     * @param config this catalog's configuration
     * @param table the table being scanned
     * @param projectionIds the columns to project, as ordinals into the
     *        table's full Arrow schema, in ascending order, or {@code null}
     *        for all of them
     * @param pushdownFiltersBytes the encoded {@code InitRequest.pushdown_filters}/
     *        {@code TableFunctionPlanRequest.pushdown_filters} batch, or
     *        {@code null} if nothing was pushed
     * @param rowLimit a fetch-limit hint pushed to every split, or {@code null}
     */
    public VgiScan(VgiWorkerClient client, VgiCatalogConfig config, VgiTable table,
            List<Integer> projectionIds, byte[] pushdownFiltersBytes, Long rowLimit) {
        this.client = client;
        this.config = config;
        this.table = table;
        this.projectionIds = projectionIds;
        this.pushdownFiltersBytes = pushdownFiltersBytes;
        this.rowLimit = rowLimit;
        // Built from table.columns() + projectionIds directly, in ordinal
        // order — NOT from prunedSchema's own (Spark-chosen, not necessarily
        // ordinal) field order — because VgiPartitionReader emits columns in
        // exactly this same ordinal order (see its own use of projectionIds),
        // and the two must never disagree about which column is where.
        Column[] columns = table.columns();
        List<Integer> ordinals = projectionIds;
        if (ordinals == null) {
            ordinals = new ArrayList<>(columns.length);
            for (int i = 0; i < columns.length; i++) ordinals.add(i);
        }
        StructField[] fields = new StructField[ordinals.size()];
        for (int i = 0; i < ordinals.size(); i++) {
            Column c = columns[ordinals.get(i)];
            fields[i] = DataTypes.createStructField(c.name(), c.dataType(), c.nullable());
        }
        this.readSchema = DataTypes.createStructType(fields);
    }

    @Override
    public StructType readSchema() {
        return readSchema;
    }

    @Override
    public Batch toBatch() {
        return this;
    }

    @Override
    public ColumnarSupportMode columnarSupportMode() {
        return ColumnarSupportMode.SUPPORTED;
    }

    @Override
    public InputPartition[] planInputPartitions() {
        BindRequest bindRequest = new BindRequest(
                table.scanFunctionName(),
                table.scanFunctionArguments(),
                "TABLE",
                null,           // input_schema — producer-mode table function
                null,           // settings
                null,           // secrets
                null,           // attach_opaque_data — filled in per-connection below
                null,           // transaction_opaque_data
                false,          // resolved_secrets_provided
                table.atUnit(), table.atValue(),
                null, null,     // copy_from / copy_to
                // NOT table.schemaName(): catalog_table_scan_function_get resolves a table's
                // backing scan function, but doesn't say which schema that function itself is
                // registered in — it can differ from the table's own schema. null lets the
                // worker's dispatcher search every schema by name.
                null);

        // withConnection's own attach handle must be the SAME one baked into
        // the serialized bindCall below — table_function_plan and every
        // split's init() redeem that exact bytes blob, potentially on a
        // DIFFERENT pooled connection, so the handle can't be re-derived
        // afterward from whichever connection happens to be borrowed next.
        byte[][] attachHandleUsed = new byte[1][];
        byte[] bindOpaqueData = client.withConnection(a -> {
            attachHandleUsed[0] = a.handle();
            BindResponse bound = a.service().bind(withAttachHandle(bindRequest, a.handle()), null);
            return bound.opaque_data();
        });
        byte[] bindCall = RecordCodec.serializeToBytes(withAttachHandle(bindRequest, attachHandleUsed[0]));

        List<InputPartition> partitions = new ArrayList<>();
        byte[] cursor = null;
        int pagesFetched = 0;
        while (true) {
            pagesFetched++;
            if (pagesFetched > config.maxPlanPages()) {
                // Stopping early and returning what was already collected
                // would turn this into a SILENT SUBSET — a correct-looking
                // answer missing rows, with no error — which is worse than
                // failing outright. See VgiCatalogConfig.maxPlanPages's own
                // javadoc.
                throw new RuntimeException("table_function_plan exceeded the scan-planning page cap ("
                        + config.maxPlanPages() + " pages, catalog option max-plan-pages) — the worker "
                        + "either has an unusually large split enumeration or never stops cursoring; "
                        + "raise max-plan-pages if the former");
            }
            int cap = Math.max(1, config.maxSplitsPerResponse());
            TableFunctionPlanRequest request = new TableFunctionPlanRequest(
                    bindCall, bindOpaqueData,
                    projectionIds,
                    pushdownFiltersBytes,
                    null,           // join_keys
                    rowLimit,
                    config.targetSplitBytes(),
                    config.minSplits(),
                    (long) cap,
                    cursor,
                    null,           // refined_filters
                    true,           // filters_complete — no dynamic filtering in v1
                    null, null,     // start/end position
                    null, null, null, null,     // order-by hint
                    null, null);    // tablesample hint

            PlanResponse response = client.withConnection(a ->
                    a.service().table_function_plan(RecordCodec.serializeToBytes(request), null));

            boolean sawSentinel = false;
            for (byte[] blob : response.splits()) {
                ScanSplit split = RecordCodec.deserializeFromBytes(blob, ScanSplit.class);
                if (split.token().length == 0) {
                    // The not-split-capable sentinel: exactly one such split,
                    // standing for the whole scan. Stop here regardless of
                    // next_cursors.
                    partitions.add(new VgiInputPartition(bindCall, bindOpaqueData, new byte[0], 0L, new String[0]));
                    sawSentinel = true;
                    break;
                }
                long estimatedBytes = split.estimated_bytes() == null ? 0L : split.estimated_bytes();
                partitions.add(new VgiInputPartition(bindCall, bindOpaqueData, split.token(),
                        estimatedBytes, resolveAddresses(response, split)));
            }
            if (sawSentinel) break;

            List<byte[]> nextCursors = response.next_cursors();
            if (nextCursors == null || nextCursors.isEmpty()) {
                break;
            }
            // v1 follows only the first continuation cursor — see
            // PlanResponse.next_cursors's own javadoc on why more than one is
            // safe to ignore rather than fan out further here.
            cursor = nextCursors.get(0);
        }
        return partitions.toArray(new InputPartition[0]);
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new VgiPartitionReaderFactory(
                config, table.outputSchemaBytes(), projectionIds, pushdownFiltersBytes, rowLimit);
    }

    private static BindRequest withAttachHandle(BindRequest request, byte[] attachHandle) {
        return new BindRequest(
                request.function_name(), request.arguments(), request.function_type(),
                request.input_schema(), request.settings(), request.secrets(),
                attachHandle, request.transaction_opaque_data(), request.resolved_secrets_provided(),
                request.at_unit(), request.at_value(), request.copy_from(), request.copy_to(),
                request.schema_name());
    }

    private static String[] resolveAddresses(PlanResponse response, ScanSplit split) {
        List<Long> locationIds = split.location_ids();
        List<String> locations = response.locations();
        if (locationIds == null || locationIds.isEmpty() || locations == null || locations.isEmpty()) {
            return new String[0];
        }
        List<String> out = new ArrayList<>(locationIds.size());
        for (Long id : locationIds) {
            if (id != null && id >= 0 && id < locations.size()) {
                out.add(locations.get(id.intValue()));
            }
        }
        return out.toArray(new String[0]);
    }
}
