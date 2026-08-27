// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgispark.scan;

import farm.query.vgi.client.ColumnStatisticsDecoder;
import farm.query.vgi.protocol.BindRequest;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.protocol.PlanResponse;
import farm.query.vgi.protocol.ScanSplit;
import farm.query.vgi.protocol.TableFunctionPlanRequest;
import farm.query.vgirpc.marshal.RecordCodec;
import farm.query.vgispark.VgiCatalogConfig;
import farm.query.vgispark.VgiTable;
import farm.query.vgispark.branch.VgiBranch;
import farm.query.vgispark.branch.VgiFormatScanBranch;
import farm.query.vgispark.branch.VgiScanBranch;
import farm.query.vgispark.client.VgiWorkerClient;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.spark.sql.connector.catalog.Column;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.connector.read.SupportsReportStatistics;
import org.apache.spark.sql.connector.read.colstats.ColumnStatistics;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

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
public final class VgiScan implements Scan, Batch, SupportsReportStatistics {

    private final VgiWorkerClient client;
    private final VgiCatalogConfig config;
    private final VgiTable table;
    private final List<Integer> projectionIds;
    private final byte[] pushdownFiltersBytes;
    private final Long rowLimit;
    private final StructType readSchema;

    // Lazily computed and cached: estimateStatistics() is an optimizer hook
    // Spark's driver-side planning can call more than once for the same Scan
    // (CBO, broadcast-join sizing, ...) — recomputing would mean a fresh
    // catalog_table_column_statistics_get round trip per call for no benefit,
    // since nothing about this immutable Scan's answer can change between calls.
    private volatile Statistics cachedStatistics;

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

    /**
     * Row-count (and, best-effort, per-column) statistics for the optimizer —
     * roadmap tier 2 item 8. {@code TableInfo.cardinality_estimate} is already
     * threaded through onto {@link VgiTable} at table-load time (no extra RPC:
     * see {@link VgiTable#cardinalityEstimate()}), so {@link #numRows} here is
     * free. Per-column stats are NOT free — they need their own {@code
     * catalog_table_column_statistics_get} round trip, fetched lazily and
     * cached in {@link #cachedStatistics} the first time the optimizer asks.
     *
     * <p>{@code sizeInBytes} is deliberately {@link OptionalLong#empty()}
     * rather than a guess: this connector has no column-width model to
     * multiply a row count by (unlike {@code FileScan}'s compression-factor
     * heuristic over an actual on-disk file size) and {@code TableInfo}
     * doesn't offer a byte estimate directly — fabricating one would be a
     * silent guess the "fail closed and loudly" philosophy elsewhere in this
     * connector argues against, not a genuinely informative default.
     *
     * <p>Full "prune the scan away entirely at plan time" parity with what
     * {@code table/column_statistics.test}'s {@code EXPLAIN} assertions check
     * isn't attempted here (see the roadmap item's own note: Spark's
     * constant-folding/pruning differs from DuckDB's) — this just reports
     * honest numbers for Spark's own optimizer (join reordering, broadcast
     * thresholds, {@code CBO} filter selectivity) to use however it already
     * knows how to.
     */
    @Override
    public Statistics estimateStatistics() {
        Statistics cached = cachedStatistics;
        if (cached != null) return cached;
        Statistics computed = new VgiStatistics(
                OptionalLong.empty(),
                table.cardinalityEstimate() == null ? OptionalLong.empty() : OptionalLong.of(table.cardinalityEstimate()),
                fetchColumnStatistics());
        cachedStatistics = computed;
        return computed;
    }

    /**
     * Best-effort per-column stats via {@code catalog_table_column_statistics_get}
     * (the same RPC {@code vgi-trino}'s {@code getTableStatistics} uses,
     * unused by this connector before this item) — decoded with vgi-java's own
     * {@link ColumnStatisticsDecoder}, the exact inverse of the worker-side
     * serializer, rather than hand-rolling a second decoder here.
     *
     * <p>This is an OPTIMIZER HINT, not a correctness-bearing read — a worker
     * that doesn't implement the RPC already answers with empty bytes (see
     * {@code VgiService.catalog_table_column_statistics_get}'s own default),
     * decoded as "no statistics" rather than an error, and a genuine
     * connection failure here degrades the SAME way rather than aborting
     * query planning entirely: unlike a wrong scan result, mis-estimated
     * cardinality/selectivity is never a wrong ANSWER, only a worse plan, so
     * swallowing the failure and reporting no column stats is the fail-closed
     * choice here (query planning proceeding on an honest "unknown" belief),
     * not a fail-open one.
     */
    private Map<NamedReference, ColumnStatistics> fetchColumnStatistics() {
        byte[] raw;
        try {
            raw = client.withConnection(a -> a.service().catalog_table_column_statistics_get(
                    a.handle(), table.schemaName(), table.tableName(), null, null));
        } catch (RuntimeException e) {
            return Map.of();
        }
        List<farm.query.vgi.catalog.ColumnStatistics> decoded;
        try {
            decoded = ColumnStatisticsDecoder.decode(raw);
        } catch (IllegalStateException e) {
            return Map.of();
        }
        if (decoded.isEmpty()) return Map.of();
        Map<NamedReference, ColumnStatistics> out = new LinkedHashMap<>();
        for (farm.query.vgi.catalog.ColumnStatistics s : decoded) {
            out.put(Expressions.column(s.columnName()), toSparkColumnStatistics(s));
        }
        return Map.copyOf(out);
    }

    private ColumnStatistics toSparkColumnStatistics(farm.query.vgi.catalog.ColumnStatistics s) {
        OptionalLong distinctCount = s.distinctCount() == null ? OptionalLong.empty() : OptionalLong.of(s.distinctCount());
        OptionalLong maxLen = s.maxStringLength() == null ? OptionalLong.empty() : OptionalLong.of(s.maxStringLength());
        return new VgiColumnStatistics(distinctCount, convertBound(s.arrowType(), s.min()),
                convertBound(s.arrowType(), s.max()), deriveNullCount(s), maxLen);
    }

    /**
     * {@code has_null}/{@code has_not_null} are booleans, not a count — an
     * exact {@code nullCount} is only derivable at the two boundaries: no
     * nulls at all (0), or every row is null (the table's own row-count
     * estimate, when known). Anywhere in between is a genuine "some rows are
     * null, unknown how many" — reported as {@link OptionalLong#empty()}
     * rather than guessed.
     */
    private OptionalLong deriveNullCount(farm.query.vgi.catalog.ColumnStatistics s) {
        if (!s.hasNull()) return OptionalLong.of(0L);
        if (!s.hasNotNull()) {
            Long cardinality = table.cardinalityEstimate();
            return cardinality == null ? OptionalLong.empty() : OptionalLong.of(cardinality);
        }
        return OptionalLong.empty();
    }

    /**
     * Converts a decoded VGI bound to the JVM representation
     * {@code Statistics.scala}'s own scaladoc requires: CATALYST'S INTERNAL
     * type for the column's Catalyst data type, not necessarily its external
     * Java type — they coincide for the numeric types VGI's wire format
     * carries (INT64 -> {@link Long}, matching {@code LongType}'s internal
     * representation exactly; FLOAT64 -> {@link Double}, same story for
     * {@code DoubleType}), but NOT for a UTF8 bound: Catalyst's internal
     * representation for {@code StringType} is {@link UTF8String}, not
     * {@link String}, so a plain decoded {@link String} is wrapped here
     * rather than handed to the optimizer as-is (which risks a
     * {@code ClassCastException} deep inside Catalyst's own filter-estimation
     * code — confirmed by reading {@code DataSourceV2Relation}'s
     * {@code transformV2Stats}, which passes this value through unconverted).
     *
     * <p>Anything else (geometry's WKB {@code binary} bounds, or an
     * unrecognized union member) is omitted rather than guessed — this
     * connector's {@code VgiTypeMapping} has no established Spark scalar type
     * for a spatial bounding box, and a wrong guess here is worse than
     * reporting "unknown".
     */
    private static Optional<Object> convertBound(ArrowType arrowType, Object value) {
        if (value == null) return Optional.empty();
        if (arrowType instanceof ArrowType.Utf8 || arrowType instanceof ArrowType.LargeUtf8) {
            return Optional.of(UTF8String.fromString((String) value));
        }
        if (arrowType instanceof ArrowType.Int || arrowType instanceof ArrowType.FloatingPoint) {
            return Optional.of(value);
        }
        return Optional.empty();
    }

    private record VgiStatistics(OptionalLong sizeInBytes, OptionalLong numRows,
            Map<NamedReference, ColumnStatistics> columnStats) implements Statistics {}

    private record VgiColumnStatistics(OptionalLong distinctCount, Optional<Object> min, Optional<Object> max,
            OptionalLong nullCount, OptionalLong maxLen) implements ColumnStatistics {}

    @Override
    public InputPartition[] planInputPartitions() {
        // Multi-branch: each branch is bound and planned independently, and
        // every branch's partitions simply concatenate into one array — this
        // falls out for free because a VgiInputPartition already carries its
        // OWN bindCall/bindOpaqueData (never a scan-wide shared one), so nothing
        // downstream (VgiPartitionReaderFactory/VgiPartitionReader) needs to
        // know which branch a partition came from.
        List<InputPartition> partitions = new ArrayList<>();
        for (VgiBranch branch : table.branches()) {
            switch (branch) {
                case VgiScanBranch fn -> partitions.addAll(planBranchPartitions(fn));
                case VgiFormatScanBranch fmt -> partitions.addAll(planFormatBranchPartitions(fmt));
            }
        }
        return partitions.toArray(new InputPartition[0]);
    }

    /**
     * One {@link VgiFormatInputPartition} per location — no VGI RPC at all;
     * {@code branch.locations()} is already resolved (from {@code
     * catalog_table_scan_branches_get}'s response), so this is purely local
     * bookkeeping, unlike {@link #planBranchPartitions} which must bind and
     * plan against the worker.
     */
    private List<InputPartition> planFormatBranchPartitions(VgiFormatScanBranch branch) {
        List<InputPartition> partitions = new ArrayList<>(branch.locations().size());
        for (String location : branch.locations()) {
            partitions.add(new VgiFormatInputPartition(branch.formatName(), location, branch.formatOptions()));
        }
        return partitions;
    }

    private List<InputPartition> planBranchPartitions(VgiScanBranch branch) {
        BindRequest bindRequest = new BindRequest(
                branch.functionName(),
                branch.scanFunctionArguments(),
                "TABLE",
                null,           // input_schema — producer-mode table function
                null,           // settings
                null,           // secrets
                null,           // attach_opaque_data — filled in per-connection below
                null,           // transaction_opaque_data
                false,          // resolved_secrets_provided
                table.atUnit(), table.atValue(),
                null, null,     // copy_from / copy_to
                // NOT table.schemaName(): the scan function's own registered schema can
                // differ from the table's schema (declarative tables — e.g. the
                // reference fixture's data.numbers table scans via main.sequence, a
                // DIFFERENT schema). Resolved instead via VgiWorkerClient
                // .scanFunctionSchema, a one-time catalog-wide sweep keyed by
                // function name — VGI protocol 1.1.0 requires a bind to name its
                // owning schema (a strict worker refuses an unqualified bind outright
                // rather than searching every schema by name, the assumption this
                // code used to make); null falls through only for a scan function the
                // worker deliberately hid from its own catalog listing, which the
                // wire dispatch rules accept unqualified.
                client.scanFunctionSchema(branch.functionName()));

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
        return partitions;
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
