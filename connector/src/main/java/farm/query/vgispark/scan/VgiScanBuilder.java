// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgispark.scan;

import farm.query.vgi.client.EncodedPushdownFilters;
import farm.query.vgispark.VgiCatalogConfig;
import farm.query.vgispark.VgiTable;
import farm.query.vgispark.client.VgiWorkerClient;
import farm.query.vgispark.filter.VgiFilterTranslator;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownLimit;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.connector.read.SupportsPushDownV2Filters;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Builds one {@link VgiScan} for a table read, with projection, filter, and
 * limit pushdown.
 *
 * <p>Filter and limit pushdown are both deliberately non-authoritative —
 * {@link #pushPredicates} always returns every input predicate (Spark
 * re-checks all of them regardless of what got translated) and {@link
 * #pushLimit} always returns {@code false} (each split's {@code row_limit}
 * caps that split alone; the union across splits can still exceed the
 * requested limit, so Spark must keep enforcing its own top-level {@code
 * Limit} — see {@code VgiCatalogConfig}/{@code InitRequest.row_limit}'s own
 * javadoc on "over-production is legal"). Only column pruning is exact: a
 * column this builder doesn't request is genuinely never read.
 *
 * <p><strong>Filter translation is deferred to {@link #build}, not done in
 * {@link #pushPredicates}.</strong> {@code column_index} in the encoded wire
 * form is relative to the FINAL projected column list (see {@link
 * VgiFilterTranslator}'s own javadoc), but Spark does not guarantee {@link
 * #pruneColumns} runs before {@link #pushPredicates} — confirmed empirically
 * here: for a query like {@code SELECT count(*) FROM t WHERE bbox.xmin >= 0},
 * Spark calls {@code pushPredicates} while the projection is still
 * unrestricted (every column), THEN calls {@code pruneColumns} to narrow it
 * to just {@code bbox}. Translating at push-time baked in a {@code
 * column_index} for the WRONG (wider) projection, silently corrupting the
 * scan (the worker read past the end of its own narrower output batch and
 * threw). Recomputing in {@code build()} — after every pushdown callback has
 * definitely run — uses the projection that's actually sent, so the two can
 * never disagree.
 */
public final class VgiScanBuilder implements ScanBuilder,
        SupportsPushDownRequiredColumns, SupportsPushDownV2Filters, SupportsPushDownLimit {

    private final VgiWorkerClient client;
    private final VgiCatalogConfig config;
    private final VgiTable table;

    private StructType prunedSchema;
    private Predicate[] rawPredicates = new Predicate[0];
    private Predicate[] pushedPredicates = new Predicate[0];
    private long limit = -1;

    /**
     * @param client the pooled connection to this catalog's VGI worker
     * @param config this catalog's configuration
     * @param table the table being scanned
     */
    public VgiScanBuilder(VgiWorkerClient client, VgiCatalogConfig config, VgiTable table) {
        this.client = client;
        this.config = config;
        this.table = table;
    }

    @Override
    public void pruneColumns(StructType requiredSchema) {
        this.prunedSchema = requiredSchema;
    }

    @Override
    public Predicate[] pushPredicates(Predicate[] predicates) {
        this.rawPredicates = predicates;
        // Informational only here (see class javadoc): whatever the CURRENT
        // (possibly not-yet-final) projection translates to, good enough for
        // SupportsPushDownV2Filters#pushedPredicates/EXPLAIN output. The
        // authoritative translation — the one whose column_index actually
        // reaches the worker — happens in build(), against the final
        // projection.
        this.pushedPredicates = translate().pushedPredicates();
        // The full input array, unchanged: nothing here is authoritative, so
        // Spark must still evaluate every one of these against every row —
        // see this class's own javadoc.
        return predicates;
    }

    @Override
    public Predicate[] pushedPredicates() {
        return pushedPredicates;
    }

    @Override
    public boolean pushLimit(int limit) {
        this.limit = limit;
        return false; // informational only — see this class's own javadoc
    }

    @Override
    public Scan build() {
        // Recompute against the FINAL projection — see class javadoc on why
        // this can't reuse whatever pushPredicates() computed earlier.
        VgiFilterTranslator.Result filterResult = translate();
        this.pushedPredicates = filterResult.pushedPredicates();
        checkRequiredFilters(filterResult);
        List<Integer> projectionIds = projectionIds();
        EncodedPushdownFilters encoded = filterResult.encoded();
        byte[] pushdownFiltersBytes = encoded == null ? null : encoded.pushdownFilters();
        Long rowLimit = limit < 0 ? null : limit;
        return new VgiScan(client, config, table, projectionIds, pushdownFiltersBytes, rowLimit);
    }

    /** Translate {@link #rawPredicates} against the CURRENT projection (see class javadoc for why this is re-run). */
    private VgiFilterTranslator.Result translate() {
        Schema arrow = table.outputSchema();
        List<Field> fields = arrow == null ? List.of() : arrow.getFields();
        return VgiFilterTranslator.translate(rawPredicates, fields, projectedWireNames());
    }

    /**
     * The wire (base-schema) names of the columns this scan actually
     * projects, in schema order — what {@code table_function_plan}'s {@code
     * projection_ids} names and what {@link VgiFilterTranslator} roots a
     * filter's column index against. {@code null} {@link #prunedSchema}
     * (pruneColumns was never called — e.g. a {@code SELECT *}) means every
     * column.
     */
    private List<String> projectedWireNames() {
        Schema arrow = table.outputSchema();
        List<Field> fields = arrow == null ? List.of() : arrow.getFields();
        if (prunedSchema == null) {
            List<String> all = new ArrayList<>(fields.size());
            for (Field f : fields) all.add(f.getName());
            return all;
        }
        List<Integer> ordinals = projectionIds();
        List<String> out = new ArrayList<>(ordinals.size());
        for (int ordinal : ordinals) out.add(fields.get(ordinal).getName());
        return out;
    }

    /** The pruned columns' ordinals in the table's full Arrow schema, in schema order, or {@code null} for all. */
    private List<Integer> projectionIds() {
        if (prunedSchema == null) return null;
        Map<String, Integer> ordinalsByDisplayName = table.ordinalsByDisplayName();
        List<Integer> out = new ArrayList<>(prunedSchema.length());
        for (StructField field : prunedSchema.fields()) {
            Integer ordinal = ordinalsByDisplayName.get(field.name());
            if (ordinal != null) out.add(ordinal);
        }
        // Ordered by schema position, not by requiredSchema's (arbitrary)
        // order — table_function_plan/init's projection_ids IS the order the
        // worker emits batch columns in, and VgiPartitionReader looks columns
        // up by wire name regardless, so schema order is simplest and correct.
        out.sort(null);
        // Empty means "every column was pruned away" (e.g. SELECT count(*)) —
        // sending that through as projection_ids=[] was tried against vgi-trino's
        // own reference fixture worker and is wrong: a projection_pushdown=true
        // worker returned ZERO ROWS instead of the correct row count, not
        // merely zero-width ones. Treat empty as "no restriction" (null)
        // instead — correct, if not maximally I/O-efficient, for that case.
        return out.isEmpty() ? null : out;
    }

    /**
     * Fail closed if {@code TableInfo.required_filters} names a group of
     * columns none of which actually got a filter pushed. Checked against
     * what {@link VgiFilterTranslator} managed to translate and send, not
     * merely what appears in the query's WHERE clause — an untranslatable
     * predicate (crosses columns, unsupported operator) gives the worker
     * nothing to prune on, which is exactly the situation {@code
     * required_filters} exists to refuse rather than let through as an
     * unbounded scan.
     */
    private void checkRequiredFilters(VgiFilterTranslator.Result filterResult) {
        List<List<String>> requiredGroups = table.requiredFilters();
        if (requiredGroups == null || requiredGroups.isEmpty()) return;
        java.util.Set<String> covered = filterResult.coveredColumns();
        for (List<String> group : requiredGroups) {
            boolean satisfied = group.stream().anyMatch(covered::contains);
            if (!satisfied) {
                throw new IllegalStateException("table '" + table.schemaName() + "." + table.tableName()
                        + "' requires a filter on one of " + group + " — this worker refuses to scan without "
                        + "it (TableInfo.required_filters). Add a WHERE clause on one of those columns.");
            }
        }
    }
}
