// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgispark.scan;

import farm.query.vgi.client.ScalarValue;
import farm.query.vgi.client.ScanFunctionArguments;
import farm.query.vgispark.branch.VgiNativeScanBranch;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.execution.datasources.v2.csv.CSVDataSourceV2;
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetDataSourceV2;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Supplier;

/**
 * Native scan-function delegation — {@link VgiNativeScanBranch}'s consumer.
 * The Java analog of {@code vgi-polars}'s {@code _native_scan.py}, which
 * found and fixed this exact gap first (see that module's own docstring):
 * {@code ScanFunctionResult.function_name} can name a reader the CALLING
 * engine should run itself, not a VGI-hosted function — the DuckDB C++
 * extension resolves this by checking its own function catalog before ever
 * treating it as an RPC target.
 *
 * <p>Deliberately uses only {@link TableProvider}'s three base methods
 * ({@link TableProvider#inferSchema}, {@link TableProvider#inferPartitioning},
 * {@link TableProvider#getTable(StructType, Transform[], Map)}) rather than
 * each format's own convenience method — this is Spark's real, public
 * connector SPI (not an internal shortcut), and the same three-call sequence
 * works uniformly for {@code read_parquet}/{@code read_csv} (Spark's own
 * built-in {@link ParquetDataSourceV2}/{@link CSVDataSourceV2}, confirmed via
 * {@code javap} to be real, public, no-arg-constructible {@code
 * TableProvider}s — the exact classes {@code spark.read.format("parquet"/
 * "csv")} builds internally) and {@code iceberg_scan} (Iceberg's own {@code
 * org.apache.iceberg.spark.source.IcebergSource}, which implements {@code
 * SupportsCatalogOptions extends TableProvider} rather than Spark's
 * file-source convenience trait, so it has no matching 1-arg shortcut).
 * {@code "path"} is the one Spark-standard option key every one of these
 * three readers resolves a bare path/glob/table-location from (confirmed via
 * {@code javap -c} on {@code FileDataSourceV2.getPaths} — it reads {@code
 * options.get("path")} — and via Iceberg's own documented {@code
 * spark.read.format("iceberg").load(path)} usage, which resolves the same
 * way through {@code IcebergSource#extractIdentifier}).
 *
 * <p><strong>{@code read_parquet} is confirmed live against a real worker
 * ({@code vgi-overture-maps-typescript},
 * {@code https://vgi-overture.rusty-bb6.workers.dev} — see {@link
 * VgiNativeScanBranch}'s own javadoc); {@code read_csv} and {@code
 * iceberg_scan} are not.</strong> Both are built conservatively from
 * documented Spark/Iceberg signatures, matching {@code vgi-polars}'s own
 * choice not to guess a named-argument mapping without a live worker to
 * verify against: {@code read_csv} gets an empty named-argument allowlist
 * (any named argument raises rather than being silently dropped), and {@code
 * iceberg_scan} maps only {@code snapshot_from_id} to Iceberg's own {@code
 * org.apache.iceberg.spark.SparkReadOptions#SNAPSHOT_ID} ({@code
 * "snapshot-id"}, confirmed via {@code javap -v}'s constant pool against the
 * real {@code iceberg-spark-runtime-4.0_2.13:1.11.0} jar) — the one safe,
 * unambiguous correspondence between DuckDB's {@code iceberg_scan} and
 * Iceberg's own Spark reader; {@code allow_moved_paths}/{@code
 * snapshot_from_timestamp}/{@code version} have no confirmed Iceberg-Spark
 * equivalent and are refused, not guessed.
 *
 * <p><strong>Iceberg is a {@code compileOnly} dependency</strong> ({@code
 * connector/build.gradle.kts}) — an operator's Spark cluster must add {@code
 * iceberg-spark-runtime} itself to use {@code iceberg_scan} delegation, the
 * same tier Spark itself is provided at. Every reference to an Iceberg class
 * in this file is deliberately confined to code paths that only execute when
 * an actual {@code iceberg_scan} branch is being resolved (never inside a
 * static field initializer) — a lambda body (not a bare method reference)
 * for the provider constructor specifically, since a method reference like
 * {@code IcebergSource::new} would force the JVM to resolve {@code
 * IcebergSource} as part of building the {@link #TARGETS} map itself, which
 * would poison this whole class (and every OTHER native-scan target with
 * it — including {@code read_parquet}, which needs no Iceberg jar at all) the
 * first time ANY native-delegating table is resolved on a cluster that
 * doesn't have the Iceberg jar. A missing jar surfaces instead as a clear,
 * caught {@link NoClassDefFoundError} naming the table and what to add.
 */
public final class VgiNativeScanResolver {

    private VgiNativeScanResolver() {}

    private record NativeScanTarget(Supplier<TableProvider> providerFactory, Set<String> allowedNamedArgs) {}

    /**
     * {@code function_name} (lowercase) -> how to build+call its native
     * reader. Case-insensitive lookup matches this connector's own
     * established lesson (CLAUDE.md's wire-protocol landmines section) that
     * VGI wire enum-ish strings need case-insensitive comparison.
     */
    private static final Map<String, NativeScanTarget> TARGETS = Map.of(
            "read_parquet", new NativeScanTarget(() -> new ParquetDataSourceV2(), Set.of()),
            "read_csv", new NativeScanTarget(() -> new CSVDataSourceV2(), Set.of()),
            "iceberg_scan", new NativeScanTarget(
                    () -> new org.apache.iceberg.spark.source.IcebergSource(), Set.of("snapshot_from_id")));

    /** @return whether {@code functionName} is a known native-delegation target (case-insensitive). */
    public static boolean isNativeScanFunction(String functionName) {
        return functionName != null && TARGETS.containsKey(functionName.toLowerCase(Locale.ROOT));
    }

    /**
     * Build the native Spark {@link Table} a {@link VgiNativeScanBranch}
     * delegates to.
     *
     * @param tableDisplayName {@code schema.table}, for error messages only
     * @param branch the native-delegating branch to resolve
     * @param requiredFilters {@code TableInfo.required_filters} for the
     *        table this branch belongs to — the normal enforcement ({@code
     *        VgiScanBuilder.checkRequiredFilters}) never runs for a
     *        natively-delegated table (it's bypassed entirely, along with
     *        {@code VgiTable}/{@code VgiScanBuilder}/{@code VgiScan}), so
     *        this is the one place left to enforce it
     * @param acknowledgeRequiredFilters mirrors {@code
     *        spark.sql.catalog.&lt;name&gt;.acknowledge-native-scan-required-filters}
     *        — required because Spark's native Parquet/CSV/Iceberg {@code
     *        Table} has no concept of VGI's {@code required_filters} cost-safety
     *        gate at all; refusing by default beats silently dropping it
     * @return Spark's own real {@link Table} for this delegation target
     * @throws IllegalStateException if {@code functionName} isn't a known
     *         target, the arguments don't carry a usable path, an unmapped
     *         named argument was passed, {@code requiredFilters} is
     *         non-empty and unacknowledged, or the target's own runtime
     *         dependency (Iceberg) isn't on this Spark cluster's classpath
     */
    public static Table resolve(String tableDisplayName, VgiNativeScanBranch branch,
            List<List<String>> requiredFilters, boolean acknowledgeRequiredFilters) {
        if (requiredFilters != null && !requiredFilters.isEmpty() && !acknowledgeRequiredFilters) {
            throw new IllegalStateException(tableDisplayName + ": natively delegates to '"
                    + branch.functionName() + "' and declares required_filters " + requiredFilters
                    + " that vgi-spark cannot enforce for a native scan (Spark's own Parquet/CSV/Iceberg "
                    + "Table has no hook into VGI's required_filters concept at all). Set "
                    + "spark.sql.catalog.<name>.acknowledge-native-scan-required-filters=true once you've "
                    + "applied the equivalent filter(s) yourself, or you WILL trigger a full, possibly "
                    + "enormous, unfiltered read.");
        }

        String key = branch.functionName() == null ? "" : branch.functionName().toLowerCase(Locale.ROOT);
        NativeScanTarget target = TARGETS.get(key);
        if (target == null) {
            // resolveBranches only builds a VgiNativeScanBranch for a name
            // isNativeScanFunction already confirmed — reaching here would be
            // this class's own bug, not a worker/user error.
            throw new IllegalStateException(
                    tableDisplayName + ": unknown native scan function '" + branch.functionName() + "'");
        }

        ScanFunctionArguments.Decoded args = branch.arguments();
        if (args.positional().isEmpty()) {
            throw new IllegalStateException(tableDisplayName + ": worker delegated to " + branch.functionName()
                    + " with no positional arguments (expected the file/glob/table path as argument 0)");
        }
        Object pathValue = args.positional().get(0).value();
        if (!(pathValue instanceof String path)) {
            throw new IllegalStateException(tableDisplayName + ": " + branch.functionName() + "'s first argument "
                    + "is a " + (pathValue == null ? "null" : pathValue.getClass().getSimpleName())
                    + ", expected a path/glob/table-location string");
        }

        Set<String> unknown = new TreeSet<>(args.named().keySet());
        unknown.removeAll(target.allowedNamedArgs());
        if (!unknown.isEmpty()) {
            throw new IllegalStateException(tableDisplayName + ": worker's " + branch.functionName()
                    + " delegation passed named argument(s) " + unknown + " vgi-spark doesn't know how to "
                    + "translate (known: " + target.allowedNamedArgs() + ")");
        }

        Map<String, String> options = new LinkedHashMap<>();
        options.put("path", path);
        for (Map.Entry<String, ScalarValue> e : args.named().entrySet()) {
            if (e.getValue() == null || e.getValue().value() == null) continue;
            String[] translated = translateNamedArg(key, e.getKey(), String.valueOf(e.getValue().value()));
            options.put(translated[0], translated[1]);
        }

        TableProvider provider;
        try {
            provider = target.providerFactory().get();
        } catch (NoClassDefFoundError | ExceptionInInitializerError missingDependency) {
            throw new IllegalStateException(tableDisplayName + ": delegates to '" + branch.functionName()
                    + "', which needs " + ("iceberg_scan".equals(key)
                            ? "iceberg-spark-runtime (see README's \"Optional dependencies\" section)"
                            : "a Spark file-source class")
                    + " on this Spark cluster's own classpath -- not found", missingDependency);
        }
        CaseInsensitiveStringMap csMap = new CaseInsensitiveStringMap(options);
        StructType schema = provider.inferSchema(csMap);
        Transform[] partitioning = provider.inferPartitioning(csMap);
        return provider.getTable(schema, partitioning, csMap);
    }

    /**
     * @return {@code {translatedOptionKey, value}} for one named argument —
     *         only {@code iceberg_scan}'s {@code snapshot_from_id} has a
     *         confirmed translation today (see this class's own javadoc);
     *         every other target's {@code allowedNamedArgs} is empty, so this
     *         is never reached for them. Deliberately references {@link
     *         org.apache.iceberg.spark.SparkReadOptions} only inside this
     *         method body (never eagerly) — see this class's own javadoc on
     *         why that matters for operators without the Iceberg jar
     */
    private static String[] translateNamedArg(String functionKey, String wireName, String value) {
        if ("iceberg_scan".equals(functionKey) && "snapshot_from_id".equals(wireName)) {
            return new String[] {org.apache.iceberg.spark.SparkReadOptions.SNAPSHOT_ID, value};
        }
        return new String[] {wireName, value};
    }
}
