// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgispark;

import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * Per-catalog configuration, parsed from the options Spark hands
 * {@link VgiCatalog#initialize} — {@code spark.sql.catalog.&lt;name&gt;.*}
 * session/cluster config entries, with the {@code spark.sql.catalog.&lt;name&gt;.}
 * prefix and {@code catalog-impl} already stripped by Spark itself.
 *
 * <p>One Spark catalog is one VGI {@code ATTACH} — the same granularity
 * DuckDB and Trino use. Multiple VGI-backed Spark catalogs are just multiple
 * {@code spark.sql.catalog.<name>=farm.query.vgispark.VgiCatalog} entries with
 * different {@code location} options.
 *
 * @param location the worker to attach: a bare shell command (subprocess
 *        transport), {@code unix:///path/to.sock}, {@code tcp://host:port},
 *        or {@code http(s)://host:port/path} — an already-running HTTP
 *        server, unlike the other three schemes, which each spawn or connect
 *        to their own worker instance per pooled connection
 * @param catalogName the VGI-side catalog name to request from
 *        {@code catalog_attach} — one of the names {@code catalog_catalogs()}
 *        advertises (e.g. {@code "example"} for the reference fixture worker),
 *        NOT the Spark catalog name. Spark's own catalog name (the
 *        {@code spark.sql.catalog.<name>} key) is a separate, purely local
 *        alias the worker never sees — exactly the {@code alias}/{@code name}
 *        split in DuckDB's own {@code ATTACH 'name' AS alias (TYPE vgi, ...)}
 * @param connections how many independent worker connections to pool. VGI's
 *        RPC is lockstep per connection — one request in flight at a time —
 *        so on the driver (where this pool is used for discovery/bind/plan
 *        calls) this bounds how many of those can run concurrently. Executors
 *        open their own connections per task, outside this pool
 * @param targetSplitBytes requested split size passed to {@code table_function_plan}
 *        as {@code target_split_bytes}, or {@code null} to let the worker decide
 * @param minSplits parallelism floor passed as {@code min_splits}, or {@code null}
 * @param maxSplitsPerResponse pagination cap per {@code table_function_plan} call
 * @param connectionAcquireTimeoutMillis how long {@code VgiWorkerClient.borrow()}
 *        waits for a pooled connection before giving up. A generous but FINITE
 *        bound turns a connector or worker that's genuinely stuck into a clear,
 *        diagnosable failure instead of hanging the calling thread forever
 * @param maxPlanPages the pagination bound on {@code table_function_plan}:
 *        {@code Batch.planInputPartitions()} runs on the driver and must
 *        return a fixed array, so it follows {@code next_cursors} across
 *        calls to completion rather than streaming them — a worker that never
 *        stops cursoring (by accident as easily as on purpose) would
 *        otherwise make it follow forever. Stopping early and using only what
 *        was collected would turn that hang into a SILENT SUBSET — a
 *        correct-looking answer missing rows, with no error — which is worse,
 *        so this throws instead once hit, naming the cap in the message.
 *        Mirrors the C++ VGI extension's own {@code vgi_split_plan_max_pages}
 *        setting (default 1024, matched here)
 * @param httpBearerToken a static bearer token sent as {@code Authorization:
 *        Bearer <token>} on every request, for an {@code http(s)://} {@link
 *        #location} that requires one, or {@code null} for none. Ignored
 *        for every other transport scheme
 */
public record VgiCatalogConfig(
        String location,
        String catalogName,
        int connections,
        Long targetSplitBytes,
        Long minSplits,
        int maxSplitsPerResponse,
        long connectionAcquireTimeoutMillis,
        int maxPlanPages,
        String httpBearerToken) implements java.io.Serializable {

    /** Default connection-pool size when {@code connections} is unset. */
    public static final int DEFAULT_CONNECTIONS = 4;

    /** Default {@code max_splits_per_response} when {@code max-splits-per-response} is unset. */
    public static final int DEFAULT_MAX_SPLITS_PER_RESPONSE = 1000;

    /** Default connection-acquire wait, when {@code connection-acquire-timeout-millis} is unset. */
    public static final long DEFAULT_CONNECTION_ACQUIRE_TIMEOUT_MILLIS = 30_000L;

    /** Default {@code table_function_plan} pagination bound, when {@code max-plan-pages} is unset. */
    public static final int DEFAULT_MAX_PLAN_PAGES = 1024;

    /**
     * Parse the catalog options Spark provides to {@link VgiCatalog#initialize}.
     *
     * @param options the catalog's options, with the {@code spark.sql.catalog.<name>.}
     *        prefix already stripped by Spark
     * @return the parsed config
     * @throws IllegalArgumentException if {@code location} or {@code catalog-name} is missing
     */
    public static VgiCatalogConfig fromOptions(CaseInsensitiveStringMap options) {
        String location = options.get("location");
        if (location == null || location.isBlank()) {
            throw new IllegalArgumentException(
                    "spark.sql.catalog.<name>.location is required (a subprocess command, "
                            + "unix://path, tcp://host:port, or http(s)://host:port/path)");
        }
        String catalogName = options.get("catalog-name");
        if (catalogName == null || catalogName.isBlank()) {
            throw new IllegalArgumentException(
                    "spark.sql.catalog.<name>.catalog-name is required: the VGI-side catalog to "
                            + "attach (see catalog_catalogs() on the worker), not the Spark catalog name");
        }
        int connections = options.getInt("connections", DEFAULT_CONNECTIONS);
        Long targetSplitBytes = parseLongOrNull(options.get("target-split-size-bytes"));
        Long minSplits = parseLongOrNull(options.get("min-splits"));
        int maxSplitsPerResponse = options.getInt("max-splits-per-response", DEFAULT_MAX_SPLITS_PER_RESPONSE);
        long connectionAcquireTimeoutMillis = options.getLong(
                "connection-acquire-timeout-millis", DEFAULT_CONNECTION_ACQUIRE_TIMEOUT_MILLIS);
        int maxPlanPages = options.getInt("max-plan-pages", DEFAULT_MAX_PLAN_PAGES);
        String httpBearerToken = options.get("http-bearer-token");
        return new VgiCatalogConfig(location, catalogName, connections, targetSplitBytes, minSplits,
                maxSplitsPerResponse, connectionAcquireTimeoutMillis, maxPlanPages, httpBearerToken);
    }

    private static Long parseLongOrNull(String value) {
        return value == null ? null : Long.parseLong(value.trim());
    }
}
