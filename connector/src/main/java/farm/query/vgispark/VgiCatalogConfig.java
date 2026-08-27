// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgispark;

import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.LinkedHashMap;
import java.util.Map;

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
 * @param launcherEnabled whether a BARE-COMMAND {@link #location} (no
 *        recognized scheme prefix) is spawned via the shared {@code launch:}
 *        launcher instead of a fresh, unshared subprocess per connection —
 *        {@code true} by default, a deliberate departure from every other
 *        VGI client's own default (DuckDB/vgi-trino require an EXPLICIT
 *        {@code launch:} prefix to opt in). Only takes effect when the
 *        runtime actually supports it, though: {@code launch:} needs JDK
 *        22+ ({@code flock(2)}/{@code geteuid()} via the Foreign Function
 *        and Memory API — see {@code
 *        farm.query.vgirpc.launcher.PosixLauncherSupport}'s own javadoc),
 *        so on an older JVM a bare command silently falls back to the old
 *        per-connection-spawn behavior regardless of this flag — a graceful
 *        degrade, not a hard failure, since nobody asked for launcher
 *        semantics BY NAME in this case (an EXPLICIT {@code launch:}
 *        location, unlike this default path, still surfaces a clear error
 *        on an unsupported runtime — see {@code VgiWorkerClient
 *        .openTransport}'s own comment). Spark's process topology is why:
 *        unlike a single DuckDB process, a VGI-backed Spark catalog opens
 *        {@link #connections} pooled connections on the DRIVER *and* one
 *        more unpooled connection per TASK on every EXECUTOR — each an
 *        entirely separate JVM process. A bare command would otherwise spawn
 *        a fresh worker process for every one of those, which for anything
 *        with real startup cost (a JVM worker, a large Python import) is the
 *        "many minutes" case the launcher protocol exists to avoid — see
 *        {@code docs/launcher-protocol.md} (the {@code vgi} repo) and this
 *        connector's own roadmap. The worker argv fed to the launcher is
 *        {@code ["/bin/sh", "-c", location]} — byte-for-byte the same argv a
 *        bare command already ran through via {@link
 *        org.apache.spark.sql.SparkSession}-independent subprocess spawning,
 *        so shell semantics (env expansion, globbing, pipelines) are fully
 *        preserved; only the SHARING behavior changes. Set to {@code false}
 *        to restore the old per-connection-spawn default (e.g. for a worker
 *        with per-connection-mutable local state that must never be shared).
 *        Has no effect on an EXPLICIT {@code launch:}/{@code unix://}/
 *        {@code tcp://}/{@code http(s)://} {@link #location}, each of which
 *        already has unambiguous, scheme-determined behavior.
 * @param launcherIdleTimeoutSeconds forwarded as the launched worker's
 *        {@code --idle-timeout} — self-shutdown after this many seconds with
 *        no connected clients, {@code 0} for no timeout — or {@code null} to
 *        use {@code LaunchConfig}'s own default (300s). Mirrors the C++ VGI
 *        extension's {@code launcher_idle_timeout} ATTACH option. Only
 *        meaningful when {@link #location} actually resolves to launcher
 *        semantics (see {@link #launcherEnabled}) — {@link #fromOptions}
 *        refuses it otherwise, matching the C++ extension's own validation
 * @param launcherStateDir override for the launcher's per-user state
 *        directory (lockfiles/sockets/{@code .meta} files), or {@code null}
 *        for the OS-derived default. Mirrors {@code launcher_state_dir}.
 *        Same "only meaningful for launcher semantics" restriction as
 *        {@link #launcherIdleTimeoutSeconds}
 * @param dataVersionSpec a requested data-version constraint (e.g. a semver
 *        range) sent as {@code CatalogAttachRequest.data_version_spec}, or
 *        {@code null} to request none (the worker picks its own default)
 * @param implementationVersion a requested worker implementation version
 *        sent as {@code CatalogAttachRequest.implementation_version}, or
 *        {@code null} to request none
 * @param attachOptions custom, worker-declared ATTACH-time options (VGI's
 *        {@code AttachOptionSpec} mechanism — see that class's own javadoc),
 *        sent as {@code CatalogAttachRequest.options}. Every value travels
 *        as a plain UTF-8 string: Spark's catalog config is an untyped flat
 *        string map (unlike DuckDB's typed {@code ATTACH (opt 42, ...)}
 *        clause, whose SQL literal syntax itself carries the type), so
 *        there is no client-side signal to encode a numeric/boolean/complex
 *        option as anything more specific — the worker's own {@code
 *        catalog_attach} is expected to validate/coerce, the same "fail
 *        closed, loudly" contract this connector already relies on
 *        elsewhere rather than guessing a type from a string's shape
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
        String httpBearerToken,
        boolean launcherEnabled,
        Double launcherIdleTimeoutSeconds,
        String launcherStateDir,
        String dataVersionSpec,
        String implementationVersion,
        Map<String, String> attachOptions) implements java.io.Serializable {

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

        boolean launcherEnabled = options.getBoolean("launcher-enabled", true);
        Double launcherIdleTimeoutSeconds = parseDoubleOrNull(options.get("launcher-idle-timeout-seconds"));
        if (launcherIdleTimeoutSeconds != null && launcherIdleTimeoutSeconds < 0) {
            throw new IllegalArgumentException(
                    "spark.sql.catalog.<name>.launcher-idle-timeout-seconds must be >= 0, got "
                            + launcherIdleTimeoutSeconds);
        }
        String launcherStateDir = options.get("launcher-state-dir");
        if (launcherStateDir != null && launcherStateDir.isBlank()) {
            throw new IllegalArgumentException(
                    "spark.sql.catalog.<name>.launcher-state-dir, if set, must not be empty");
        }
        if ((launcherIdleTimeoutSeconds != null || launcherStateDir != null)
                && !isLaunchEligible(location, launcherEnabled)) {
            throw new IllegalArgumentException(
                    "spark.sql.catalog.<name>.launcher-idle-timeout-seconds / launcher-state-dir are only "
                            + "valid when the location resolves to launch: semantics (an explicit launch: "
                            + "location, or a bare-command location with launcher-enabled left at its "
                            + "default of true) — this location is '" + location + "' with launcher-enabled="
                            + launcherEnabled);
        }

        String dataVersionSpec = options.get("data-version-spec");
        String implementationVersion = options.get("implementation-version");

        Map<String, String> attachOptions = new LinkedHashMap<>();
        String prefix = "attach-option.";
        for (Map.Entry<String, String> entry : options.entrySet()) {
            // CaseInsensitiveStringMap itself lowercases every key it stores
            // (that's the whole point of its name) — an operator writing
            // attach-option.MyOpt therefore arrives here as
            // attach-option.myopt, so a worker-declared option name that is
            // itself case-sensitive can't be reached exactly this way. A
            // real, inherent Spark limitation (not something to work around
            // client-side), same category as the plain-string-only value
            // limitation this class's own javadoc already documents.
            if (entry.getKey().startsWith(prefix) && entry.getKey().length() > prefix.length()) {
                attachOptions.put(entry.getKey().substring(prefix.length()), entry.getValue());
            }
        }

        return new VgiCatalogConfig(location, catalogName, connections, targetSplitBytes, minSplits,
                maxSplitsPerResponse, connectionAcquireTimeoutMillis, maxPlanPages, httpBearerToken,
                launcherEnabled, launcherIdleTimeoutSeconds, launcherStateDir, dataVersionSpec,
                implementationVersion, Map.copyOf(attachOptions));
    }

    /**
     * @return whether {@code location} resolves to {@code launch:} launcher
     *         semantics: an explicit {@code launch:} prefix always does; a
     *         bare command (no recognized scheme prefix) does too, exactly
     *         when {@code launcherEnabled} — see this record's own javadoc
     *         on {@link #launcherEnabled}. {@code unix://}/{@code tcp://}/
     *         {@code http(s)://} never do, regardless of {@code
     *         launcherEnabled}, since each already has unambiguous,
     *         scheme-determined behavior of its own
     */
    public static boolean isLaunchEligible(String location, boolean launcherEnabled) {
        if (location.startsWith("launch:")) return true;
        if (location.startsWith("unix://") || location.startsWith("tcp://")
                || location.startsWith("http://") || location.startsWith("https://")) {
            return false;
        }
        return launcherEnabled;
    }

    private static Long parseLongOrNull(String value) {
        return value == null ? null : Long.parseLong(value.trim());
    }

    private static Double parseDoubleOrNull(String value) {
        return value == null ? null : Double.parseDouble(value.trim());
    }
}
