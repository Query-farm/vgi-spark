// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgispark;

import farm.query.vgi.client.ScanFunctionArguments;
import farm.query.vgi.client.TableInfoDecoder;
import farm.query.vgi.protocol.ItemsResponse;
import farm.query.vgi.protocol.SchemaInfo;
import farm.query.vgi.protocol.TableInfo;
import farm.query.vgi.protocol.TableScanFunctionGetResponse;
import farm.query.vgirpc.MethodNotImplementedError;
import farm.query.vgirpc.marshal.RecordCodec;
import farm.query.vgispark.branch.FormatOptionsDecoder;
import farm.query.vgispark.branch.ScanBranchesDecoder;
import farm.query.vgispark.branch.VgiBranch;
import farm.query.vgispark.branch.VgiFormatScanBranch;
import farm.query.vgispark.branch.VgiScanBranch;
import farm.query.vgispark.client.VgiWorkerClient;
import farm.query.vgispark.function.VgiAggregateFunctions;
import farm.query.vgispark.function.VgiScalarFunctions;
import farm.query.vgispark.procedure.VgiTableProcedures;
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchFunctionException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.FunctionCatalog;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.ProcedureCatalog;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.connector.catalog.procedures.UnboundProcedure;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A Spark catalog backed by one VGI {@code ATTACH} — the same granularity
 * DuckDB and {@code vgi-trino} use. Registered via
 * {@code spark.sql.catalog.<name>=farm.query.vgispark.VgiCatalog} plus
 * {@code spark.sql.catalog.<name>.location}/{@code .catalog-name} (see
 * {@link VgiCatalogConfig}).
 *
 * <p>Folds together what {@code vgi-trino} splits across {@code VgiPlugin} +
 * {@code VgiConnectorFactory} + {@code VgiConnector} + {@code VgiMetadata} —
 * Spark's {@link org.apache.spark.sql.connector.catalog.CatalogPlugin} model
 * is flatter, one class instantiated by Spark via a public no-arg
 * constructor and {@link #initialize}.
 *
 * <p>v1 scope: read-only discovery of declarative/function-backed tables —
 * both the legacy single-function scan path and genuinely multi-branch
 * tables (function branches only; see {@link #resolveBranches} for what's
 * refused) — with real multi-split parallel scans (see {@link
 * farm.query.vgispark.scan.VgiScan}) and projection/filter/limit pushdown
 * (see {@link farm.query.vgispark.scan.VgiScanBuilder}), plus a scoped
 * subset of catalog scalar functions (see {@link VgiScalarFunctions} for
 * exactly what's supported), plain table functions callable via {@code CALL}
 * (see {@link VgiTableProcedures} — NOT {@code FROM func(args)}/{@code
 * LATERAL func(...)}, which remain a genuine Spark SQL-grammar ceiling), and
 * time travel ({@code VERSION AS OF}/{@code TIMESTAMP AS OF} — see {@link
 * #loadTable(Identifier, String)}/{@link #loadTable(Identifier, long)}). No
 * views — see {@code docs/ROADMAP.md} for what's next.
 */
public final class VgiCatalog implements TableCatalog, SupportsNamespaces, FunctionCatalog, ProcedureCatalog {

    private String name;
    private VgiCatalogConfig config;
    private VgiWorkerClient client;

    @Override
    public void initialize(String name, CaseInsensitiveStringMap options) {
        this.name = name;
        this.config = VgiCatalogConfig.fromOptions(options);
        this.client = new VgiWorkerClient(config);
    }

    @Override
    public String name() {
        return name;
    }

    // ------------------------------------------------------------------
    // SupportsNamespaces — VGI schemas are single-level; there is no nested
    // namespace concept, so every namespace-typed argument here is expected
    // to be either zero-length (the catalog root) or exactly one element (a
    // schema name).
    // ------------------------------------------------------------------

    @Override
    public String[][] listNamespaces() {
        return client.withConnection(a -> {
            List<String[]> out = new ArrayList<>();
            for (byte[] item : a.service().catalog_schemas(a.handle(), null).items()) {
                out.add(new String[] {RecordCodec.deserializeFromBytes(item, SchemaInfo.class).name()});
            }
            return out.toArray(new String[0][]);
        });
    }

    @Override
    public String[][] listNamespaces(String[] namespace) throws NoSuchNamespaceException {
        if (namespace.length == 0) {
            return listNamespaces();
        }
        if (!namespaceExists(namespace)) {
            throw new NoSuchNamespaceException(namespace);
        }
        // VGI schemas have no nested sub-namespaces.
        return new String[0][];
    }

    @Override
    public boolean namespaceExists(String[] namespace) {
        if (namespace.length != 1) return false;
        return client.withConnection(a ->
                !a.service().catalog_schema_get(a.handle(), namespace[0], null).items().isEmpty());
    }

    @Override
    public Map<String, String> loadNamespaceMetadata(String[] namespace) throws NoSuchNamespaceException {
        if (namespace.length != 1) {
            throw new NoSuchNamespaceException(namespace);
        }
        // The checked NoSuchNamespaceException below can't be thrown from
        // inside withConnection's plain Function<Attached, T> lambda, so the
        // RPC result is fetched first and the exception thrown afterward.
        ItemsResponse resp = client.withConnection(a -> a.service().catalog_schema_get(a.handle(), namespace[0], null));
        if (resp.items().isEmpty()) {
            throw new NoSuchNamespaceException(namespace);
        }
        SchemaInfo info = RecordCodec.deserializeFromBytes(resp.items().get(0), SchemaInfo.class);
        return info.tags() == null ? Map.of() : info.tags();
    }

    @Override
    public void createNamespace(String[] namespace, Map<String, String> metadata)
            throws NamespaceAlreadyExistsException {
        throw new UnsupportedOperationException("vgi-spark is read-only in this version: cannot create a namespace");
    }

    @Override
    public void alterNamespace(String[] namespace, NamespaceChange... changes) throws NoSuchNamespaceException {
        throw new UnsupportedOperationException("vgi-spark is read-only in this version: cannot alter a namespace");
    }

    @Override
    public boolean dropNamespace(String[] namespace, boolean cascade) throws NoSuchNamespaceException {
        throw new UnsupportedOperationException("vgi-spark is read-only in this version: cannot drop a namespace");
    }

    // ------------------------------------------------------------------
    // TableCatalog
    // ------------------------------------------------------------------

    @Override
    public Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException {
        if (namespace.length != 1) {
            throw new NoSuchNamespaceException(namespace);
        }
        return client.withConnection(a -> {
            ItemsResponse tables = a.service().catalog_schema_contents_tables(a.handle(), namespace[0], null, null);
            List<Identifier> out = new ArrayList<>(tables.items().size());
            for (byte[] item : tables.items()) {
                TableInfo info = TableInfoDecoder.decode(item);
                out.add(Identifier.of(new String[] {info.schema_name()}, info.name()));
            }
            return out.toArray(new Identifier[0]);
        });
    }

    @Override
    public Table loadTable(Identifier ident) throws NoSuchTableException {
        return loadTable(ident, null, null);
    }

    /**
     * Time travel by version — {@code SELECT ... FROM t VERSION AS OF '<version>'}. Spark hands the
     * clause's literal through verbatim (no parsing/validation of its own), matching VGI's own
     * {@code at_unit="version"}/{@code at_value=<version>} convention exactly — see
     * {@code docs/ROADMAP.md} tier 1 item 4.
     */
    @Override
    public Table loadTable(Identifier ident, String version) throws NoSuchTableException {
        return loadTable(ident, "version", version);
    }

    /**
     * Time travel by timestamp — {@code SELECT ... FROM t TIMESTAMP AS OF <expr>}. Spark resolves
     * the clause to microseconds since the Unix epoch (UTC) before calling this overload; VGI's
     * {@code at_value} wants a plain timestamp STRING (workers parse it themselves — DuckDB's own
     * {@code AT (TIMESTAMP => ...)} clause passes one through the same way), so the microseconds are
     * formatted back into one here.
     */
    @Override
    public Table loadTable(Identifier ident, long timestampMicros) throws NoSuchTableException {
        java.time.Instant instant = java.time.Instant.ofEpochSecond(
                Math.floorDiv(timestampMicros, 1_000_000L), Math.floorMod(timestampMicros, 1_000_000L) * 1_000L);
        String atValue = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                .withZone(java.time.ZoneOffset.UTC)
                .format(instant);
        return loadTable(ident, "timestamp", atValue);
    }

    private Table loadTable(Identifier ident, String atUnit, String atValue) throws NoSuchTableException {
        String schemaName = schemaNameOf(ident);
        String tableName = ident.name();
        // Same constraint as loadNamespaceMetadata above: the checked
        // NoSuchTableException can't be thrown from inside withConnection's
        // lambda, so this is two separate borrow/release round trips
        // (catalog_table_get, then catalog_table_scan_function_get) rather
        // than one connection held across both calls.
        ItemsResponse tableResp = client.withConnection(a ->
                a.service().catalog_table_get(a.handle(), schemaName, tableName, atUnit, atValue, null, null));
        if (tableResp.items().isEmpty()) {
            throw new NoSuchTableException(ident);
        }
        TableInfo info = TableInfoDecoder.decode(tableResp.items().get(0));
        List<VgiBranch> branches = resolveBranches(schemaName, tableName, atUnit, atValue);

        return new VgiTable(info.schema_name(), info.name(), branches,
                info.columns(), info.cardinality_estimate(), atUnit, atValue, info.required_filters(),
                client, config);
    }

    /**
     * Resolve a table's scan branches: try the multi-branch RPC first,
     * falling back to the legacy single-function path on {@link
     * MethodNotImplementedError} — mirrors the documented C++-extension
     * client contract ("caches a per-attach capability and falls back... only
     * when the worker raises method-not-implemented").
     *
     * <p>Refuses (fail closed, loudly — see {@link VgiScanBranch}'s own
     * javadoc) rather than silently dropping or mis-scanning: a catalog-table
     * branch (not yet supported — {@code docs/ROADMAP.md} tracks it as
     * follow-up work), a format branch whose {@code format_name} isn't {@code
     * "csv"} (parquet/delta/iceberg — item 11's own scope note), or any
     * branch declaring a non-empty {@code branch_filter} (translating VGI's
     * branch-filter grammar into a per-branch pushdown isn't wired up yet).
     */
    private List<VgiBranch> resolveBranches(String schemaName, String tableName, String atUnit, String atValue) {
        byte[] branchesResponse;
        try {
            branchesResponse = client.withConnection(a ->
                    a.service().catalog_table_scan_branches_get(
                            a.handle(), schemaName, tableName, atUnit, atValue, null, null));
        } catch (MethodNotImplementedError notMultiBranch) {
            TableScanFunctionGetResponse scan = client.withConnection(a ->
                    a.service().catalog_table_scan_function_get(
                            a.handle(), schemaName, tableName, atUnit, atValue, null, null));
            byte[] bindArguments = ScanFunctionArguments.toBindArguments(scan.arguments());
            return List.of(new VgiScanBranch(scan.function_name(), bindArguments));
        }

        ScanBranchesDecoder.Result decoded = ScanBranchesDecoder.decode(branchesResponse);
        List<VgiBranch> branches = new ArrayList<>(decoded.branches().size());
        for (int i = 0; i < decoded.branches().size(); i++) {
            ScanBranchesDecoder.DecodedBranch branch = decoded.branches().get(i);
            if (branch.branchFilter() != null && !branch.branchFilter().isBlank()) {
                throw new UnsupportedOperationException("table '" + schemaName + "." + tableName
                        + "': branch " + i + " (" + branch.functionName() + ") declares branch_filter '"
                        + branch.branchFilter() + "', which vgi-spark doesn't translate into pushdown yet"
                        + " (see docs/ROADMAP.md, the branch_filter note under \"Multi-branch: format"
                        + " branches\") — refusing rather than silently scanning unfiltered");
            }
            switch (branch.kind()) {
                case FUNCTION -> {
                    byte[] bindArguments = ScanFunctionArguments.toBindArguments(branch.arguments());
                    branches.add(new VgiScanBranch(branch.functionName(), bindArguments));
                }
                case FORMAT -> {
                    if (!"csv".equalsIgnoreCase(branch.formatName())) {
                        throw new UnsupportedOperationException("table '" + schemaName + "." + tableName
                                + "': branch " + i + " is a FORMAT branch of format '" + branch.formatName()
                                + "', which vgi-spark only reads for \"csv\" (see docs/ROADMAP.md,"
                                + " \"Multi-branch: format branches\")");
                    }
                    Map<String, Object> options = FormatOptionsDecoder.decode(branch.formatOptions());
                    List<String> locations = branch.formatLocations() == null ? List.of() : branch.formatLocations();
                    branches.add(new VgiFormatScanBranch(branch.formatName(), locations, options));
                }
                case CATALOG_TABLE -> throw new UnsupportedOperationException("table '" + schemaName + "."
                        + tableName + "': branch " + i + " is a CATALOG_TABLE branch, which vgi-spark doesn't"
                        + " support scanning yet (see docs/ROADMAP.md, \"Multi-branch: format branches\")"
                        + " — only function and csv-format branches are supported");
            }
        }
        if (branches.isEmpty()) {
            // The table itself was already confirmed to exist (via
            // catalog_table_get, above) — a worker declaring zero branches
            // for it is an anomaly in the worker's own response, not "table
            // not found," so this isn't NoSuchTableException (which is also
            // a checked exception this private helper doesn't declare).
            throw new IllegalStateException(
                    "table '" + schemaName + "." + tableName + "': worker returned zero scan branches");
        }
        return branches;
    }

    @Override
    public boolean tableExists(Identifier ident) {
        try {
            loadTable(ident);
            return true;
        } catch (NoSuchTableException e) {
            return false;
        }
    }

    @Override
    public Table alterTable(Identifier ident, TableChange... changes) throws NoSuchTableException {
        throw new UnsupportedOperationException("vgi-spark is read-only in this version: cannot alter a table");
    }

    @Override
    public boolean dropTable(Identifier ident) {
        throw new UnsupportedOperationException("vgi-spark is read-only in this version: cannot drop a table");
    }

    @Override
    public void renameTable(Identifier oldIdent, Identifier newIdent)
            throws NoSuchTableException, TableAlreadyExistsException {
        throw new UnsupportedOperationException("vgi-spark is read-only in this version: cannot rename a table");
    }

    // ------------------------------------------------------------------
    // FunctionCatalog — see VgiScalarFunctions for what's actually supported
    // ------------------------------------------------------------------

    @Override
    public Identifier[] listFunctions(String[] namespace) {
        Identifier[] scalars = VgiScalarFunctions.listFunctions(client, namespace);
        Identifier[] aggregates = VgiAggregateFunctions.listFunctions(client, namespace);
        Identifier[] out = new Identifier[scalars.length + aggregates.length];
        System.arraycopy(scalars, 0, out, 0, scalars.length);
        System.arraycopy(aggregates, 0, out, scalars.length, aggregates.length);
        return out;
    }

    /**
     * A VGI name is unique only within (schema, function_type) — a schema
     * could in principle declare a scalar and an aggregate under the same
     * name — but Spark's {@code FunctionCatalog} has one lookup for both
     * kinds, so scalar is tried first (the more common case) and aggregate
     * only on a scalar miss, rather than resolving {@code FunctionInfo} once
     * and dispatching by its own {@code function_type} — a second RPC round
     * trip on a scalar miss, accepted for v1 since function resolution isn't
     * a hot path.
     */
    @Override
    public UnboundFunction loadFunction(Identifier ident) throws NoSuchFunctionException {
        try {
            return VgiScalarFunctions.loadFunction(client, config, ident);
        } catch (NoSuchFunctionException scalarMiss) {
            return VgiAggregateFunctions.loadFunction(client, config, ident);
        }
    }

    // ------------------------------------------------------------------
    // ProcedureCatalog — plain table functions via CALL; see
    // VgiTableProcedures for what's actually supported and why CALL rather
    // than FROM func(args)/LATERAL func(...).
    // ------------------------------------------------------------------

    @Override
    public Identifier[] listProcedures(String[] namespace) {
        return VgiTableProcedures.listProcedures(client, namespace);
    }

    @Override
    public UnboundProcedure loadProcedure(Identifier ident) {
        return VgiTableProcedures.loadProcedure(client, config, ident);
    }

    /**
     * VGI has a single-level namespace (schema), so an {@link Identifier}'s
     * namespace must be exactly one element — a Spark identifier like
     * {@code catalog.a.b.table} (a multi-level namespace) can never resolve
     * to a real VGI table.
     */
    private static String schemaNameOf(Identifier ident) throws NoSuchTableException {
        if (ident.namespace().length != 1) {
            throw new NoSuchTableException(ident);
        }
        return ident.namespace()[0];
    }
}
