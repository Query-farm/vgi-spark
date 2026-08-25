// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgispark;

import farm.query.vgi.client.ScanFunctionArguments;
import farm.query.vgi.client.TableInfoDecoder;
import farm.query.vgi.protocol.ItemsResponse;
import farm.query.vgi.protocol.SchemaInfo;
import farm.query.vgi.protocol.TableInfo;
import farm.query.vgi.protocol.TableScanFunctionGetResponse;
import farm.query.vgirpc.marshal.RecordCodec;
import farm.query.vgispark.client.VgiWorkerClient;
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
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
 * <p>v1 scope: read-only discovery of declarative/function-backed tables via
 * the legacy single-function scan path (real multi-split parallel scans —
 * see {@link farm.query.vgispark.scan.VgiScan}). No pushdown yet (a later
 * phase), no multi-branch tables ({@code catalog_table_scan_branches_get}),
 * no views, no time travel, no catalog scalar functions yet — see the plan's
 * phased delivery.
 */
public final class VgiCatalog implements TableCatalog, SupportsNamespaces {

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
        String schemaName = schemaNameOf(ident);
        String tableName = ident.name();
        // Same constraint as loadNamespaceMetadata above: the checked
        // NoSuchTableException can't be thrown from inside withConnection's
        // lambda, so this is two separate borrow/release round trips
        // (catalog_table_get, then catalog_table_scan_function_get) rather
        // than one connection held across both calls.
        ItemsResponse tableResp = client.withConnection(a ->
                a.service().catalog_table_get(a.handle(), schemaName, tableName, null, null, null, null));
        if (tableResp.items().isEmpty()) {
            throw new NoSuchTableException(ident);
        }
        TableInfo info = TableInfoDecoder.decode(tableResp.items().get(0));

        TableScanFunctionGetResponse scan = client.withConnection(a ->
                a.service().catalog_table_scan_function_get(
                        a.handle(), schemaName, tableName, null, null, null, null));
        byte[] bindArguments = ScanFunctionArguments.toBindArguments(scan.arguments());

        return new VgiTable(info.schema_name(), info.name(), scan.function_name(), bindArguments,
                info.columns(), info.cardinality_estimate(), null, null, client, config);
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
