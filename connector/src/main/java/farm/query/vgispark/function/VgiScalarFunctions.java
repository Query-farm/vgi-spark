// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgispark.function;

import farm.query.vgi.protocol.FunctionInfo;
import farm.query.vgi.protocol.ItemsResponse;
import farm.query.vgirpc.marshal.RecordCodec;
import farm.query.vgispark.VgiCatalogConfig;
import farm.query.vgispark.client.VgiWorkerClient;
import org.apache.spark.sql.catalyst.analysis.NoSuchFunctionException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * Catalog scalar-function discovery and resolution, called from {@code
 * VgiCatalog}'s {@code FunctionCatalog} implementation.
 *
 * <p>Unlike {@code vgi-trino}'s {@code VgiScalarFunctions} (which discovers
 * every function across every schema once, eagerly, at catalog-attach time,
 * into a registry keyed by a Trino {@code FunctionId} — needed there because
 * Trino resolves a previously-handed-out {@code FunctionId} back to its
 * metadata on later, separate calls), Spark's {@code FunctionCatalog}
 * resolves everything through one self-contained call —
 * {@code loadFunction(Identifier)} — so this looks a function up directly,
 * scoped to the one schema named, with no persistent registry needed.
 */
public final class VgiScalarFunctions {

    private VgiScalarFunctions() {}

    /**
     * List every scalar function's identifier in one schema.
     *
     * @param client the pooled connection to this catalog's VGI worker
     * @param namespace the namespace to list — must be exactly one element
     *        (a VGI schema); anything else lists as empty
     * @return the identifiers, in the worker's own order
     */
    public static Identifier[] listFunctions(VgiWorkerClient client, String[] namespace) {
        if (namespace.length != 1) return new Identifier[0];
        String schemaName = namespace[0];
        ItemsResponse resp = client.withConnection(a -> a.service()
                .catalog_schema_contents_functions(a.handle(), schemaName, "SCALAR_FUNCTION", null, null));
        List<Identifier> out = new ArrayList<>(resp.items().size());
        for (byte[] item : resp.items()) {
            FunctionInfo info = RecordCodec.deserializeFromBytes(item, FunctionInfo.class);
            out.add(Identifier.of(new String[] {schemaName}, info.name()));
        }
        return out.toArray(new Identifier[0]);
    }

    /**
     * Resolve one scalar function by exact identifier.
     *
     * @param client the pooled connection to this catalog's VGI worker
     * @param config this catalog's configuration
     * @param ident the function identifier — its namespace must be exactly
     *        one element (a VGI schema), or empty, in which case it resolves
     *        against the worker's own {@code CatalogAttachResult
     *        .default_schema} — the shape an unqualified call like {@code
     *        catalog.multiply_by_setting(v)} takes (Spark's 2-part
     *        catalog-then-name resolution hands {@code loadFunction} an
     *        Identifier with a zero-length namespace, not one naming the
     *        schema)
     * @return the unbound function
     * @throws NoSuchFunctionException if no scalar function of that name
     *         exists in that schema
     * @throws UnsupportedOperationException if the function exists but its
     *         shape isn't supported yet (see {@code VgiUnboundScalarFunction}'s
     *         own validation) — thrown rather than reported as "not found",
     *         since the caller asked for this exact function by name and
     *         deserves to know why it can't be called, not a misleading
     *         "does not exist"
     */
    public static UnboundFunction loadFunction(VgiWorkerClient client, VgiCatalogConfig config, Identifier ident)
            throws NoSuchFunctionException {
        String schemaName;
        if (ident.namespace().length == 1) {
            schemaName = ident.namespace()[0];
        } else if (ident.namespace().length == 0 && client.defaultSchema() != null) {
            schemaName = client.defaultSchema();
        } else {
            throw new NoSuchFunctionException(ident);
        }
        String functionName = ident.name();
        ItemsResponse resp = client.withConnection(a -> a.service()
                .catalog_schema_contents_functions(a.handle(), schemaName, "SCALAR_FUNCTION", null, null));
        for (byte[] item : resp.items()) {
            FunctionInfo info = RecordCodec.deserializeFromBytes(item, FunctionInfo.class);
            if (info.name().equalsIgnoreCase(functionName)) {
                return VgiUnboundScalarFunction.tryBuild(client, config, schemaName, info);
            }
        }
        throw new NoSuchFunctionException(ident);
    }
}
