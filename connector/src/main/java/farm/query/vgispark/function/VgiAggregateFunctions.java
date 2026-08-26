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
 * Catalog aggregate-function discovery and resolution, called from {@code
 * VgiCatalog}'s {@code FunctionCatalog} implementation — the {@code
 * AggregateFunction} analog of {@link VgiScalarFunctions}. Same
 * discover-by-(schema,name) shape, no persistent registry (see that class's
 * own javadoc for why Spark's model doesn't need one the way Trino's does).
 *
 * <p>v1 scope: only {@code FunctionInfo.function_type() == "AGGREGATE"}.
 */
public final class VgiAggregateFunctions {

    private VgiAggregateFunctions() {}

    /**
     * List every aggregate function's identifier in one schema.
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
                .catalog_schema_contents_functions(a.handle(), schemaName, "AGGREGATE_FUNCTION", null, null));
        List<Identifier> out = new ArrayList<>(resp.items().size());
        for (byte[] item : resp.items()) {
            FunctionInfo info = RecordCodec.deserializeFromBytes(item, FunctionInfo.class);
            if (!"AGGREGATE".equals(info.function_type())) continue;
            out.add(Identifier.of(new String[] {schemaName}, info.name()));
        }
        return out.toArray(new Identifier[0]);
    }

    /**
     * Resolve one aggregate function by exact identifier.
     *
     * @param client the pooled connection to this catalog's VGI worker
     * @param config this catalog's configuration
     * @param ident the function identifier — its namespace must be exactly
     *        one element (a VGI schema), or empty (resolves against {@link
     *        VgiWorkerClient#defaultSchema()}, mirroring {@link
     *        VgiScalarFunctions#loadFunction})
     * @return the unbound function
     * @throws NoSuchFunctionException if no aggregate function of that name
     *         exists in that schema
     * @throws UnsupportedOperationException if the function exists but its
     *         shape isn't supported yet (see {@link VgiUnboundAggregateFunction
     *         #tryBuild}'s own validation)
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
                .catalog_schema_contents_functions(a.handle(), schemaName, "AGGREGATE_FUNCTION", null, null));
        for (byte[] item : resp.items()) {
            FunctionInfo info = RecordCodec.deserializeFromBytes(item, FunctionInfo.class);
            if (info.name().equalsIgnoreCase(functionName) && "AGGREGATE".equals(info.function_type())) {
                return VgiUnboundAggregateFunction.tryBuild(client, config, schemaName, info);
            }
        }
        throw new NoSuchFunctionException(ident);
    }
}
