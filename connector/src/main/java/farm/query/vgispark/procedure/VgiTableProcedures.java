// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgispark.procedure;

import farm.query.vgi.protocol.FunctionInfo;
import farm.query.vgi.protocol.ItemsResponse;
import farm.query.vgirpc.marshal.RecordCodec;
import farm.query.vgispark.VgiCatalogConfig;
import farm.query.vgispark.client.VgiWorkerClient;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.procedures.UnboundProcedure;

import java.util.ArrayList;
import java.util.List;

/**
 * Catalog table-function discovery and resolution, called from {@code
 * VgiCatalog}'s {@code ProcedureCatalog} implementation — the {@code CALL}
 * analog of {@code VgiScalarFunctions} for scalar functions. See {@link
 * VgiUnboundTableProcedure}'s own javadoc for why {@code CALL} rather than
 * {@code FROM func(args)}/{@code LATERAL func(...)}.
 *
 * <p>v1 scope: only {@code FunctionInfo.function_type() == "TABLE"} —
 * table-in-out functions ({@code "table_in_out"}, which take a TABLE
 * argument) and buffering functions ({@code "table_buffering"}) are excluded
 * here, not merely by {@link VgiUnboundTableProcedure#tryBuild}'s own
 * per-argument checks, since neither shape has a {@code CALL}-compatible
 * calling convention at all (a TABLE argument has no {@code CALL}-argument
 * syntax to supply it through).
 */
public final class VgiTableProcedures {

    private VgiTableProcedures() {}

    /**
     * List every plain table function's identifier in one schema.
     *
     * @param client the pooled connection to this catalog's VGI worker
     * @param namespace the namespace to list — must be exactly one element
     *        (a VGI schema); anything else lists as empty
     * @return the identifiers, in the worker's own order
     */
    public static Identifier[] listProcedures(VgiWorkerClient client, String[] namespace) {
        if (namespace.length != 1) return new Identifier[0];
        String schemaName = namespace[0];
        ItemsResponse resp = client.withConnection(a -> a.service()
                .catalog_schema_contents_functions(a.handle(), schemaName, "TABLE_FUNCTION", null, null));
        List<Identifier> out = new ArrayList<>(resp.items().size());
        for (byte[] item : resp.items()) {
            FunctionInfo info = RecordCodec.deserializeFromBytes(item, FunctionInfo.class);
            if (!"TABLE".equals(info.function_type())) continue; // table_in_out/table_buffering excluded — see class javadoc
            out.add(Identifier.of(new String[] {schemaName}, info.name()));
        }
        return out.toArray(new Identifier[0]);
    }

    /**
     * Resolve one table function by exact identifier.
     *
     * @param client the pooled connection to this catalog's VGI worker
     * @param config this catalog's configuration
     * @param ident the procedure identifier — its namespace must be exactly
     *        one element (a VGI schema), or empty (resolves against {@link
     *        VgiWorkerClient#defaultSchema()} — the unqualified {@code CALL
     *        catalog.func(...)} shape, mirroring {@code VgiScalarFunctions
     *        .loadFunction}'s identical handling)
     * @return the unbound procedure
     * @throws IllegalArgumentException if no plain table function of that
     *         name exists in that schema, or the namespace can't be
     *         resolved — {@code ProcedureCatalog.loadProcedure} declares no
     *         checked exception at all (unlike {@code FunctionCatalog
     *         .loadFunction}'s {@code NoSuchFunctionException}), so an
     *         unchecked, clearly-worded refusal is the contract here
     * @throws UnsupportedOperationException if the function exists but its
     *         shape isn't supported yet (see {@link VgiUnboundTableProcedure
     *         #tryBuild}'s own validation)
     */
    public static UnboundProcedure loadProcedure(VgiWorkerClient client, VgiCatalogConfig config, Identifier ident) {
        String schemaName;
        if (ident.namespace().length == 1) {
            schemaName = ident.namespace()[0];
        } else if (ident.namespace().length == 0 && client.defaultSchema() != null) {
            schemaName = client.defaultSchema();
        } else {
            throw new IllegalArgumentException("no such procedure: " + ident);
        }
        String procName = ident.name();
        ItemsResponse resp = client.withConnection(a -> a.service()
                .catalog_schema_contents_functions(a.handle(), schemaName, "TABLE_FUNCTION", null, null));
        for (byte[] item : resp.items()) {
            FunctionInfo info = RecordCodec.deserializeFromBytes(item, FunctionInfo.class);
            if (info.name().equalsIgnoreCase(procName) && "TABLE".equals(info.function_type())) {
                return VgiUnboundTableProcedure.tryBuild(client, config, schemaName, info);
            }
        }
        throw new IllegalArgumentException("no such procedure: " + schemaName + "." + procName);
    }
}
