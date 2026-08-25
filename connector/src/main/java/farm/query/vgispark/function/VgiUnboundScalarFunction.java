// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgispark.function;

import farm.query.vgi.client.ArgumentsEncoder;
import farm.query.vgi.protocol.BindRequest;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.protocol.FunctionInfo;
import farm.query.vgirpc.marshal.RecordCodec;
import farm.query.vgispark.VgiCatalogConfig;
import farm.query.vgispark.client.VgiWorkerClient;
import farm.query.vgispark.types.ArrowSchemaCodec;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.connector.catalog.functions.BoundFunction;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;

import java.util.List;

/**
 * A discovered, not-yet-bound VGI scalar function.
 *
 * <p>{@link #bind} does the actual VGI {@code bind()} RPC — once, on the
 * driver, when Spark resolves a call site during query analysis (unlike
 * {@code vgi-trino}'s {@code VgiScalarFunctions.BindCache}, which needs a
 * cache because Trino's {@code instanceFactory} runs once per {@code Driver}
 * with no visibility into per-call constant argument values; Spark calls
 * {@code bind(StructType)} exactly once per call site with nothing to
 * re-bind for later, since v1 supports no constant arguments at all — see
 * this class's own validation).
 */
final class VgiUnboundScalarFunction implements UnboundFunction {

    private final VgiWorkerClient client;
    private final VgiCatalogConfig config;
    private final String schemaName;
    private final FunctionInfo info;
    private final Schema argsSchema;
    private final DataType[] inputTypes;
    private final DataType returnType;
    private final boolean deterministic;

    private VgiUnboundScalarFunction(VgiWorkerClient client, VgiCatalogConfig config, String schemaName,
            FunctionInfo info, Schema argsSchema, DataType[] inputTypes, DataType returnType,
            boolean deterministic) {
        this.client = client;
        this.config = config;
        this.schemaName = schemaName;
        this.info = info;
        this.argsSchema = argsSchema;
        this.inputTypes = inputTypes;
        this.returnType = returnType;
        this.deterministic = deterministic;
    }

    @Override
    public String name() {
        return info.name();
    }

    @Override
    public String description() {
        return info.description() == null ? "" : info.description();
    }

    @Override
    public BoundFunction bind(StructType inputSchema) {
        if (inputSchema.length() != inputTypes.length) {
            throw new UnsupportedOperationException(schemaName + "." + info.name() + " takes "
                    + inputTypes.length + " argument(s), got " + inputSchema.length());
        }
        byte[] emptyArguments = ArgumentsEncoder.builder().encode();
        byte[] inputSchemaBytes = ArrowSchemaCodec.serializeSchema(argsSchema);
        BindRequest bindRequest = new BindRequest(
                info.name(),
                emptyArguments,
                "SCALAR",
                inputSchemaBytes,
                null,           // settings — deferred, see class javadoc
                null,           // secrets — deferred, see class javadoc
                null,           // attach_opaque_data — filled in per-connection below
                null,           // transaction_opaque_data
                false,          // resolved_secrets_provided
                null, null,     // at_unit / at_value — not applicable to scalars
                null, null,     // copy_from / copy_to
                schemaName);

        byte[][] attachHandleUsed = new byte[1][];
        BindResponse[] boundHolder = new BindResponse[1];
        byte[] opaqueData = client.withConnection(a -> {
            attachHandleUsed[0] = a.handle();
            BindResponse bound = a.service().bind(withAttachHandle(bindRequest, a.handle()), null);
            boundHolder[0] = bound;
            return bound.opaque_data();
        });
        byte[] bindCallBytes = RecordCodec.serializeToBytes(withAttachHandle(bindRequest, attachHandleUsed[0]));

        // InitRequest.output_schema is the RESULT batch's schema (one "result"
        // column of the return type) — BindResponse.output_schema(), NOT this
        // function's own input schema. Conflating the two here previously sent
        // the wrong declared shape to every init() call.
        return new VgiScalarFunction(config, info.name(), bindCallBytes, boundHolder[0].output_schema(), opaqueData,
                inputSchemaBytes, inputTypes, returnType, deterministic);
    }

    private static BindRequest withAttachHandle(BindRequest request, byte[] attachHandle) {
        return new BindRequest(
                request.function_name(), request.arguments(), request.function_type(),
                request.input_schema(), request.settings(), request.secrets(),
                attachHandle, request.transaction_opaque_data(), request.resolved_secrets_provided(),
                request.at_unit(), request.at_value(), request.copy_from(), request.copy_to(),
                request.schema_name());
    }

    /**
     * Build the unbound function for {@code info}, or throw a clear {@link
     * UnsupportedOperationException} naming exactly what isn't supported yet
     * — a caller resolving a specific function by exact name (Spark's normal
     * {@code loadFunction} path) gets an actionable error rather than a
     * generic "function not found".
     *
     * <p>v1 supports only: every argument positional, non-const, non-vararg,
     * non-{@code any}-typed, with a concrete type {@link VgiScalarValueBridge}
     * can bridge; and a single, statically-typed (non-dynamic) return column
     * of one of those same types.
     */
    static VgiUnboundScalarFunction tryBuild(
            VgiWorkerClient client, VgiCatalogConfig config, String schemaName, FunctionInfo info) {
        String context = schemaName + "." + info.name();
        Schema argsSchema = ArrowSchemaCodec.deserializeSchema(info.arguments());
        List<Field> argFields = argsSchema == null ? List.of() : argsSchema.getFields();
        DataType[] inputTypes = new DataType[argFields.size()];
        for (int i = 0; i < argFields.size(); i++) {
            Field field = argFields.get(i);
            java.util.Map<String, String> md = field.getMetadata();
            String vgiType = md == null ? null : md.get("vgi_type");
            if ("table".equals(vgiType)) {
                throw new UnsupportedOperationException(context + ": argument '" + field.getName()
                        + "' is TABLE-typed, not a scalar argument");
            }
            if ("any".equals(vgiType)) {
                throw new UnsupportedOperationException(context + ": argument '" + field.getName()
                        + "' is any-typed (a dynamic/generic type resolved per call site) — not "
                        + "supported yet");
            }
            if (md != null && "named".equals(md.get("vgi_arg"))) {
                throw new UnsupportedOperationException(context + ": argument '" + field.getName()
                        + "' is a named argument — only positional arguments are supported yet");
            }
            if (md != null && "true".equals(md.get("vgi_const"))) {
                throw new UnsupportedOperationException(context + ": argument '" + field.getName()
                        + "' is a bind-time constant (vgi_const) — not supported yet");
            }
            if (md != null && "true".equals(md.get("vgi_varargs"))) {
                throw new UnsupportedOperationException(context + ": argument '" + field.getName()
                        + "' is variadic (vgi_varargs) — not supported yet");
            }
            DataType type;
            try {
                type = farm.query.vgispark.types.VgiTypeMapping.toSparkType(field);
            } catch (UnsupportedOperationException e) {
                throw new UnsupportedOperationException(
                        context + ": argument '" + field.getName() + "': " + e.getMessage(), e);
            }
            if (!VgiScalarValueBridge.isSupported(type)) {
                throw new UnsupportedOperationException(context + ": argument '" + field.getName()
                        + "' has type " + type + ", which scalar-function calls don't bridge yet "
                        + "(struct/list/map/decimal arguments are a later phase)");
            }
            inputTypes[i] = type;
        }

        Schema outSchema = ArrowSchemaCodec.deserializeSchema(info.output_schema());
        if (outSchema == null || outSchema.getFields().size() != 1) {
            throw new UnsupportedOperationException(context + ": expected exactly one output column, got "
                    + (outSchema == null ? "none" : outSchema.getFields().size()));
        }
        Field outField = outSchema.getFields().get(0);
        java.util.Map<String, String> outMd = outField.getMetadata();
        boolean dynamicReturn = (outMd != null && "true".equals(outMd.get("vgi:any")))
                || outField.getType().getTypeID() == org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID.Null;
        if (dynamicReturn) {
            throw new UnsupportedOperationException(context + ": its return type is computed dynamically at "
                    + "bind time (on_bind) — not supported yet (Spark resolves a function's return type "
                    + "statically, before any RPC happens)");
        }
        DataType returnType;
        try {
            returnType = farm.query.vgispark.types.VgiTypeMapping.toSparkType(outField);
        } catch (UnsupportedOperationException e) {
            throw new UnsupportedOperationException(context + ": return type: " + e.getMessage(), e);
        }
        if (!VgiScalarValueBridge.isSupported(returnType)) {
            throw new UnsupportedOperationException(context + ": return type " + returnType
                    + " is not bridged yet (struct/list/map/decimal returns are a later phase)");
        }

        // CONSISTENT is VGI's default; anything else (VOLATILE, CONSISTENT_WITHIN_QUERY) means
        // Spark must not constant-fold or otherwise assume repeated calls agree.
        boolean deterministic = info.stability() == null || "CONSISTENT".equals(info.stability());

        return new VgiUnboundScalarFunction(
                client, config, schemaName, info, argsSchema == null ? new Schema(List.of()) : argsSchema,
                inputTypes, returnType, deterministic);
    }
}
