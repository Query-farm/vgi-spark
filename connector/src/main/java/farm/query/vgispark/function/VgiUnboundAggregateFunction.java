// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgispark.function;

import farm.query.vgi.protocol.FunctionInfo;
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
import java.util.Map;

/**
 * A discovered, not-yet-bound VGI aggregate function ({@code
 * FunctionInfo.function_type() == "AGGREGATE"}).
 *
 * <p>Unlike a VGI scalar function's own {@code bind()} — which does a real
 * {@code bind} RPC immediately, since Spark hands it nothing more to learn
 * later — this class's {@link #bind} does NO RPC at all. VGI's own {@code
 * aggregate_bind} needs a live connection, and per {@link VgiAggregateFunction}'s
 * own javadoc, that connection is opened lazily, per Spark TASK, not per bind
 * call site (an aggregate expression's {@code UnboundFunction.bind} runs on
 * the driver during analysis, long before any executor task exists to own a
 * connection). So {@code bind()} here only validates shape and returns a
 * {@link VgiAggregateFunction} carrying everything needed to bind for real,
 * lazily, the first time a task actually calls {@code produceResult}.
 *
 * <p>v1 scope, mirroring {@link VgiUnboundScalarFunction}'s: every argument
 * positional, non-const, non-vararg, non-{@code any}-typed, with a concrete
 * type {@link VgiScalarValueBridge} can bridge (zero arguments — a nullary
 * aggregate like {@code count()} — is fine, matching VGI's own model); a
 * single, statically-typed (non-dynamic) result column of one of those same
 * types (VGI's {@code aggregate_bind} technically allows a per-call
 * bind-time-computed output schema — same as table functions — but v1 keeps
 * the scalar-function restriction here too, since Spark still needs {@code
 * resultType()} answered before any RPC can run).
 */
final class VgiUnboundAggregateFunction implements UnboundFunction {

    private final VgiCatalogConfig config;
    private final String schemaName;
    private final FunctionInfo info;
    private final byte[] argsSchemaBytes;
    private final DataType[] inputTypes;
    private final DataType returnType;
    private final boolean deterministic;

    private VgiUnboundAggregateFunction(VgiCatalogConfig config, String schemaName, FunctionInfo info,
            byte[] argsSchemaBytes, DataType[] inputTypes, DataType returnType, boolean deterministic) {
        this.config = config;
        this.schemaName = schemaName;
        this.info = info;
        this.argsSchemaBytes = argsSchemaBytes;
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
        return new VgiAggregateFunction(
                config, schemaName, info.name(), argsSchemaBytes, inputTypes, returnType, deterministic);
    }

    /**
     * Build the unbound function for {@code info}, or throw a clear {@link
     * UnsupportedOperationException} — mirrors {@link VgiUnboundScalarFunction
     * #tryBuild}'s own contract and reasoning exactly, minus the "at least one
     * argument" assumption (a nullary aggregate is a real, valid VGI shape).
     */
    static VgiUnboundAggregateFunction tryBuild(
            VgiWorkerClient client, VgiCatalogConfig config, String schemaName, FunctionInfo info) {
        String context = schemaName + "." + info.name();
        Schema argsSchema = ArrowSchemaCodec.deserializeSchema(info.arguments());
        List<Field> argFields = argsSchema == null ? List.of() : argsSchema.getFields();
        DataType[] inputTypes = new DataType[argFields.size()];
        for (int i = 0; i < argFields.size(); i++) {
            Field field = argFields.get(i);
            Map<String, String> md = field.getMetadata();
            String vgiType = md == null ? null : md.get("vgi_type");
            if ("table".equals(vgiType)) {
                throw new UnsupportedOperationException(context + ": argument '" + field.getName()
                        + "' is TABLE-typed, not a scalar aggregate argument");
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
                        + "' has type " + type + ", which aggregate-function calls don't bridge yet "
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
        Map<String, String> outMd = outField.getMetadata();
        boolean dynamicReturn = (outMd != null && "true".equals(outMd.get("vgi:any")))
                || outField.getType().getTypeID() == org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID.Null;
        if (dynamicReturn) {
            throw new UnsupportedOperationException(context + ": its return type is computed dynamically at "
                    + "bind time — not supported yet (Spark resolves a function's return type statically, "
                    + "before any RPC happens)");
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

        // CONSISTENT is VGI's default; anything else means Spark must not
        // constant-fold or otherwise assume repeated calls agree.
        boolean deterministic = info.stability() == null || "CONSISTENT".equals(info.stability());

        return new VgiUnboundAggregateFunction(config, schemaName, info,
                info.arguments(), inputTypes, returnType, deterministic);
    }
}
