// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgispark.function;

import farm.query.vgi.SettingSpec;
import farm.query.vgi.client.ArgumentsEncoder;
import farm.query.vgi.client.ScalarValue;
import farm.query.vgi.client.SettingsEncoder;
import farm.query.vgi.protocol.BindRequest;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.protocol.FunctionInfo;
import farm.query.vgirpc.marshal.RecordCodec;
import farm.query.vgispark.VgiCatalogConfig;
import farm.query.vgispark.client.VgiWorkerClient;
import farm.query.vgispark.types.ArrowSchemaCodec;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.functions.BoundFunction;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.Map;

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
 *
 * <p><strong>Return type is resolved from the REAL {@code bind()} response,
 * not static discovery-time metadata</strong> — see {@link #bind}. A VGI
 * scalar function's return type can be {@code on_bind}-computed from the
 * actual input types (e.g. {@code DECIMAL(10,2)} in, {@code DECIMAL(11,2)}
 * out — confirmed live against {@code scalar/numeric_promotion.test}), and
 * {@code bind()} already does a real RPC round trip with the call site's real
 * argument schema, so there's no reason to insist on a static type the way
 * {@code AggregateFunction}'s v1 scope still does — this class simply doesn't
 * attempt to know {@code resultType()} before that RPC has actually run.
 *
 * <p><strong>An {@code any}-typed ARGUMENT is resolved the same way,
 * symmetrically</strong>: {@link #bind(StructType)} receives the call site's
 * real, concrete Spark argument types directly as its parameter — for any
 * argument index whose static {@code vgi_type} metadata was {@code "any"},
 * that real {@link DataType} (not the static discovery-time placeholder)
 * becomes both the {@code BindRequest.input_schema} field (via {@link
 * farm.query.vgispark.types.VgiTypeMapping#toArrowField}, the reverse of the
 * usual Arrow→Spark direction) and the {@code inputTypes()} entry {@link
 * VgiScalarFunction} bridges values with at call time. Confirmed live
 * necessary: {@code numeric_promotion.test}'s {@code double(value)} and
 * {@code add_values(a, b)} declare BOTH their arguments and their return
 * {@code any}-typed, resolved per call site from whatever concrete numeric
 * literal type the caller writes.
 */
final class VgiUnboundScalarFunction implements UnboundFunction {

    private final VgiWorkerClient client;
    private final VgiCatalogConfig config;
    private final String schemaName;
    private final FunctionInfo info;
    private final Schema argsSchema;
    private final DataType[] inputTypes;
    private final boolean[] anyArgs;
    private final boolean deterministic;

    private VgiUnboundScalarFunction(VgiWorkerClient client, VgiCatalogConfig config, String schemaName,
            FunctionInfo info, Schema argsSchema, DataType[] inputTypes, boolean[] anyArgs,
            boolean deterministic) {
        this.client = client;
        this.config = config;
        this.schemaName = schemaName;
        this.info = info;
        this.argsSchema = argsSchema;
        this.inputTypes = inputTypes;
        this.anyArgs = anyArgs;
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
        String context = schemaName + "." + info.name();
        if (inputSchema.length() != inputTypes.length) {
            throw new UnsupportedOperationException(
                    context + " takes " + inputTypes.length + " argument(s), got " + inputSchema.length());
        }

        // Resolve any "any"-typed arguments from THIS call site's real,
        // concrete Spark argument types — see this class's own javadoc.
        // Non-"any" arguments keep their statically-discovered field/type
        // unchanged.
        DataType[] resolvedInputTypes = inputTypes;
        Schema resolvedArgsSchema = argsSchema;
        boolean anyDynamic = false;
        for (boolean b : anyArgs) if (b) { anyDynamic = true; break; }
        if (anyDynamic) {
            List<Field> staticFields = argsSchema.getFields();
            List<Field> resolvedFields = new java.util.ArrayList<>(staticFields);
            resolvedInputTypes = inputTypes.clone();
            for (int i = 0; i < anyArgs.length; i++) {
                if (!anyArgs[i]) continue;
                org.apache.spark.sql.types.StructField callSiteField = inputSchema.fields()[i];
                DataType realType = callSiteField.dataType();
                if (!VgiScalarValueBridge.isSupported(realType)) {
                    throw new UnsupportedOperationException(context + ": argument '" + staticFields.get(i).getName()
                            + "' is any-typed and this call site's real type " + realType
                            + " is not bridged (struct/list/map arguments are a later phase)");
                }
                resolvedInputTypes[i] = realType;
                resolvedFields.set(i, farm.query.vgispark.types.VgiTypeMapping.toArrowField(
                        staticFields.get(i).getName(), realType, callSiteField.nullable()));
            }
            resolvedArgsSchema = new Schema(resolvedFields);
        }

        byte[] emptyArguments = ArgumentsEncoder.builder().encode();
        byte[] inputSchemaBytes = ArrowSchemaCodec.serializeSchema(resolvedArgsSchema);
        BindRequest bindRequest = new BindRequest(
                info.name(),
                emptyArguments,
                "SCALAR",
                inputSchemaBytes,
                currentSettingsBytes(client),
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
        byte[] outputSchemaBytes = boundHolder[0].output_schema();
        DataType returnType = resolveReturnType(outputSchemaBytes);
        return new VgiScalarFunction(config, info.name(), bindCallBytes, outputSchemaBytes, opaqueData,
                inputSchemaBytes, resolvedInputTypes, returnType, deterministic);
    }

    /**
     * Resolve {@code resultType()} from the REAL, just-bound output schema —
     * see this class's own javadoc for why this can't be done earlier, at
     * {@link #tryBuild} time.
     */
    private DataType resolveReturnType(byte[] outputSchemaBytes) {
        String context = schemaName + "." + info.name();
        Schema outSchema = ArrowSchemaCodec.deserializeSchema(outputSchemaBytes);
        if (outSchema == null || outSchema.getFields().size() != 1) {
            throw new UnsupportedOperationException(context + ": expected exactly one output column from bind(), "
                    + "got " + (outSchema == null ? "none" : outSchema.getFields().size()));
        }
        Field outField = outSchema.getFields().get(0);
        Map<String, String> outMd = outField.getMetadata();
        boolean stillDynamic = (outMd != null && "true".equals(outMd.get("vgi:any")))
                || outField.getType().getTypeID() == org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID.Null;
        if (stillDynamic) {
            // The worker's own on_bind, given this call site's REAL argument
            // types, still couldn't resolve a concrete type — genuinely
            // unresolvable, not merely deferred (unlike the old discovery-time
            // check this replaces, which refused every dynamic-return function
            // outright even when a real bind() would have resolved it fine).
            throw new UnsupportedOperationException(context + ": its return type is still dynamic even after "
                    + "bind() with this call site's real argument types — not supported");
        }
        DataType returnType;
        try {
            returnType = farm.query.vgispark.types.VgiTypeMapping.toSparkType(outField);
        } catch (UnsupportedOperationException e) {
            throw new UnsupportedOperationException(context + ": return type: " + e.getMessage(), e);
        }
        if (!VgiScalarValueBridge.isSupported(returnType)) {
            throw new UnsupportedOperationException(context + ": return type " + returnType
                    + " is not bridged yet (struct/list/map returns are a later phase)");
        }
        return returnType;
    }

    /**
     * Encode {@code BindRequest.settings} from whatever of the worker's own
     * declared settings ({@link VgiWorkerClient#declaredSettings()}, from
     * {@code CatalogAttachResult.settings}) the current Spark session
     * actually has a value for — {@code SET name = value} / {@code
     * spark.conf.set(name, value)}. Only names the worker itself declared are
     * considered: Spark's session config is full of unrelated {@code
     * spark.*}/{@code spark.sql.*} entries with nothing to do with any VGI
     * setting, so intersecting against the worker's own declared names (not
     * a blanket prefix guess) is both simpler and exactly correct.
     *
     * <p>Spark's own {@code SET} stores every value as a plain string
     * regardless of how it was written (unlike DuckDB's typed {@code SET}),
     * so each matched value is parsed into the setting's DECLARED Arrow type
     * — {@link SettingSpec#type()} — rather than sent as a string and hoped
     * over; a setting whose declared type isn't one of the handful bridged
     * here is skipped, not guessed at (same "refuse rather than mis-scan"
     * stance the rest of this connector takes).
     *
     * @return the encoded settings batch, or {@code null} if nothing matched
     *         (letting the worker fall back to its own registered defaults
     *         for every setting, exactly as if this feature didn't exist)
     */
    private static byte[] currentSettingsBytes(VgiWorkerClient client) {
        Map<String, SettingSpec> declared = client.declaredSettings();
        if (declared.isEmpty()) return null;

        SparkSession spark = SparkSession.active();
        RuntimeConfig conf = spark.conf();

        SettingsEncoder encoder = SettingsEncoder.builder();
        boolean any = false;
        for (SettingSpec spec : declared.values()) {
            String raw = conf.get(spec.name(), null);
            if (raw == null) continue; // not SET this session — let the worker's own default apply
            Object parsed = parseSettingValue(spec.type(), raw);
            if (parsed == null) continue; // an unbridged type — see this method's own javadoc
            encoder.setting(spec.name(), ScalarValue.of(spec.type(), parsed));
            any = true;
        }
        return any ? encoder.encode() : null;
    }

    /**
     * Parse a Spark {@code SET}-command string into the Java value shape
     * {@link ScalarValue#of(ArrowType, Object)} expects for {@code type} —
     * {@code null} if {@code type} isn't one of the handful bridged here
     * (matches {@link VgiScalarValueBridge}'s own scoped-type stance, not a
     * silent gap: exotic setting types are a follow-up, not attempted here).
     */
    private static Object parseSettingValue(ArrowType type, String raw) {
        return switch (type.getTypeID()) {
            case Bool -> Boolean.parseBoolean(raw);
            case Int -> Long.parseLong(raw); // narrows correctly for any declared width — see VectorScalarCodec.write
            case FloatingPoint -> Double.parseDouble(raw);
            case Utf8, LargeUtf8 -> raw;
            default -> null;
        };
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
     * with a concrete type {@link VgiScalarValueBridge} can bridge; and
     * exactly one output column (the STRUCTURAL shape every VGI scalar
     * function has, checked here). An argument's actual TYPE — including an
     * {@code any}-typed one — is resolved later, from the real {@code bind()}
     * call, not here (see {@link #bind}, {@link #resolveReturnType}, and this
     * class's own javadoc); at discovery time an {@code any}-typed argument's
     * static Arrow field is simply skipped rather than type-checked, since it
     * carries no real type yet to check.
     */
    static VgiUnboundScalarFunction tryBuild(
            VgiWorkerClient client, VgiCatalogConfig config, String schemaName, FunctionInfo info) {
        String context = schemaName + "." + info.name();
        Schema argsSchema = ArrowSchemaCodec.deserializeSchema(info.arguments());
        List<Field> argFields = argsSchema == null ? List.of() : argsSchema.getFields();
        DataType[] inputTypes = new DataType[argFields.size()];
        boolean[] anyArgs = new boolean[argFields.size()];
        for (int i = 0; i < argFields.size(); i++) {
            Field field = argFields.get(i);
            java.util.Map<String, String> md = field.getMetadata();
            String vgiType = md == null ? null : md.get("vgi_type");
            if ("table".equals(vgiType)) {
                throw new UnsupportedOperationException(context + ": argument '" + field.getName()
                        + "' is TABLE-typed, not a scalar argument");
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
            if ("any".equals(vgiType)) {
                // Real type resolved later, from bind(StructType)'s real
                // call-site argument type — see this class's own javadoc.
                anyArgs[i] = true;
                continue;
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
                        + "(struct/list/map arguments are a later phase)");
            }
            inputTypes[i] = type;
        }

        // NOT validated here anymore: the return type itself (could be
        // on_bind-computed from real argument types, e.g. decimal precision/
        // scale promotion — see this class's own javadoc, and resolveReturnType,
        // called from bind() with the REAL post-bind output schema instead).
        // Still worth confirming the shape is at least structurally sane
        // (exactly one output column) at discovery time, since that count is
        // a fixed VGI scalar-function invariant, not something on_bind varies.
        Schema outSchema = ArrowSchemaCodec.deserializeSchema(info.output_schema());
        if (outSchema == null || outSchema.getFields().size() != 1) {
            throw new UnsupportedOperationException(context + ": expected exactly one output column, got "
                    + (outSchema == null ? "none" : outSchema.getFields().size()));
        }

        // CONSISTENT is VGI's default; anything else (VOLATILE, CONSISTENT_WITHIN_QUERY) means
        // Spark must not constant-fold or otherwise assume repeated calls agree.
        boolean deterministic = info.stability() == null || "CONSISTENT".equals(info.stability());

        return new VgiUnboundScalarFunction(
                client, config, schemaName, info, argsSchema == null ? new Schema(List.of()) : argsSchema,
                inputTypes, anyArgs, deterministic);
    }
}
