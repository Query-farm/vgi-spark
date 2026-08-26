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
 * driver, when Spark resolves a call site during query analysis, PROVIDED
 * the function has no {@code vgi_const} argument. A {@code vgi_const}
 * argument's VALUE (not just its type) is unknowable at this point — Spark's
 * {@code bind(StructType)} sees types only — so for a const-bearing function
 * this method does no RPC at all and instead builds a {@link
 * VgiScalarFunction} that binds lazily, per observed value, the first time
 * (and again whenever it changes) {@code produceResult} actually sees a row;
 * see {@link VgiScalarFunction}'s own javadoc for the full rationale (the
 * same limitation {@code vgi-trino}'s {@code VgiScalarFunctions.BindCache}
 * exists to work around, simplified here since Spark already scopes one
 * {@code VgiScalarFunction} instance to one call site).
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
    private final boolean[] constArgs;
    private final boolean hasVarargs; // true iff the LAST declared argument is vgi_varargs
    private final boolean deterministic;

    private VgiUnboundScalarFunction(VgiWorkerClient client, VgiCatalogConfig config, String schemaName,
            FunctionInfo info, Schema argsSchema, DataType[] inputTypes, boolean[] anyArgs,
            boolean[] constArgs, boolean hasVarargs, boolean deterministic) {
        this.client = client;
        this.config = config;
        this.schemaName = schemaName;
        this.info = info;
        this.argsSchema = argsSchema;
        this.inputTypes = inputTypes;
        this.anyArgs = anyArgs;
        this.constArgs = constArgs;
        this.hasVarargs = hasVarargs;
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
        int declaredCount = inputTypes.length;
        int callArity = inputSchema.length();
        if (hasVarargs) {
            // The trailing vararg position may repeat zero or more times —
            // callArity must be at least the fixed (non-vararg) prefix count.
            if (callArity < declaredCount - 1) {
                throw new UnsupportedOperationException(context + " takes at least " + (declaredCount - 1)
                        + " argument(s), got " + callArity);
            }
        } else if (callArity != declaredCount) {
            throw new UnsupportedOperationException(
                    context + " takes " + declaredCount + " argument(s), got " + callArity);
        }

        // Resolve every argument's REAL, call-site type where it isn't
        // statically known: an "any"-typed argument (see this class's own
        // javadoc), and — separately — every position the vararg group
        // expands to (see below). Non-"any", non-vararg-expanded arguments
        // keep their statically-discovered field/type unchanged. Always
        // builds a full, CALL-SITE-length (not declared-length) resolved
        // list, since both the eager and const-deferred paths below need it
        // uniformly, and a vararg call site's effective arity can differ
        // from the declared one.
        DataType[] resolvedInputTypes = new DataType[callArity];
        List<Field> resolvedFields = new java.util.ArrayList<>(callArity);
        boolean[] resolvedConstArgs = new boolean[callArity];
        List<Field> staticFields = argsSchema.getFields();
        int fixedCount = hasVarargs ? declaredCount - 1 : declaredCount;
        for (int i = 0; i < fixedCount; i++) {
            Field staticField = staticFields.get(i);
            resolvedConstArgs[i] = constArgs[i];
            if (!anyArgs[i]) {
                resolvedInputTypes[i] = inputTypes[i];
                resolvedFields.add(staticField);
                continue;
            }
            org.apache.spark.sql.types.StructField callSiteField = inputSchema.fields()[i];
            boolean wasNullType = callSiteField.dataType() instanceof org.apache.spark.sql.types.NullType;
            DataType realType = resolveCallSiteType(callSiteField.dataType());
            if (!VgiScalarValueBridge.isSupported(realType)) {
                throw new UnsupportedOperationException(context + ": argument '" + staticField.getName()
                        + "' is any-typed and this call site's real type " + realType
                        + " is not bridged (struct/list/map arguments are a later phase)");
            }
            resolvedInputTypes[i] = realType;
            resolvedFields.add(farm.query.vgispark.types.VgiTypeMapping.toArrowField(
                    staticField.getName(), realType, wasNullType || callSiteField.nullable()));
        }
        if (hasVarargs) {
            // Every expanded vararg position is resolved from THIS call
            // site's real type independently — a repeating vararg group's
            // whole point is per-call flexibility (potentially even a
            // different concrete type at each position, e.g. sum_values'
            // on_bind computing its return from just the FIRST one) — never
            // refused for being "any"-typed, unlike a plain fixed argument.
            Field varargSpec = staticFields.get(declaredCount - 1);
            for (int i = fixedCount; i < callArity; i++) {
                org.apache.spark.sql.types.StructField callSiteField = inputSchema.fields()[i];
                boolean wasNullType = callSiteField.dataType() instanceof org.apache.spark.sql.types.NullType;
                DataType realType = resolveCallSiteType(callSiteField.dataType());
                if (!VgiScalarValueBridge.isSupported(realType)) {
                    throw new UnsupportedOperationException(context + ": vararg argument '" + varargSpec.getName()
                            + "' at position " + i + " has this call site's real type " + realType
                            + ", which is not bridged (struct/list/map arguments are a later phase)");
                }
                resolvedInputTypes[i] = realType;
                resolvedFields.add(farm.query.vgispark.types.VgiTypeMapping.toArrowField(
                        varargSpec.getName() + "_" + (i - fixedCount), realType,
                        wasNullType || callSiteField.nullable()));
                resolvedConstArgs[i] = false; // vgi_const + vgi_varargs is refused together at discovery
            }
        }

        boolean hasConstArgs = false;
        for (boolean b : resolvedConstArgs) if (b) { hasConstArgs = true; break; }

        // Row (non-const) vs. const argument split, in signature order —
        // identical to [0..n) / [] when there are no const arguments at all.
        List<Integer> rowIdx = new java.util.ArrayList<>();
        List<Integer> constIdx = new java.util.ArrayList<>();
        List<Field> rowFields = new java.util.ArrayList<>();
        for (int i = 0; i < resolvedInputTypes.length; i++) {
            if (resolvedConstArgs[i]) constIdx.add(i);
            else { rowIdx.add(i); rowFields.add(resolvedFields.get(i)); }
        }
        int[] rowArgIndices = rowIdx.stream().mapToInt(Integer::intValue).toArray();
        int[] constArgIndices = constIdx.stream().mapToInt(Integer::intValue).toArray();
        Schema rowInputSchema = new Schema(rowFields);
        byte[] rowInputSchemaBytes = ArrowSchemaCodec.serializeSchema(rowInputSchema);
        Field[] resolvedFieldsArray = resolvedFields.toArray(new Field[0]);

        if (!hasConstArgs) {
            // No vgi_const arguments: unchanged from before this class
            // supported them — bind ONCE, now, on the driver, and replay the
            // same bindCallBytes/opaqueData at every later init() call,
            // wherever Spark schedules this call site's rows.
            byte[] emptyArguments = ArgumentsEncoder.builder().encode();
            BindRequest bindRequest = new BindRequest(
                    info.name(), emptyArguments, "SCALAR", rowInputSchemaBytes, currentSettingsBytes(client),
                    null, null, null, false, null, null, null, null, schemaName);
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
            // function's own input schema.
            byte[] outputSchemaBytes = boundHolder[0].output_schema();
            DataType returnType = resolveReturnType(outputSchemaBytes);
            return VgiScalarFunction.eager(config, info.name(), bindCallBytes, outputSchemaBytes, opaqueData,
                    rowInputSchemaBytes, resolvedInputTypes, rowArgIndices, returnType, deterministic);
        }

        // At least one vgi_const argument: its VALUE (not just type) is only
        // known once a real row exists — Spark's bind(StructType) sees types
        // only. Defer the real bind() entirely to the first produceResult()
        // call, per call site, and rebind whenever the observed const value
        // actually changes — see VgiScalarFunction's own javadoc for the
        // full rationale (mirrors vgi-trino's BindCache, simplified since
        // Spark already scopes one VgiScalarFunction instance to one call
        // site, unlike Trino's shared-across-Drivers cache).
        DataType returnType = resolveStaticReturnType(context);
        return VgiScalarFunction.lazyConst(config, schemaName, info.name(), resolvedInputTypes, resolvedFieldsArray,
                rowArgIndices, constArgIndices, rowInputSchemaBytes, returnType, deterministic);
    }

    /**
     * A bare {@code NULL} literal with no cast (e.g. {@code sum_values(1,
     * NULL, 3)}) analyzes to Spark's {@code NullType} — a real type with no
     * bridgeable Arrow counterpart, but also not a genuine "this argument's
     * type is unknowable" case: the cell will be null on the wire regardless
     * of which declared type it travels as (VGI already round-trips a null
     * cell correctly for any Arrow type), so rather than refusing the whole
     * call site, this substitutes a safe, universally-bridgeable placeholder
     * ({@link DataTypes#LongType}) wherever the call site's real type is
     * {@code NullType}. DuckDB's own binder instead unifies a NULL literal's
     * type from its sibling arguments' types before ever reaching {@code
     * bind()}; Spark's analyzer does no such contextual coercion for a
     * custom {@code ScalarFunction}'s arguments, so this is the closest
     * faithful equivalent available — confirmed live against {@code
     * sum_values.test}'s NULL-handling section, where the row-level RESULT
     * is null either way, independent of which placeholder type was used to
     * declare the (always-null) wire cell.
     */
    private static DataType resolveCallSiteType(DataType raw) {
        return raw instanceof org.apache.spark.sql.types.NullType ? org.apache.spark.sql.types.DataTypes.LongType
                : raw;
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
     * Resolve {@code resultType()} from the STATIC discovery-time output
     * schema — {@code FunctionInfo.output_schema()}, no RPC — used only for
     * a function with at least one {@code vgi_const} argument, where the
     * real {@code bind()} RPC (and hence a real, call-site-specific output
     * schema) is deferred to the first {@code produceResult()} call (see
     * {@link #bind}). Spark commits to a call site's {@code resultType()}
     * at ANALYSIS time, before any row — including any const argument's
     * actual value — is ever seen, so a return type that itself depends on
     * a const argument's VALUE (as opposed to depending on argument TYPES,
     * which {@link #resolveReturnType} already handles from a real bind())
     * is a genuine, documented v1 ceiling: refused here exactly like an
     * argument-type-dependent dynamic return is refused in {@link
     * #resolveReturnType}.
     */
    private DataType resolveStaticReturnType(String context) {
        Schema outSchema = ArrowSchemaCodec.deserializeSchema(info.output_schema());
        if (outSchema == null || outSchema.getFields().size() != 1) {
            throw new UnsupportedOperationException(context + ": expected exactly one output column, got "
                    + (outSchema == null ? "none" : outSchema.getFields().size()));
        }
        Field outField = outSchema.getFields().get(0);
        Map<String, String> outMd = outField.getMetadata();
        boolean stillDynamic = (outMd != null && "true".equals(outMd.get("vgi:any")))
                || outField.getType().getTypeID() == org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID.Null;
        if (stillDynamic) {
            throw new UnsupportedOperationException(context + ": has a vgi_const argument AND an on_bind-dynamic "
                    + "return type — the return type would need this call site's real const VALUE (not just its "
                    + "type) to resolve, but Spark commits to resultType() before any row is ever seen — not "
                    + "supported");
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
        return currentSettingsBytes(client.declaredSettings());
    }

    /**
     * Package-private overload also used by {@link VgiScalarFunction}'s
     * lazy const-argument bind path, which has only an executor-side
     * unpooled {@code Attached} connection (its {@code
     * CatalogAttachResult.settings()}, decoded the same way) — not the
     * driver's pooled {@link VgiWorkerClient} instance {@link
     * VgiWorkerClient#declaredSettings()} itself caches on.
     */
    static byte[] currentSettingsBytes(Map<String, SettingSpec> declared) {
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
     * <p>v1 supports only: every argument positional, non-vararg, with a
     * concrete type {@link VgiScalarValueBridge} can bridge; and exactly one
     * output column (the STRUCTURAL shape every VGI scalar function has,
     * checked here). An argument's actual TYPE — including an {@code
     * any}-typed one — is resolved later, from the real {@code bind()} call,
     * not here (see {@link #bind}, {@link #resolveReturnType}, and this
     * class's own javadoc); at discovery time an {@code any}-typed argument's
     * static Arrow field is simply skipped rather than type-checked, since it
     * carries no real type yet to check. A {@code vgi_const} argument's VALUE
     * is resolved later still, lazily per row — see {@link #bind} and {@link
     * VgiScalarFunction}'s own javadoc.
     */
    static VgiUnboundScalarFunction tryBuild(
            VgiWorkerClient client, VgiCatalogConfig config, String schemaName, FunctionInfo info) {
        String context = schemaName + "." + info.name();
        Schema argsSchema = ArrowSchemaCodec.deserializeSchema(info.arguments());
        List<Field> argFields = argsSchema == null ? List.of() : argsSchema.getFields();
        DataType[] inputTypes = new DataType[argFields.size()];
        boolean[] anyArgs = new boolean[argFields.size()];
        boolean[] constArgs = new boolean[argFields.size()];
        boolean hasVarargs = false;
        for (int i = 0; i < argFields.size(); i++) {
            Field field = argFields.get(i);
            java.util.Map<String, String> md = field.getMetadata();
            String vgiType = md == null ? null : md.get("vgi_type");
            boolean isVarargs = md != null && "true".equals(md.get("vgi_varargs"));
            boolean isConst = md != null && "true".equals(md.get("vgi_const"));
            if ("table".equals(vgiType)) {
                throw new UnsupportedOperationException(context + ": argument '" + field.getName()
                        + "' is TABLE-typed, not a scalar argument");
            }
            if (md != null && "named".equals(md.get("vgi_arg"))) {
                throw new UnsupportedOperationException(context + ": argument '" + field.getName()
                        + "' is a named argument — only positional arguments are supported yet");
            }
            if (isVarargs && i != argFields.size() - 1) {
                throw new UnsupportedOperationException(context + ": argument '" + field.getName()
                        + "' is vgi_varargs but isn't the LAST argument — a repeating trailing argument "
                        + "is the only vararg shape supported");
            }
            if (isVarargs && isConst) {
                throw new UnsupportedOperationException(context + ": argument '" + field.getName()
                        + "' is both vgi_const and vgi_varargs, a combination with no clear wire meaning");
            }
            if (isConst) {
                // Value (not just type) resolved later, per call, from the
                // real InternalRow — see bind()/VgiScalarFunction's own
                // lazy-bind-cache javadoc.
                constArgs[i] = true;
            }
            if (isVarargs) {
                // Every expanded call-site position resolved later, from
                // bind(StructType) — see this class's own javadoc and bind's
                // own vararg-expansion comment. No static type to check here
                // at all (unlike a plain "any" argument, this one repeats).
                hasVarargs = true;
                continue;
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
                inputTypes, anyArgs, constArgs, hasVarargs, deterministic);
    }
}
