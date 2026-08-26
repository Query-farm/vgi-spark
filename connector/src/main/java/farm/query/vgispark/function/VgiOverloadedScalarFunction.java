// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgispark.function;

import farm.query.vgi.protocol.FunctionInfo;
import farm.query.vgispark.VgiCatalogConfig;
import farm.query.vgispark.client.VgiWorkerClient;
import farm.query.vgispark.types.ArrowSchemaCodec;
import farm.query.vgispark.types.VgiTypeMapping;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.connector.catalog.functions.BoundFunction;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.NullType;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.Map;

/**
 * A VGI scalar function name with MORE THAN ONE {@link FunctionInfo} —
 * distinguished by argument count, argument type, or both (e.g. {@code
 * format_number} dispatches on {@code ConstParam} count: 1/2/3 total
 * arguments; {@code type_info} dispatches on a single argument's TYPE:
 * {@code int32}/{@code int64}/{@code varchar}; {@code smart_format} on a
 * {@code ConstParam}'s type at the same arity).
 *
 * <p>Spark's {@code FunctionCatalog.loadFunction(Identifier)} returns exactly
 * ONE {@link UnboundFunction} per name — there is no "hand back N candidates,
 * let Spark pick" mechanism the way native/built-in function overloads work.
 * So all resolution happens here, inside {@link #bind}, the one point where
 * Spark hands over the call site's real arity and argument types: filter the
 * discovered candidates by arity compatibility, then by per-fixed-argument
 * type compatibility (an {@code any}-typed or {@code NULL}-literal position
 * matches anything), and delegate to {@link VgiUnboundScalarFunction} for
 * whichever single candidate remains — refusing outright, with a clear
 * message, if zero or more-than-one candidate matches (never a silent,
 * possibly-wrong pick).
 *
 * <p>Only ARITY and per-position TYPE participate in dispatch — not a
 * {@code vgi_const} argument's actual VALUE, which (per {@link
 * VgiUnboundScalarFunction}'s own const-argument support) isn't knowable
 * until a real row exists, long after overload resolution has already had
 * to complete during query analysis.
 */
final class VgiOverloadedScalarFunction implements UnboundFunction {

    private final VgiWorkerClient client;
    private final VgiCatalogConfig config;
    private final String schemaName;
    private final List<FunctionInfo> candidates;

    VgiOverloadedScalarFunction(
            VgiWorkerClient client, VgiCatalogConfig config, String schemaName, List<FunctionInfo> candidates) {
        this.client = client;
        this.config = config;
        this.schemaName = schemaName;
        this.candidates = candidates;
    }

    @Override
    public String name() {
        return candidates.get(0).name();
    }

    @Override
    public String description() {
        return schemaName + "." + name() + ": " + candidates.size() + " overloads";
    }

    @Override
    public BoundFunction bind(StructType inputSchema) {
        String context = schemaName + "." + name();
        int callArity = inputSchema.length();

        List<FunctionInfo> arityMatches = candidates.stream()
                .filter(c -> arityCompatible(c, callArity))
                .toList();
        if (arityMatches.isEmpty()) {
            throw new UnsupportedOperationException(context + ": no overload accepts " + callArity
                    + " argument(s) — declared arities: " + candidates.stream()
                    .map(VgiOverloadedScalarFunction::describeArity).distinct().toList());
        }

        // Two-tier: prefer an EXACT per-position type match; only if NOTHING
        // matches exactly, retry allowing safe implicit numeric widening
        // (e.g. Spark's own DECIMAL-literal default type against a
        // DOUBLE-declared overload — see isImplicitlyCompatible). Standard
        // SQL overload-resolution shape: exact beats widened, and widening
        // is only even consulted once exact has already come up empty.
        List<FunctionInfo> typeMatches = arityMatches.stream()
                .filter(c -> typeCompatible(c, inputSchema, false))
                .toList();
        if (typeMatches.isEmpty()) {
            typeMatches = arityMatches.stream().filter(c -> typeCompatible(c, inputSchema, true)).toList();
        }
        if (typeMatches.isEmpty()) {
            throw new UnsupportedOperationException(context + ": no overload matches argument types "
                    + java.util.Arrays.toString(inputSchema.fields()) + " among the " + arityMatches.size()
                    + " overload(s) accepting " + callArity + " argument(s)");
        }
        if (typeMatches.size() > 1) {
            // A genuine Arrow-type collision, not a real ambiguity from the
            // user's SQL: e.g. int64/uint32/uint64 all widen to Spark's one
            // LongType (no unsigned integer type exists to keep them
            // distinct — the same ceiling VgiTypeMapping's own javadoc
            // documents). Prefer whichever candidate(s) use no UNSIGNED
            // Arrow int at any matched position — mirroring vgi-trino's own
            // "prefer the lossless mapping" tie-break for the identical
            // collision, and the natural default since Spark has no syntax
            // to ever express "this literal is specifically unsigned" in
            // the first place.
            List<FunctionInfo> preferSigned = typeMatches.stream().filter(c -> !usesUnsignedArg(c)).toList();
            if (preferSigned.size() == 1) {
                typeMatches = preferSigned;
            } else {
                throw new UnsupportedOperationException(context + ": call is ambiguous — " + typeMatches.size()
                        + " overloads all match argument types " + java.util.Arrays.toString(inputSchema.fields()));
            }
        }

        return VgiUnboundScalarFunction.tryBuild(client, config, schemaName, typeMatches.get(0)).bind(inputSchema);
    }

    private static boolean arityCompatible(FunctionInfo info, int callArity) {
        List<Field> fields = argFields(info);
        boolean hasVarargs = !fields.isEmpty() && isTrue(fields.get(fields.size() - 1), "vgi_varargs");
        int declaredCount = fields.size();
        return hasVarargs ? callArity >= declaredCount - 1 : callArity == declaredCount;
    }

    /**
     * Whether every FIXED (non-vararg) argument position's statically
     * declared type is compatible with {@code inputSchema}'s real type
     * there — an {@code any}-typed position, or a call-site {@code NullType}
     * (a bare {@code NULL} literal, which conveys no type of its own),
     * always matches. The vararg tail, if any, is never a dispatch
     * criterion — its individual positions are resolved independently,
     * later, by {@link VgiUnboundScalarFunction#bind} itself.
     *
     * @param allowImplicitCast when {@code false}, a position matches only
     *        on EXACT Spark-type equality; when {@code true}, also matches
     *        via {@link #isImplicitlyCompatible} — see {@link #bind}'s own
     *        two-tier ordering for why a caller tries {@code false} first
     */
    private static boolean typeCompatible(FunctionInfo info, StructType inputSchema, boolean allowImplicitCast) {
        List<Field> fields = argFields(info);
        boolean hasVarargs = !fields.isEmpty() && isTrue(fields.get(fields.size() - 1), "vgi_varargs");
        int fixedCount = hasVarargs ? fields.size() - 1 : fields.size();
        for (int i = 0; i < fixedCount; i++) {
            Field field = fields.get(i);
            Map<String, String> md = field.getMetadata();
            String vgiType = md == null ? null : md.get("vgi_type");
            if ("any".equals(vgiType)) continue;
            DataType real = inputSchema.fields()[i].dataType();
            if (real instanceof NullType) continue;
            DataType declared;
            try {
                declared = VgiTypeMapping.toSparkType(field);
            } catch (UnsupportedOperationException e) {
                return false; // a type this connector can't map at all can't be a match either
            }
            if (declared.equals(real)) continue;
            if (allowImplicitCast && isImplicitlyCompatible(real, declared)) continue;
            return false;
        }
        return true;
    }

    /**
     * A narrow, deliberately conservative implicit-numeric-widening check —
     * NOT full SQL type-coercion — used only as overload dispatch's second
     * tier, after an exact match has already come up empty (see {@link
     * #bind}). Covers what this corpus actually needs: a Spark numeric
     * LITERAL with a decimal point defaults to {@link DecimalType}, not
     * {@code DoubleType}/{@code FloatType} (confirmed live against {@code
     * format_number(3.14)} — its one arity-1 overload declares {@code
     * DoubleType}), and the standard integer-widening lattice (a narrower
     * signed int literal is compatible with a wider-declared one).
     */
    private static boolean isImplicitlyCompatible(DataType real, DataType declared) {
        if (real instanceof DecimalType && (declared.equals(DataTypes.DoubleType)
                || declared.equals(DataTypes.FloatType))) {
            return true;
        }
        int realRank = integerWideningRank(real);
        int declaredRank = integerWideningRank(declared);
        return realRank >= 0 && declaredRank >= 0 && realRank <= declaredRank;
    }

    /** @return this integer type's widening rank (0=byte..3=long), or -1 if not a plain signed integer type. */
    private static int integerWideningRank(DataType type) {
        if (type.equals(DataTypes.ByteType)) return 0;
        if (type.equals(DataTypes.ShortType)) return 1;
        if (type.equals(DataTypes.IntegerType)) return 2;
        if (type.equals(DataTypes.LongType)) return 3;
        return -1;
    }

    /** @return whether ANY fixed argument position {@code info} declares is an UNSIGNED Arrow integer. */
    private static boolean usesUnsignedArg(FunctionInfo info) {
        List<Field> fields = argFields(info);
        boolean hasVarargs = !fields.isEmpty() && isTrue(fields.get(fields.size() - 1), "vgi_varargs");
        int fixedCount = hasVarargs ? fields.size() - 1 : fields.size();
        for (int i = 0; i < fixedCount; i++) {
            if (fields.get(i).getType() instanceof ArrowType.Int intType && !intType.getIsSigned()) return true;
        }
        return false;
    }

    private static List<Field> argFields(FunctionInfo info) {
        Schema schema = ArrowSchemaCodec.deserializeSchema(info.arguments());
        return schema == null ? List.of() : schema.getFields();
    }

    private static boolean isTrue(Field field, String key) {
        Map<String, String> md = field.getMetadata();
        return md != null && "true".equals(md.get(key));
    }

    private static String describeArity(FunctionInfo info) {
        List<Field> fields = argFields(info);
        boolean hasVarargs = !fields.isEmpty() && isTrue(fields.get(fields.size() - 1), "vgi_varargs");
        return hasVarargs ? (fields.size() - 1) + "+" : String.valueOf(fields.size());
    }
}
