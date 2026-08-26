// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgispark.types;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.List;

/**
 * Arrow → Spark {@link DataType} mapping.
 *
 * <p>Spark's own internal Arrow↔Catalyst converter
 * ({@code org.apache.spark.sql.util.ArrowUtils}) is not public API, so this is
 * a self-contained mapper — the same reason {@code vgi-trino}'s
 * {@code VgiTypeMapping} exists rather than depending on some Trino-internal
 * equivalent. Reading the actual column values needs no per-type code at all:
 * {@link org.apache.spark.sql.vectorized.ArrowColumnVector} wraps an Arrow
 * {@code ValueVector} directly and already knows how to read every shape this
 * class maps to (including nested struct/list/map), which is the whole reason
 * a Spark connector's type-mapping surface is smaller than an equivalent
 * Trino one (which must hand-build a {@code Block} per column).
 *
 * <p>Covers what VGI's own {@code farm.query.vgi.types.Schemas} helper and the
 * common declarative-catalog column types actually produce: signed/unsigned
 * integers up to 64 bits (widened to the next signed Spark width; VGI's own
 * {@code UInt64} has no exact Spark counterpart and maps to {@code LongType}
 * with a documented wraparound caveat, exactly as vgi-trino's mapper does),
 * both float widths, UTF-8 strings, binary, booleans, dates, timestamps both
 * with and without a time zone (Spark, unlike Trino's connector SPI as VGI
 * uses it, has first-class types for both — {@link DataTypes#TimestampType}
 * and {@link DataTypes#TimestampNTZType}), 128-bit decimals, and arbitrarily
 * nested {@code Struct}/{@code List}/{@code FixedSizeList}/{@code Map}. Arrow
 * types with no mapping here (half-precision floats, duration, union) throw
 * {@link UnsupportedOperationException} rather than silently truncating or
 * mis-typing a column.
 */
public final class VgiTypeMapping {

    private VgiTypeMapping() {}

    /**
     * Map one Arrow field to its Spark {@link DataType}.
     *
     * @param field the Arrow field (its {@link ArrowType} plus, for decimals,
     *        precision/scale)
     * @return the corresponding Spark type
     * @throws UnsupportedOperationException if this field's Arrow type has no
     *         mapping (see the class javadoc for what's covered)
     */
    public static DataType toSparkType(Field field) {
        ArrowType type = field.getType();
        return switch (type.getTypeID()) {
            case Bool -> DataTypes.BooleanType;
            case Int -> {
                ArrowType.Int i = (ArrowType.Int) type;
                yield switch (i.getBitWidth()) {
                    case 8 -> i.getIsSigned() ? DataTypes.ByteType : DataTypes.ShortType;
                    case 16 -> i.getIsSigned() ? DataTypes.ShortType : DataTypes.IntegerType;
                    case 32 -> i.getIsSigned() ? DataTypes.IntegerType : DataTypes.LongType;
                    // Signed 64-bit is exact; unsigned 64-bit has no exact Spark
                    // type and LongType is the closest fit, exact for every value
                    // <= Long.MAX_VALUE (the vast majority of real row counts /
                    // ids) — a value above that wraps negative on the Spark side.
                    default -> DataTypes.LongType;
                };
            }
            case FloatingPoint -> switch (((ArrowType.FloatingPoint) type).getPrecision()) {
                case SINGLE -> DataTypes.FloatType;
                case DOUBLE -> DataTypes.DoubleType;
                default -> throw unsupported(type, field.getName());
            };
            case Utf8, LargeUtf8 -> DataTypes.StringType;
            case Binary, LargeBinary, FixedSizeBinary -> DataTypes.BinaryType;
            case Date -> DataTypes.DateType;
            case Timestamp -> {
                ArrowType.Timestamp ts = (ArrowType.Timestamp) type;
                // Spark's TimestampType is always session-local-timezone semantics
                // regardless of which zone the Arrow field itself names — the zone
                // only distinguishes "has one" (-> TimestampType) from "naive"
                // (-> TimestampNTZType); it is not otherwise consulted.
                yield ts.getTimezone() != null ? DataTypes.TimestampType : DataTypes.TimestampNTZType;
            }
            case Decimal -> {
                ArrowType.Decimal d = (ArrowType.Decimal) type;
                if (d.getBitWidth() != 128) {
                    throw new UnsupportedOperationException("column '" + field.getName()
                            + "': only 128-bit decimals are supported, got " + d.getBitWidth() + "-bit");
                }
                yield DataTypes.createDecimalType(d.getPrecision(), d.getScale());
            }
            case Struct -> {
                List<Field> children = field.getChildren();
                StructField[] fields = new StructField[children.size()];
                for (int i = 0; i < children.size(); i++) {
                    Field child = children.get(i);
                    fields[i] = DataTypes.createStructField(child.getName(), toSparkType(child), child.isNullable());
                }
                yield DataTypes.createStructType(fields);
            }
            // A list's single child field carries the element type and its own
            // nullability; its own name (conventionally "item") carries no
            // meaning Spark's ArrayType preserves.
            case List, LargeList, FixedSizeList -> {
                Field element = field.getChildren().get(0);
                yield DataTypes.createArrayType(toSparkType(element), element.isNullable());
            }
            // A Map's single child is a non-nullable "entries" struct of exactly
            // two fields, conventionally named "key"/"value" — Arrow's own
            // MapVector layout, mirrored by ArrowColumnVector.getMap().
            case Map -> {
                Field entries = field.getChildren().get(0);
                List<Field> kv = entries.getChildren();
                Field keyField = kv.get(0);
                Field valueField = kv.get(1);
                yield DataTypes.createMapType(
                        toSparkType(keyField), toSparkType(valueField), valueField.isNullable());
            }
            case Null -> DataTypes.NullType;
            default -> throw unsupported(type, field.getName());
        };
    }

    /**
     * Map an Arrow {@link org.apache.arrow.vector.types.pojo.Schema} to a
     * Spark {@link StructType}, in field order.
     *
     * @param fields the Arrow fields, typically {@code schema.getFields()}
     * @return the corresponding Spark schema
     */
    public static StructType toSparkSchema(List<Field> fields) {
        StructField[] out = new StructField[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            out[i] = DataTypes.createStructField(
                    VgiColumnNames.displayName(field), toSparkType(field), field.isNullable());
        }
        return DataTypes.createStructType(out);
    }

    /**
     * Map one Spark {@link DataType} to an Arrow {@link Field} — the reverse
     * of {@link #toSparkType(Field)}, needed only for the one case where a
     * VGI scalar-function ARGUMENT is declared {@code vgi_type=="any"} (a
     * dynamic/generic type resolved per call site): {@code
     * UnboundFunction#bind(StructType)} already receives the call site's
     * real, concrete Spark argument type, so rather than refusing statically
     * this builds the Arrow field for that one call site's {@code
     * BindRequest.input_schema} on demand.
     *
     * <p>Deliberately narrower than {@link #toSparkType}'s full range: only
     * the primitive types {@code VgiScalarValueBridge} can actually read/
     * write one-value-at-a-time (bool/byte/short/int/long/float/double/
     * string/binary/date/timestamp/decimal) — the same set {@code
     * VgiScalarValueBridge.isSupported} accepts. Struct/list/map arguments
     * are out of scope for scalar functions in v1 regardless of whether they
     * are statically or dynamically typed, so this method doesn't need to
     * handle them.
     *
     * @throws UnsupportedOperationException if {@code type} isn't one of the
     *         primitive types listed above
     */
    public static Field toArrowField(String name, DataType type, boolean nullable) {
        ArrowType arrowType;
        if (type.equals(DataTypes.BooleanType)) {
            arrowType = new ArrowType.Bool();
        } else if (type.equals(DataTypes.ByteType)) {
            arrowType = new ArrowType.Int(8, true);
        } else if (type.equals(DataTypes.ShortType)) {
            arrowType = new ArrowType.Int(16, true);
        } else if (type.equals(DataTypes.IntegerType)) {
            arrowType = new ArrowType.Int(32, true);
        } else if (type.equals(DataTypes.LongType)) {
            arrowType = new ArrowType.Int(64, true);
        } else if (type.equals(DataTypes.FloatType)) {
            arrowType = new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
        } else if (type.equals(DataTypes.DoubleType)) {
            arrowType = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
        } else if (type.equals(DataTypes.StringType)) {
            arrowType = new ArrowType.Utf8();
        } else if (type.equals(DataTypes.BinaryType)) {
            arrowType = new ArrowType.Binary();
        } else if (type.equals(DataTypes.DateType)) {
            arrowType = new ArrowType.Date(DateUnit.DAY);
        } else if (type.equals(DataTypes.TimestampType)) {
            // Spark's TimestampType is session-local-timezone semantics; any
            // non-null zone name round-trips through toSparkType's "has a
            // zone -> TimestampType" check the same way, so the zone value
            // itself carries no further meaning here.
            arrowType = new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC");
        } else if (type.equals(DataTypes.TimestampNTZType)) {
            arrowType = new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
        } else if (type instanceof DecimalType dt) {
            arrowType = new ArrowType.Decimal(dt.precision(), dt.scale(), 128);
        } else {
            throw new UnsupportedOperationException(
                    "argument '" + name + "': no Arrow mapping for Spark type " + type.typeName()
                            + " (only the primitive types a scalar-function call can bridge one value at a time)");
        }
        return new Field(name, new FieldType(nullable, arrowType, null), null);
    }

    private static UnsupportedOperationException unsupported(ArrowType type, String columnName) {
        return new UnsupportedOperationException(
                "column '" + columnName + "': no Spark mapping for Arrow type " + type);
    }
}
