// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgispark.function;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * One-value read/write bridge between a Spark {@link InternalRow} column and
 * an Arrow vector cell — the scalar-function analog of what {@code
 * ArrowColumnVector} does generically for a whole batch on the table-scan
 * read path. Needed here because a scalar function's argument write and
 * result read are each exactly one value at a time, with no {@code
 * ColumnVector} wrapper Spark provides for that shape.
 *
 * <p>Covers the same core scalar types {@link
 * farm.query.vgispark.types.VgiTypeMapping} maps for table columns, minus
 * struct/list/map/decimal — see {@code VgiUnboundScalarFunction}'s own
 * validation for why those are out of scope for v1 scalar functions (not
 * silently dropped: an argument or return of one of those types makes {@code
 * loadFunction} refuse the function outright, with a clear message).
 */
final class VgiScalarValueBridge {

    private VgiScalarValueBridge() {}

    /**
     * Write {@code row}'s column {@code ordinal} into row 0 of {@code vector},
     * or a null cell if the source column is null.
     */
    static void write(FieldVector vector, DataType type, InternalRow row, int ordinal) {
        writeAt(vector, type, row, ordinal, 0);
    }

    /**
     * Write {@code row}'s column {@code ordinal} into row {@code targetIndex}
     * of {@code vector} — the multi-row sibling of {@link #write} (which is
     * always {@code targetIndex == 0}), needed when building a real multi-row
     * batch (e.g. an aggregate's buffered group rows) rather than a
     * scalar-function call's single-row argument vector.
     */
    static void writeAt(FieldVector vector, DataType type, InternalRow row, int ordinal, int targetIndex) {
        if (row.isNullAt(ordinal)) {
            vector.setNull(targetIndex);
            return;
        }
        switch (vector) {
            case BitVector v -> v.setSafe(targetIndex, row.getBoolean(ordinal) ? 1 : 0);
            case TinyIntVector v -> v.setSafe(targetIndex, row.getByte(ordinal));
            case SmallIntVector v -> v.setSafe(targetIndex, row.getShort(ordinal));
            case IntVector v -> v.setSafe(targetIndex, row.getInt(ordinal));
            case BigIntVector v -> v.setSafe(targetIndex, row.getLong(ordinal));
            case Float4Vector v -> v.setSafe(targetIndex, row.getFloat(ordinal));
            case Float8Vector v -> v.setSafe(targetIndex, row.getDouble(ordinal));
            case VarCharVector v -> v.setSafe(targetIndex, row.getUTF8String(ordinal).getBytes());
            case VarBinaryVector v -> v.setSafe(targetIndex, row.getBinary(ordinal));
            case DateDayVector v -> v.setSafe(targetIndex, row.getInt(ordinal));
            case TimeStampMicroVector v -> v.setSafe(targetIndex, row.getLong(ordinal));
            case DecimalVector v -> {
                DecimalType dt = (DecimalType) type;
                v.setSafe(targetIndex, row.getDecimal(ordinal, dt.precision(), dt.scale()).toJavaBigDecimal());
            }
            default -> throw new UnsupportedOperationException(
                    "no scalar-function argument writer for Arrow vector type " + vector.getClass().getSimpleName());
        }
    }

    /**
     * Read row 0 of {@code vector} as the boxed value {@code
     * ScalarFunction#produceResult} should return for a column of Spark type
     * {@code type} — the Catalyst-internal representation ({@link UTF8String}
     * for strings, not {@link String}), since that's what {@link InternalRow}
     * accessors elsewhere in the engine expect back.
     */
    static Object read(FieldVector vector, DataType type, int row) {
        if (vector.isNull(row)) return null;
        return switch (vector) {
            case BitVector v -> v.get(row) != 0;
            case TinyIntVector v -> v.get(row);
            case SmallIntVector v -> v.get(row);
            case IntVector v -> v.get(row);
            case BigIntVector v -> v.get(row);
            case Float4Vector v -> v.get(row);
            case Float8Vector v -> v.get(row);
            case VarCharVector v -> UTF8String.fromBytes(v.get(row));
            case VarBinaryVector v -> v.get(row);
            case DateDayVector v -> v.get(row);
            case TimeStampMicroVector v -> v.get(row);
            case DecimalVector v -> {
                DecimalType dt = (DecimalType) type;
                yield Decimal.apply(v.getObject(row), dt.precision(), dt.scale());
            }
            default -> throw new UnsupportedOperationException(
                    "no scalar-function result reader for Arrow vector type " + vector.getClass().getSimpleName());
        };
    }

    /**
     * Read {@code row}'s column {@code ordinal} as a PLAIN Java value (a
     * boxed {@code Boolean}/{@code Number}/{@code String}/{@code byte[]}/
     * {@code BigDecimal}, not a Catalyst-internal shape) — what {@code
     * farm.query.vgi.client.ScalarValue#of(ArrowType, Object)} /
     * {@code VectorScalarCodec.write} expect, needed for a {@code vgi_const}
     * argument's VALUE (not just its type) to travel into {@code
     * BindRequest.arguments} rather than a per-row Arrow vector cell. Covers
     * the same types {@link #isSupported} accepts.
     *
     * @return the value, or {@code null} if {@code row.isNullAt(ordinal)}
     */
    static Object readPlainValue(InternalRow row, int ordinal, DataType type) {
        if (row.isNullAt(ordinal)) return null;
        if (type.equals(DataTypes.BooleanType)) return row.getBoolean(ordinal);
        if (type.equals(DataTypes.ByteType)) return row.getByte(ordinal);
        if (type.equals(DataTypes.ShortType)) return row.getShort(ordinal);
        if (type.equals(DataTypes.IntegerType)) return row.getInt(ordinal);
        if (type.equals(DataTypes.LongType)) return row.getLong(ordinal);
        if (type.equals(DataTypes.FloatType)) return row.getFloat(ordinal);
        if (type.equals(DataTypes.DoubleType)) return row.getDouble(ordinal);
        if (type.equals(DataTypes.StringType)) return row.getUTF8String(ordinal).toString();
        if (type.equals(DataTypes.BinaryType)) return row.getBinary(ordinal);
        // DateDayVector/TimeStampMicroVector's write() accepts a plain epoch-day/epoch-micros
        // Number just as readily as a java.time value — see VectorScalarCodec.write.
        if (type.equals(DataTypes.DateType)) return row.getInt(ordinal);
        if (type.equals(DataTypes.TimestampType) || type.equals(DataTypes.TimestampNTZType)) {
            return row.getLong(ordinal);
        }
        if (type instanceof DecimalType dt) {
            return row.getDecimal(ordinal, dt.precision(), dt.scale()).toJavaBigDecimal();
        }
        throw new UnsupportedOperationException(
                "no const-argument value reader for Spark type " + type
                        + " — isSupported() should have refused this at discovery time");
    }

    /** @return whether {@code type} is one {@link #write}/{@link #read} can bridge. */
    static boolean isSupported(DataType type) {
        return type.equals(DataTypes.BooleanType) || type.equals(DataTypes.ByteType)
                || type.equals(DataTypes.ShortType) || type.equals(DataTypes.IntegerType)
                || type.equals(DataTypes.LongType) || type.equals(DataTypes.FloatType)
                || type.equals(DataTypes.DoubleType) || type.equals(DataTypes.StringType)
                || type.equals(DataTypes.BinaryType) || type.equals(DataTypes.DateType)
                || type.equals(DataTypes.TimestampType) || type.equals(DataTypes.TimestampNTZType)
                || type instanceof DecimalType;
    }
}
