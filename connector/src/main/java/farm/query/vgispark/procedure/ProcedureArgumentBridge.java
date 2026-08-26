// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgispark.procedure;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;

/**
 * Read one {@code CALL} argument's value out of the {@link InternalRow}
 * {@link org.apache.spark.sql.connector.catalog.procedures.BoundProcedure
 * #call} receives, as a plain Java object suitable for {@code
 * ScalarValue.of(ArrowType, Object)}/{@code ArgumentsEncoder}.
 *
 * <p>A deliberately small, separate type list from {@code
 * VgiScalarValueBridge} (that class bridges {@code FieldVector} cells, not
 * {@code InternalRow} columns directly, and is package-private to {@code
 * function} anyway) — v1 scope: the primitive scalar types VGI's own
 * argument-value wire form ({@code ScalarValue}/{@code VectorScalarCodec})
 * already accepts as plain boxed numbers/strings/bytes. Date/timestamp/
 * decimal/struct/list arguments are refused at {@code tryBuild} time (see
 * {@code VgiUnboundTableProcedure}), not attempted here.
 */
final class ProcedureArgumentBridge {

    private ProcedureArgumentBridge() {}

    /** @return {@code true} if {@link #read} can handle a column of Spark type {@code type} */
    static boolean isSupported(DataType type) {
        return type instanceof BooleanType
                || type instanceof ByteType
                || type instanceof ShortType
                || type instanceof IntegerType
                || type instanceof LongType
                || type instanceof FloatType
                || type instanceof DoubleType
                || type instanceof StringType
                || type instanceof BinaryType;
    }

    /**
     * @param row the argument row {@code call()} received
     * @param ordinal which argument
     * @param type the Spark type {@link #isSupported} already confirmed for this ordinal
     * @return the value as a plain Java object; the caller has already
     *         confirmed {@code !row.isNullAt(ordinal)}
     */
    static Object read(InternalRow row, int ordinal, DataType type) {
        if (type instanceof BooleanType) return row.getBoolean(ordinal);
        if (type instanceof ByteType) return row.getByte(ordinal);
        if (type instanceof ShortType) return row.getShort(ordinal);
        if (type instanceof IntegerType) return row.getInt(ordinal);
        if (type instanceof LongType) return row.getLong(ordinal);
        if (type instanceof FloatType) return row.getFloat(ordinal);
        if (type instanceof DoubleType) return row.getDouble(ordinal);
        if (type instanceof StringType) return row.getUTF8String(ordinal).toString();
        if (type instanceof BinaryType) return row.getBinary(ordinal);
        throw new IllegalStateException("no CALL-argument reader for Spark type " + type
                + " — isSupported() should have refused this at discovery time");
    }
}
