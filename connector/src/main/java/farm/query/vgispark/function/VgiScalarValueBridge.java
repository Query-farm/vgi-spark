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
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * One-value read/write bridge between a Spark {@link InternalRow} column and
 * an Arrow vector cell — the scalar-function analog of what {@code
 * ArrowColumnVector} does generically for a whole batch on the table-scan
 * read path. Needed here because a scalar function's argument write and
 * result read are each exactly one value at a time, with no {@code
 * ColumnVector} wrapper Spark provides for that shape.
 *
 * <p>Covers the same core scalar types {@link
 * farm.query.vgispark.types.VgiTypeMapping} maps for table columns, PLUS
 * (roadmap tier 2 item 7a) {@code struct}/{@code list}/{@code
 * fixed_size_list}, recursively — a {@code map} argument/return is still out
 * of scope (no real fixture needs one; {@code isSupported} still refuses it).
 * Struct/list VALUES route through {@link InternalRow#get(int, DataType)}/
 * {@link ArrayData#get(int, DataType)} — Spark's own generic, type-dispatched
 * accessors — rather than this class hand-rolling a parallel dispatch for
 * every primitive a SECOND time once nesting is involved; the top-level
 * {@link #writeAt}/{@link #read} entry points keep their original direct-typed-
 * getter shape for the non-nested case (unchanged from before this item),
 * since {@code InternalRow.get(ordinal, dataType)} is documented to resolve to
 * exactly those same typed getters internally — behaviorally identical, just
 * reached two different ways depending on nesting depth.
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
        writeValue(vector, type, row.get(ordinal, type), targetIndex);
    }

    /**
     * Write an already-extracted value into row {@code targetIndex} of
     * {@code vector} — {@link #writeAt}'s recursive core, also used directly
     * for a struct's child value / a list's element value, each of which is
     * an already-boxed Catalyst value ({@link InternalRow} for a nested
     * struct, {@link ArrayData} for a nested list) with no {@code InternalRow}
     * column/ordinal of its own to read from.
     */
    private static void writeValue(FieldVector vector, DataType type, Object value, int targetIndex) {
        if (value == null) {
            vector.setNull(targetIndex);
            return;
        }
        switch (vector) {
            case BitVector v -> v.setSafe(targetIndex, ((Boolean) value) ? 1 : 0);
            case TinyIntVector v -> v.setSafe(targetIndex, (Byte) value);
            case SmallIntVector v -> v.setSafe(targetIndex, (Short) value);
            case IntVector v -> v.setSafe(targetIndex, (Integer) value);
            case BigIntVector v -> v.setSafe(targetIndex, (Long) value);
            case Float4Vector v -> v.setSafe(targetIndex, (Float) value);
            case Float8Vector v -> v.setSafe(targetIndex, (Double) value);
            case VarCharVector v -> v.setSafe(targetIndex, ((UTF8String) value).getBytes());
            case VarBinaryVector v -> v.setSafe(targetIndex, (byte[]) value);
            case DateDayVector v -> v.setSafe(targetIndex, (Integer) value);
            case TimeStampMicroVector v -> v.setSafe(targetIndex, (Long) value);
            case DecimalVector v -> v.setSafe(targetIndex, ((Decimal) value).toJavaBigDecimal());
            case ListVector v -> writeList(v, (ArrayType) type, (ArrayData) value, targetIndex);
            case FixedSizeListVector v -> writeFixedSizeList(v, (ArrayType) type, (ArrayData) value, targetIndex);
            case StructVector v -> writeStruct(v, (StructType) type, (InternalRow) value, targetIndex);
            default -> throw new UnsupportedOperationException(
                    "no scalar-function argument writer for Arrow vector type " + vector.getClass().getSimpleName());
        }
    }

    /**
     * A {@code ListVector} cell: Arrow's variable-length-list write protocol
     * — {@code startNewValue} reserves the next contiguous run in the shared
     * data vector, elements are written into that run, {@code endValue}
     * records how many. Growing {@code data} manually (rather than relying on
     * {@code setSafe}, which {@code ListVector} itself has no equivalent of)
     * mirrors {@code farm.query.vgi.internal.VectorScalarCodec}'s own
     * {@code writeList} — the worker-side wire-format precedent for this
     * exact Arrow write pattern (not directly reusable from here: that class
     * lives in an {@code internal} package this module can't depend on, and
     * works off plain {@code List}/{@code Map} values rather than Spark's
     * {@code ArrayData}/{@code InternalRow}).
     */
    private static void writeList(ListVector lv, ArrayType type, ArrayData arr, int targetIndex) {
        DataType elementType = type.elementType();
        int startOffset = lv.startNewValue(targetIndex);
        FieldVector data = lv.getDataVector();
        int n = arr.numElements();
        int needed = startOffset + n;
        while (data.getValueCapacity() < needed) data.reAlloc();
        for (int i = 0; i < n; i++) {
            writeValue(data, elementType, arr.isNullAt(i) ? null : arr.get(i, elementType), startOffset + i);
        }
        lv.endValue(targetIndex, n);
        if (needed > data.getValueCount()) data.setValueCount(needed);
    }

    /**
     * A {@code FixedSizeListVector} cell: unlike {@link ListVector}, every
     * entry occupies exactly {@code getListSize()} data-vector slots at a
     * FIXED offset ({@code targetIndex * width}) — no {@code startNewValue}/
     * {@code endValue} bookkeeping. {@code arr} is trusted to have exactly
     * {@code width} elements (VGI's own worker fixtures guarantee this for a
     * fixed-size-list-typed argument/return; a mismatched count would
     * silently misalign rather than error, so this isn't attempted for a
     * dynamically-shaped list — only ever reached for a statically
     * fixed-width Arrow field).
     */
    private static void writeFixedSizeList(FixedSizeListVector lv, ArrayType type, ArrayData arr, int targetIndex) {
        DataType elementType = type.elementType();
        int width = lv.getListSize();
        FieldVector data = lv.getDataVector();
        int start = targetIndex * width;
        int needed = start + width;
        while (data.getValueCapacity() < needed) data.reAlloc();
        int n = arr.numElements();
        for (int i = 0; i < width; i++) {
            Object element = (i < n && !arr.isNullAt(i)) ? arr.get(i, elementType) : null;
            writeValue(data, elementType, element, start + i);
        }
        lv.setNotNull(targetIndex);
        if (needed > data.getValueCount()) data.setValueCount(needed);
    }

    /**
     * A {@code StructVector} cell: each child field is its own separate
     * {@code FieldVector}, looked up by NAME (matching {@code
     * VectorScalarCodec.writeStruct}'s own precedent) — safe because {@code
     * type}'s field names were themselves derived from this same Arrow
     * struct's child names via {@code VgiTypeMapping.toSparkType}, so the two
     * always agree.
     */
    private static void writeStruct(StructVector sv, StructType type, InternalRow struct, int targetIndex) {
        StructField[] childFields = type.fields();
        for (int i = 0; i < childFields.length; i++) {
            StructField cf = childFields[i];
            FieldVector childVector = sv.getChild(cf.name());
            writeValue(childVector, cf.dataType(),
                    struct.isNullAt(i) ? null : struct.get(i, cf.dataType()), targetIndex);
        }
        sv.setIndexDefined(targetIndex);
    }

    /**
     * Read row 0 of {@code vector} as the boxed value {@code
     * ScalarFunction#produceResult} should return for a column of Spark type
     * {@code type} — the Catalyst-internal representation ({@link UTF8String}
     * for strings, {@link InternalRow} for a struct, {@link ArrayData} for a
     * list — not {@link String}/{@link java.util.List}), since that's what
     * {@link InternalRow} accessors elsewhere in the engine expect back.
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
            case ListVector v -> readList(v, (ArrayType) type, row);
            case FixedSizeListVector v -> readFixedSizeList(v, (ArrayType) type, row);
            case StructVector v -> readStruct(v, (StructType) type, row);
            default -> throw new UnsupportedOperationException(
                    "no scalar-function result reader for Arrow vector type " + vector.getClass().getSimpleName());
        };
    }

    private static ArrayData readList(ListVector lv, ArrayType type, int row) {
        DataType elementType = type.elementType();
        int start = lv.getElementStartIndex(row);
        int end = lv.getElementEndIndex(row);
        FieldVector data = lv.getDataVector();
        Object[] elements = new Object[end - start];
        for (int i = start; i < end; i++) {
            elements[i - start] = data.isNull(i) ? null : read(data, elementType, i);
        }
        return new GenericArrayData(elements);
    }

    private static ArrayData readFixedSizeList(FixedSizeListVector lv, ArrayType type, int row) {
        DataType elementType = type.elementType();
        int width = lv.getListSize();
        int start = row * width;
        FieldVector data = lv.getDataVector();
        Object[] elements = new Object[width];
        for (int i = 0; i < width; i++) {
            elements[i] = data.isNull(start + i) ? null : read(data, elementType, start + i);
        }
        return new GenericArrayData(elements);
    }

    private static InternalRow readStruct(StructVector sv, StructType type, int row) {
        StructField[] childFields = type.fields();
        Object[] values = new Object[childFields.length];
        for (int i = 0; i < childFields.length; i++) {
            FieldVector childVector = sv.getChild(childFields[i].name());
            values[i] = childVector.isNull(row) ? null : read(childVector, childFields[i].dataType(), row);
        }
        return new GenericInternalRow(values);
    }

    /**
     * Read {@code row}'s column {@code ordinal} as a PLAIN Java value (a
     * boxed {@code Boolean}/{@code Number}/{@code String}/{@code byte[]}/
     * {@code BigDecimal}/{@link Map}/{@link java.util.List}, not a
     * Catalyst-internal shape) — what {@code
     * farm.query.vgi.client.ScalarValue#of(ArrowType, Object)} /
     * {@code VectorScalarCodec.write} expect, needed for a {@code vgi_const}
     * argument's VALUE (not just its type) to travel into {@code
     * BindRequest.arguments} rather than a per-row Arrow vector cell. Covers
     * the same types {@link #isSupported} accepts — a struct becomes a {@link
     * LinkedHashMap} (field name → recursively-plain child value, matching
     * {@code VectorScalarCodec.writeStruct}'s own expected shape) and a list
     * becomes a plain {@link java.util.List} (matching {@code writeList}'s).
     *
     * @return the value, or {@code null} if {@code row.isNullAt(ordinal)}
     */
    static Object readPlainValue(InternalRow row, int ordinal, DataType type) {
        if (row.isNullAt(ordinal)) return null;
        return toPlainValue(row.get(ordinal, type), type);
    }

    private static Object toPlainValue(Object value, DataType type) {
        if (value == null) return null;
        if (type.equals(DataTypes.StringType)) return value.toString(); // UTF8String -> String
        if (type instanceof DecimalType) return ((Decimal) value).toJavaBigDecimal();
        if (type instanceof StructType st) {
            InternalRow struct = (InternalRow) value;
            StructField[] fields = st.fields();
            Map<String, Object> out = new LinkedHashMap<>();
            for (int i = 0; i < fields.length; i++) {
                out.put(fields[i].name(),
                        struct.isNullAt(i) ? null : toPlainValue(struct.get(i, fields[i].dataType()), fields[i].dataType()));
            }
            return out;
        }
        if (type instanceof ArrayType at) {
            ArrayData arr = (ArrayData) value;
            DataType elementType = at.elementType();
            java.util.List<Object> out = new java.util.ArrayList<>(arr.numElements());
            for (int i = 0; i < arr.numElements(); i++) {
                out.add(arr.isNullAt(i) ? null : toPlainValue(arr.get(i, elementType), elementType));
            }
            return out;
        }
        // DateDayVector/TimeStampMicroVector's write() accepts a plain epoch-day/epoch-micros
        // Number just as readily as a java.time value — see VectorScalarCodec.write. Every other
        // remaining type (bool/byte/short/int/long/float/double/binary) is already the right
        // plain shape as returned by InternalRow.get(ordinal, type) — Boolean/Byte/Short/
        // Integer/Long/Float/Double/byte[] respectively.
        return value;
    }

    /** @return whether {@code type} is one {@link #write}/{@link #read} can bridge. */
    static boolean isSupported(DataType type) {
        if (type.equals(DataTypes.BooleanType) || type.equals(DataTypes.ByteType)
                || type.equals(DataTypes.ShortType) || type.equals(DataTypes.IntegerType)
                || type.equals(DataTypes.LongType) || type.equals(DataTypes.FloatType)
                || type.equals(DataTypes.DoubleType) || type.equals(DataTypes.StringType)
                || type.equals(DataTypes.BinaryType) || type.equals(DataTypes.DateType)
                || type.equals(DataTypes.TimestampType) || type.equals(DataTypes.TimestampNTZType)
                || type instanceof DecimalType) {
            return true;
        }
        if (type instanceof StructType st) {
            for (StructField f : st.fields()) if (!isSupported(f.dataType())) return false;
            return true;
        }
        if (type instanceof ArrayType at) {
            return isSupported(at.elementType());
        }
        return false;
    }
}
