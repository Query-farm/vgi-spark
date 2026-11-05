// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgispark.scan;

import farm.query.vgirpc.wire.Allocators;
import farm.query.vgirpc.wire.IpcStreamReader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.vectorized.ArrowColumnVector;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.sql.types.DataType;

import farm.query.vgispark.types.VgiTypeMapping;

import java.io.ByteArrayInputStream;

/** Decodes the one-row Arrow key tuple carried by a protocol-3 scan split. */
final class PartitionKeyDecoder {
    private PartitionKeyDecoder() {}

    static InternalRow decode(byte[] bytes, DataType[] expectedTypes) {
        if (bytes == null) return null;
        int expectedWidth = expectedTypes.length;
        try (IpcStreamReader reader = new IpcStreamReader(
                new ByteArrayInputStream(bytes), Allocators.root())) {
            if (reader.readNextBatch() == null) {
                throw new IllegalArgumentException("partition key IPC has no batch");
            }
            VectorSchemaRoot root = reader.root();
            if (root.getRowCount() != 1 || root.getFieldVectors().size() != expectedWidth) {
                throw new IllegalArgumentException("partition key shape is "
                        + root.getRowCount() + "x" + root.getFieldVectors().size()
                        + ", expected 1x" + expectedWidth);
            }
            ColumnVector[] columns = new ColumnVector[expectedWidth];
            for (int i = 0; i < expectedWidth; i++) {
                DataType actualType = VgiTypeMapping.toSparkType(root.getSchema().getFields().get(i));
                if (!actualType.equals(expectedTypes[i])) {
                    throw new IllegalArgumentException("partition key column " + i
                            + " has type " + actualType + ", expected " + expectedTypes[i]);
                }
                columns[i] = new ArrowColumnVector(root.getVector(i));
            }
            try (ColumnarBatch batch = new ColumnarBatch(columns, 1)) {
                InternalRow key = batch.getRow(0).copy();
                if (reader.readNextBatch() != null) {
                    throw new IllegalArgumentException("partition key IPC has more than one batch");
                }
                return key;
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("invalid partition key IPC", e);
        }
    }
}
