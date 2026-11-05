// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgispark.scan;

import farm.query.vgi.protocol.FieldPath;
import farm.query.vgi.protocol.KeyGrouping;
import farm.query.vgi.protocol.TransformSpec;
import farm.query.vgirpc.marshal.RecordCodec;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** Conservative protocol-3 to Spark partition-transform mapping tests. */
class VgiPartitioningTest {

    @Test
    void identityGroupingPreservesRawFieldPathComponents() {
        byte[] path = RecordCodec.serializeToBytes(new FieldPath(List.of("a.b", "tick`name")));
        byte[] transform = RecordCodec.serializeToBytes(
                new TransformSpec("vgi.identity/1", List.of(path), null));
        byte[] grouping = RecordCodec.serializeToBytes(new KeyGrouping(List.of(transform)));

        StructType nested = DataTypes.createStructType(new org.apache.spark.sql.types.StructField[] {
                DataTypes.createStructField("tick`name", DataTypes.LongType, true)
        });
        StructType output = DataTypes.createStructType(new org.apache.spark.sql.types.StructField[] {
                DataTypes.createStructField("a.b", nested, true)
        });
        Expression[] keys = VgiScan.decodeGrouping(grouping, output).expressions();

        assertEquals(1, keys.length);
        assertEquals("`a.b`.`tick``name`", keys[0].describe());
    }

    @Test
    void unsupportedOrParameterizedTransformsDowngradeToUnknown() {
        byte[] path = RecordCodec.serializeToBytes(new FieldPath(List.of("id")));
        byte[] bucket = RecordCodec.serializeToBytes(
                new TransformSpec("iceberg.bucket/1", List.of(path), new byte[] {1}));
        byte[] grouping = RecordCodec.serializeToBytes(new KeyGrouping(List.of(bucket)));

        StructType output = DataTypes.createStructType(new org.apache.spark.sql.types.StructField[] {
                DataTypes.createStructField("id", DataTypes.LongType, true)
        });
        assertNull(VgiScan.decodeGrouping(grouping, output));
    }

    @Test
    void groupingOnAProjectedOutFieldDowngradesToUnknown() {
        byte[] path = RecordCodec.serializeToBytes(new FieldPath(List.of("id")));
        byte[] transform = RecordCodec.serializeToBytes(
                new TransformSpec("vgi.identity/1", List.of(path), null));
        byte[] grouping = RecordCodec.serializeToBytes(new KeyGrouping(List.of(transform)));

        StructType projectedOutput = DataTypes.createStructType(new org.apache.spark.sql.types.StructField[] {
                DataTypes.createStructField("name", DataTypes.StringType, true)
        });
        assertNull(VgiScan.decodeGrouping(grouping, projectedOutput));
    }
}
