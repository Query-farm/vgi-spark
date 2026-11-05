// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgispark.scan;

import farm.query.vgi.protocol.FieldPath;
import farm.query.vgi.protocol.KeyGrouping;
import farm.query.vgi.protocol.TransformSpec;
import farm.query.vgirpc.marshal.RecordCodec;
import farm.query.vgispark.types.VgiTypeMapping;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
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
        List<Field> logical = List.of(VgiTypeMapping.toArrowField("a.b", nested, true));
        Expression[] keys = VgiScan.decodeGrouping(
                grouping, new Schema(logical), logical, output).expressions();

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
        List<Field> logical = List.of(VgiTypeMapping.toArrowField("id", DataTypes.LongType, true));
        assertNull(VgiScan.decodeGrouping(grouping, new Schema(logical), logical, output));
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
        List<Field> authoritative = List.of(
                VgiTypeMapping.toArrowField("id", DataTypes.LongType, true),
                VgiTypeMapping.toArrowField("name", DataTypes.StringType, true));
        assertNull(VgiScan.decodeGrouping(
                grouping, new Schema(authoritative), authoritative, projectedOutput));
    }

    @Test
    void authoritativeWorkerNameMapsPositionallyToLogicalCatalogName() {
        byte[] path = RecordCodec.serializeToBytes(new FieldPath(List.of("worker_id")));
        byte[] transform = RecordCodec.serializeToBytes(
                new TransformSpec("vgi.identity/1", List.of(path), null));
        byte[] grouping = RecordCodec.serializeToBytes(new KeyGrouping(List.of(transform)));
        List<Field> authoritative = List.of(
                VgiTypeMapping.toArrowField("worker_id", DataTypes.LongType, true));
        List<Field> logical = List.of(
                VgiTypeMapping.toArrowField("catalog_id", DataTypes.LongType, true));
        StructType output = DataTypes.createStructType(new org.apache.spark.sql.types.StructField[] {
                DataTypes.createStructField("catalog_id", DataTypes.LongType, true)
        });

        VgiScan.DecodedGrouping decoded = VgiScan.decodeGrouping(
                grouping, new Schema(authoritative), logical, output);

        assertEquals("catalog_id", decoded.expressions()[0].describe());
    }

    @Test
    void maxWorkersCoalescesEveryTokenWithoutDroppingWork() {
        List<VgiInputPartition> partitions = java.util.stream.IntStream.range(0, 5)
                .mapToObj(i -> new VgiInputPartition(
                        new byte[] {1}, null, new byte[] {2}, null,
                        List.of(new byte[] {(byte) i}), 10L, new String[] {"host" + i}))
                .toList();

        VgiScan.CoalescedPartitions coalesced = VgiScan.coalesceForMaxWorkers(
                partitions, java.util.Collections.nCopies(5, null), 2, false);

        assertEquals(2, coalesced.partitions().size());
        assertEquals(5, coalesced.partitions().stream().mapToInt(p -> p.tokens().size()).sum());
        assertEquals(List.of(0, 2, 4, 1, 3), coalesced.partitions().stream()
                .flatMap(p -> p.tokens().stream())
                .map(token -> Byte.toUnsignedInt(token[0]))
                .toList());
        assertEquals(50L, coalesced.partitions().stream()
                .mapToLong(VgiInputPartition::estimatedBytes).sum());
    }
}
