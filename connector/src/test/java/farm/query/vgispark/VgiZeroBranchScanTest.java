// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgispark;

import farm.query.vgi.internal.SchemaUtil;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Protocol 3.2's zero-branch rule, no worker involved: an empty {@code
 * ScanBranchesResult} is an empty scan for a {@code scan_planning =
 * "filtered"} table and still a protocol error for a {@code "bind"} one.
 */
class VgiZeroBranchScanTest {

    @Test
    void zeroBranchesAreAcceptedForAFilteredTable() {
        VgiCatalog.requireBranches("data.t", "filtered", 0);
        VgiCatalog.requireBranches("data.t", "bind", 1);
    }

    @Test
    void zeroBranchesAreStillAnErrorForABindTable() {
        IllegalStateException bind = assertThrows(IllegalStateException.class,
                () -> VgiCatalog.requireBranches("data.t", "bind", 0));
        assertTrue(bind.getMessage().contains("zero scan branches"), bind.getMessage());
        // TableInfo normalizes a missing (pre-3.2) scan_planning to "bind"; null reads the same.
        assertThrows(IllegalStateException.class, () -> VgiCatalog.requireBranches("data.t", null, 0));
    }

    /** A filtered table with zero branches keeps its declared columns and plans no input partitions. */
    @Test
    void aTableWithNoBranchesPlansNoPartitions() {
        Schema schema = new Schema(List.of(
                new Field("event_date", FieldType.nullable(new ArrowType.Date(
                        org.apache.arrow.vector.types.DateUnit.DAY)), null),
                new Field("n", FieldType.nullable(new ArrowType.Int(64, true)), null)));
        VgiCatalogConfig config = VgiCatalogConfig.fromOptions(new CaseInsensitiveStringMap(
                Map.of("location", "unix:///nonexistent.sock", "catalog-name", "example")));
        VgiTable table = new VgiTable(List.of("data"), "planned_days", List.of(),
                SchemaUtil.serializeSchema(schema), null, null, null, List.of(), null, config);

        assertEquals(2, table.columns().length);
        Scan scan = table.newScanBuilder(CaseInsensitiveStringMap.empty()).build();
        assertEquals(2, scan.readSchema().fields().length);
        assertEquals(0, scan.toBatch().planInputPartitions().length);
    }
}
