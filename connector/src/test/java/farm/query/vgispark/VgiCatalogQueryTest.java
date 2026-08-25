// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgispark;

import farm.query.vgispark.testing.VgiWorkerHarness;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end: a real local {@link SparkSession}, a real {@link VgiCatalog}
 * registered against it, and a real Python fixture worker subprocess behind
 * that — catalog/schema/table discovery, bind, {@code table_function_plan}
 * (the not-split-capable sentinel path — {@code numbers} is backed by
 * {@code sequence}, which doesn't implement a real plan), and reading actual
 * row values back through a {@link org.apache.spark.sql.vectorized.ArrowColumnVector}
 * without going through Spark SQL's DataFrame API at all for value
 * comparison — that IS the object under test.
 *
 * <p>Mirrors the shape of {@code vgi-trino}'s own
 * {@code VgiConnectorQueryRunnerTest}, adapted to a local {@link SparkSession}
 * in place of a {@code DistributedQueryRunner}.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class VgiCatalogQueryTest {

    private static final File VGI_PYTHON = new File(System.getProperty("user.home"), "Development/vgi-python");
    private static final String CATALOG = "vgi_example";

    private VgiWorkerHarness.Handle worker;
    private SparkSession spark;

    @BeforeAll
    void start() throws Exception {
        Assumptions.assumeTrue(VGI_PYTHON.isDirectory(),
                "~/Development/vgi-python not present — skipping VGI catalog query test");
        worker = VgiWorkerHarness.subprocess(VGI_PYTHON);

        spark = SparkSession.builder()
                .master("local[2]")
                .appName("vgi-spark-catalog-query-test")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.catalog." + CATALOG, VgiCatalog.class.getName())
                .config("spark.sql.catalog." + CATALOG + ".location", worker.location())
                .config("spark.sql.catalog." + CATALOG + ".catalog-name", "example")
                .getOrCreate();
    }

    @AfterAll
    void stop() throws Exception {
        if (spark != null) spark.stop();
        if (worker != null) worker.teardown().close();
    }

    @Test
    @Timeout(60)
    void listsTheDataNamespace() {
        List<Row> namespaces = spark.sql("SHOW NAMESPACES IN " + CATALOG).collectAsList();
        List<String> names = namespaces.stream().map(r -> r.getString(0)).toList();
        assertTrue(names.contains("data"), "expected a 'data' namespace, got " + names);
    }

    @Test
    @Timeout(60)
    void listsTheNumbersTable() {
        List<Row> tables = spark.sql("SHOW TABLES IN " + CATALOG + ".data").collectAsList();
        List<String> names = tables.stream().map(r -> r.getString(1)).toList();
        assertTrue(names.contains("numbers"), "expected a 'numbers' table, got " + names);
    }

    @Test
    @Timeout(60)
    void readsRealRowsFromTheNumbersTable() {
        // The fixture worker's own catalog declares this table's column as
        // "value" (Table(name="numbers", columns=schema(value=pa.int64()), ...)
        // in vgi-python's worker.py) — distinct from the underlying
        // sequence() function's own output column name "n". This is a real
        // assertion about VgiTable faithfully reporting the WORKER's declared
        // schema, not the scan function's.
        Dataset<Row> df = spark.sql("SELECT * FROM " + CATALOG + ".data.numbers ORDER BY value");
        assertEquals(1, df.schema().fields().length, "expected exactly one column");
        assertEquals("value", df.schema().fields()[0].name());

        List<Row> rows = df.collectAsList();
        assertEquals(100, rows.size(), "sequence(100) should produce 100 rows");
        for (int i = 0; i < 100; i++) {
            assertEquals((long) i, rows.get(i).getLong(0), "row " + i + " mismatch");
        }
    }

    @Test
    @Timeout(60)
    void aggregatesCorrectly() {
        Row result = spark.sql("SELECT count(*) AS c, sum(value) AS s FROM " + CATALOG + ".data.numbers")
                .collectAsList().get(0);
        assertEquals(100L, result.getLong(0));
        assertEquals(4950L, result.getLong(1)); // sum(0..99)
    }

    @Test
    @Timeout(60)
    void filterPushdownReturnsCorrectRows() {
        // A real correctness check on VgiFilterTranslator/PushdownFiltersEncoder,
        // not just "the query didn't crash": if EQ/GT/AND translation or the
        // worker's own filter application were wrong in either direction (too
        // permissive OR too strict), this would return the wrong row set.
        List<Row> rows = spark.sql(
                "SELECT value FROM " + CATALOG + ".data.numbers WHERE value > 90 AND value <= 95 ORDER BY value")
                .collectAsList();
        assertEquals(5, rows.size());
        for (int i = 0; i < 5; i++) {
            assertEquals(91L + i, rows.get(i).getLong(0));
        }
    }

    @Test
    @Timeout(60)
    void projectionPushdownReturnsOnlyRequestedColumns() {
        // versioned_data declares two columns (id: int64, score: float64);
        // selecting only "id" must prune "score" from the wire, not merely
        // from the DataFrame after a full-width read — this is a real
        // assertion about SupportsPushDownRequiredColumns, not just about
        // Spark's own projection re-check after the fact.
        Dataset<Row> df = spark.sql("SELECT id FROM " + CATALOG + ".data.versioned_data ORDER BY id");
        assertEquals(1, df.schema().fields().length, "expected exactly one projected column");
        assertEquals("id", df.schema().fields()[0].name());

        List<Row> rows = df.collectAsList();
        assertTrue(!rows.isEmpty(), "expected at least one row from versioned_data");
    }
}
