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
        // unix(), not subprocess() — see VgiSqlLogicTestSweepTest's identical
        // comment: avoids forking a fresh worker subprocess per Spark task.
        worker = VgiWorkerHarness.unix(VGI_PYTHON);

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

    /**
     * Spark's own catalog-introspection commands ({@code DESCRIBE TABLE},
     * {@code SHOW COLUMNS}, {@code SHOW FUNCTIONS}, {@code SHOW CATALOGS})
     * all work against {@code VgiCatalog} with zero connector-specific
     * code — they're generic Catalyst implementations that call exactly the
     * {@code TableCatalog}/{@code SupportsNamespaces}/{@code FunctionCatalog}
     * SPI methods this catalog already implements. Only DuckDB's OWN
     * introspection ({@code duckdb_*()}, {@code vgi_*()} — table-valued
     * functions with a DuckDB-specific row shape) has no Spark equivalent;
     * {@code DESCRIBE} itself is real Spark syntax that runs fine here, just
     * with Spark's 3-column shape (col_name, data_type, comment) rather than
     * DuckDB's 6-column one — see {@code docs/ROADMAP.md}'s corpus-overview
     * table and this test's own assertions below for the distinction.
     */
    @Test
    @Timeout(60)
    void describesAndListsCatalogMetadataViaSparksOwnCommands() {
        List<Row> described = spark.sql("DESCRIBE TABLE " + CATALOG + ".data.numbers").collectAsList();
        assertEquals(1, described.size());
        assertEquals("value", described.get(0).getString(0));
        assertEquals("bigint", described.get(0).getString(1));

        List<Row> columns = spark.sql("SHOW COLUMNS FROM " + CATALOG + ".data.numbers").collectAsList();
        assertEquals(List.of("value"), columns.stream().map(r -> r.getString(0)).toList());

        List<Row> functions = spark.sql("SHOW FUNCTIONS IN " + CATALOG + ".main").collectAsList();
        List<String> functionNames = functions.stream().map(r -> r.getString(0)).toList();
        assertTrue(functionNames.contains(CATALOG + ".main.global_scalar"),
                "expected a catalog scalar function in SHOW FUNCTIONS, got " + functionNames);

        List<Row> catalogs = spark.sql("SHOW CATALOGS").collectAsList();
        assertTrue(catalogs.stream().anyMatch(r -> CATALOG.equals(r.getString(0))),
                "expected '" + CATALOG + "' in SHOW CATALOGS, got " + catalogs);
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

    @Test
    @Timeout(60)
    void callsARealCatalogScalarFunction() {
        // global_scalar(value: int64) -> string, registered in the "main"
        // schema, single positional non-const argument — exactly VgiCatalog's
        // (FunctionCatalog) v1 scope. Exercises the full bind-once/exchange-
        // per-row path against a live worker, including the InternalRow <->
        // Arrow value bridge for both a numeric argument and a string result.
        Row result = spark.sql("SELECT " + CATALOG + ".main.global_scalar(7) AS r").collectAsList().get(0);
        assertEquals("global_scalar:7", result.getString(0));
    }

    @Test
    @Timeout(60)
    void callsACatalogScalarFunctionAcrossManyRows() {
        // Runs the function once per row of a real scan (100 rows), on
        // whatever executor task processes them — proves the lazy
        // per-task connection + TaskContext cleanup path, not just a
        // single-call smoke test.
        List<Row> rows = spark.sql(
                "SELECT " + CATALOG + ".main.global_scalar(value) AS r FROM " + CATALOG + ".data.numbers "
                        + "WHERE value < 5 ORDER BY value")
                .collectAsList();
        assertEquals(5, rows.size());
        for (int i = 0; i < 5; i++) {
            assertEquals("global_scalar:" + i, rows.get(i).getString(0));
        }
    }

    @Test
    @Timeout(60)
    void filtersOnAStructSubfield() {
        // rff_struct requires filters on BOTH s.a and s.b (TableInfo
        // .required_filters) — this also exercises VgiFilterTranslator's
        // struct-path resolution (FilterPredicate.StructField) for two
        // different subfields of the same top-level column.
        List<Row> rows = spark.sql(
                "SELECT s.a, s.b FROM " + CATALOG + ".data.rff_struct WHERE s.a = 2 AND s.b = 20")
                .collectAsList();
        assertEquals(1, rows.size());
        assertEquals(2L, rows.get(0).getLong(0));
        assertEquals(20L, rows.get(0).getLong(1));
    }

    @Test
    @Timeout(60)
    void filtersOnA3DeepNestedStructPath() {
        List<Row> rows = spark.sql(
                "SELECT wrapper.mid.leaf FROM " + CATALOG + ".data.rff_nested WHERE wrapper.mid.leaf = 2")
                .collectAsList();
        assertEquals(1, rows.size());
        assertEquals(2L, rows.get(0).getLong(0));
    }

    @Test
    @Timeout(60)
    void filtersOnFourSubfieldsOfOneStructColumnNotInTheOutputProjection() {
        // Regression test: a query whose SELECT list doesn't reference the
        // struct column at all (only its WHERE clause does) is exactly the
        // shape that exposed a real bug — Spark calls pushPredicates() (which
        // used to translate filters into column_index immediately) BEFORE
        // pruneColumns() narrows the projection down to just this one struct
        // column, so a filter's column_index baked in at push-time pointed
        // past the end of the FINAL (narrower) projected batch the worker
        // actually returned. VgiScanBuilder now defers translation to
        // build(), after every pushdown callback has run. Also exercises
        // more than 2 struct children combined into one column, unlike the
        // rff_struct case above (which only has 2).
        List<Row> rows = spark.sql(
                "SELECT count(*) FROM " + CATALOG + ".data.rff_rowid "
                        + "WHERE bbox.xmin >= 0 AND bbox.ymin > 0 AND bbox.ymax <= 100 AND bbox.xmax <= 100.2")
                .collectAsList();
        assertEquals(1, rows.size());
        assertEquals(10L, rows.get(0).getLong(0));
    }

    @Test
    @Timeout(60)
    void timeTravelsByVersion() {
        // versioned_data has 3 schema-evolving versions (see the real
        // table/time_travel.test this mirrors): v1 is (id) with 3 rows, v2 is
        // (id, name, score, active) with 5 rows, v3/current is (id, score)
        // with 4 rows. VERSION AS OF drives VgiCatalog.loadTable(Identifier,
        // String) -> at_unit="version".
        List<Row> v1 = spark.sql(
                "SELECT * FROM " + CATALOG + ".data.versioned_data VERSION AS OF '1' ORDER BY id")
                .collectAsList();
        assertEquals(3, v1.size());
        assertEquals(1, v1.get(0).length()); // v1's schema is (id) only

        List<Row> v2 = spark.sql(
                "SELECT name FROM " + CATALOG + ".data.versioned_data VERSION AS OF '2' ORDER BY name")
                .collectAsList();
        assertEquals(List.of("alice", "bob", "carol", "dave", "eve"),
                v2.stream().map(r -> r.getString(0)).toList());

        List<Row> current = spark.sql("SELECT * FROM " + CATALOG + ".data.versioned_data ORDER BY id")
                .collectAsList();
        assertEquals(4, current.size());
    }

    @Test
    @Timeout(60)
    void timeTravelsByTimestamp() {
        // TIMESTAMP AS OF drives VgiCatalog.loadTable(Identifier, long) —
        // Spark resolves the literal to epoch micros before calling it, and
        // the connector formats that back into a plain timestamp string for
        // at_value (see that overload's own javadoc). Same year -> version
        // mapping as the real table/time_travel.test.
        List<Row> at2020 = spark.sql(
                "SELECT count(*) FROM " + CATALOG + ".data.versioned_data TIMESTAMP AS OF '2020-06-15'")
                .collectAsList();
        assertEquals(3L, at2020.get(0).getLong(0));

        List<Row> at2021 = spark.sql(
                "SELECT count(*) FROM " + CATALOG + ".data.versioned_data TIMESTAMP AS OF '2021-06-15'")
                .collectAsList();
        assertEquals(5L, at2021.get(0).getLong(0));

        List<Row> at2022 = spark.sql(
                "SELECT count(*) FROM " + CATALOG + ".data.versioned_data TIMESTAMP AS OF '2022-06-15'")
                .collectAsList();
        assertEquals(4L, at2022.get(0).getLong(0));
    }

    @Test
    @Timeout(60)
    void scalarFunctionReadsAnIntSettingViaSet() {
        // multiply_by_setting(value) multiplies its input by the "multiplier"
        // worker-declared int setting, read via BindRequest.settings — see
        // VgiUnboundScalarFunction.currentSettingsBytes. Called unqualified
        // (catalog.function, no schema — resolves against the worker's own
        // default_schema, "main") exactly like the real
        // settings/multiply_by_setting.test does.
        spark.sql("SET multiplier = 5");
        List<Row> rows = spark.sql(
                "SELECT " + CATALOG + ".multiply_by_setting(v) FROM (VALUES (1), (2), (3)) AS t(v) ORDER BY v")
                .collectAsList();
        assertEquals(List.of(5L, 10L, 15L), rows.stream().map(r -> r.getLong(0)).toList());

        // Changing the setting mid-session is reflected on the next bind —
        // each call site re-binds (see VgiUnboundScalarFunction's own class
        // javadoc: bind() runs once per call site, not cached across SETs).
        spark.sql("SET multiplier = 10");
        List<Row> rows2 = spark.sql(
                "SELECT " + CATALOG + ".multiply_by_setting(v) FROM (VALUES (1), (2), (3)) AS t(v) ORDER BY v")
                .collectAsList();
        assertEquals(List.of(10L, 20L, 30L), rows2.stream().map(r -> r.getLong(0)).toList());

        spark.sql("RESET multiplier");
    }

    @Test
    @Timeout(60)
    void scalarFunctionReadsAFloatSettingViaSet() {
        // scale_by_setting(value) multiplies its input by the "scale_factor"
        // worker-declared DOUBLE setting.
        spark.sql("SET scale_factor = 2.5");
        Row r1 = spark.sql("SELECT " + CATALOG + ".scale_by_setting(4.0)").collectAsList().get(0);
        assertEquals(10.0, r1.getDouble(0), 0.0001);

        spark.sql("SET scale_factor = 0.5");
        Row r2 = spark.sql("SELECT " + CATALOG + ".scale_by_setting(10.0)").collectAsList().get(0);
        assertEquals(5.0, r2.getDouble(0), 0.0001);

        spark.sql("RESET scale_factor");
    }
}
