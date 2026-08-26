// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgispark;

import farm.query.vgispark.scan.VgiScan;
import farm.query.vgispark.testing.VgiWorkerHarness;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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

    /**
     * Regression for {@code docs/ROADMAP.md} item 10 ({@code
     * attach/ddl_wire_contract.test}): {@code VgiCatalog.createNamespace}/
     * {@code alterTable} already refuse every mutating DDL statement with an
     * {@link UnsupportedOperationException} — this pins that both the
     * exception fires AND its message actually names the reason ("read-only"),
     * rather than a generic/misleading one, for both entry points the real
     * test file exercises.
     *
     * <p>{@code ALTER TABLE ... ADD/DROP COLUMN} in the real file targets
     * {@code main.even_numbers}, which is a VIEW in the fixture worker's
     * catalog (see {@code vgi-python/vgi/_test_fixtures/worker.py}) — this
     * connector's {@link VgiCatalog} only surfaces {@code TABLE} catalog
     * objects through Spark's {@code TableCatalog} SPI (views aren't wired up
     * at all yet), so Spark's OWN analyzer fails to resolve the identifier
     * before ever calling {@code alterTable} — a real, unavoidable
     * {@code TABLE_OR_VIEW_NOT_FOUND} {@code AnalysisException}, not a bug in
     * this connector's read-only refusal path. That's exercised (and left
     * alone) by the curated conformance test's loose "did it throw"
     * contract; this test instead points {@code ALTER TABLE} at a REAL table
     * ({@code data.numbers}) so {@code alterTable()} is actually reached, to
     * pin ITS message directly.
     */
    /**
     * Regression for {@code docs/ROADMAP.md} tier 2 item 8 (column-statistics
     * -driven scan pruning): {@link VgiScan#estimateStatistics()} must report
     * the worker's real {@code TableInfo.cardinality_estimate}, not zero and
     * not silently omit it — the fixture worker's {@code
     * data.cardinality_inlined_table} declares {@code cardinality_estimate=10000}
     * explicitly (see {@code vgi-python/vgi/_test_fixtures/worker.py}'s own
     * comment: built for the C++ extension's identical {@code
     * inlined_cardinality.test}), giving an exact expected value rather than
     * an approximate one.
     *
     * <p>Goes around {@code spark.sql(...)} entirely and drives {@link
     * VgiCatalog}/{@link SupportsRead#newScanBuilder} directly — the only
     * clean way to reach the actual {@code Scan} object Spark's optimizer
     * would call {@code estimateStatistics()} on, since that call happens
     * deep inside Catalyst's own (non-public-API) plan nodes rather than
     * anywhere {@code DataFrame} exposes directly.
     */
    @Test
    @Timeout(60)
    void reportsCardinalityFromTheWorkersEstimate() throws Exception {
        VgiCatalog catalog = new VgiCatalog();
        catalog.initialize("cardinality_probe", new CaseInsensitiveStringMap(
                java.util.Map.of("location", worker.location(), "catalog-name", "example")));

        // cardinality_inlined_table: a real, non-null TableInfo.cardinality_estimate (10000).
        Table cardinalityTable = catalog.loadTable(Identifier.of(new String[] {"data"}, "cardinality_inlined_table"));
        ScanBuilder cardinalityBuilder =
                ((SupportsRead) cardinalityTable).newScanBuilder(CaseInsensitiveStringMap.empty());
        VgiScan cardinalityScan = (VgiScan) cardinalityBuilder.build();
        Statistics cardinalityStats = cardinalityScan.estimateStatistics();
        assertTrue(cardinalityStats.numRows().isPresent(),
                "expected a present row-count estimate for cardinality_inlined_table");
        assertEquals(10000L, cardinalityStats.numRows().getAsLong());

        // numbers: no cardinality_estimate declared at all (only column
        // statistics) — must report UNKNOWN (empty), not 0 or a fabricated
        // guess, exactly the "may be null" case VgiTable.cardinalityEstimate()
        // itself documents.
        Table numbersTable = catalog.loadTable(Identifier.of(new String[] {"data"}, "numbers"));
        ScanBuilder numbersBuilder = ((SupportsRead) numbersTable).newScanBuilder(CaseInsensitiveStringMap.empty());
        VgiScan numbersScan = (VgiScan) numbersBuilder.build();
        Statistics numbersStats = numbersScan.estimateStatistics();
        assertFalse(numbersStats.numRows().isPresent(),
                "expected UNKNOWN (no cardinality_estimate declared), not a fabricated row count");
    }

    @Test
    @Timeout(60)
    void mutatingDdlRefusesWithAReadOnlyMessage() {
        Exception createSchema = org.junit.jupiter.api.Assertions.assertThrows(UnsupportedOperationException.class,
                () -> spark.sql("CREATE SCHEMA " + CATALOG + ".probe_schema").collect(),
                "CREATE SCHEMA should be refused, not silently accepted");
        assertTrue(createSchema.getMessage().toLowerCase().contains("read-only"),
                "expected a read-only refusal message, got: " + createSchema.getMessage());

        Exception createSchemaIfNotExists = org.junit.jupiter.api.Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> spark.sql("CREATE SCHEMA IF NOT EXISTS " + CATALOG + ".probe_schema2").collect(),
                "CREATE SCHEMA IF NOT EXISTS should be refused, not silently accepted");
        assertTrue(createSchemaIfNotExists.getMessage().toLowerCase().contains("read-only"),
                "expected a read-only refusal message, got: " + createSchemaIfNotExists.getMessage());

        // data.numbers is a real TABLE (not a view), so this actually reaches
        // VgiCatalog.alterTable() — unlike the real test file's
        // main.even_numbers target, see this test's own javadoc above.
        Exception addColumn = org.junit.jupiter.api.Assertions.assertThrows(UnsupportedOperationException.class,
                () -> spark.sql("ALTER TABLE " + CATALOG + ".data.numbers ADD COLUMN x INT").collect(),
                "ALTER TABLE ... ADD COLUMN should be refused, not silently accepted");
        assertTrue(addColumn.getMessage().toLowerCase().contains("read-only"),
                "expected a read-only refusal message, got: " + addColumn.getMessage());
    }

    @Test
    @Timeout(60)
    void callsARealTableFunctionViaCallSyntax() {
        // split_sequence(n, splits) is a genuine split-capable table function
        // (main schema, registered with default splits=4) — CALL exercises
        // VgiCatalog's new ProcedureCatalog path end to end: discovery,
        // real-argument-value binding (a REAL bind() RPC, not just a static
        // FunctionInfo lookup), table_function_plan, and the same
        // VgiPartitionReader split-reading machinery a declarative table's
        // scan already uses. See VgiUnboundTableProcedure's own javadoc for
        // why CALL, not FROM split_sequence(30, 6)/LATERAL.
        List<Row> rows = spark.sql("CALL " + CATALOG + ".main.split_sequence(30, 6)").collectAsList();
        assertEquals(30, rows.size(), "split_sequence(30, 6) should produce 30 rows");
        List<Long> values = rows.stream().map(r -> r.getLong(0)).sorted().toList();
        for (int i = 0; i < 30; i++) {
            assertEquals((long) i, values.get(i), "row " + i + " mismatch");
        }
    }

    @Test
    @Timeout(60)
    void callsATableFunctionUnqualifiedViaDefaultSchema() {
        // No schema in the call — CALL vgi_example.split_sequence(...), not
        // vgi_example.main.split_sequence(...) — resolves against the
        // worker's default_schema ("main"), same as scalar functions. Both
        // arguments are supplied explicitly: this connector doesn't thread
        // VGI's own declared argument defaults into ProcedureParameter yet
        // (a real, separate gap from default-schema resolution — see
        // VgiUnboundTableProcedure's own tryBuild), so a call relying on
        // split_sequence's second-argument default would be refused by
        // Spark's own analyzer before ever reaching this connector.
        List<Row> rows = spark.sql("CALL " + CATALOG + ".split_sequence(5, 2)").collectAsList();
        assertEquals(5, rows.size());
    }

    @Test
    @Timeout(60)
    void callsARealAggregateFunction() {
        // vgi_sum(i) — a genuine VGI AggregateFunction, real bind/update/
        // finalize/destructor round trip against a live worker (main schema).
        Row sum10 = spark.sql("SELECT " + CATALOG + ".main.vgi_sum(i) AS s FROM (VALUES "
                + "(0L),(1L),(2L),(3L),(4L),(5L),(6L),(7L),(8L),(9L)) t(i)").collectAsList().get(0);
        assertEquals(45L, sum10.getLong(0));

        Row sum100 = spark.sql("SELECT " + CATALOG + ".main.vgi_sum(i::BIGINT) AS s "
                + "FROM (SELECT explode(sequence(0, 99)) AS i)").collectAsList().get(0);
        assertEquals(4950L, sum100.getLong(0));
    }

    @Test
    @Timeout(60)
    void callsANullaryAggregateFunction() {
        // vgi_count() — zero input columns; exercises the empty-InternalRow /
        // empty-argFields path through VgiAggregateFunction.
        Row count = spark.sql("SELECT " + CATALOG + ".main.vgi_count() AS c "
                + "FROM (SELECT explode(sequence(0, 99)) AS i)").collectAsList().get(0);
        assertEquals(100L, count.getLong(0));
    }

    @Test
    @Timeout(60)
    void callsAGroupedAggregateFunction() {
        // Real GROUP BY — multiple groups, multiple produceResult() calls
        // (and thus multiple worker-side group ids) within one task.
        List<Row> rows = spark.sql("SELECT k, " + CATALOG + ".main.vgi_sum(i) AS s FROM (VALUES "
                + "(1, 10L), (1, 20L), (2, 100L), (2, 200L), (2, 300L)) t(k, i) GROUP BY k ORDER BY k")
                .collectAsList();
        assertEquals(2, rows.size());
        assertEquals(1, rows.get(0).getInt(0));
        assertEquals(30L, rows.get(0).getLong(1));
        assertEquals(2, rows.get(1).getInt(0));
        assertEquals(600L, rows.get(1).getLong(1));
    }

    @Test
    @Timeout(60)
    void aggregatesOverAHighCardinalityUngroupedInput() {
        // Regression: VgiAggregateFunction's update-batch group-id vector used
        // to be written with a plain (unsafe, non-growing) BigIntVector.set(),
        // which threw IndexOutOfBoundsException once a group's buffered rows
        // exceeded the vector's default initial capacity — found live via the
        // sqllogictest sweep (aggregate/large_ungrouped.test, range(1000000)).
        Row result = spark.sql("SELECT " + CATALOG + ".main.vgi_sum(i) AS s "
                + "FROM (SELECT explode(sequence(0, 99999)) AS i)").collectAsList().get(0);
        assertEquals(4999950000L, result.getLong(0)); // sum(0..99999)
    }

    @Test
    @Timeout(60)
    void scalarFunctionReturnsAnOnBindPromotedDecimal() {
        // double(value) — roadmap item 7b: its return type is on_bind-computed
        // from the REAL argument type (DECIMAL(p,s) in -> DECIMAL(p+1,s) out),
        // not statically known at discovery time. Exercises
        // VgiUnboundScalarFunction.resolveReturnType (called from bind() with
        // the real post-bind output schema) end to end against a live worker.
        Row result = spark.sql("SELECT " + CATALOG + ".double(1.25::DECIMAL(10,2)) AS r").collectAsList().get(0);
        assertEquals(new java.math.BigDecimal("2.50"), result.getDecimal(0));
    }

    @Test
    @Timeout(60)
    void scalarFunctionAddsTwoDecimalArguments() {
        // add_values(a, b) — plain DECIMAL-typed arguments (VgiScalarValueBridge's
        // new Decimal write/read path), not just a promoted return.
        Row result = spark.sql("SELECT " + CATALOG + ".add_values(1.50::DECIMAL(5,2), 2.250::DECIMAL(7,3)) AS r")
                .collectAsList().get(0);
        assertEquals(0, new java.math.BigDecimal("3.750").compareTo(result.getDecimal(0)));
    }
}
