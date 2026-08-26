// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgispark.conformance;

import farm.query.vgispark.VgiCatalog;
import farm.query.vgispark.testing.VgiWorkerHarness;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Runs the ACTUAL {@code .test} sqllogictest files from
 * {@code ~/Development/vgi/test/sql/integration/} against this connector —
 * not hand-ported equivalents, the real files, parsed by {@link
 * SqlLogicTestFile}, with their real {@code ATTACH}-derived catalog
 * reference rewritten to this Spark catalog's name and their real expected
 * output compared against a real Spark query result.
 *
 * <p>Adapted from {@code vgi-trino}'s identical class — same two files (the
 * ones that repo already found to be the portable core of the 327-file
 * suite: mostly plain declarative-table {@code SELECT}/{@code WHERE}/{@code
 * ORDER BY}, not DuckDB-only introspection or table-function CALL syntax),
 * same non-portable-marker lists, adapted to a local {@link SparkSession} in
 * place of a {@code DistributedQueryRunner}. Spark's own dialect gaps
 * (re-triaged here, not assumed to match Trino's) turned out identical for
 * both files — see each test's own marker list for exactly what and why.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class VgiSqlLogicTestConformanceTest {

    private static final File VGI_PYTHON = new File(System.getProperty("user.home"), "Development/vgi-python");
    private static final File VGI_TEST_ROOT =
            new File(System.getProperty("user.home"), "Development/vgi/test/sql/integration");
    private static final String SPARK_CATALOG = "vgi_example";
    private static final String VGI_CATALOG_NAME = "example";

    private VgiWorkerHarness.Handle worker;
    private SparkSession spark;

    @BeforeAll
    void start() throws Exception {
        Assumptions.assumeTrue(VGI_PYTHON.isDirectory(),
                "~/Development/vgi-python not present — skipping sqllogictest conformance run");
        Assumptions.assumeTrue(VGI_TEST_ROOT.isDirectory(),
                "~/Development/vgi/test/sql/integration not present — skipping sqllogictest conformance run");
        // unix(), not subprocess() — see VgiSqlLogicTestSweepTest's identical
        // comment: avoids forking a fresh worker subprocess per Spark task.
        worker = VgiWorkerHarness.unix(VGI_PYTHON);

        spark = SparkSession.builder()
                .master("local[2]")
                .appName("vgi-spark-sqllogictest-conformance")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.catalog." + SPARK_CATALOG, VgiCatalog.class.getName())
                .config("spark.sql.catalog." + SPARK_CATALOG + ".location", worker.location())
                .config("spark.sql.catalog." + SPARK_CATALOG + ".catalog-name", VGI_CATALOG_NAME)
                .getOrCreate();
    }

    @AfterAll
    void stop() throws Exception {
        if (spark != null) spark.stop();
        if (worker != null) worker.teardown().close();
    }

    /**
     * Substrings that mark a record as needing something this connector (or
     * Spark itself) doesn't have — a curated denylist for this specific
     * file, not a general static analyzer.
     *
     * <ul>
     *   <li>{@code "ATTACH "} — DuckDB's own {@code ATTACH} syntax; this
     *       harness attaches via {@code spark.sql.catalog.*} config instead.</li>
     *   <li>{@code "DESCRIBE"} — DuckDB's 6-column {@code DESCRIBE} shape
     *       (name, type, null, key, default, extra) has no equivalent in
     *       Spark's own {@code DESCRIBE TABLE} (3 columns: col_name,
     *       data_type, comment).</li>
     *   <li>{@code "rowid_struct"} — the underlying VALUES are correct (this
     *       connector maps Arrow {@code Struct} fields, and {@code
     *       rowid.a}/{@code rowid.b} field-access queries read from real
     *       columns), but printing a whole struct value differs in TEXT
     *       FORMAT between DuckDB ({@code {'a': 0, 'b': s_0}}) and Spark's
     *       {@code Row.toString()} — a display-format mismatch, not a
     *       functional one, so this runner's plain string comparison would
     *       flag a false failure.</li>
     *   <li>{@code "rowid_sequence("} — DuckDB table-function CALL syntax;
     *       Spark has no SQL-level equivalent (see the plan's non-goals).</li>
     *   <li>{@code "SELECT * FROM example.data.rowid_first LIMIT 3"} — VGI's
     *       row-id pseudo-column is hidden from {@code SELECT *} on DuckDB
     *       and vgi-trino, but exposed as an ordinary (renamed) column here:
     *       Spark's matching mechanism ({@code SupportsMetadataColumns}) had
     *       a real physical-plan attribute-binding bug when tried (see {@code
     *       VgiTable.columns()}'s own note), and a crash is a worse outcome
     *       than an extra visible column — reverted, tracked as follow-up
     *       work rather than shipped broken.</li>
     * </ul>
     */
    private static final List<String> ROWID_NON_PORTABLE_MARKERS = List.of("ATTACH ", "DESCRIBE", "rowid_struct",
            "rowid_sequence(", "SELECT * FROM example.data.rowid_first LIMIT 3");

    @Test
    @Timeout(180)
    void rowIdColumnsMatchTheRealTestFile() throws Exception {
        Path testFile = VGI_TEST_ROOT.toPath().resolve("table/rowid.test");
        Assumptions.assumeTrue(testFile.toFile().isFile(), testFile + " not present");

        SqlLogicTestRunner.Result result = SqlLogicTestRunner.run(
                spark, testFile, "example.", SPARK_CATALOG, ROWID_NON_PORTABLE_MARKERS);

        if (!result.failures().isEmpty()) {
            fail(result.executed() + " executed, " + result.skipped() + " skipped, "
                    + result.failures().size() + " FAILED:\n" + String.join("\n---\n", result.failures()));
        }
        // One more skip than vgi-trino's own run of this file (9 vs. 8) — the
        // extra one is the SELECT * marker above, a Spark-specific gap Trino
        // doesn't share. Assert the skip count so a future edit to this
        // file's portable content, or a regression that silently starts
        // skipping MORE than expected, shows up here rather than as a
        // quietly-shrinking executed count.
        assertEquals(9, result.skipped(), "expected skip count changed — see ROWID_NON_PORTABLE_MARKERS");
        assertEquals(7, result.executed(), "expected executed-record count changed");
    }

    /**
     * {@code catalog/window_self_join.test} is a DuckDB optimizer regression
     * test (its whole point is a C++-side deep-copy bug in a window-function
     * self-join rewrite), and most of the queries it runs to exercise that
     * path are plain window-function/correlated-subquery SQL against a real
     * declarative table — portable, apart from its {@code ATTACH}/{@code
     * DETACH}, its one query using DuckDB's {@code QUALIFY} clause (Spark
     * has no {@code QUALIFY} either — confirmed by actually running it, not
     * assumed: {@code Syntax error at or near 'QUALIFY'}), and its trailing
     * {@code duckdb_functions()} introspection check.
     */
    private static final List<String> WINDOW_SELF_JOIN_NON_PORTABLE_MARKERS =
            List.of("ATTACH ", "DETACH", "QUALIFY", "duckdb_functions(");

    @Test
    @Timeout(180)
    void windowSelfJoinMatchesTheRealTestFile() throws Exception {
        Path testFile = VGI_TEST_ROOT.toPath().resolve("catalog/window_self_join.test");
        Assumptions.assumeTrue(testFile.toFile().isFile(), testFile + " not present");

        SqlLogicTestRunner.Result result = SqlLogicTestRunner.run(spark, testFile,
                "example.", SPARK_CATALOG, WINDOW_SELF_JOIN_NON_PORTABLE_MARKERS);

        if (!result.failures().isEmpty()) {
            fail(result.executed() + " executed, " + result.skipped() + " skipped, "
                    + result.failures().size() + " FAILED:\n" + String.join("\n---\n", result.failures()));
        }
        // 4 non-portable records: ATTACH, the QUALIFY query, duckdb_functions(), DETACH — same
        // split vgi-trino's own run found.
        assertEquals(4, result.skipped(), "expected skip count changed — see WINDOW_SELF_JOIN_NON_PORTABLE_MARKERS");
        assertEquals(3, result.executed(), "expected executed-record count changed");
    }

    /**
     * {@code splits/multi_branch.test} — the one file in the whole 327-file
     * corpus that exercises real split-plan machinery declaratively, with no
     * table-function-call syntax needed at all (see {@code docs/ROADMAP.md},
     * tier 1 item 1). Its fixture table has one split-capable arm ({@code
     * split_sequence}, 30 rows over 6 splits) and one plain arm ({@code
     * sequence}, 20 rows), unioned by {@code VgiCatalog}'s new multi-branch
     * support. Non-portable: {@code ATTACH}/{@code DETACH}, DuckDB's {@code
     * SET threads}/{@code RESET threads} (no Spark equivalent), {@code
     * vgi_table_branches()} introspection, and the trailing query's {@code
     * example.main.split_sequence(...)} table-function call (the Spark
     * SQL-language ceiling — see the "Won't implement" section).
     */
    private static final List<String> MULTI_BRANCH_NON_PORTABLE_MARKERS = List.of(
            "ATTACH ", "DETACH", "SET threads", "RESET threads", "vgi_table_branches()",
            "example.main.split_sequence(");

    @Test
    @Timeout(180)
    void multiBranchSplitMatchesTheRealTestFile() throws Exception {
        Path testFile = VGI_TEST_ROOT.toPath().resolve("splits/multi_branch.test");
        Assumptions.assumeTrue(testFile.toFile().isFile(), testFile + " not present");

        SqlLogicTestRunner.Result result = SqlLogicTestRunner.run(spark, testFile,
                "example.", SPARK_CATALOG, MULTI_BRANCH_NON_PORTABLE_MARKERS);

        if (!result.failures().isEmpty()) {
            fail(result.executed() + " executed, " + result.skipped() + " skipped, "
                    + result.failures().size() + " FAILED:\n" + String.join("\n---\n", result.failures()));
        }
        // 7 non-portable: ATTACH, SET threads, RESET threads, 2x vgi_table_branches(), the
        // UNION ALL table-function-call query, DETACH.
        assertEquals(7, result.skipped(), "expected skip count changed — see MULTI_BRANCH_NON_PORTABLE_MARKERS");
        assertEquals(5, result.executed(), "expected executed-record count changed");
    }

    /**
     * {@code table/required_filters_struct.test} — struct-subfield required
     * paths (see {@code docs/ROADMAP.md}, tier 1 item 3): {@code rff_struct}
     * requires filters on both {@code s.a} and {@code s.b}, {@code rff_nested}
     * requires one on the 3-deep path {@code wrapper.mid.leaf}. Every record
     * is plain declarative SQL — no table-function-call syntax, no DuckDB-only
     * introspection — so only {@code ATTACH}/{@code DETACH} are non-portable.
     */
    private static final List<String> REQUIRED_FILTERS_STRUCT_NON_PORTABLE_MARKERS = List.of("ATTACH ", "DETACH");

    @Test
    @Timeout(180)
    void requiredFiltersStructMatchesTheRealTestFile() throws Exception {
        Path testFile = VGI_TEST_ROOT.toPath().resolve("table/required_filters_struct.test");
        Assumptions.assumeTrue(testFile.toFile().isFile(), testFile + " not present");

        SqlLogicTestRunner.Result result = SqlLogicTestRunner.run(spark, testFile,
                "example.", SPARK_CATALOG, REQUIRED_FILTERS_STRUCT_NON_PORTABLE_MARKERS);

        if (!result.failures().isEmpty()) {
            fail(result.executed() + " executed, " + result.skipped() + " skipped, "
                    + result.failures().size() + " FAILED:\n" + String.join("\n---\n", result.failures()));
        }
        assertEquals(2, result.skipped(), "expected skip count changed — see REQUIRED_FILTERS_STRUCT_NON_PORTABLE_MARKERS");
        assertEquals(8, result.executed(), "expected executed-record count changed");
    }

    /**
     * {@code table/required_filters_rowid.test} — {@code required_filters}
     * coexisting with a virtual row-id column (see {@code docs/ROADMAP.md},
     * tier 1 item 3's {@code required_filters_rowid.test} unlock). Every
     * record is plain declarative SQL.
     */
    private static final List<String> REQUIRED_FILTERS_ROWID_NON_PORTABLE_MARKERS = List.of("ATTACH ", "DETACH");

    @Test
    @Timeout(180)
    void requiredFiltersRowidMatchesTheRealTestFile() throws Exception {
        Path testFile = VGI_TEST_ROOT.toPath().resolve("table/required_filters_rowid.test");
        Assumptions.assumeTrue(testFile.toFile().isFile(), testFile + " not present");

        SqlLogicTestRunner.Result result = SqlLogicTestRunner.run(spark, testFile,
                "example.", SPARK_CATALOG, REQUIRED_FILTERS_ROWID_NON_PORTABLE_MARKERS);

        if (!result.failures().isEmpty()) {
            fail(result.executed() + " executed, " + result.skipped() + " skipped, "
                    + result.failures().size() + " FAILED:\n" + String.join("\n---\n", result.failures()));
        }
        assertEquals(2, result.skipped(), "expected skip count changed — see REQUIRED_FILTERS_ROWID_NON_PORTABLE_MARKERS");
        assertEquals(4, result.executed(), "expected executed-record count changed");
    }

    /**
     * {@code settings/multiply_by_setting.test} (see {@code docs/ROADMAP.md},
     * tier 2 item 6): {@code multiply_by_setting(value)} reads the
     * worker-declared int setting {@code multiplier} via {@code
     * BindRequest.settings}, called unqualified ({@code example
     * .multiply_by_setting(v)} — no schema — resolving against the worker's
     * {@code default_schema}, "main"). Every record is plain declarative SQL.
     */
    private static final List<String> MULTIPLY_BY_SETTING_NON_PORTABLE_MARKERS = List.of("ATTACH ", "DETACH");

    @Test
    @Timeout(180)
    void multiplyBySettingMatchesTheRealTestFile() throws Exception {
        Path testFile = VGI_TEST_ROOT.toPath().resolve("settings/multiply_by_setting.test");
        Assumptions.assumeTrue(testFile.toFile().isFile(), testFile + " not present");

        SqlLogicTestRunner.Result result = SqlLogicTestRunner.run(spark, testFile,
                "example.", SPARK_CATALOG, MULTIPLY_BY_SETTING_NON_PORTABLE_MARKERS);

        if (!result.failures().isEmpty()) {
            fail(result.executed() + " executed, " + result.skipped() + " skipped, "
                    + result.failures().size() + " FAILED:\n" + String.join("\n---\n", result.failures()));
        }
        assertEquals(2, result.skipped(), "expected skip count changed — see MULTIPLY_BY_SETTING_NON_PORTABLE_MARKERS");
        assertEquals(5, result.executed(), "expected executed-record count changed");
    }

    /**
     * {@code settings/settings_types.test} — a DOUBLE setting ({@code
     * scale_factor}) read via the same settings-passthrough path. Non-portable:
     * {@code ATTACH}/{@code DETACH} plus one {@code duckdb_settings()}
     * introspection query (DuckDB-only, no Spark equivalent — unrelated to the
     * settings feature itself, see {@code docs/ROADMAP.md}'s "Won't implement"
     * section on DuckDB-only introspection).
     */
    private static final List<String> SETTINGS_TYPES_NON_PORTABLE_MARKERS =
            List.of("ATTACH ", "DETACH", "duckdb_settings(");

    @Test
    @Timeout(180)
    void settingsTypesMatchesTheRealTestFile() throws Exception {
        Path testFile = VGI_TEST_ROOT.toPath().resolve("settings/settings_types.test");
        Assumptions.assumeTrue(testFile.toFile().isFile(), testFile + " not present");

        SqlLogicTestRunner.Result result = SqlLogicTestRunner.run(spark, testFile,
                "example.", SPARK_CATALOG, SETTINGS_TYPES_NON_PORTABLE_MARKERS);

        if (!result.failures().isEmpty()) {
            fail(result.executed() + " executed, " + result.skipped() + " skipped, "
                    + result.failures().size() + " FAILED:\n" + String.join("\n---\n", result.failures()));
        }
        assertEquals(3, result.skipped(), "expected skip count changed — see SETTINGS_TYPES_NON_PORTABLE_MARKERS");
        assertEquals(5, result.executed(), "expected executed-record count changed");
    }

    /**
     * {@code attach/ddl_wire_contract.test} (see {@code docs/ROADMAP.md},
     * tier 1 item 10): pins that mutating catalog DDL against this read-only
     * connector fails, not succeeds — every {@code statement error} record
     * expects {@code catalog is read-only}, but per this runner's own
     * {@code STATEMENT_ERROR} contract (see {@link SqlLogicTestRunner}) only
     * "did it throw" is checked, never the exact wording, so this test
     * doesn't (and can't) pin the literal DuckDB-side message text.
     *
     * <p>Every record here DOES throw when replayed, but by two different
     * routes, confirmed by actually running each statement (not assumed):
     * <ul>
     *   <li>{@code CREATE SCHEMA}/{@code CREATE SCHEMA IF NOT EXISTS} reach
     *       {@link VgiCatalog#createNamespace} directly, which already
     *       throws a clear {@link UnsupportedOperationException} naming
     *       "read-only" — see {@code VgiCatalogQueryTest
     *       .mutatingDdlRefusesWithAReadOnlyMessage} for a message-content
     *       pin on that path.</li>
     *   <li>{@code CREATE OR REPLACE SCHEMA} isn't valid Spark SQL at all
     *       ({@code CREATE OR REPLACE} has no schema/namespace form in
     *       Spark's grammar — confirmed by running it: {@code
     *       [PARSE_SYNTAX_ERROR] Syntax error at or near 'SCHEMA'}) — a
     *       Spark dialect gap, unrelated to this connector, but it still
     *       throws, so the loose contract above is satisfied without a
     *       marker.</li>
     *   <li>{@code ALTER TABLE ... ADD/DROP COLUMN} target {@code
     *       main.even_numbers}, which is a VIEW in the real fixture worker's
     *       catalog, not a TABLE (see {@code
     *       vgi-python/vgi/_test_fixtures/worker.py}'s {@code main} schema:
     *       no {@code tables=[...]}, only {@code views=[...]}). This
     *       connector's {@link VgiCatalog} doesn't surface views through
     *       Spark's {@code TableCatalog} SPI at all yet, so Spark's own
     *       analyzer fails to resolve the identifier ({@code
     *       TABLE_OR_VIEW_NOT_FOUND}) before {@code VgiCatalog.alterTable}
     *       is ever called — confirmed directly by also running {@code
     *       ALTER TABLE} against a REAL table ({@code data.numbers}), which
     *       DOES reach {@code alterTable()} and throws the same clear
     *       "read-only" message as {@code createNamespace} (also pinned by
     *       {@code VgiCatalogQueryTest
     *       .mutatingDdlRefusesWithAReadOnlyMessage}). Still a genuine
     *       "statement error" either way, so no marker needed here either —
     *       this is exactly the "Spark's own analyzer doesn't even reach our
     *       code" case {@code docs/ROADMAP.md} flagged as a possibility for
     *       this item, and it turned out not to need a fix: nothing here
     *       silently succeeds, and no message is confusing enough to be
     *       worth improving.</li>
     * </ul>
     *
     * <p>Only {@code ATTACH}/{@code DETACH} are non-portable (same reason as
     * every other curated test here — the harness attaches via {@code
     * spark.sql.catalog.*} config instead of a real {@code ATTACH}
     * statement).
     */
    private static final List<String> DDL_WIRE_CONTRACT_NON_PORTABLE_MARKERS = List.of("ATTACH ", "DETACH");

    @Test
    @Timeout(180)
    void ddlWireContractMatchesTheRealTestFile() throws Exception {
        Path testFile = VGI_TEST_ROOT.toPath().resolve("attach/ddl_wire_contract.test");
        Assumptions.assumeTrue(testFile.toFile().isFile(), testFile + " not present");

        SqlLogicTestRunner.Result result = SqlLogicTestRunner.run(spark, testFile,
                "example.", SPARK_CATALOG, DDL_WIRE_CONTRACT_NON_PORTABLE_MARKERS);

        if (!result.failures().isEmpty()) {
            fail(result.executed() + " executed, " + result.skipped() + " skipped, "
                    + result.failures().size() + " FAILED:\n" + String.join("\n---\n", result.failures()));
        }
        // 2 non-portable: ATTACH, DETACH. The other 7 (3x CREATE SCHEMA, 2x
        // ADD COLUMN, 2x DROP COLUMN) all execute and all throw — see this
        // test's own javadoc for exactly how each one fails.
        assertEquals(2, result.skipped(), "expected skip count changed — see DDL_WIRE_CONTRACT_NON_PORTABLE_MARKERS");
        assertEquals(7, result.executed(), "expected executed-record count changed");
    }

    /**
     * {@code table/column_statistics.test} (see {@code docs/ROADMAP.md},
     * tier 2 item 8): the correctness-bearing (non-{@code EXPLAIN}) portion
     * only, per that item's own "Unlocks" note — partial credit, since most
     * of this file's records assert DuckDB {@code EXPLAIN} physical-plan
     * text specifically, which has no portable equivalent here (confirmed by
     * actually running one: this runner's {@code query} record contract is
     * plain string-per-cell comparison, no {@code <REGEX>:} support, and
     * Spark's own {@code EXPLAIN} doesn't emit a {@code physical_plan}
     * column-named two-cell row the way DuckDB's {@code EXPLAIN} does — a
     * literal-text mismatch on every single one, not a near-miss).
     *
     * <p>Non-portable, beyond {@code ATTACH}:
     * <ul>
     *   <li>{@code "EXPLAIN "} — every {@code EXPLAIN SELECT ...} record in
     *       this file, all asserting a DuckDB-specific {@code <REGEX>:
     *       .*EMPTY_RESULT.*}/{@code .*VGI_TABLE_SCAN.*} physical-plan
     *       string — see above.</li>
     *   <li>{@code "vgi_table_statistics("} — DuckDB's own diagnostic
     *       table-valued function surfacing {@code
     *       catalog_table_column_statistics_get}'s raw per-column output
     *       (min/max/distinct_count/has_null/...); Spark has no SQL-level
     *       table-function-call syntax to reach an equivalent (see the
     *       plan's non-goals) and this connector doesn't register one.</li>
     * </ul>
     *
     * <p>What's left is genuinely correctness-bearing, not just filler: plain
     * {@code count(*)}/{@code SELECT *} reads against the SAME fixture tables
     * ({@code numbers}, {@code departments}, {@code products}, {@code colors},
     * {@code versioned_data}, {@code volatile_numbers}) the stats-bearing
     * records exercise, proving those tables read correctly end-to-end
     * through {@link farm.query.vgispark.scan.VgiScan} — including the one
     * whose declared statistics have TTL {@code 0} ({@code
     * volatile_numbers}) and the one with NO statistics at all ({@code
     * versioned_data}), i.e. {@code Statistics}/column-stats fetching (or its
     * absence) doesn't perturb an ordinary scan either way.
     */
    private static final List<String> COLUMN_STATISTICS_NON_PORTABLE_MARKERS =
            List.of("ATTACH ", "EXPLAIN ", "vgi_table_statistics(");

    @Test
    @Timeout(180)
    void columnStatisticsMatchesTheRealTestFile() throws Exception {
        Path testFile = VGI_TEST_ROOT.toPath().resolve("table/column_statistics.test");
        Assumptions.assumeTrue(testFile.toFile().isFile(), testFile + " not present");

        SqlLogicTestRunner.Result result = SqlLogicTestRunner.run(spark, testFile,
                "example.", SPARK_CATALOG, COLUMN_STATISTICS_NON_PORTABLE_MARKERS);

        if (!result.failures().isEmpty()) {
            fail(result.executed() + " executed, " + result.skipped() + " skipped, "
                    + result.failures().size() + " FAILED:\n" + String.join("\n---\n", result.failures()));
        }
        // 65 non-portable: 1x ATTACH, 20x EXPLAIN, 44x vgi_table_statistics(). The other 9
        // (plain count(*)/SELECT * reads) execute and match.
        assertEquals(65, result.skipped(), "expected skip count changed — see COLUMN_STATISTICS_NON_PORTABLE_MARKERS");
        assertEquals(9, result.executed(), "expected executed-record count changed");
    }

    /**
     * {@code aggregate/basic.test} (roadmap tier 1 item 2): plain ungrouped
     * {@code vgi_sum}/{@code vgi_count}/{@code vgi_avg} calls, NULL handling,
     * an empty-table case, and a byte-equal-state regression — all real
     * {@code AggregateFunction} round trips against a live worker. Every
     * {@code range(...)} call is Spark's OWN built-in table-valued function,
     * not a VGI one, so those need no marker. Non-portable: {@code ATTACH}/
     * {@code DETACH}, and the one window-aggregate record ({@code OVER (...)})
     * — VGI aggregate windowed-frame support needs a materially different RPC
     * surface (Spark's {@code PartitionEvaluator} SPI), explicitly out of
     * v1 scope (see {@code docs/ROADMAP.md} item 2's own "Explicitly
     * deferred" list) — confirmed by actually running it, not assumed.
     */
    private static final List<String> AGGREGATE_BASIC_NON_PORTABLE_MARKERS =
            List.of("ATTACH ", "DETACH", "OVER (ORDER BY i ROWS BETWEEN");

    @Test
    @Timeout(180)
    void aggregateBasicMatchesTheRealTestFile() throws Exception {
        Path testFile = VGI_TEST_ROOT.toPath().resolve("aggregate/basic.test");
        Assumptions.assumeTrue(testFile.toFile().isFile(), testFile + " not present");

        SqlLogicTestRunner.Result result = SqlLogicTestRunner.run(spark, testFile,
                "example.", SPARK_CATALOG, AGGREGATE_BASIC_NON_PORTABLE_MARKERS);

        if (!result.failures().isEmpty()) {
            fail(result.executed() + " executed, " + result.skipped() + " skipped, "
                    + result.failures().size() + " FAILED:\n" + String.join("\n---\n", result.failures()));
        }
        assertEquals(3, result.skipped(), "expected skip count changed — see AGGREGATE_BASIC_NON_PORTABLE_MARKERS");
        assertEquals(12, result.executed(), "expected executed-record count changed");
    }

    /**
     * {@code aggregate/grouped.test}: real {@code GROUP BY} — multiple groups
     * within one task, exercising {@code VgiAggregateFunction}'s per-group
     * bind-once/update/finalize/destructor sequence repeatedly, up to 100
     * groups over 1000 rows in its last record. {@code SET threads=1} is
     * DuckDB's own pragma — Spark's generic {@code SET} accepts any key as a
     * harmless string property (same permissive behavior settings passthrough,
     * item 6, already relies on), so it executes as a no-op rather than
     * needing a skip marker.
     */
    private static final List<String> AGGREGATE_GROUPED_NON_PORTABLE_MARKERS = List.of("ATTACH ", "DETACH");

    @Test
    @Timeout(180)
    void aggregateGroupedMatchesTheRealTestFile() throws Exception {
        Path testFile = VGI_TEST_ROOT.toPath().resolve("aggregate/grouped.test");
        Assumptions.assumeTrue(testFile.toFile().isFile(), testFile + " not present");

        SqlLogicTestRunner.Result result = SqlLogicTestRunner.run(spark, testFile,
                "example.", SPARK_CATALOG, AGGREGATE_GROUPED_NON_PORTABLE_MARKERS);

        if (!result.failures().isEmpty()) {
            fail(result.executed() + " executed, " + result.skipped() + " skipped, "
                    + result.failures().size() + " FAILED:\n" + String.join("\n---\n", result.failures()));
        }
        assertEquals(2, result.skipped(), "expected skip count changed — see AGGREGATE_GROUPED_NON_PORTABLE_MARKERS");
        assertEquals(5, result.executed(), "expected executed-record count changed");
    }

    /**
     * {@code scalar/conditional_message.test} (roadmap tier 2 item 7c):
     * {@code conditional_message(repeat_count, message, condition)} has TWO
     * {@code vgi_const} arguments ({@code repeat_count}, {@code message}) and
     * one plain per-row column argument ({@code condition}) — exercises
     * {@code VgiScalarFunction}'s lazy const-bind cache repeatedly across
     * many distinct observed const-argument combinations within one query,
     * including its "rebind on change" path (the const values genuinely
     * differ record to record) and, in the file's own regression record, a
     * VARCHAR-literal-to-BIGINT implicit cast at the {@code repeat_count}
     * position. Non-portable: {@code ATTACH}/{@code DETACH}, and the two
     * records using {@code unnest(generate_series(...))} (a DuckDB-only
     * table-valued function unrelated to VGI or this item), and the one
     * {@code typeof(...)} record — a pre-existing, corpus-wide, unrelated
     * gap already noted under roadmap item 7b: the runner does no DuckDB→
     * Spark {@code typeof()} string normalization ({@code "VARCHAR"} vs.
     * {@code "string"}), so every {@code typeof()} record anywhere in the
     * corpus already mismatches on naming regardless of this item.
     */
    private static final List<String> CONDITIONAL_MESSAGE_NON_PORTABLE_MARKERS =
            List.of("ATTACH ", "DETACH", "unnest(generate_series", "typeof(");

    @Test
    @Timeout(180)
    void conditionalMessageMatchesTheRealTestFile() throws Exception {
        Path testFile = VGI_TEST_ROOT.toPath().resolve("scalar/conditional_message.test");
        Assumptions.assumeTrue(testFile.toFile().isFile(), testFile + " not present");

        SqlLogicTestRunner.Result result = SqlLogicTestRunner.run(spark, testFile,
                "example.", SPARK_CATALOG, CONDITIONAL_MESSAGE_NON_PORTABLE_MARKERS);

        if (!result.failures().isEmpty()) {
            fail(result.executed() + " executed, " + result.skipped() + " skipped, "
                    + result.failures().size() + " FAILED:\n" + String.join("\n---\n", result.failures()));
        }
        assertEquals(5, result.skipped(),
                "expected skip count changed — see CONDITIONAL_MESSAGE_NON_PORTABLE_MARKERS");
        assertEquals(19, result.executed(), "expected executed-record count changed");
    }

    /**
     * {@code scalar/sum_values.test} (roadmap tier 2 item 7d): {@code
     * sum_values(values...)} is a single {@code vgi_varargs} argument,
     * called at 1, 2, 3, 4, 5, 6, 7, 8, and 10 arguments across this file —
     * exercises {@code VgiUnboundScalarFunction.bind}'s vararg-expansion path
     * at many distinct effective arities within one query file, its
     * per-position any-typed resolution, NULL propagation through a
     * multi-column vararg group, real multi-partition execution over a
     * 5000-row table, and the zero-arg regression ({@code sum_values()}
     * must surface the worker's own clean validation error, not crash this
     * connector). Non-portable: {@code ATTACH}/{@code DETACH}, {@code
     * typeof(...)} records (same pre-existing casing gap as 7b/7c), and
     * {@code unnest(generate_series(...))} (DuckDB-only, unrelated) — and,
     * consequently, every record referencing the three tables that file's
     * own (skipped) {@code unnest(generate_series(...))}-based {@code CREATE
     * TABLE}s would have populated ({@code large_triplets}/{@code
     * large_quads}/{@code large_double_triplets}), since those tables are
     * never actually created here.
     */
    private static final List<String> SUM_VALUES_NON_PORTABLE_MARKERS =
            List.of("ATTACH ", "DETACH", "unnest(generate_series", "typeof(",
                    "large_triplets", "large_quads", "large_double_triplets");

    @Test
    @Timeout(300)
    void sumValuesMatchesTheRealTestFile() throws Exception {
        Path testFile = VGI_TEST_ROOT.toPath().resolve("scalar/sum_values.test");
        Assumptions.assumeTrue(testFile.toFile().isFile(), testFile + " not present");

        SqlLogicTestRunner.Result result = SqlLogicTestRunner.run(spark, testFile,
                "example.", SPARK_CATALOG, SUM_VALUES_NON_PORTABLE_MARKERS);

        if (!result.failures().isEmpty()) {
            fail(result.executed() + " executed, " + result.skipped() + " skipped, "
                    + result.failures().size() + " FAILED:\n" + String.join("\n---\n", result.failures()));
        }
        assertEquals(28, result.skipped(), "expected skip count changed — see SUM_VALUES_NON_PORTABLE_MARKERS");
        assertEquals(53, result.executed(), "expected executed-record count changed");
    }
}
