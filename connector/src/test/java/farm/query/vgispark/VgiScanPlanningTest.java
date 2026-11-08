// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgispark;

import farm.query.vgispark.testing.VgiWorkerHarness;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Protocol 3.2 filtered scan planning, end to end against {@code
 * vgi-example-worker}'s {@code data.planned_days} fixture: a {@code
 * scan_planning = "filtered"} table that redirects to {@code
 * read_parquet([...one file per day...])} and records each planning call in
 * {@code $VGI_TEST_BRANCH_DIR/planned_days/last_plan.json}.
 *
 * <p>vgi-spark resolves branches in {@code TableCatalog.loadTable}, before
 * Spark has any predicate, so it always sends {@code pushdown_filters =
 * null} (permitted by the protocol). The worker then returns every day's
 * file and Spark applies the {@code WHERE} itself. Unlike the native
 * extension's {@code scan_planning_filtered.test}, all five day files are
 * created here, since no pruning happens at planning time.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class VgiScanPlanningTest {

    private static final File VGI_RUST = new File(System.getProperty("user.home"), "Development/vgi-rust");
    private static final String CATALOG = "vgi_example";

    private Path branchDir;
    private VgiWorkerHarness.Handle worker;
    private SparkSession spark;

    @BeforeAll
    void start() throws Exception {
        branchDir = Files.createTempDirectory("vgi-spark-planned-days-");
        Assumptions.assumeTrue(VGI_RUST.isDirectory(),
                "~/Development/vgi-rust not present — skipping scan planning test");
        worker = VgiWorkerHarness.unix(VGI_RUST, "example", Map.of("VGI_TEST_BRANCH_DIR", branchDir.toString()));
        spark = SparkSession.builder()
                .master("local[2]")
                .appName("vgi-spark-scan-planning-test")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.catalog." + CATALOG, VgiCatalog.class.getName())
                .config("spark.sql.catalog." + CATALOG + ".location", worker.location())
                .config("spark.sql.catalog." + CATALOG + ".catalog-name", "example")
                .getOrCreate();
        // Ten rows per day, n = (day - 1) * 10 + 0..9. Spark writes each
        // "file" as a parquet directory, which its own reader accepts as a path.
        for (int day = 1; day <= 5; day++) {
            spark.range(10)
                    .selectExpr("DATE '2026-10-0" + day + "' AS event_date",
                            "CAST(id + " + (day - 1) * 10 + " AS BIGINT) AS n")
                    .write()
                    .parquet(branchDir.resolve("planned_days").resolve("2026-10-0" + day + ".parquet").toString());
        }
    }

    @AfterAll
    void stop() throws Exception {
        if (spark != null) spark.stop();
        if (worker != null) worker.teardown().close();
    }

    private String lastPlan() throws Exception {
        return Files.readString(branchDir.resolve("planned_days").resolve("last_plan.json"));
    }

    @Test
    @Timeout(60)
    void filteredTablePlansWithNullPushdownFiltersAndReadsEveryBranchFile() throws Exception {
        Row row = spark.sql("SELECT count(*), sum(n) FROM " + CATALOG + ".data.planned_days")
                .collectAsList().get(0);
        assertEquals(50L, row.getLong(0));
        assertEquals(1225L, row.getLong(1));

        String plan = lastPlan().replace(" ", "");
        assertTrue(plan.contains("\"received_filters\":false"), plan);
        assertTrue(plan.contains("\"2026-10-05\""), "all five days expected without pushdown: " + plan);
    }

    @Test
    @Timeout(60)
    void filteredTableStillAppliesTheWhereClause() {
        List<Row> rows = spark.sql("SELECT n FROM " + CATALOG + ".data.planned_days"
                + " WHERE event_date = DATE '2026-10-02' ORDER BY n").collectAsList();
        assertEquals(10, rows.size());
        for (int i = 0; i < 10; i++) assertEquals(10L + i, rows.get(i).getLong(0));
    }

    @Test
    @Timeout(60)
    void filteredTableExposesItsDeclaredColumns() {
        List<Row> described = spark.sql("DESCRIBE TABLE " + CATALOG + ".data.planned_days").collectAsList();
        assertEquals("event_date", described.get(0).getString(0));
        assertEquals("date", described.get(0).getString(1));
        assertEquals("n", described.get(1).getString(0));
        assertEquals("bigint", described.get(1).getString(1));
    }
}
