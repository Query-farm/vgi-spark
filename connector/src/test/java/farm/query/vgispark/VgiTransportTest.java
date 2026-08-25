// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgispark;

import farm.query.vgispark.testing.VgiWorkerHarness;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * One correctness test per transport {@link VgiWorkerHarness} supports —
 * subprocess, {@code unix://}, {@code tcp://}, {@code http://} — proving
 * this connector actually works over each, not just the subprocess default
 * every other test in this module happens to use.
 *
 * <p>Each test gets its own fresh {@link SparkSession} (unlike {@code
 * VgiCatalogQueryTest}'s shared-session-across-methods pattern) since the
 * whole point here is that catalog configuration — and therefore the
 * session — differs per transport.
 */
class VgiTransportTest {

    private static final File VGI_PYTHON = new File(System.getProperty("user.home"), "Development/vgi-python");
    private static final String CATALOG = "vgi_example";

    private VgiWorkerHarness.Handle worker;
    private SparkSession spark;

    @AfterEach
    void stop() throws Exception {
        if (spark != null) spark.stop();
        if (worker != null) worker.teardown().close();
    }

    private void start(VgiWorkerHarness.Handle handle) {
        this.worker = handle;
        this.spark = SparkSession.builder()
                .master("local[2]")
                .appName("vgi-spark-transport-test")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.catalog." + CATALOG, VgiCatalog.class.getName())
                .config("spark.sql.catalog." + CATALOG + ".location", handle.location())
                .config("spark.sql.catalog." + CATALOG + ".catalog-name", "example")
                .getOrCreate();
    }

    private void assertNumbersReadsCorrectly() {
        Row result = spark.sql("SELECT count(*) AS c, sum(value) AS s FROM " + CATALOG + ".data.numbers")
                .collectAsList().get(0);
        assertEquals(100L, result.getLong(0));
        assertEquals(4950L, result.getLong(1));
    }

    @Test
    @Timeout(60)
    void subprocessTransport() throws Exception {
        Assumptions.assumeTrue(VGI_PYTHON.isDirectory(), "~/Development/vgi-python not present");
        start(VgiWorkerHarness.subprocess(VGI_PYTHON));
        assertNumbersReadsCorrectly();
    }

    @Test
    @Timeout(60)
    void unixSocketTransport() throws Exception {
        Assumptions.assumeTrue(VGI_PYTHON.isDirectory(), "~/Development/vgi-python not present");
        start(VgiWorkerHarness.unix(VGI_PYTHON));
        assertNumbersReadsCorrectly();
    }

    @Test
    @Timeout(60)
    void tcpTransport() throws Exception {
        Assumptions.assumeTrue(VGI_PYTHON.isDirectory(), "~/Development/vgi-python not present");
        start(VgiWorkerHarness.tcp(VGI_PYTHON));
        assertNumbersReadsCorrectly();
    }

    @Test
    @Timeout(90)
    void httpTransport() throws Exception {
        Assumptions.assumeTrue(VGI_PYTHON.isDirectory(), "~/Development/vgi-python not present");
        start(VgiWorkerHarness.http(VGI_PYTHON));
        assertNumbersReadsCorrectly();
    }

    /** Real values, not just row counts — filter/projection pushdown work over any transport identically
     *  (the wire encoding doesn't change), but worth pinning against at least one non-default transport. */
    @Test
    @Timeout(60)
    void tcpTransportReadsRealFilteredValues() throws Exception {
        Assumptions.assumeTrue(VGI_PYTHON.isDirectory(), "~/Development/vgi-python not present");
        start(VgiWorkerHarness.tcp(VGI_PYTHON));
        List<Row> rows = spark.sql(
                "SELECT value FROM " + CATALOG + ".data.numbers WHERE value >= 97 ORDER BY value")
                .collectAsList();
        assertEquals(3, rows.size());
        assertEquals(97L, rows.get(0).getLong(0));
        assertEquals(98L, rows.get(1).getLong(0));
        assertEquals(99L, rows.get(2).getLong(0));
    }
}
