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
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Custom, worker-declared ATTACH-time options (roadmap tier 3: VGI's {@code
 * AttachOptionSpec} mechanism, {@code CatalogAttachRequest.options}) against
 * the {@code "attach_options"}/{@code "attach_options_required"} catalogs
 * {@code vgi-rust}'s {@code vgi-example-worker} serves — the standard {@code
 * "example"} catalog declares none, so this needs its own catalog selection
 * (matches {@code VgiVersionNegotiationTest}'s own note: a real capability,
 * no sqllogictest corpus payoff, since the C++ extension's own typed {@code
 * ATTACH (opt 42, ...)} SQL clause has no Spark equivalent and this fixture
 * has no dedicated corpus files anyway).
 *
 * <p>Values are read back via {@code CALL ...main.echo_attach_options()} —
 * roadmap tier 2 item 12's {@code ProcedureCatalog} work — since the
 * function is a table function with no {@code FROM func(...)} way to call
 * it (confirmed structurally unreachable, see the roadmap's "Won't
 * implement" section); reusing item 12's capability here to verify a
 * completely different item is a deliberate, direct payoff of having built it.
 *
 * <p>{@code opt_string} is the only declared option type this connector's
 * string-only encoding (see {@code VgiCatalogConfig#attachOptions}'s own
 * javadoc) can round-trip exactly — a numeric/boolean/complex-typed option
 * would arrive at the worker as a UTF-8 value where it expects, say, an
 * int64 one, which is exactly the honest limitation that javadoc documents,
 * not something this test works around.
 */
class VgiCustomAttachOptionsTest {

    private static final File VGI_RUST = new File(System.getProperty("user.home"), "Development/vgi-rust");
    private static final String CATALOG = "vgi_attach_opts";

    private VgiWorkerHarness.Handle worker;
    private SparkSession spark;

    @AfterEach
    void stop() throws Exception {
        if (spark != null) spark.stop();
        if (worker != null) worker.teardown().close();
    }

    private SparkSession.Builder baseBuilder(String appName, String vgiCatalogName) throws Exception {
        // "attach_options" and "attach_options_required" both live on the SAME
        // vgi-example-worker process (main.rs registers the latter as a
        // secondary catalog alongside the former) — VGI_WORKER_CATALOG_NAME
        // selects which one is primary, but either catalog name is reachable
        // regardless, matching this test's own per-test vgiCatalogName choice.
        worker = VgiWorkerHarness.unix(VGI_RUST, "attach_options");
        return SparkSession.builder()
                .master("local[2]")
                .appName(appName)
                .config("spark.ui.enabled", "false")
                .config("spark.sql.catalog." + CATALOG, VgiCatalog.class.getName())
                .config("spark.sql.catalog." + CATALOG + ".location", worker.location())
                .config("spark.sql.catalog." + CATALOG + ".catalog-name", vgiCatalogName);
    }

    @Test
    @Timeout(60)
    void noCustomOptionsUsesTheWorkersOwnDefaults() throws Exception {
        Assumptions.assumeTrue(VGI_RUST.isDirectory(), "~/Development/vgi-rust not present");
        spark = baseBuilder("vgi-spark-attach-options-defaults-test", "attach_options").getOrCreate();
        List<Row> rows = spark.sql("CALL " + CATALOG + ".main.echo_attach_options()").collectAsList();
        assertEquals(1, rows.size());
        assertEquals("hello", rows.get(0).getAs("opt_string"));
    }

    @Test
    @Timeout(60)
    void aCustomStringOptionOverridesTheWorkersDefault() throws Exception {
        Assumptions.assumeTrue(VGI_RUST.isDirectory(), "~/Development/vgi-rust not present");
        spark = baseBuilder("vgi-spark-attach-options-override-test", "attach_options")
                .config("spark.sql.catalog." + CATALOG + ".attach-option.opt_string", "custom-value")
                .getOrCreate();
        List<Row> rows = spark.sql("CALL " + CATALOG + ".main.echo_attach_options()").collectAsList();
        assertEquals(1, rows.size());
        assertEquals("custom-value", rows.get(0).getAs("opt_string"));
    }

    @Test
    @Timeout(60)
    void aMissingRequiredOptionFailsAttachLoudly() throws Exception {
        Assumptions.assumeTrue(VGI_RUST.isDirectory(), "~/Development/vgi-rust not present");
        spark = baseBuilder("vgi-spark-attach-options-required-missing-test", "attach_options_required")
                .getOrCreate();
        // The worker's own validate_required_attach_options raises — this
        // connector never pre-validates client-side, matching the "worker
        // is the source of truth" contract VgiCatalogConfig#attachOptions's
        // own javadoc already documents.
        assertThrows(Exception.class, () -> spark.sql("SHOW NAMESPACES IN " + CATALOG).collectAsList());
    }

    @Test
    @Timeout(60)
    void aSuppliedRequiredOptionAttachesSuccessfully() throws Exception {
        // Not an echo-back check like the two tests above: echo_attach_options
        // is only homed under the PRIMARY "attach_options" catalog's own
        // (catalog, schema) scope on this worker (vgi-example-worker's own
        // main.rs registers attach_options_required as a secondary catalog
        // with an empty function list — deliberately, per its own comment —
        // so an exact schema-qualified CALL against attach_options_required
        // can't reach it; DuckDB's own attach_options_required.test proves
        // ATTACH success/failure the same way, via vgi_catalogs() and a plain
        // ATTACH, never by calling the function through the gated catalog).
        // A successful namespace listing after supplying the required option
        // is exactly that same proof, ported to Spark's SQL surface.
        Assumptions.assumeTrue(VGI_RUST.isDirectory(), "~/Development/vgi-rust not present");
        spark = baseBuilder("vgi-spark-attach-options-required-supplied-test", "attach_options_required")
                .config("spark.sql.catalog." + CATALOG + ".attach-option.api_key", "mykey123")
                .getOrCreate();
        List<Row> namespaces = spark.sql("SHOW NAMESPACES IN " + CATALOG).collectAsList();
        assertEquals(1, namespaces.size());
        assertEquals("main", namespaces.get(0).getString(0));
    }
}
