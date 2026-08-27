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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * ATTACH-time data-version/implementation-version negotiation (roadmap tier
 * 3: {@code data_version_spec}/{@code implementation_version}) against the
 * {@code "versioned"} catalog {@code vgi-rust}'s {@code vgi-example-worker}
 * serves — the default {@code "example"} catalog never validates these
 * fields at all, so this needs its own catalog selection (mirrors the
 * roadmap's own note: all 10 related corpus files need a dedicated
 * versioned-fixture worker too — none of them are replayable here
 * regardless, since they assert via DuckDB's own {@code duckdb_databases()}
 * introspection, which has no Spark equivalent; this is a real-capability
 * live test, not a curated {@code .test} replay, the same "zero
 * sqllogictest payoff, real production feature" shape several other roadmap
 * items this connector already covers took).
 *
 * <p>The fixture ({@code vgi-example-worker/src/catalog_def.rs}'s {@code
 * versioned()} — a line-for-line port of the Python {@code
 * vgi/_test_fixtures/versioned.py}) serves catalog {@code "versioned"},
 * implementation version {@code "1.0.0"}, and accepts exactly {@code
 * "1.0.0"}/{@code "1.1.0"}/{@code "1.2.0"} as {@code data_version_spec}
 * (its own {@code supported_data_versions} allowlist, not real semver-range
 * parsing) — an unsatisfiable request for either raises a clear error the
 * worker surfaces as the ATTACH failure.
 */
class VgiVersionNegotiationTest {

    private static final File VGI_RUST = new File(System.getProperty("user.home"), "Development/vgi-rust");
    private static final String CATALOG = "vgi_versioned";

    private VgiWorkerHarness.Handle worker;
    private SparkSession spark;

    @AfterEach
    void stop() throws Exception {
        if (spark != null) spark.stop();
        if (worker != null) worker.teardown().close();
    }

    private SparkSession.Builder baseBuilder(String appName) throws Exception {
        worker = VgiWorkerHarness.unix(VGI_RUST, "versioned");
        return SparkSession.builder()
                .master("local[2]")
                .appName(appName)
                .config("spark.ui.enabled", "false")
                .config("spark.sql.catalog." + CATALOG, VgiCatalog.class.getName())
                .config("spark.sql.catalog." + CATALOG + ".location", worker.location())
                .config("spark.sql.catalog." + CATALOG + ".catalog-name", "versioned");
    }

    @Test
    @Timeout(60)
    void noVersionRequestResolvesToTheWorkersDefault() throws Exception {
        Assumptions.assumeTrue(VGI_RUST.isDirectory(), "~/Development/vgi-rust not present");
        spark = baseBuilder("vgi-spark-version-negotiation-default-test").getOrCreate();
        // Force the catalog to actually attach (Spark resolves catalogs
        // lazily) via a metadata call, then read the resolved versions back
        // through VgiWorkerClient directly — there is no Spark SQL surface
        // for this (no duckdb_databases() equivalent), matching
        // VgiWorkerClient#resolvedDataVersion's own javadoc.
        spark.sql("SHOW NAMESPACES IN " + CATALOG).collectAsList();
        VgiCatalog catalog = (VgiCatalog) spark.sessionState().catalogManager().catalog(CATALOG);
        assertEquals("1.2.0", catalog.client().resolvedDataVersion());
        assertEquals("1.0.0", catalog.client().resolvedImplementationVersion());
    }

    @Test
    @Timeout(60)
    void requestingASupportedDataVersionResolvesToExactlyThat() throws Exception {
        Assumptions.assumeTrue(VGI_RUST.isDirectory(), "~/Development/vgi-rust not present");
        spark = baseBuilder("vgi-spark-version-negotiation-requested-test")
                .config("spark.sql.catalog." + CATALOG + ".data-version-spec", "1.1.0")
                .config("spark.sql.catalog." + CATALOG + ".implementation-version", "1.0.0")
                .getOrCreate();
        spark.sql("SHOW NAMESPACES IN " + CATALOG).collectAsList();
        VgiCatalog catalog = (VgiCatalog) spark.sessionState().catalogManager().catalog(CATALOG);
        assertEquals("1.1.0", catalog.client().resolvedDataVersion());
        assertEquals("1.0.0", catalog.client().resolvedImplementationVersion());
    }

    @Test
    @Timeout(60)
    void requestingAnUnsatisfiableDataVersionFailsAttachLoudly() throws Exception {
        Assumptions.assumeTrue(VGI_RUST.isDirectory(), "~/Development/vgi-rust not present");
        SparkSession.Builder builder = baseBuilder("vgi-spark-version-negotiation-unsatisfiable-test")
                .config("spark.sql.catalog." + CATALOG + ".data-version-spec", "9.9.9");
        spark = builder.getOrCreate();
        // The worker's own ValueError ("Unsupported data_version_spec...")
        // surfaces as the ATTACH failure — this connector doesn't need to
        // pre-validate anything client-side, matching the "fail closed,
        // loudly" contract the worker's own version-negotiation check relies on.
        Exception e = assertThrows(Exception.class,
                () -> spark.sql("SHOW NAMESPACES IN " + CATALOG).collectAsList());
        assertThatMessageMentions(e, "data_version_spec");
    }

    private static void assertThatMessageMentions(Throwable t, String needle) {
        Throwable current = t;
        while (current != null) {
            if (current.getMessage() != null && current.getMessage().contains(needle)) return;
            current = current.getCause();
        }
        throw new AssertionError("expected an exception whose message chain mentions '" + needle
                + "', got: " + t, t);
    }

    @Test
    @Timeout(60)
    void versionedCatalogReadsRealData() throws Exception {
        // Not just an ATTACH-level check — confirm the catalog is actually
        // usable afterward (schema listing works), matching this fixture's
        // own comment that ATTACH validation is the whole point, not reading
        // rows (its one schema, "main", declares no tables).
        Assumptions.assumeTrue(VGI_RUST.isDirectory(), "~/Development/vgi-rust not present");
        spark = baseBuilder("vgi-spark-version-negotiation-usable-test").getOrCreate();
        java.util.List<Row> namespaces = spark.sql("SHOW NAMESPACES IN " + CATALOG).collectAsList();
        assertEquals(1, namespaces.size());
        assertEquals("main", namespaces.get(0).getString(0));
    }
}
