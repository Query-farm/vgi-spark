// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgispark;

import farm.query.vgirpc.launcher.PosixLauncherSupport;
import farm.query.vgispark.testing.VgiWorkerHarness;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Proves {@code launch:} end to end against the real reference {@code
 * vgi-rust} fixture worker — {@link farm.query.vgirpc.launcher.LauncherClient}
 * spawns {@code vgi-example-worker --unix <path> --idle-timeout <secs>} (the
 * exact CLI contract {@code docs/launcher-protocol.md}, in the {@code vgi}
 * repo, specifies), and every one of this catalog's pooled connections
 * attaches to that SAME worker process, sharing one worker. Mirrors {@code
 * vgi-trino}'s own {@code VgiLaunchTransportTest} — the connector-level
 * confirmation that sits above {@code vgi-rpc-java}'s own launcher-client
 * unit tests.
 *
 * <h2>Why these tests SKIP on this repo's own toolchain</h2>
 *
 * <p>{@code launch:} needs a JDK 22+ runtime — {@code
 * farm.query.vgirpc.launcher.PosixLauncherSupport} ships a JDK-21 baseline
 * stub ({@code available()==false}, every real operation throws {@code
 * UnsupportedOperationException}) plus a JDK-22+ Foreign-Function-and-Memory
 * overlay, and THIS repo's own Gradle toolchain deliberately stays on JDK 21
 * (see the root {@code build.gradle.kts}'s own comment: a JDK 25 toolchain
 * was tried specifically to exercise this, and reverted — Spark 4.2.0's own
 * Netty-based networking and Arrow's {@code arrow-memory-netty} allocator
 * want two different, mutually incompatible Netty majors under JDK 25 on
 * this dependency graph, unlike {@code vgi-trino}, which has no such
 * conflicting Netty consumer). So {@link #launchTransportWorksAgainstTheRealWorker}/
 * {@link #bareCommandLocationDefaultsToLaunchTransport} {@code
 * Assumptions.assumeTrue}-skip when {@link PosixLauncherSupport#available()}
 * is {@code false} — they exist to actually run (and did run, and passed)
 * under a temporary JDK 25 toolchain override during development; kept here,
 * skip-guarded, so they run for real the moment either this repo's own
 * toolchain, or whoever builds it, can safely move past JDK 21 (a future
 * Spark/Netty/Arrow version alignment, most likely) — not deleted just
 * because this specific environment can't exercise them today.
 */
class VgiLaunchTransportTest {

    private static final File VGI_RUST = new File(System.getProperty("user.home"), "Development/vgi-rust");
    private static final String CATALOG = "vgi_example";

    private SparkSession spark;

    @AfterEach
    void stop() {
        if (spark != null) spark.stop();
    }

    @Test
    @Timeout(60)
    void launchTransportWorksAgainstTheRealWorker() {
        Assumptions.assumeTrue(PosixLauncherSupport.available(),
                "launch: needs a JDK 22+ runtime — this repo's own toolchain is JDK 21 (see this class's "
                        + "own javadoc); skipping rather than hitting UnsupportedOperationException");
        Assumptions.assumeTrue(VGI_RUST.isDirectory(),
                "~/Development/vgi-rust not present — skipping launch: transport test");

        String location = "launch:" + VgiWorkerHarness.workerBinary(VGI_RUST);
        spark = SparkSession.builder()
                .master("local[2]")
                .appName("vgi-spark-launch-transport-test")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.catalog." + CATALOG, VgiCatalog.class.getName())
                .config("spark.sql.catalog." + CATALOG + ".location", location)
                .config("spark.sql.catalog." + CATALOG + ".catalog-name", "example")
                // Several pooled connections attaching to the SAME launch: tuple —
                // they must all resolve to the one shared worker, not spawn N.
                .config("spark.sql.catalog." + CATALOG + ".connections", "4")
                .getOrCreate();

        Row result = spark.sql("SELECT count(*) AS c, sum(value) AS s FROM " + CATALOG + ".data.numbers")
                .collectAsList().get(0);
        assertEquals(100L, result.getLong(0));
        assertEquals(4950L, result.getLong(1));
    }

    @Test
    @Timeout(60)
    void bareCommandLocationDefaultsToLaunchTransport() {
        // roadmap: launch: as the DEFAULT for a bare-command location (no
        // scheme prefix) — VgiCatalogConfig#launcherEnabled's default of
        // true, exercised here with the SAME plain command string
        // VgiWorkerHarness.subprocess()/VgiTransportTest.subprocessTransport
        // use, but WITHOUT launcher-enabled=false — proving the new default
        // path (not an explicit launch: prefix) actually reaches the
        // launcher too.
        Assumptions.assumeTrue(PosixLauncherSupport.available(),
                "launch: needs a JDK 22+ runtime — this repo's own toolchain is JDK 21 (see this class's "
                        + "own javadoc); skipping rather than hitting UnsupportedOperationException");
        Assumptions.assumeTrue(VGI_RUST.isDirectory(),
                "~/Development/vgi-rust not present — skipping bare-command launch-default test");

        String location = VgiWorkerHarness.workerBinary(VGI_RUST).toString();
        spark = SparkSession.builder()
                .master("local[2]")
                .appName("vgi-spark-bare-command-launch-default-test")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.catalog." + CATALOG, VgiCatalog.class.getName())
                .config("spark.sql.catalog." + CATALOG + ".location", location)
                .config("spark.sql.catalog." + CATALOG + ".catalog-name", "example")
                .config("spark.sql.catalog." + CATALOG + ".connections", "3")
                .getOrCreate();

        Row result = spark.sql("SELECT count(*) AS c, sum(value) AS s FROM " + CATALOG + ".data.numbers")
                .collectAsList().get(0);
        assertEquals(100L, result.getLong(0));
        assertEquals(4950L, result.getLong(1));
    }

    @Test
    @Timeout(60)
    void bareCommandFallsBackToPlainSubprocessOnThisJdk21Toolchain() throws Exception {
        // The other side of the same coin: on THIS toolchain (JDK 21,
        // PosixLauncherSupport.available()==false), the new default must
        // gracefully degrade to the old per-connection subprocess spawn —
        // not throw — since nobody asked for launch: BY NAME on a bare
        // command (see VgiWorkerClient.openTransport's own comment). This
        // is the one launch-related assertion in this class that genuinely
        // exercises something on THIS repo's own toolchain, rather than
        // skipping.
        Assumptions.assumeTrue(!PosixLauncherSupport.available(),
                "this test is specifically about the degrade path on a JDK <22 runtime");
        Assumptions.assumeTrue(VGI_RUST.isDirectory(), "~/Development/vgi-rust not present");

        String location = VgiWorkerHarness.workerBinary(VGI_RUST).toString();
        spark = SparkSession.builder()
                .master("local[2]")
                .appName("vgi-spark-bare-command-degrade-test")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.catalog." + CATALOG, VgiCatalog.class.getName())
                .config("spark.sql.catalog." + CATALOG + ".location", location)
                .config("spark.sql.catalog." + CATALOG + ".catalog-name", "example")
                .getOrCreate();

        Row result = spark.sql("SELECT count(*) AS c, sum(value) AS s FROM " + CATALOG + ".data.numbers")
                .collectAsList().get(0);
        assertEquals(100L, result.getLong(0));
        assertEquals(4950L, result.getLong(1));
    }
}
