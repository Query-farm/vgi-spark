// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgispark.scan;

import farm.query.vgi.client.ScalarValue;
import farm.query.vgi.client.ScanFunctionArguments;
import farm.query.vgispark.branch.VgiNativeScanBranch;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Table;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * Pure-unit coverage of {@link VgiNativeScanResolver} — synthetic {@link
 * VgiNativeScanBranch}es, no VGI worker involved (mirrors {@code
 * VgiScalarNullArgumentTest}'s own direct-call style). {@code read_parquet}
 * gets a real local-file round trip in addition to the error-path coverage
 * every target shares; {@code iceberg_scan} is exercised only through its
 * argument-validation and missing-dependency paths — this repo doesn't
 * depend on {@code iceberg-spark-runtime} at test runtime (it's {@code
 * compileOnly}, an operator-supplied cluster dependency — see {@code
 * connector/build.gradle.kts}), so a real Iceberg table isn't available to
 * round-trip against here; {@link VgiNativeScanResolver}'s own javadoc
 * documents this target as unverified against a live worker for the same
 * reason.
 */
class VgiNativeScanResolverTest {

    private SparkSession spark;

    @AfterEach
    void stop() {
        if (spark != null) spark.stop();
    }

    private void startSpark(String appName) {
        spark = SparkSession.builder().master("local[1]").appName(appName)
                .config("spark.ui.enabled", "false").getOrCreate();
    }

    private static ScanFunctionArguments.Decoded decoded(String path, Map<String, Object> named) {
        List<ScalarValue> positional = path == null ? List.of() : List.of(ScalarValue.of(path));
        java.util.Map<String, ScalarValue> namedScalars = new java.util.LinkedHashMap<>();
        named.forEach((k, v) -> namedScalars.put(k, ScalarValue.of(v)));
        return new ScanFunctionArguments.Decoded(positional, namedScalars);
    }

    @Test
    @Timeout(30)
    void readParquetResolvesARealLocalFile() throws IOException {
        startSpark("vgi-spark-native-scan-read-parquet");
        Path dir = Files.createTempDirectory("vgi-native-scan-parquet");
        String path = dir.resolve("data").toString();
        spark.range(3).write().parquet(path);

        VgiNativeScanBranch branch = new VgiNativeScanBranch("read_parquet", decoded(path, Map.of()));
        Table table = VgiNativeScanResolver.resolve("data.numbers", branch, List.of(), false);

        Assertions.assertNotNull(table);
        Assertions.assertEquals(1, table.columns().length);
        Assertions.assertEquals("id", table.columns()[0].name());
    }

    @Test
    @Timeout(30)
    void readCsvResolvesARealLocalFile() throws IOException {
        startSpark("vgi-spark-native-scan-read-csv");
        Path file = Files.createTempFile("vgi-native-scan", ".csv");
        Files.writeString(file, "a,b\n1,x\n2,y\n");

        VgiNativeScanBranch branch = new VgiNativeScanBranch(
                "read_csv", decoded(file.toString(), Map.of()));
        Table table = VgiNativeScanResolver.resolve("data.rows", branch, List.of(), false);

        Assertions.assertNotNull(table);
        // CSVDataSourceV2's own schema inference reads the file's own header
        // by default off, so this just confirms A schema was inferred at
        // all, not the exact (headerless "_c0"/"_c1"-shaped) column names.
        Assertions.assertEquals(2, table.columns().length);
    }

    @Test
    void noPositionalArgumentsIsRefused() {
        VgiNativeScanBranch branch = new VgiNativeScanBranch("read_parquet", decoded(null, Map.of()));
        IllegalStateException e = Assertions.assertThrows(IllegalStateException.class,
                () -> VgiNativeScanResolver.resolve("data.t", branch, List.of(), false));
        Assertions.assertTrue(e.getMessage().contains("no positional arguments"), e.getMessage());
    }

    @Test
    void nonStringPathArgumentIsRefused() {
        ScanFunctionArguments.Decoded args =
                new ScanFunctionArguments.Decoded(List.of(ScalarValue.of(42L)), Map.of());
        VgiNativeScanBranch branch = new VgiNativeScanBranch("read_parquet", args);
        IllegalStateException e = Assertions.assertThrows(IllegalStateException.class,
                () -> VgiNativeScanResolver.resolve("data.t", branch, List.of(), false));
        Assertions.assertTrue(e.getMessage().contains("expected a path"), e.getMessage());
    }

    @Test
    void unknownNamedArgumentIsRefusedForReadParquet() {
        VgiNativeScanBranch branch = new VgiNativeScanBranch(
                "read_parquet", decoded("/nonexistent", Map.of("hive_partitioning", true)));
        IllegalStateException e = Assertions.assertThrows(IllegalStateException.class,
                () -> VgiNativeScanResolver.resolve("data.t", branch, List.of(), false));
        Assertions.assertTrue(e.getMessage().contains("hive_partitioning"), e.getMessage());
    }

    @Test
    void unknownNamedArgumentIsRefusedForReadCsv() {
        VgiNativeScanBranch branch = new VgiNativeScanBranch(
                "read_csv", decoded("/nonexistent", Map.of("delim", ";")));
        IllegalStateException e = Assertions.assertThrows(IllegalStateException.class,
                () -> VgiNativeScanResolver.resolve("data.t", branch, List.of(), false));
        Assertions.assertTrue(e.getMessage().contains("delim"), e.getMessage());
    }

    @Test
    void requiredFiltersRefusedWithoutAcknowledgment() {
        VgiNativeScanBranch branch = new VgiNativeScanBranch(
                "read_parquet", decoded("/nonexistent", Map.of()));
        IllegalStateException e = Assertions.assertThrows(IllegalStateException.class, () ->
                VgiNativeScanResolver.resolve(
                        "places.place", branch, List.of(List.of("bbox.xmin")), false));
        Assertions.assertTrue(e.getMessage().contains("acknowledge-native-scan-required-filters"),
                e.getMessage());
    }

    @Test
    @Timeout(30)
    void requiredFiltersSucceedsWhenAcknowledged() throws IOException {
        startSpark("vgi-spark-native-scan-required-filters-ack");
        Path dir = Files.createTempDirectory("vgi-native-scan-required-filters");
        String path = dir.resolve("data").toString();
        spark.range(3).write().parquet(path);

        VgiNativeScanBranch branch = new VgiNativeScanBranch("read_parquet", decoded(path, Map.of()));
        Table table = VgiNativeScanResolver.resolve(
                "places.place", branch, List.of(List.of("bbox.xmin")), true);
        Assertions.assertNotNull(table);
    }

    @Test
    void icebergScanRefusesAnUnmappedNamedArgument() {
        VgiNativeScanBranch branch = new VgiNativeScanBranch(
                "iceberg_scan", decoded("/nonexistent", Map.of("allow_moved_paths", true)));
        IllegalStateException e = Assertions.assertThrows(IllegalStateException.class,
                () -> VgiNativeScanResolver.resolve("data.t", branch, List.of(), false));
        Assertions.assertTrue(e.getMessage().contains("allow_moved_paths"), e.getMessage());
    }

    /**
     * {@code iceberg-spark-runtime} isn't on this test's runtime classpath
     * (compileOnly, an operator-supplied cluster dependency) — resolving a
     * valid iceberg_scan delegation should still fail with THIS connector's
     * own clear, actionable message (naming what's missing), never a bare
     * {@link NoClassDefFoundError} escaping uncaught. This is exactly the
     * scenario an operator who hasn't added the Iceberg jar yet would hit.
     */
    @Test
    void icebergScanWithoutTheRuntimeJarFailsWithAClearError() {
        VgiNativeScanBranch branch = new VgiNativeScanBranch(
                "iceberg_scan", decoded("/nonexistent", Map.of("snapshot_from_id", 123L)));
        IllegalStateException e = Assertions.assertThrows(IllegalStateException.class,
                () -> VgiNativeScanResolver.resolve("data.t", branch, List.of(), false));
        Assertions.assertTrue(e.getMessage().contains("iceberg-spark-runtime"), e.getMessage());
        Assertions.assertInstanceOf(NoClassDefFoundError.class, e.getCause());
    }
}
