// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgispark.conformance;

import farm.query.vgispark.VgiCatalog;
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
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runs every eligible {@code .test} file in {@code
 * ~/Development/vgi/test/sql/integration/} against a single live worker and
 * a single Spark catalog attach, and reports honest, whole-corpus pass/fail
 * counts — the answer to "how many of the ~327 files actually pass", not
 * just the two files {@link VgiSqlLogicTestConformanceTest} curates markers
 * for.
 *
 * <h2>Why "eligible" and not "every file"</h2>
 *
 * <p>This is a NO-CURATION sweep by design — no per-file non-portable-marker
 * lists (327 files is not a reasonable size to hand-curate), so it can't
 * distinguish "genuinely not portable to Spark" from "needs infrastructure
 * this sweep doesn't set up" the way {@link VgiSqlLogicTestConformanceTest}
 * does. What it DOES do at the file level is filter down to files this
 * harness can plausibly run at all, via two cheap, mechanical checks against
 * each file's own text:
 *
 * <ul>
 *   <li>Its {@code ATTACH} line is EXACTLY the plain default form —
 *       {@code ATTACH 'example' AS example (TYPE vgi, LOCATION '${VGI_TEST_WORKER}');}
 *       — the one the huge majority of the suite uses (210 of 327 files).
 *       Files attaching under a different alias/catalog name, with extra
 *       ATTACH options (bearer tokens, {@code pool false}, a companion
 *       catalog), are running a DIFFERENT scenario (auth, pooling,
 *       multi-catalog) this sweep doesn't set up a second worker/catalog
 *       for — skipped at the file level, counted separately below.</li>
 *   <li>Its {@code require-env} directives name ONLY {@code VGI_TEST_WORKER}
 *       — a file additionally requiring e.g. {@code VGI_TEST_BRANCH_DIR}
 *       (multi-branch native-delegation fixtures) or {@code
 *       VGI_WORKER_SUPPORTS_DYNAMIC_CODE} needs environment this sweep
 *       doesn't provide — skipped, counted separately.</li>
 * </ul>
 *
 * <p>Within an eligible file, EVERY {@code statement ok}/{@code statement
 * error}/{@code query} record is attempted — no per-record marker list
 * either. A record using DuckDB-only syntax (table-function CALL syntax,
 * {@code DESCRIBE}, {@code duckdb_*()} introspection, {@code QUALIFY}, ...)
 * is expected to fail, and does; that's real information, not noise, which
 * is the whole point of running the actual files instead of a hand-picked
 * subset. The full per-file breakdown is written to {@code
 * build/sqllogictest-sweep-report.txt} for inspection; only the aggregate
 * numbers and a sample of failures are printed to the test log.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class VgiSqlLogicTestSweepTest {

    private static final File VGI_PYTHON = new File(System.getProperty("user.home"), "Development/vgi-python");
    private static final File VGI_TEST_ROOT =
            new File(System.getProperty("user.home"), "Development/vgi/test/sql/integration");
    private static final String SPARK_CATALOG = "vgi_example";
    private static final String STANDARD_ATTACH_LINE =
            "ATTACH 'example' AS example (TYPE vgi, LOCATION '${VGI_TEST_WORKER}');";

    private VgiWorkerHarness.Handle worker;
    private SparkSession spark;

    @BeforeAll
    void start() throws Exception {
        Assumptions.assumeTrue(VGI_PYTHON.isDirectory(),
                "~/Development/vgi-python not present — skipping sqllogictest sweep");
        Assumptions.assumeTrue(VGI_TEST_ROOT.isDirectory(),
                "~/Development/vgi/test/sql/integration not present — skipping sqllogictest sweep");
        worker = VgiWorkerHarness.subprocess(VGI_PYTHON);

        spark = SparkSession.builder()
                .master("local[2]")
                .appName("vgi-spark-sqllogictest-sweep")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.catalog." + SPARK_CATALOG, VgiCatalog.class.getName())
                .config("spark.sql.catalog." + SPARK_CATALOG + ".location", worker.location())
                .config("spark.sql.catalog." + SPARK_CATALOG + ".catalog-name", "example")
                .getOrCreate();
    }

    @AfterAll
    void stop() throws Exception {
        if (spark != null) spark.stop();
        if (worker != null) worker.teardown().close();
    }

    private record FileOutcome(Path file, int passed, int failed, int erroredRecords,
                                List<String> failureSamples, String skipReason) {}

    @Test
    @Timeout(1800)
    void sweepTheWholeSuite() throws Exception {
        List<Path> allFiles;
        try (Stream<Path> walk = Files.walk(VGI_TEST_ROOT.toPath())) {
            allFiles = walk.filter(p -> p.toString().endsWith(".test")).sorted().toList();
        }

        List<FileOutcome> eligible = new ArrayList<>();
        Map<String, Integer> skippedByReason = new LinkedHashMap<>();

        int totalPassed = 0;
        int totalFailed = 0;
        List<String> allFailureSamples = new ArrayList<>();

        for (Path file : allFiles) {
            String skipReason = eligibilityGate(file);
            if (skipReason != null) {
                skippedByReason.merge(skipReason, 1, Integer::sum);
                continue;
            }
            FileOutcome outcome;
            try {
                outcome = runFile(file);
            } catch (Exception e) {
                // One file's unexpected failure (not a per-record SQL error,
                // which runFile already catches — something structural, e.g.
                // a session-level fault) must not lose every other file's
                // already-accumulated results. Catches Exception, not just
                // RuntimeException: Spark's own AnalysisException (thrown for
                // e.g. an unresolved routine) is a CHECKED exception —
                // confirmed the hard way once already in this codebase (see
                // VgiCatalog's loadTable/loadNamespaceMetadata) and re-learned
                // here when it escaped a narrower catch and aborted the whole
                // sweep after only one file.
                outcome = new FileOutcome(file, 0, 1, 1,
                        List.of("FILE-LEVEL ERROR: " + firstLine(e)), null);
            }
            eligible.add(outcome);
            totalPassed += outcome.passed();
            totalFailed += outcome.failed();
            allFailureSamples.addAll(outcome.failureSamples());
        }

        int filesWithFailures = (int) eligible.stream().filter(o -> o.failed() > 0).count();
        int filesFullyPassing = eligible.size() - filesWithFailures;

        StringBuilder report = new StringBuilder();
        report.append("VGI sqllogictest sweep — ").append(allFiles.size()).append(" files under ")
                .append(VGI_TEST_ROOT).append("\n\n");
        report.append("Eligible for this sweep (standard plain ATTACH, require-env VGI_TEST_WORKER only): ")
                .append(eligible.size()).append(" files\n");
        report.append("Skipped at the file level (different catalog/worker/auth scenario, not run at all):\n");
        skippedByReason.forEach((reason, count) -> report.append("  ").append(count).append("  ").append(reason)
                .append("\n"));
        report.append("\nRecord-level results across the ").append(eligible.size()).append(" eligible files:\n");
        report.append("  passed: ").append(totalPassed).append("\n");
        report.append("  failed: ").append(totalFailed).append("\n");
        report.append("  files fully passing: ").append(filesFullyPassing).append(" / ").append(eligible.size())
                .append("\n");
        report.append("  files with at least one failure: ").append(filesWithFailures).append("\n\n");

        report.append("Per-file breakdown (files with any failure only; fully-passing files omitted):\n");
        eligible.stream()
                .filter(o -> o.failed() > 0)
                .sorted(Comparator.comparing(o -> o.file().toString()))
                .forEach(o -> {
                    Path rel = VGI_TEST_ROOT.toPath().relativize(o.file());
                    report.append("  ").append(rel).append(": ").append(o.passed()).append(" passed, ")
                            .append(o.failed()).append(" failed\n");
                    for (String sample : o.failureSamples()) {
                        report.append("      ").append(sample.replace("\n", "\n      ")).append("\n");
                    }
                });

        Path reportPath = Path.of("build", "sqllogictest-sweep-report.txt");
        Files.createDirectories(reportPath.getParent());
        Files.writeString(reportPath, report.toString());

        System.out.println(report);
        System.out.println("Full report written to " + reportPath.toAbsolutePath());

        // A loose sanity floor, not a tight regression pin — this sweep's
        // whole point is exploratory honesty about a corpus most of which is
        // legitimately DuckDB-specific, not a curated 100%-pass suite. Catches
        // "something broke badly" (e.g. the worker never started, or a
        // regression that suddenly fails everything) without being brittle
        // against every small dialect-coverage change.
        assertTrue(totalPassed > 0, "expected at least some records to pass — see " + reportPath.toAbsolutePath());
    }

    /** @return a human-readable skip reason if {@code file} isn't eligible for this sweep, or {@code null} if it is. */
    private String eligibilityGate(Path file) throws IOException {
        List<String> lines = Files.readAllLines(file);
        boolean hasStandardAttach = lines.stream().map(String::strip).anyMatch(STANDARD_ATTACH_LINE::equals);
        if (!hasStandardAttach) {
            return "non-standard ATTACH (different alias/catalog, auth options, or pooling options)";
        }
        Set<String> requiredEnv = lines.stream()
                .map(String::strip)
                .filter(l -> l.startsWith("require-env "))
                .map(l -> l.substring("require-env ".length()).strip())
                .collect(java.util.stream.Collectors.toCollection(java.util.LinkedHashSet::new));
        if (!requiredEnv.equals(Set.of("VGI_TEST_WORKER"))) {
            return "requires additional environment beyond VGI_TEST_WORKER: " + requiredEnv;
        }
        return null;
    }

    private FileOutcome runFile(Path file) throws IOException {
        List<SqlLogicTestFile.Record> records = SqlLogicTestFile.parse(file);
        int passed = 0;
        int failed = 0;
        List<String> samples = new ArrayList<>();

        for (SqlLogicTestFile.Record record : records) {
            if (record.kind() != SqlLogicTestFile.Kind.QUERY
                    && record.kind() != SqlLogicTestFile.Kind.STATEMENT_OK
                    && record.kind() != SqlLogicTestFile.Kind.STATEMENT_ERROR) {
                continue;
            }
            String sql = String.join("\n", record.sql());
            if (sql.isBlank()) continue;
            if (sql.contains("ATTACH ") || sql.contains("DETACH")) {
                // Structural, not curation: EVERY file in the corpus opens
                // with an ATTACH and most close with a DETACH — this harness
                // attaches via spark.sql.catalog.* config instead, so these
                // statements are never meant to run as SQL against Spark,
                // the same way a JDBC connection string isn't a query. Not
                // counted as failed OR passed — this file's OWN attach/detach
                // is superseded, not a Spark-portability gap being hidden.
                continue;
            }

            String sparkSql = sql.replace("example.", SPARK_CATALOG + ".").strip();
            sparkSql = sparkSql.endsWith(";") ? sparkSql.substring(0, sparkSql.length() - 1) : sparkSql;

            try {
                switch (record.kind()) {
                    case QUERY -> {
                        List<Row> rows = spark.sql(sparkSql).collectAsList();
                        List<List<String>> actual = new ArrayList<>(rows.size());
                        for (Row row : rows) {
                            List<String> cells = new ArrayList<>(row.length());
                            for (int i = 0; i < row.length(); i++) {
                                Object v = row.isNullAt(i) ? null : row.get(i);
                                cells.add(v == null ? "NULL" : v.toString());
                            }
                            actual.add(cells);
                        }
                        if (actual.equals(record.expectedRows())) {
                            passed++;
                        } else {
                            failed++;
                            addSample(samples, "MISMATCH: " + sparkSql);
                        }
                    }
                    case STATEMENT_OK -> {
                        spark.sql(sparkSql).collect();
                        passed++;
                    }
                    case STATEMENT_ERROR -> {
                        try {
                            spark.sql(sparkSql).collect();
                            failed++;
                            addSample(samples, "EXPECTED ERROR, GOT NONE: " + sparkSql);
                        } catch (Exception expected) {
                            passed++;
                        }
                    }
                    default -> { }
                }
            } catch (Exception e) {
                // Exception, not RuntimeException: Spark's AnalysisException
                // (unresolved routine/column, parse errors, ...) is checked —
                // see the file-level catch above for where this was learned.
                failed++;
                addSample(samples, "ERROR: " + sparkSql + "\n  -> " + firstLine(e));
            }
        }
        return new FileOutcome(file, passed, failed, failed, samples, null);
    }

    private static void addSample(List<String> samples, String text) {
        if (samples.size() < 3) samples.add(text); // cap per-file samples — the full text is huge; enough to triage by pattern
    }

    private static String firstLine(Throwable t) {
        String msg = t.getMessage();
        if (msg == null) msg = t.toString();
        int nl = msg.indexOf('\n');
        return nl < 0 ? msg : msg.substring(0, nl);
    }
}
