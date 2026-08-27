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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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

    private static final File VGI_RUST = new File(System.getProperty("user.home"), "Development/vgi-rust");
    private static final File VGI_TEST_ROOT =
            new File(System.getProperty("user.home"), "Development/vgi/test/sql/integration");
    private static final String SPARK_CATALOG = "vgi_example";
    private static final String STANDARD_ATTACH_LINE =
            "ATTACH 'example' AS example (TYPE vgi, LOCATION '${VGI_TEST_WORKER}');";
    // How many files run concurrently. Each file's own records still run one
    // at a time (runFile is a plain sequential loop) — this is file-level
    // parallelism only. Bounded, not "as many as files": the worker is a
    // single vgi-example-worker process serving connections on its own
    // threads, so unlimited concurrency wouldn't keep scaling this driver
    // machine's own resources indefinitely, just add contention. Derived from
    // the ACTUAL core count rather than a hardcoded guess (a stale "8" here
    // once meant "a plausible dev-machine core count" but silently starved a
    // run on a bigger box instead of scaling up to it) — capped at 32 since
    // the Rust worker's own --unix mode, while threaded, is still one
    // process, and past some width the RPC lockstep-per-connection
    // constraint means more concurrent connections mostly just add
    // contention on that one process rather than real throughput.
    private static final int FILE_PARALLELISM =
            Math.max(2, Math.min(32, Runtime.getRuntime().availableProcessors()));

    private VgiWorkerHarness.Handle worker;
    private SparkSession spark;

    @BeforeAll
    void start() throws Exception {
        Assumptions.assumeTrue(VGI_RUST.isDirectory(),
                "~/Development/vgi-rust not present — skipping sqllogictest sweep");
        Assumptions.assumeTrue(VGI_TEST_ROOT.isDirectory(),
                "~/Development/vgi/test/sql/integration not present — skipping sqllogictest sweep");
        // unix(), not subprocess(): a bare-command location makes VgiWorkerClient
        // fork a FRESH worker subprocess for every pooled connection AND every
        // per-task connection VgiPartitionReader opens (see that class's own
        // javadoc — executors don't share the driver's pool) — with ~2900
        // records across 191 files, that's thousands of subprocess spawns.
        // unix() starts ONE real worker process up front and hands out a
        // unix:// socket location instead, so every one of those "new
        // connection" calls becomes a cheap socket connect to an
        // already-running, already-warm worker (the reference worker's own
        // --unix mode defaults to --threaded, i.e. built to serve many
        // concurrent connections — this is exactly the "pool of one warm
        // worker" shape the VGI protocol's own launch: scheme formalizes,
        // see docs/ROADMAP.md tier 3, just without that scheme's cross-process
        // sharing). See VgiWorkerHarness's own javadoc for the SEPARATE,
        // larger win this sweep gets from vgi-rust's compiled binary over
        // vgi-python's uv-run-launched interpreter: the per-process startup
        // cost this ONE worker pays once, up front, before any of the ~2900
        // records run.
        worker = VgiWorkerHarness.unix(VGI_RUST);

        spark = SparkSession.builder()
                // local[8], not local[2]: sweepTheWholeSuite() now submits
                // FILE_PARALLELISM files' queries concurrently from multiple
                // driver threads (one SparkSession safely serves concurrent
                // spark.sql() calls from many threads — a supported,
                // well-established pattern), and a 2-thread local executor
                // would just serialize their actual task execution behind
                // FILE_PARALLELISM concurrent submitters instead of the
                // sequential loop this used to have — no real gain.
                .master("local[" + FILE_PARALLELISM + "]")
                .appName("vgi-spark-sqllogictest-sweep")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.catalog." + SPARK_CATALOG, VgiCatalog.class.getName())
                .config("spark.sql.catalog." + SPARK_CATALOG + ".location", worker.location())
                .config("spark.sql.catalog." + SPARK_CATALOG + ".catalog-name", "example")
                // Bumped from the connector's own default (4): the driver-side
                // VgiWorkerClient pool (catalog/table discovery, every scan's
                // bind+plan) is shared across every concurrently-running file's
                // thread — matches FILE_PARALLELISM so concurrent files don't
                // serialize waiting on pool borrows.
                .config("spark.sql.catalog." + SPARK_CATALOG + ".connections", String.valueOf(FILE_PARALLELISM))
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

        List<Path> toRun = new ArrayList<>();
        Map<String, Integer> skippedByReason = new LinkedHashMap<>();
        for (Path file : allFiles) {
            String skipReason = eligibilityGate(file);
            if (skipReason != null) {
                skippedByReason.merge(skipReason, 1, Integer::sum);
            } else {
                toRun.add(file);
            }
        }

        // Files run concurrently (up to FILE_PARALLELISM at once); each
        // file's OWN records still run sequentially inside runFile — this is
        // file-level, not record-level, parallelism. Submitting first and
        // collecting after (rather than accumulating totals inside the
        // per-file task) keeps every shared mutable variable
        // (eligible/totalPassed/totalFailed/allFailureSamples below)
        // touched from this one thread only, so none of it needs
        // synchronization.
        List<FileOutcome> eligible = new ArrayList<>();
        int totalPassed = 0;
        int totalFailed = 0;
        List<String> allFailureSamples = new ArrayList<>();

        ExecutorService pool = Executors.newFixedThreadPool(FILE_PARALLELISM,
                r -> { Thread t = new Thread(r, "vgi-sweep-file"); t.setDaemon(true); return t; });
        try {
            List<Future<FileOutcome>> futures = new ArrayList<>(toRun.size());
            for (Path file : toRun) {
                futures.add(pool.submit(() -> {
                    try {
                        return runFile(file);
                    } catch (Exception e) {
                        // One file's unexpected failure (not a per-record SQL
                        // error, which runFile already catches — something
                        // structural, e.g. a session-level fault) must not lose
                        // every other file's already-accumulated results.
                        // Catches Exception, not just RuntimeException: Spark's
                        // own AnalysisException (thrown for e.g. an unresolved
                        // routine) is a CHECKED exception — confirmed the hard
                        // way once already in this codebase (see VgiCatalog's
                        // loadTable/loadNamespaceMetadata) and re-learned here
                        // when it escaped a narrower catch and aborted the
                        // whole sweep after only one file.
                        return new FileOutcome(file, 0, 1, 1,
                                List.of("FILE-LEVEL ERROR: " + firstLine(e)), null);
                    }
                }));
            }
            for (Future<FileOutcome> future : futures) {
                FileOutcome outcome = future.get();
                eligible.add(outcome);
                totalPassed += outcome.passed();
                totalFailed += outcome.failed();
                allFailureSamples.addAll(outcome.failureSamples());
            }
        } finally {
            pool.shutdownNow();
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
        // A fresh session PER FILE, not the shared field: SET/RESET (e.g.
        // settings/multiply_by_setting.test) mutate SESSION-scoped runtime
        // config, and with FILE_PARALLELISM files now running concurrently
        // against what used to be one shared session, one file's SET could
        // race another's — caught for real here (multiply_by_setting.test
        // intermittently saw "requires settings: ['multiplier']" once file
        // parallelism landed). SparkSession.newSession() gives isolated SQL
        // config/temp views while still sharing the underlying SparkContext
        // and this session's already-attached catalogs (VgiCatalog itself
        // isn't re-initialized — Spark caches catalog plugin instances at
        // the session-shared level, so this doesn't reopen worker
        // connections per file).
        SparkSession session = spark.newSession();
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
                        List<Row> rows = session.sql(sparkSql).collectAsList();
                        List<List<String>> actual = new ArrayList<>(rows.size());
                        for (Row row : rows) {
                            List<String> cells = new ArrayList<>(row.length());
                            for (int i = 0; i < row.length(); i++) {
                                Object v = row.isNullAt(i) ? null : row.get(i);
                                // "(empty)" is DuckDB sqllogictest's own convention for an
                                // empty-string cell — mirrored on the actual side the same way
                                // "NULL" already mirrors a null cell. Kept in sync with
                                // SqlLogicTestRunner's identical normalization (this class
                                // duplicates that replay loop rather than reusing it, for its
                                // own per-file bookkeeping — see this class's own notes).
                                String s = v == null ? "NULL" : v.toString();
                                cells.add(s.isEmpty() ? "(empty)" : s);
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
                        session.sql(sparkSql).collect();
                        passed++;
                    }
                    case STATEMENT_ERROR -> {
                        try {
                            session.sql(sparkSql).collect();
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
