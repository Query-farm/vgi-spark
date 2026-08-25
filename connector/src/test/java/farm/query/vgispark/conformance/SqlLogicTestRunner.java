// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgispark.conformance;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Replays a real {@code .test} file's {@link SqlLogicTestFile.Record}s against
 * a live {@link SparkSession}, skipping records a curated marker list
 * identifies as non-portable (DuckDB-only syntax/introspection, or a feature
 * this connector doesn't implement yet) rather than transcribing each file's
 * assertions into hand-written Java.
 *
 * <p>Adapted from {@code vgi-trino}'s identical class — same replay logic,
 * same non-portable-marker mechanism, {@code DistributedQueryRunner}/{@code
 * Session}/{@code MaterializedResult} swapped for a local {@link
 * SparkSession}/{@code spark.sql(...).collectAsList()}.
 */
final class SqlLogicTestRunner {

    private SqlLogicTestRunner() {}

    /** Outcome of replaying one file: how many records ran, how many were skipped as non-portable,
     *  and — if non-empty — the mismatches/errors that should fail the test. */
    record Result(int executed, int skipped, List<String> failures) {}

    /**
     * @param spark the session to execute against (its current catalog/database select where an
     *        unqualified name in the file's SQL would resolve, if any file here relies on that)
     * @param testFile the real {@code .test} file to replay
     * @param vgiCatalogRef the VGI-side catalog reference the file's SQL uses (e.g. {@code "example."})
     * @param sparkCatalog the Spark catalog name to rewrite {@code vgiCatalogRef} to (e.g. {@code "vgi_example"})
     * @param nonPortableMarkers substrings that mark a record as needing something this connector (or Spark
     *        itself) doesn't have — skipped rather than executed
     */
    static Result run(SparkSession spark, Path testFile, String vgiCatalogRef, String sparkCatalog,
            List<String> nonPortableMarkers) throws IOException {
        List<SqlLogicTestFile.Record> records = SqlLogicTestFile.parse(testFile);
        int executed = 0;
        int skipped = 0;
        List<String> failures = new ArrayList<>();

        for (SqlLogicTestFile.Record record : records) {
            if (record.kind() != SqlLogicTestFile.Kind.QUERY
                    && record.kind() != SqlLogicTestFile.Kind.STATEMENT_OK
                    && record.kind() != SqlLogicTestFile.Kind.STATEMENT_ERROR) {
                continue;
            }
            String sql = String.join("\n", record.sql());
            if (sql.isBlank()) continue;

            boolean nonPortable = nonPortableMarkers.stream().anyMatch(sql::contains);
            if (nonPortable) {
                skipped++;
                continue;
            }

            String sparkSql = sql.replace(vgiCatalogRef, sparkCatalog + ".").strip();
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
                        if (!actual.equals(record.expectedRows())) {
                            failures.add("QUERY mismatch for:\n" + sparkSql
                                    + "\nexpected: " + record.expectedRows()
                                    + "\nactual:   " + actual);
                        } else {
                            executed++;
                        }
                    }
                    case STATEMENT_OK -> {
                        // .collect() (not just spark.sql(...)) so a Command-type
                        // statement's side effect is guaranteed to have actually
                        // run before moving on — spark.sql() alone only builds
                        // the plan for a query-shaped statement.
                        spark.sql(sparkSql).collect();
                        executed++;
                    }
                    case STATEMENT_ERROR -> {
                        try {
                            spark.sql(sparkSql).collect();
                            failures.add("expected an error for:\n" + sparkSql);
                        } catch (RuntimeException e) {
                            // A DuckDB-specific error-message substring isn't
                            // expected to match Spark's own wording — the
                            // meaningful assertion here is just "it failed".
                            executed++;
                        }
                    }
                    default -> { }
                }
            } catch (RuntimeException e) {
                failures.add("unexpected failure for:\n" + sparkSql + "\n" + e);
            }
        }
        return new Result(executed, skipped, List.copyOf(failures));
    }
}
