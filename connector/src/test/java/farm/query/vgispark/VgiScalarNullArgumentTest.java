// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgispark;

import farm.query.vgispark.client.VgiWorkerClient;
import farm.query.vgispark.function.VgiScalarFunctions;
import farm.query.vgispark.testing.VgiWorkerHarness;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.functions.ScalarFunction;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.util.Map;

/**
 * Isolates the {@code scalarOverloadMatchesTheRealTestFile} NULL-argument
 * failure (see {@code CLAUDE.md}'s "Open bugs" section) from Spark's own
 * {@code ConstantFolding} optimizer entirely — a single, direct {@code
 * produceResult} call, no {@code spark.sql()}, no query planning, no
 * repeated calls on the same bound-function instance. If this reproduces
 * the same {@code NoSuchElementException}, the bug is purely "exchange a
 * single NULL-valued row to a scalar function," nothing to do with
 * {@code ConstantFolding} or the earlier (disproven) connection-reuse
 * hypothesis — a much smaller surface to debug from there.
 */
class VgiScalarNullArgumentTest {

    private static final File VGI_RUST = new File(System.getProperty("user.home"), "Development/vgi-rust");

    private VgiWorkerHarness.Handle worker;
    private VgiWorkerClient client;

    @AfterEach
    void stop() throws Exception {
        if (client != null) client.close();
        if (worker != null) worker.teardown().close();
    }

    @Test
    @Timeout(30)
    void formatNumberDefaultOverloadAcceptsANullValueArgument() throws Exception {
        Assumptions.assumeTrue(VGI_RUST.isDirectory(), "~/Development/vgi-rust not present");
        worker = VgiWorkerHarness.unix(VGI_RUST);
        VgiCatalogConfig config = VgiCatalogConfig.fromOptions(new CaseInsensitiveStringMap(
                Map.of("location", worker.location(), "catalog-name", "example")));
        client = new VgiWorkerClient(config);

        UnboundFunction unbound = VgiScalarFunctions.loadFunction(client, config,
                Identifier.of(new String[] {"main"}, "format_number"));
        StructType inputSchema = new StructType(new StructField[] {
                new StructField("value", DataTypes.DoubleType, true, Metadata.empty())
        });
        @SuppressWarnings("unchecked")
        ScalarFunction<Object> bound = (ScalarFunction<Object>) unbound.bind(inputSchema);

        InternalRow row = new GenericInternalRow(new Object[] {null});
        // format_number(NULL::DOUBLE) -> NULL, per the worker's own
        // FormatNumber::Default::process: vals.iter().map(|v| v.map(...))
        // maps a None straight through. If this throws instead, the bug
        // reproduces with ZERO Spark involvement beyond bind() itself.
        Object result = bound.produceResult(row);
        org.junit.jupiter.api.Assertions.assertNull(result);
    }
}
