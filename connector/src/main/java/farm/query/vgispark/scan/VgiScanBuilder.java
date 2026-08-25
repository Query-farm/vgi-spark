// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgispark.scan;

import farm.query.vgispark.VgiCatalogConfig;
import farm.query.vgispark.VgiTable;
import farm.query.vgispark.client.VgiWorkerClient;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;

/**
 * Builds one {@link VgiScan} for a table read.
 *
 * <p>v1 does not yet implement {@code SupportsPushDownRequiredColumns}/
 * {@code SupportsPushDownV2Filters}/{@code SupportsPushDownLimit} — every
 * scan reads every column with no predicate or limit pushed to the worker,
 * exactly like {@code vgi-trino}'s {@code VgiSplitManager} before its own
 * pushdown phase. See the plan's Phase 3.
 */
public final class VgiScanBuilder implements ScanBuilder {

    private final VgiWorkerClient client;
    private final VgiCatalogConfig config;
    private final VgiTable table;

    /**
     * @param client the pooled connection to this catalog's VGI worker
     * @param config this catalog's configuration
     * @param table the table being scanned
     */
    public VgiScanBuilder(VgiWorkerClient client, VgiCatalogConfig config, VgiTable table) {
        this.client = client;
        this.config = config;
        this.table = table;
    }

    @Override
    public Scan build() {
        return new VgiScan(client, config, table);
    }
}
