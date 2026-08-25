// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgispark;

import farm.query.vgispark.client.VgiWorkerClient;
import farm.query.vgispark.scan.VgiScanBuilder;
import farm.query.vgispark.types.ArrowSchemaCodec;
import farm.query.vgispark.types.VgiColumnNames;
import farm.query.vgispark.types.VgiTypeMapping;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.connector.catalog.Column;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Set;

/**
 * A bound VGI table: which schema/table, and the scan function + arguments
 * {@code catalog_table_scan_function_get} resolved for it — the Spark analog
 * of {@code vgi-trino}'s {@code VgiTableHandle}, folded together with what
 * Trino splits into a separate {@code ConnectorMetadata} for column/type
 * lookups, since Spark's {@link Table} is itself the unit that carries both.
 *
 * @param schemaName the VGI schema this table lives in
 * @param tableName the table name
 * @param scanFunctionName the table function {@code catalog_table_scan_function_get}
 *        named to actually perform the scan
 * @param scanFunctionArguments the scan function's bound arguments, already
 *        re-encoded as {@code BindRequest.arguments} bytes (see
 *        {@code farm.query.vgi.client.ScanFunctionArguments#toBindArguments})
 * @param outputSchemaBytes the table's full (unprojected) Arrow schema, IPC-encoded
 * @param cardinalityEstimate the worker's own row-count estimate
 *        ({@code TableInfo.cardinality_estimate}), or {@code null} if it offered none
 * @param atUnit the resolved time-travel AT clause's unit, or {@code null}
 *        for a plain (non-time-travel) read
 * @param atValue the resolved AT clause's value, or {@code null} exactly when
 *        {@code atUnit} is {@code null} (VGI requires both or neither)
 * @param client the pooled connection to this catalog's VGI worker, used for
 *        this table's own bind+plan (driver-side only — see {@code VgiPartitionReader}
 *        for how executors get their own, unpooled connection)
 * @param config this catalog's configuration
 */
public record VgiTable(
        String schemaName,
        String tableName,
        String scanFunctionName,
        byte[] scanFunctionArguments,
        byte[] outputSchemaBytes,
        Long cardinalityEstimate,
        String atUnit,
        String atValue,
        VgiWorkerClient client,
        VgiCatalogConfig config) implements Table, SupportsRead {

    /** @return the table's full (unprojected) Arrow schema, decoded from {@link #outputSchemaBytes} */
    public Schema outputSchema() {
        return ArrowSchemaCodec.deserializeSchema(outputSchemaBytes);
    }

    @Override
    public String name() {
        return schemaName + "." + tableName;
    }

    // Table.schema() is deprecated in favor of columns() as of Spark 4.x —
    // columns() below is the source of truth; Table's own default schema()
    // derives a StructType from it, so this class doesn't override schema()
    // directly.

    @Override
    public Column[] columns() {
        Schema arrow = outputSchema();
        java.util.List<Field> fields = arrow == null ? java.util.List.of() : arrow.getFields();
        Column[] out = new Column[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            out[i] = Column.create(
                    VgiColumnNames.displayName(field), VgiTypeMapping.toSparkType(field), field.isNullable());
        }
        return out;
    }

    @Override
    public Set<TableCapability> capabilities() {
        return Set.of(TableCapability.BATCH_READ);
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        return new VgiScanBuilder(client, config, this);
    }

    /** @return this table's columns, indexed by wire field name (not display name) to ordinal. */
    public java.util.Map<String, Integer> ordinalsByWireName() {
        Schema arrow = outputSchema();
        java.util.Map<String, Integer> out = new java.util.LinkedHashMap<>();
        if (arrow == null) return out;
        java.util.List<Field> fields = arrow.getFields();
        for (int i = 0; i < fields.size(); i++) {
            out.put(fields.get(i).getName(), i);
        }
        return out;
    }
}
