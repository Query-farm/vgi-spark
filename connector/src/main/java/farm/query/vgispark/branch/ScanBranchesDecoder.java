// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgispark.branch;

import farm.query.vgirpc.marshal.Marshalling;
import farm.query.vgirpc.wire.Allocators;
import farm.query.vgirpc.wire.IpcStreamReader;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Decode the {@code ScanBranchesResult} bytes {@code catalog_table_scan_branches_get}
 * returns.
 *
 * <p>The inverse of the worker-side {@code ScanBranchesResultSerializer}
 * (vgi-java, {@code farm.query.vgi.internal}) — a hand-rolled wire shape (a
 * {@code list<binary>} of independently-IPC-encoded branch blobs, each itself
 * a 1-row batch), not a plain {@code ArrowSerializableRecord}, so {@code
 * RecordCodec} can't read it back — the same reason {@link
 * farm.query.vgi.client.TableInfoDecoder} exists for {@code TableInfo}. No
 * JVM consumer has needed the CLIENT side of this decode before now (neither
 * {@code vgi-java}'s own client package nor {@code vgi-trino} implement
 * multi-branch tables); this is a first port, built directly from
 * {@code ScanBranchesResultSerializer}'s own encode logic, field-for-field.
 *
 * <p>Outer schema: {@code {branches: list<binary>, required_extensions: list<utf8>}}.
 * Each branch blob's schema, in field order: {@code function_name: utf8},
 * {@code arguments: binary}, {@code branch_filter: utf8?}, {@code writable: bool},
 * {@code source_catalog: utf8?}, {@code source_schema: utf8?}, {@code source_table: utf8?},
 * {@code format_name: utf8?}, {@code format_locations: list&lt;utf8&gt;?},
 * {@code format_options: binary?}.
 */
public final class ScanBranchesDecoder {

    private ScanBranchesDecoder() {}

    /** One decoded branch — see {@code ScanBranch}'s own javadoc (vgi-java) for the three mutually-exclusive kinds. */
    public record DecodedBranch(
            String functionName,
            byte[] arguments,
            String branchFilter,
            boolean writable,
            String sourceCatalog,
            String sourceSchema,
            String sourceTable,
            String formatName,
            List<String> formatLocations,
            byte[] formatOptions) {

        public enum Kind { FUNCTION, CATALOG_TABLE, FORMAT }

        /** @return which of the three mutually-exclusive branch kinds this is. */
        public Kind kind() {
            if (sourceTable != null && !sourceTable.isEmpty()) return Kind.CATALOG_TABLE;
            if (formatName != null && !formatName.isEmpty()) return Kind.FORMAT;
            return Kind.FUNCTION;
        }
    }

    /**
     * @param branches one decoded branch per unit, in the worker's own order
     * @param requiredExtensions DuckDB extension names the branches depend on — meaningless to
     *        Spark, carried through only for completeness/diagnostics
     */
    public record Result(List<DecodedBranch> branches, List<String> requiredExtensions) {}

    /**
     * Decode the outer {@code ScanBranchesResult} bytes.
     *
     * @param bytes the raw {@code catalog_table_scan_branches_get} response
     * @return the decoded branches
     */
    public static Result decode(byte[] bytes) {
        Map<String, Object> row = readOneRow(bytes);
        @SuppressWarnings("unchecked")
        List<Object> rawBranches = (List<Object>) row.get("branches");
        @SuppressWarnings("unchecked")
        List<Object> rawExtensions = (List<Object>) row.get("required_extensions");

        List<DecodedBranch> branches = new ArrayList<>(rawBranches == null ? 0 : rawBranches.size());
        if (rawBranches != null) {
            for (Object item : rawBranches) {
                branches.add(decodeBranch((byte[]) item));
            }
        }
        List<String> extensions = new ArrayList<>(rawExtensions == null ? 0 : rawExtensions.size());
        if (rawExtensions != null) {
            for (Object item : rawExtensions) extensions.add(item == null ? null : item.toString());
        }
        return new Result(List.copyOf(branches), List.copyOf(extensions));
    }

    private static DecodedBranch decodeBranch(byte[] blob) {
        Map<String, Object> row = readOneRow(blob);
        return new DecodedBranch(
                str(row, "function_name"),
                bytes(row, "arguments"),
                str(row, "branch_filter"),
                Boolean.TRUE.equals(row.get("writable")),
                str(row, "source_catalog"),
                str(row, "source_schema"),
                str(row, "source_table"),
                str(row, "format_name"),
                stringList(row, "format_locations"),
                bytes(row, "format_options"));
    }

    private static Map<String, Object> readOneRow(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            throw new IllegalStateException("ScanBranchesDecoder: empty item bytes");
        }
        try (IpcStreamReader r = new IpcStreamReader(new ByteArrayInputStream(bytes), Allocators.root())) {
            if (r.readNextBatch() == null) {
                throw new IllegalStateException("ScanBranchesResult item carried no batch");
            }
            VectorSchemaRoot root = r.root();
            if (root.getRowCount() != 1) {
                throw new IllegalStateException(
                        "ScanBranchesResult item must be a single row, got " + root.getRowCount());
            }
            return Marshalling.decodeRow(root, r.dictionaryProvider(), r.wireSchema());
        } catch (Exception e) {
            if (e instanceof IllegalStateException ise) throw ise;
            throw new IllegalStateException("ScanBranchesDecoder.decode failed", e);
        }
    }

    private static String str(Map<String, Object> row, String field) {
        Object v = row.get(field);
        return v == null ? null : v.toString();
    }

    private static byte[] bytes(Map<String, Object> row, String field) {
        return (byte[]) row.get(field);
    }

    @SuppressWarnings("unchecked")
    private static List<String> stringList(Map<String, Object> row, String field) {
        Object v = row.get(field);
        if (!(v instanceof List<?> l)) return List.of();
        List<String> out = new ArrayList<>(l.size());
        for (Object o : l) out.add(o == null ? null : o.toString());
        return List.copyOf(out);
    }
}
