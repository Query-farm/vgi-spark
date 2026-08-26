// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgispark.scan;

import farm.query.vgispark.types.ArrowSchemaCodec;
import farm.query.vgispark.types.VgiColumnNames;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.vectorized.ArrowColumnVector;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Reads one {@link VgiFormatInputPartition} (one CSV file) directly — no VGI
 * RPC involved, unlike {@link VgiPartitionReader}. Reads the WHOLE file into
 * one {@link ColumnarBatch} on the first {@link #next()} call (v1 scope: the
 * real corpus fixture this backs is a handful of rows; a genuinely large
 * format-branch file would want real streaming/chunked batches, tracked as a
 * follow-up rather than attempted here).
 *
 * <h2>Deliberately narrow scope — see {@code VgiFormatScanBranch}'s own javadoc</h2>
 *
 * <p>A delimiter-plus-optional-header SPLIT, not a full RFC 4180 CSV parser:
 * no quoting, no embedded delimiters/newlines inside a field, no escaping.
 * Recognizes exactly three {@code format_options} keys — {@code delim}
 * (single-character string, default {@code ","}), {@code header} (boolean,
 * default {@code false}), {@code nullstr} (string; a cell exactly equal to
 * it, or an empty cell when {@code nullstr} wasn't given, reads as {@code
 * NULL}) — an unrecognized key is ignored, not refused, matching VGI's own
 * additive-metadata philosophy. Bridges the same primitive scalar shapes
 * {@code VgiScalarValueBridge} does (bool/byte/short/int/long/float/double/
 * string) — a declared column of any other type makes this reader refuse the
 * whole partition outright, with a clear message, rather than silently
 * mis-parse it.
 */
final class VgiCsvPartitionReader implements PartitionReader<ColumnarBatch> {

    private final VgiFormatInputPartition partition;
    private final List<Field> fields;

    private ColumnarBatch currentBatch;
    private boolean delivered;
    private boolean closed;

    VgiCsvPartitionReader(VgiFormatInputPartition partition, byte[] tableOutputSchemaBytes,
            List<Integer> projectionIds) {
        this.partition = partition;
        Schema arrowSchema = ArrowSchemaCodec.deserializeSchema(tableOutputSchemaBytes);
        List<Field> all = arrowSchema.getFields();
        if (projectionIds == null) {
            this.fields = all;
        } else {
            List<Field> out = new ArrayList<>(projectionIds.size());
            for (int ordinal : projectionIds) out.add(all.get(ordinal));
            this.fields = out;
        }
    }

    @Override
    public boolean next() throws IOException {
        if (delivered) return false;
        delivered = true;
        currentBatch = readWholeFile();
        return true;
    }

    @Override
    public ColumnarBatch get() {
        return currentBatch;
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        if (currentBatch != null) currentBatch.close();
    }

    private ColumnarBatch readWholeFile() throws IOException {
        char delim = delimiterOption();
        boolean header = booleanOption("header", false);
        String nullstr = stringOption("nullstr", null);

        List<String> lines;
        try (BufferedReader reader = openReader(partition.location())) {
            lines = reader.lines().toList();
        }
        int start = 0;
        // Column-name-to-CSV-field-index mapping: header row wins if present
        // (declared columns not present in the header simply read as
        // permanently NULL — the fixture's own shape never hits this, but a
        // silent misalignment is worse than an honest empty column); with no
        // header, the file's own field order is assumed to match the
        // declared schema's field order 1:1.
        Map<String, Integer> csvIndexByName = null;
        if (header && !lines.isEmpty()) {
            String[] headerCells = splitLine(lines.get(0), delim);
            csvIndexByName = new HashMap<>();
            for (int i = 0; i < headerCells.length; i++) csvIndexByName.put(headerCells[i], i);
            start = 1;
        }
        int rowCount = Math.max(0, lines.size() - start);

        Schema batchSchema = new Schema(fields);
        // Deliberately NOT try-with-resources: this reader (unlike
        // VgiPartitionReader, whose root is owned/recycled by the RPC
        // session) owns this VectorSchemaRoot outright, and transfers that
        // ownership to the ColumnarBatch built below (each wrapping
        // ArrowColumnVector closes its underlying vector when the batch is
        // closed) — closing the root here too would double-close the same
        // vectors. This reader's own close() calls currentBatch.close(),
        // which is the one and only release.
        org.apache.arrow.vector.VectorSchemaRoot root =
                org.apache.arrow.vector.VectorSchemaRoot.create(batchSchema, farm.query.vgirpc.wire.Allocators.root());
        root.allocateNew();
        int[] csvIndexByField = new int[fields.size()];
        for (int f = 0; f < fields.size(); f++) {
            String displayName = VgiColumnNames.displayName(fields.get(f));
            if (csvIndexByName != null) {
                Integer idx = csvIndexByName.get(displayName);
                csvIndexByField[f] = idx == null ? -1 : idx;
            } else {
                csvIndexByField[f] = f;
            }
        }
        for (int row = 0; row < rowCount; row++) {
            String[] cells = splitLine(lines.get(start + row), delim);
            for (int f = 0; f < fields.size(); f++) {
                int csvIdx = csvIndexByField[f];
                String raw = (csvIdx < 0 || csvIdx >= cells.length) ? null : cells[csvIdx];
                boolean isNull = raw == null || raw.equals(nullstr) || (nullstr == null && raw.isEmpty());
                writeCell(root.getVector(f), fields.get(f), row, isNull ? null : raw);
            }
        }
        for (FieldVector v : root.getFieldVectors()) v.setValueCount(rowCount);
        root.setRowCount(rowCount);

        ColumnVector[] columns = new ColumnVector[fields.size()];
        for (int f = 0; f < fields.size(); f++) {
            columns[f] = new ArrowColumnVector(root.getVector(f));
        }
        return new ColumnarBatch(columns, rowCount);
    }

    private char delimiterOption() {
        String raw = stringOption("delim", ",");
        if (raw.length() != 1) {
            throw new UnsupportedOperationException("format branch '" + partition.location()
                    + "': delim '" + raw + "' is not a single character — vgi-spark's CSV format-branch reader"
                    + " only supports a single-character delimiter");
        }
        return raw.charAt(0);
    }

    private String stringOption(String key, String fallback) {
        Object v = partition.formatOptions().get(key);
        return v == null ? fallback : v.toString();
    }

    private boolean booleanOption(String key, boolean fallback) {
        Object v = partition.formatOptions().get(key);
        return v == null ? fallback : (Boolean) v;
    }

    private static String[] splitLine(String line, char delim) {
        return line.split(java.util.regex.Pattern.quote(String.valueOf(delim)), -1);
    }

    private static BufferedReader openReader(String location) throws IOException {
        Path path;
        if (location.startsWith("file://")) {
            path = Path.of(URI.create(location));
        } else if (location.contains("://")) {
            throw new UnsupportedOperationException("format branch location '" + location + "': vgi-spark's"
                    + " CSV format-branch reader only supports local filesystem paths / file:// URIs, not this"
                    + " scheme");
        } else {
            path = Path.of(location);
        }
        return Files.newBufferedReader(path, StandardCharsets.UTF_8);
    }

    private static void writeCell(FieldVector vector, Field field, int row, String raw) {
        if (raw == null) {
            vector.setNull(row);
            return;
        }
        switch (vector) {
            case org.apache.arrow.vector.BitVector v -> v.setSafe(row, Boolean.parseBoolean(raw) ? 1 : 0);
            case org.apache.arrow.vector.TinyIntVector v -> v.setSafe(row, Byte.parseByte(raw));
            case org.apache.arrow.vector.SmallIntVector v -> v.setSafe(row, Short.parseShort(raw));
            case org.apache.arrow.vector.IntVector v -> v.setSafe(row, Integer.parseInt(raw));
            case org.apache.arrow.vector.BigIntVector v -> v.setSafe(row, Long.parseLong(raw));
            case org.apache.arrow.vector.Float4Vector v -> v.setSafe(row, Float.parseFloat(raw));
            case org.apache.arrow.vector.Float8Vector v -> v.setSafe(row, Double.parseDouble(raw));
            case org.apache.arrow.vector.VarCharVector v -> v.setSafe(row, raw.getBytes(StandardCharsets.UTF_8));
            default -> throw new UnsupportedOperationException("format branch: column '" + field.getName()
                    + "' has Arrow type " + field.getType() + ", which vgi-spark's CSV format-branch reader"
                    + " doesn't bridge yet (only the primitive scalar types are supported)");
        }
    }
}
