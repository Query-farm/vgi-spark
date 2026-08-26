// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgispark.settings;

import farm.query.vgi.SettingSpec;
import farm.query.vgirpc.marshal.Marshalling;
import farm.query.vgirpc.wire.Allocators;
import farm.query.vgirpc.wire.IpcStreamReader;
import farm.query.vgispark.types.ArrowSchemaCodec;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Decode the {@link SettingSpec} entries {@code CatalogAttachResult.settings()}
 * carries — a worker-declared session setting's name and Arrow type, needed
 * to encode {@code BindRequest.settings} with the RIGHT type instead of
 * guessing from a plain string. Spark's own {@code SET key=value} stores
 * every value as a string regardless of how it was written (unlike DuckDB's
 * own typed {@code SET}), so a client threading Spark session config into
 * VGI settings needs each name's declared type from somewhere — this is
 * that somewhere. See {@code VgiUnboundScalarFunction}'s use of this.
 *
 * <p>The inverse of the worker-side {@code SettingSpecSerializer}: a 1-row
 * IPC batch per setting, schema {@code {name: utf8, description: utf8, type:
 * binary, default_value: binary?}}, where {@code type} is itself an
 * IPC-encoded schema with one field named {@code "value"} carrying the
 * setting's real Arrow type (and, for a nested setting, its children).
 * {@code default_value} is intentionally not decoded here — a setting this
 * client doesn't send is simply absent from {@code BindRequest.settings},
 * letting the worker fall back to its own registered default, which is
 * exactly the behavior wanted (see {@code SettingsEncoder}'s own javadoc on
 * "a setting the client leaves out simply doesn't appear").
 */
public final class SettingSpecDecoder {

    private SettingSpecDecoder() {}

    /**
     * Decode one serialised {@code SettingSpec} item.
     *
     * @param item the IPC bytes of a single entry from {@code CatalogAttachResult.settings()}
     * @return the decoded setting's name and Arrow type
     * @throws IllegalStateException if the bytes are missing, empty, or not a
     *         single-row {@code SettingSpec} batch
     */
    public static SettingSpec decode(byte[] item) {
        if (item == null || item.length == 0) {
            throw new IllegalStateException("SettingSpecDecoder.decode: empty item bytes");
        }
        Map<String, Object> row;
        try (IpcStreamReader r = new IpcStreamReader(new ByteArrayInputStream(item), Allocators.root())) {
            if (r.readNextBatch() == null) {
                throw new IllegalStateException("SettingSpec item carried no batch");
            }
            VectorSchemaRoot root = r.root();
            if (root.getRowCount() != 1) {
                throw new IllegalStateException(
                        "SettingSpec item must be a single row, got " + root.getRowCount());
            }
            row = Marshalling.decodeRow(root, r.dictionaryProvider(), r.wireSchema());
        } catch (Exception e) {
            if (e instanceof IllegalStateException ise) throw ise;
            throw new IllegalStateException("SettingSpecDecoder.decode failed", e);
        }

        Object nameObj = row.get("name");
        Object typeObj = row.get("type");
        if (nameObj == null || !(typeObj instanceof byte[] typeBytes)) {
            throw new IllegalStateException("SettingSpec item missing required 'name'/'type' field");
        }
        String description = row.get("description") == null ? "" : row.get("description").toString();

        Schema typeSchema = ArrowSchemaCodec.deserializeSchema(typeBytes);
        List<Field> typeFields = typeSchema == null ? List.of() : typeSchema.getFields();
        if (typeFields.isEmpty()) {
            throw new IllegalStateException("SettingSpec '" + nameObj + "': type schema carried no 'value' field");
        }
        Field valueField = typeFields.get(0);
        return new SettingSpec(nameObj.toString(), description, valueField.getType(), valueField.getChildren(), null);
    }

    /**
     * Decode every entry of {@code CatalogAttachResult.settings()}.
     *
     * @param items the attach result's settings list, or {@code null}
     * @return the decoded settings, in the worker's order; empty when there are none
     */
    public static List<SettingSpec> decodeAll(List<byte[]> items) {
        if (items == null || items.isEmpty()) return List.of();
        List<SettingSpec> out = new ArrayList<>(items.size());
        for (byte[] item : items) out.add(decode(item));
        return Collections.unmodifiableList(out);
    }
}
