// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgispark.branch;

import java.util.Map;

/**
 * Decode a FORMAT branch's {@code format_options} bytes — a 1-row Arrow IPC
 * batch whose COLUMN NAMES carry the option keys (the same "no static schema
 * could describe an arbitrary-typed map" trick {@code BindRequest.settings}
 * and a scalar function's {@code arguments} both already use), confirmed
 * against the Python worker's own {@code _serialize_named_scalars}. Reuses
 * {@link ScanBranchesDecoder}'s own IPC-row reader — the wire shape is
 * structurally identical, not a coincidence.
 */
public final class FormatOptionsDecoder {

    private FormatOptionsDecoder() {}

    /**
     * @param bytes the raw {@code format_options} bytes, or {@code null}/empty
     *        for "no options" (the worker sends no bytes at all when the map
     *        is empty — see the Python serializer's own doc comment)
     * @return the decoded options, name → raw Java value (e.g. {@link String},
     *         {@link Boolean}) — empty if {@code bytes} was {@code null}/empty
     */
    public static Map<String, Object> decode(byte[] bytes) {
        if (bytes == null || bytes.length == 0) return Map.of();
        return ScanBranchesDecoder.readOneRow(bytes);
    }
}
