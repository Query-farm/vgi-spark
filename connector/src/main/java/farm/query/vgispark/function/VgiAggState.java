// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgispark.function;

import org.apache.spark.sql.catalyst.InternalRow;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * One group's accumulated raw input rows, buffered client-side and never
 * touched by any RPC until {@code produceResult} — see {@link
 * VgiAggregateFunction}'s own javadoc for why. {@code Serializable} because
 * {@code AggregateFunction<S, R>} requires it (Spark's own aggregate
 * execution can move partial state across a shuffle boundary); {@link
 * InternalRow#copy} already returns a plain, genuinely serializable
 * representation (e.g. {@code GenericInternalRow}), so nothing special is
 * needed here beyond holding onto the copies.
 */
final class VgiAggState implements Serializable {

    private static final long serialVersionUID = 1L;

    final List<InternalRow> rows = new ArrayList<>();
}
