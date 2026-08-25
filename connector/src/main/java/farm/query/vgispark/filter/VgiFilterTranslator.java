// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgispark.filter;

import farm.query.vgi.client.FilterPredicate;
import farm.query.vgi.client.ProjectedColumn;
import farm.query.vgi.client.ProjectedColumns;
import farm.query.vgi.client.PushdownFiltersEncoder;
import farm.query.vgi.client.EncodedPushdownFilters;
import farm.query.vgi.client.ScalarValue;
import farm.query.vgi.pushdown.ComparisonOperator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.Literal;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.unsafe.types.UTF8String;

import java.math.BigDecimal;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Translates Spark V2 {@link Predicate}s into {@code InitRequest.pushdown_filters}
 * bytes, via {@link FilterPredicate}/{@link PushdownFiltersEncoder} — the same
 * client-side encoder VGI's own C++ extension's {@code VgiSerializeFilters} is
 * mirrored by.
 *
 * <p><strong>This is best-effort and never authoritative.</strong> Every
 * predicate handed to {@link #translate} is also returned unchanged from
 * {@code SupportsPushDownV2Filters.pushPredicates} (see {@code VgiScanBuilder}) —
 * Spark re-checks every row regardless of what this class managed to encode.
 * That mirrors {@code vgi-trino}'s own design choice (see its {@code
 * VgiFilterTranslator}'s javadoc): a worker's pushdown is a pruning
 * optimization, not a correctness guarantee, so a translation gap here costs
 * performance, never rows.
 *
 * <h2>The one real structural constraint</h2>
 *
 * <p>VGI's filter wire form is <em>column-rooted</em>: a single {@link
 * FilterPredicate} tree — including every node under an {@code AND}/{@code OR}
 * — applies to exactly one column (see {@code TableInfo.required_filters}'s
 * "dotted column path" framing and {@code FilterPredicate}'s own javadoc).
 * Multiple columns compose only by sending several top-level {@code
 * (column, predicate)} pairs to one {@link PushdownFiltersEncoder}, implicitly
 * ANDed by the worker. A Spark predicate whose descendants reference more
 * than one column — {@code a > 5 OR b < 3} is the canonical shape — has no
 * translation and is skipped rather than mistranslated.
 *
 * <h2>What's covered</h2>
 *
 * <p>{@code =}, {@code <>}, {@code >}, {@code >=}, {@code <}, {@code <=},
 * {@code IS_NULL}, {@code IS_NOT_NULL}, and same-column {@code AND}/{@code OR}.
 * Not yet covered: {@code IN}, {@code NOT}, string pattern predicates ({@code
 * STARTS_WITH} etc.) — skipped like any other untranslatable predicate, not
 * an error. Extending this is tracked as follow-up work, not a silent gap:
 * v1 ships with exactly the operators {@code ComparisonOperator} has.
 */
public final class VgiFilterTranslator {

    private VgiFilterTranslator() {}

    /** One successfully translated top-level predicate: which column it's rooted at, and the predicate itself. */
    public record Translated(String columnName, FilterPredicate predicate) {}

    /**
     * Translate as many of {@code predicates} as this class can express, and
     * report the column names actually covered (for {@code
     * TableInfo.required_filters} enforcement — see {@code VgiScanBuilder}).
     *
     * @param predicates the predicates Spark offered (one array element per
     *        top-level conjunct)
     * @param fullSchema the table's full (unprojected) Arrow schema — column
     *        names are matched against this to resolve each predicate's
     *        target field and Arrow type
     * @param projectedColumnNames the columns this scan actually projects, in
     *        projection order — {@code column_index} on the wire is relative
     *        to THIS list, not the base schema (see {@link ProjectedColumn})
     * @return the encoded filter batch (or {@code null} if nothing translated,
     *         see {@link Result#isEmpty()}), plus the set of column names covered
     */
    public static Result translate(Predicate[] predicates, List<Field> fullSchema, List<String> projectedColumnNames) {
        java.util.Map<String, ArrowType> typeByName = new java.util.LinkedHashMap<>();
        for (Field f : fullSchema) typeByName.put(f.getName(), f.getType());

        ProjectedColumns projected = ProjectedColumns.of(projectedColumnNames);
        PushdownFiltersEncoder encoder = PushdownFiltersEncoder.builder();
        Set<String> covered = new LinkedHashSet<>();
        List<Predicate> pushed = new java.util.ArrayList<>();
        boolean any = false;

        for (Predicate p : predicates == null ? new Predicate[0] : predicates) {
            Set<String> columns = new LinkedHashSet<>();
            collectColumns(p, columns);
            if (columns.size() != 1) {
                continue; // zero or multiple distinct columns — not expressible as one column-rooted tree
            }
            String columnName = columns.iterator().next();
            ArrowType columnType = typeByName.get(columnName);
            ProjectedColumn projectedColumn;
            try {
                projectedColumn = projected.column(columnName);
            } catch (IllegalArgumentException notProjected) {
                continue; // filtering on a column this scan doesn't even project — nothing to root it to
            }
            if (columnType == null) continue;

            FilterPredicate translated = translateNode(p, columnType);
            if (translated == null) continue;

            encoder.filter(projectedColumn, translated);
            covered.add(columnName);
            pushed.add(p);
            any = true;
        }

        EncodedPushdownFilters encoded = any ? encoder.encode() : null;
        return new Result(encoded, Set.copyOf(covered), pushed.toArray(new Predicate[0]));
    }

    /**
     * The outcome of {@link #translate}.
     *
     * @param pushedPredicates the input predicates actually translated and
     *        encoded — informational only (for {@code
     *        SupportsPushDownV2Filters#pushedPredicates}/{@code EXPLAIN}
     *        output). {@code VgiScanBuilder} still returns the FULL input
     *        array from {@code pushPredicates}, since none of this is
     *        authoritative — see the class javadoc
     */
    public record Result(EncodedPushdownFilters encoded, Set<String> coveredColumns, Predicate[] pushedPredicates) {
        public boolean isEmpty() {
            return encoded == null;
        }
    }

    private static void collectColumns(Expression expr, Set<String> out) {
        if (expr instanceof NamedReference ref) {
            out.add(String.join(".", ref.fieldNames()));
            return;
        }
        for (Expression child : expr.children()) {
            collectColumns(child, out);
        }
    }

    /** Translate one predicate node, given it's already confirmed to be rooted at exactly one column. */
    private static FilterPredicate translateNode(Predicate p, ArrowType columnType) {
        String name = p.name();
        Expression[] children = p.children();
        return switch (name) {
            case "IS_NULL" -> new FilterPredicate.IsNull();
            case "IS_NOT_NULL" -> new FilterPredicate.IsNotNull();
            case "AND" -> combine(children, columnType, true);
            case "OR" -> combine(children, columnType, false);
            case "=", "<>", ">", ">=", "<", "<=" -> comparison(name, children, columnType);
            default -> null; // IN, NOT, STARTS_WITH, ... — not yet covered, see class javadoc
        };
    }

    private static FilterPredicate combine(Expression[] children, ArrowType columnType, boolean and) {
        List<FilterPredicate> translatedChildren = new java.util.ArrayList<>(children.length);
        for (Expression child : children) {
            if (!(child instanceof Predicate childPredicate)) return null;
            FilterPredicate t = translateNode(childPredicate, columnType);
            if (t == null) return null; // one untranslatable child fails the whole AND/OR node
            translatedChildren.add(t);
        }
        if (translatedChildren.isEmpty()) return null;
        return and
                ? new FilterPredicate.And(translatedChildren)
                : new FilterPredicate.Or(translatedChildren);
    }

    private static FilterPredicate comparison(String operatorName, Expression[] children, ArrowType columnType) {
        if (children.length != 2) return null;
        Literal<?> literal;
        ComparisonOperator op;
        if (children[0] instanceof NamedReference && children[1] instanceof Literal<?> l) {
            literal = l;
            op = operatorFor(operatorName);
        } else if (children[1] instanceof NamedReference && children[0] instanceof Literal<?> l) {
            literal = l;
            op = flip(operatorFor(operatorName)); // "5 > col" means "col < 5"
        } else {
            return null; // column-op-column, or two literals — not a pushable single-column comparison
        }
        if (op == null) return null;
        Object value = toWireValue(literal.value());
        if (value == null) return null; // a genuine SQL NULL literal in a comparison is never true — not worth pushing
        return FilterPredicate.compare(op, ScalarValue.of(columnType, value));
    }

    private static ComparisonOperator operatorFor(String name) {
        return switch (name) {
            case "=" -> ComparisonOperator.EQ;
            case "<>" -> ComparisonOperator.NE;
            case ">" -> ComparisonOperator.GT;
            case ">=" -> ComparisonOperator.GE;
            case "<" -> ComparisonOperator.LT;
            case "<=" -> ComparisonOperator.LE;
            default -> null;
        };
    }

    private static ComparisonOperator flip(ComparisonOperator op) {
        if (op == null) return null;
        return switch (op) {
            case GT -> ComparisonOperator.LT;
            case GE -> ComparisonOperator.LE;
            case LT -> ComparisonOperator.GT;
            case LE -> ComparisonOperator.GE;
            case EQ, NE -> op; // symmetric
        };
    }

    /**
     * Normalize a Spark V2 literal value to the plain Java shape {@link
     * ScalarValue#of(ArrowType, Object)} expects. Spark's V2 expression
     * builder is not consistent about boxed-Java-primitive vs.
     * catalyst-internal representations across literal types, so this
     * defensively handles both rather than assuming one.
     */
    private static Object toWireValue(Object value) {
        if (value == null) return null;
        if (value instanceof UTF8String s) return s.toString();
        if (value instanceof org.apache.spark.sql.types.Decimal d) return d.toJavaBigDecimal();
        if (value instanceof BigDecimal) return value;
        return value; // Long/Integer/Short/Byte/Double/Float/Boolean/byte[] already match ScalarValue's expectations
    }
}
