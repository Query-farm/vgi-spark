// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgispark.filter;

import farm.query.vgi.client.EncodedPushdownFilters;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.LiteralValue;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Direct, no-worker-needed tests of the actual translation logic — verifying
 * what {@link VgiFilterTranslator} produces, not just that a query returns
 * the right rows (which a broken/no-op translator could still pass, since
 * {@code VgiScanBuilder.pushPredicates} always hands the full predicate set
 * back to Spark for re-checking regardless of what got pushed).
 */
class VgiFilterTranslatorTest {

    private static final Field N_FIELD = new Field("n", FieldType.nullable(new ArrowType.Int(64, true)), null);
    private static final Field NAME_FIELD = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
    private static final List<Field> SCHEMA = List.of(N_FIELD, NAME_FIELD);
    private static final List<String> PROJECTED = List.of("n", "name");

    private static Predicate cmp(String op, String column, Object literal, org.apache.spark.sql.types.DataType type) {
        return new Predicate(op, new Expression[] {
                FieldReference.column(column), new LiteralValue<>(literal, type)});
    }

    @Test
    void translatesASimpleComparison() {
        Predicate p = cmp(">", "n", 5L, DataTypes.LongType);
        VgiFilterTranslator.Result result = VgiFilterTranslator.translate(new Predicate[] {p}, SCHEMA, PROJECTED);

        assertTrue(!result.isEmpty(), "expected the comparison to translate");
        assertEquals(Set.of("n"), result.coveredColumns());
        assertEquals(1, result.pushedPredicates().length);
        EncodedPushdownFilters encoded = result.encoded();
        assertNotNull(encoded.pushdownFilters());
        assertTrue(encoded.pushdownFilters().length > 0);
    }

    @Test
    void flipsALiteralFirstComparison() {
        // "5 < n" means "n > 5" — same predicate, operands reversed.
        Predicate p = new Predicate("<", new Expression[] {
                new LiteralValue<>(5L, DataTypes.LongType), FieldReference.column("n")});
        VgiFilterTranslator.Result result = VgiFilterTranslator.translate(new Predicate[] {p}, SCHEMA, PROJECTED);

        assertTrue(!result.isEmpty(), "expected the flipped comparison to translate");
        assertEquals(Set.of("n"), result.coveredColumns());
    }

    @Test
    void translatesAnAndOfTwoComparisonsOnTheSameColumn() {
        Predicate p = new Predicate("AND", new Expression[] {
                cmp(">", "n", 90L, DataTypes.LongType), cmp("<=", "n", 95L, DataTypes.LongType)});
        VgiFilterTranslator.Result result = VgiFilterTranslator.translate(new Predicate[] {p}, SCHEMA, PROJECTED);

        assertTrue(!result.isEmpty(), "expected a same-column AND to translate");
        assertEquals(Set.of("n"), result.coveredColumns());
    }

    @Test
    void rejectsAnOrThatSpansTwoDifferentColumns() {
        // "n > 5 OR name = 'x'" has no VGI translation — the wire form is
        // column-rooted, and this predicate's descendants name two columns.
        Predicate p = new Predicate("OR", new Expression[] {
                cmp(">", "n", 5L, DataTypes.LongType), cmp("=", "name", "x", DataTypes.StringType)});
        VgiFilterTranslator.Result result = VgiFilterTranslator.translate(new Predicate[] {p}, SCHEMA, PROJECTED);

        assertTrue(result.isEmpty(), "a cross-column OR must not translate");
        assertEquals(Set.of(), result.coveredColumns());
        assertEquals(0, result.pushedPredicates().length);
        assertNull(result.encoded());
    }

    @Test
    void rejectsAFilterOnAColumnNotInTheProjection() {
        Predicate p = cmp("=", "name", "x", DataTypes.StringType);
        VgiFilterTranslator.Result result = VgiFilterTranslator.translate(new Predicate[] {p}, SCHEMA, List.of("n"));

        assertTrue(result.isEmpty(), "a filter on a non-projected column must not translate");
    }

    @Test
    void skipsAnUntranslatableOperatorWithoutFailingTheWholeSet() {
        // IN has no ComparisonOperator mapping in v1 — see the class javadoc.
        // A translatable predicate alongside it must still translate.
        Predicate untranslatable = new Predicate("IN", new Expression[] {
                FieldReference.column("n"), new LiteralValue<>(1L, DataTypes.LongType)});
        Predicate translatable = cmp("=", "n", 5L, DataTypes.LongType);
        VgiFilterTranslator.Result result = VgiFilterTranslator.translate(
                new Predicate[] {untranslatable, translatable}, SCHEMA, PROJECTED);

        assertTrue(!result.isEmpty());
        assertEquals(1, result.pushedPredicates().length);
    }
}
