package com.database.db.CRUD;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.time.LocalDateTime;

import com.database.db.api.Functions.*;
import com.database.db.api.Schema;
import com.database.db.table.DataType;

public class FunctionsTest {

    // Existing OPERATION tests...
    @Test
    void testGetFromFunction_allOperations_Integer() throws Exception {
        Schema schema = new Schema()
            .column("c1").type(DataType.INT).endColumn()
            .column("c2").type(DataType.INT).endColumn()
            .column("c3").type(DataType.INT).endColumn()
            .column("c4").type(DataType.INT).endColumn()
            .column("c5").type(DataType.INT).endColumn();
        Object[] row = { 2, 3, 4, 10, 3 };
        String expression = "((c1 + c2) * c3 - c4 / c5 + c4 % c3) ^ c1";
        Object resultNumber = new operationData(expression).apply(new com.database.db.table.SchemaInner(schema.get()), row, 0);
        int result = ((Number) resultNumber).intValue();
        assertNotNull(result);
        assertTrue(resultNumber instanceof Number);
        assertEquals(361, result);
    }

    @Test
    void testGetFromFunction_allOperations_Double() throws Exception {
        Schema schema = new Schema()
            .column("c1").type(DataType.DOUBLE).endColumn()
            .column("c2").type(DataType.DOUBLE).endColumn()
            .column("c3").type(DataType.DOUBLE).endColumn()
            .column("c4").type(DataType.DOUBLE).endColumn()
            .column("c5").type(DataType.DOUBLE).endColumn();
        Object[] row = { 2.0, 3.0, 4.0, 10.0, 3.0 };
        String expression = "((c1 + c2) * c3- c4 / c5 + c4 % c3) ^ c1";
        Object resultNumber = new operationData(expression).apply(new com.database.db.table.SchemaInner(schema.get()), row, 0);
        Double result = ((Number) resultNumber).doubleValue();
        assertNotNull(result);
        assertTrue(resultNumber instanceof Number);
        assertEquals(348.4444444444445, result);
    }

    @Test
    void testGetFromFunction_simpleExpression() throws Exception {
        Schema schema = new Schema()
            .column("c1").type(DataType.INT).endColumn()
            .column("c2").type(DataType.INT).endColumn()
            .column("c3").type(DataType.INT).endColumn();
        Object[] row = { 10, 5, 2 };
        String expression = "c1 + c2 * c3";
        Object result = new operationData(expression).apply(new com.database.db.table.SchemaInner(schema.get()), row, 0);
        assertNotNull(result);
        assertTrue(result instanceof Long);
        assertEquals(20L, result);
    }

    @Test
    void testGetFromFunction_withDifferentValues() throws Exception {
        Schema schema = new Schema()
            .column("x").type(DataType.INT).endColumn()
            .column("y").type(DataType.INT).endColumn()
            .column("z").type(DataType.INT).endColumn();
        Object[] row = { 7, 3, 4 };
        String expression = "x * y + z";
        Object result = new operationData(expression).apply(new com.database.db.table.SchemaInner(schema.get()), row, 0);
        assertNotNull(result);
        assertTrue(result instanceof Long);
        assertEquals(25L, result);
    }

    @Test
    void testGetFromFunction_nullExpression() throws Exception {
        Schema schema = new Schema()
            .column("a").type(DataType.INT).endColumn()
            .column("b").type(DataType.INT).endColumn();
        Object[] row = { 1, 2 };
        Object result = new operationData(null).apply(new com.database.db.table.SchemaInner(schema.get()), row, 0);
        assertNull(result);
    }

    @Test
    void testGetFromFunction_nullOperationData() {
        Object result = new operationData("a + b").apply(null, null, 0);
        assertNull(result);
    }

    //=== Tests for date functions
    @Test
    void testGetFromFunction_now() {
        LocalDateTime before = LocalDateTime.now().minusSeconds(1);
        Object result = new currentTimestamp().apply(null, null, 0);
        assertNotNull(result);
        assertTrue(result instanceof LocalDateTime);
        LocalDateTime now = (LocalDateTime) result;
        LocalDateTime after = LocalDateTime.now().plusSeconds(1);
        assertTrue(now.isAfter(before) && now.isBefore(after));
    }

    // Tests for DATE_ADD and DATE_SUB
    @Test
    void testDateAddAndSub_days() {
        LocalDateTime dt = LocalDateTime.of(2025, 7, 24, 12, 0);
        LocalDateTime added = (LocalDateTime) new dateAdd("days", 5L).apply(null, new Object[]{dt}, 0);
        assertEquals(LocalDateTime.of(2025, 7, 29, 12, 0), added);
        LocalDateTime subbed = (LocalDateTime) new dateSub("days", 10L).apply(null, new Object[]{dt}, 0);
        assertEquals(LocalDateTime.of(2025, 7, 14, 12, 0), subbed);
    }

    @Test
    void testDateAdd_months_years() {
        LocalDateTime dt = LocalDateTime.of(2020, 1, 31, 0, 0);

        LocalDateTime plusOneMonth = (LocalDateTime) new dateAdd("months", 1L).apply(null, new Object[]{dt}, 0);
        assertEquals(LocalDateTime.of(2020, 2, 29, 0, 0), plusOneMonth);

        LocalDateTime plusTwoYears = (LocalDateTime) new dateAdd("years", 2L).apply(null, new Object[]{dt}, 0);
        assertEquals(LocalDateTime.of(2022, 1, 31, 0, 0), plusTwoYears);
    }

    // Tests for EXTRACT
    @Test
    void testExtractPart() {
        LocalDateTime dt = LocalDateTime.of(2025, 12, 31, 23, 59, 58);
        assertEquals(2025, new extractPart( "year").apply(null, new Object[]{dt}, 0));
        assertEquals(12, new extractPart( "month").apply(null, new Object[]{dt}, 0));
        assertEquals(31, new extractPart( "day").apply(null, new Object[]{dt}, 0));
        assertEquals(23, new extractPart( "hour").apply(null, new Object[]{dt}, 0));
        assertEquals(59, new extractPart( "minute").apply(null, new Object[]{dt}, 0));
        assertEquals(58, new extractPart( "second").apply(null, new Object[]{dt}, 0));
    }

    @Test
    void testExtractPart_invalid() {
        LocalDateTime dt = LocalDateTime.now();
        assertThrows(IllegalArgumentException.class,
            () -> new extractPart( "millennium").apply(null, new Object[]{dt}, 0));
    }

    // Tests for FORMAT_DATE
    @Test
    void testFormatDate() {
        LocalDateTime dt = LocalDateTime.of(2025, 1, 2, 3, 4, 5);
        String formatted = (String) new formatDate("yyyy-MM-dd HH:mm:ss").apply(null, new Object[]{dt}, 0);
        assertEquals("2025-01-02 03:04:05", formatted);
    }

    // Tests for DATEDIFF and AGE
    @Test
    void testComputeDifference_days_months_years() {
        LocalDateTime from = LocalDateTime.of(2020, 1, 1, 0, 0);
        LocalDateTime to = LocalDateTime.of(2025, 1, 1, 0, 0);
        assertEquals(1827L, new dateDifference(from, to, "days").apply(null, new Object[]{from,to}, 0));
        assertEquals(60L, new dateDifference(from, to, "months").apply(null, new Object[]{from,to}, 0));
        assertEquals(5L, new dateDifference(from, to, "years").apply(null, new Object[]{from,to}, 0));
    }

    @Test
    void testComputeDifference_hours_minutes_seconds() {
        LocalDateTime from = LocalDateTime.of(2025, 7, 24, 0, 0);
        LocalDateTime to = LocalDateTime.of(2025, 7, 25, 1, 30, 15);
        assertEquals(25L, new dateDifference(from, to, "hours").apply(null, new Object[]{from,to}, 0));
        assertEquals(1530L, new dateDifference(from, to, "minutes").apply(null, new Object[]{from,to}, 0));
        assertEquals(91815L, new dateDifference(from, to, "seconds").apply(null, new Object[]{from,to}, 0));
    }

    @Test
    void testComputeDifference_invalidUnit() {
        LocalDateTime from = LocalDateTime.now().minusDays(1);
        LocalDateTime to = LocalDateTime.now();
        assertThrows(IllegalArgumentException.class,
            () -> new dateDifference(from, to, "decades").apply(null, new Object[]{from,to}, 0));
    }

    //=== Tests for string functions

    @Test
    void testConcat() {
        Object result = new concat(new Object[]{"Hello", " ", "World", 123}).apply(null, new Object[]{"Hello"}, 0);
        assertTrue(result instanceof String);
        assertEquals("Hello World123", result);
    }

    @Test
    void testUpperLowerTrim() {
        assertEquals("ABC", new upperCase().apply(null, new Object[]{"ABC"}, 0));
        assertEquals("xyz", new lowerCase().apply(null, new Object[]{"XYZ"}, 0));
        assertEquals("trimmed", new trim().apply(null, new Object[]{"  trimmed  "}, 0));
    }

    @Test
    void testSubstringLeftRight() {
        String s = "abcdef";
        assertEquals("bcd", new substring( 1, 3).apply(null, new Object[]{s}, 0));
        assertEquals("ab", new left( 2).apply(null, new Object[]{s}, 0));
        assertEquals("ef", new right( 2).apply(null, new Object[]{s}, 0));
    }

    @Test
    void testReplaceAndRegexReplace() {
        String s = "foo123bar123";
        assertEquals("fooXXXbarXXX", new replace("123", "XXX").apply(null, new Object[]{s}, 0));
        assertEquals("foobar", new regexpReplace("\\d+", "").apply(null, new Object[]{s}, 0));
    }

    @Test
    void testPadAndReverse() {
        String s = "7";
        assertEquals("0007", new leftPad(4, "0").apply(null, new Object[]{s}, 0));
        assertEquals("7xxx", new rightPad(4, "x").apply(null, new Object[]{s}, 0));
        assertEquals("dcba", new reverse().apply(null, new Object[]{"abcd"}, 0));
    }
}