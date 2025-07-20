package com.database.db.table;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.Date;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class TypeTest {
    // ======================= fromString() Tests =======================

    @ParameterizedTest
    @CsvSource({
            "INT, INT",
            "int, INT",
            "integer, INT",
            "INT4, INT",
            "FLOAT, FLOAT",
            "float, FLOAT",
            "DOUBLE, DOUBLE",
            "double, DOUBLE",
            "STRING, STRING",
            "string, STRING",
            "varchar, STRING",
            "text, STRING",
            "BOOLEAN, BOOLEAN",
            "boolean, BOOLEAN",
            "bool, BOOLEAN",
            "LONG, LONG",
            "long, LONG",
            "bigint, LONG",
            "DATE, DATE",
            "date, DATE",
            "datetime, DATE",
            "TIMESTAMP, TIMESTAMP",
            "timestamp, TIMESTAMP",
            "BINARY, BINARY",
            "binary, BINARY",
            "blob, BINARY"
    })
    void testFromStringValid(String input, Type expected) {
        assertEquals(expected, Type.fromString(input));
    }

    @ParameterizedTest
    @ValueSource(strings = { "", " ", "  ", "unknown", "array", "decimal" })
    void testFromStringInvalid(String input) {
        assertThrows(IllegalArgumentException.class, () -> Type.fromString(input));
    }

    @Test
    void testFromStringNull() {
        assertThrows(IllegalArgumentException.class, () -> Type.fromString(null));
    }

    // ======================= getFixedSize() Tests =======================

    @Test
    void testGetFixedSize() {
        assertEquals(4, Type.INT.getSize());
        assertEquals(4, Type.FLOAT.getSize());
        assertEquals(8, Type.DOUBLE.getSize());
        assertEquals(1, Type.BOOLEAN.getSize());
        assertEquals(8, Type.LONG.getSize());
        assertEquals(8, Type.DATE.getSize());
        assertEquals(16, Type.TIMESTAMP.getSize());
        assertEquals(-1, Type.VARCHAR.getSize());
        assertEquals(-1, Type.BINARY.getSize());
    }

    // ======================= validateValue() Tests =======================

    @Test
    void testValidateIntValid() {
        Type.INT.validateValue(42, 0);
        Type.INT.validateValue(0, 0);
        Type.INT.validateValue(-100, 0);
    }

    @Test
    void testValidateIntInvalid() {
        assertThrows(IllegalArgumentException.class,
                () -> Type.INT.validateValue("not an int", 0));
        assertThrows(IllegalArgumentException.class,
                () -> Type.INT.validateValue(3.14, 0));
    }

    @Test
    void testValidateFloatValid() {
        Type.FLOAT.validateValue(3.14f, 0);
        Type.FLOAT.validateValue(0.0f, 0);
        Type.FLOAT.validateValue(-1.5f, 0);
    }

    @Test
    void testValidateFloatInvalid() {
        assertThrows(IllegalArgumentException.class,
                () -> Type.FLOAT.validateValue("not a float", 0));
        assertThrows(IllegalArgumentException.class,
                () -> Type.FLOAT.validateValue(42, 0));
    }

    @Test
    void testValidateDoubleValid() {
        Type.DOUBLE.validateValue(3.14159, 0);
        Type.DOUBLE.validateValue(0.0, 0);
        Type.DOUBLE.validateValue(-1.5, 0);
    }

    @Test
    void testValidateDoubleInvalid() {
        assertThrows(IllegalArgumentException.class,
                () -> Type.DOUBLE.validateValue("not a double", 0));
        assertThrows(IllegalArgumentException.class,
                () -> Type.DOUBLE.validateValue(3.14f, 0));
    }

    @Test
    void testValidateStringValid() {
        Type.VARCHAR.validateValue("hello", 10);
        Type.VARCHAR.validateValue("", 10);
        Type.VARCHAR.validateValue("short", 5);
    }

    @Test
    void testValidateStringInvalid() {
        // Wrong type
        assertThrows(IllegalArgumentException.class,
                () -> Type.VARCHAR.validateValue(42, 10));

        // Size exceeded
        assertThrows(IllegalArgumentException.class,
                () -> Type.VARCHAR.validateValue("this is too long", 10));
    }

    @Test
    void testValidateBooleanValid() {
        Type.BOOLEAN.validateValue(true, 0);
        Type.BOOLEAN.validateValue(false, 0);
    }

    @Test
    void testValidateBooleanInvalid() {
        assertThrows(IllegalArgumentException.class,
                () -> Type.BOOLEAN.validateValue("true", 0));
        assertThrows(IllegalArgumentException.class,
                () -> Type.BOOLEAN.validateValue(1, 0));
    }

    @Test
    void testValidateLongValid() {
        Type.LONG.validateValue(1234567890123L, 0);
        Type.LONG.validateValue(0L, 0);
        Type.LONG.validateValue(-100L, 0);
    }

    @Test
    void testValidateLongInvalid() {
        assertThrows(IllegalArgumentException.class,
                () -> Type.LONG.validateValue("not a long", 0));
        assertThrows(IllegalArgumentException.class,
                () -> Type.LONG.validateValue(42, 0));
    }

    @Test
    void testValidateDateValid() {
        Type.DATE.validateValue(new Date(), 0);
        Type.DATE.validateValue(new Date(0), 0);
    }

    @Test
    void testValidateDateInvalid() {
        assertThrows(IllegalArgumentException.class,
                () -> Type.DATE.validateValue("2023-01-01", 0));
        assertThrows(IllegalArgumentException.class,
                () -> Type.DATE.validateValue(123456789, 0));
    }

    @Test
    void testValidateTimestampValid() {
        Type.TIMESTAMP.validateValue(new Timestamp(System.currentTimeMillis()), 0);
        Type.TIMESTAMP.validateValue(new Timestamp(0), 0);
    }

    @Test
    void testValidateTimestampInvalid() {
        assertThrows(IllegalArgumentException.class,
                () -> Type.TIMESTAMP.validateValue(new Date(), 0));
        assertThrows(IllegalArgumentException.class,
                () -> Type.TIMESTAMP.validateValue("2023-01-01 12:00:00", 0));
    }

    @Test
    void testValidateBinaryValid() {
        Type.BINARY.validateValue(new byte[10], 10);
        Type.BINARY.validateValue(new byte[0], 10);
        Type.BINARY.validateValue(new byte[5], 5);
    }

    @Test
    void testValidateBinaryInvalid() {
        // Wrong type
        assertThrows(IllegalArgumentException.class,
                () -> Type.BINARY.validateValue("binary data", 10));

        // Size exceeded
        byte[] largeData = new byte[11];
        assertThrows(IllegalArgumentException.class,
                () -> Type.BINARY.validateValue(largeData, 10));
    }

    // ======================= parseValue() Tests =======================

    @Test
    void testParseInt() {
        assertEquals(42, Type.INT.parseValue("42"));
        assertEquals(0, Type.INT.parseValue("0"));
        assertEquals(-100, Type.INT.parseValue("-100"));
    }

    @Test
    void testParseFloat() {
        assertEquals(3.14f, Type.FLOAT.parseValue("3.14"));
        assertEquals(0.0f, Type.FLOAT.parseValue("0.0"));
        assertEquals(-1.5f, Type.FLOAT.parseValue("-1.5"));
    }

    @Test
    void testParseDouble() {
        assertEquals(3.14159, Type.DOUBLE.parseValue("3.14159"));
        assertEquals(0.0, Type.DOUBLE.parseValue("0.0"));
        assertEquals(-1.5, Type.DOUBLE.parseValue("-1.5"));
    }

    @Test
    void testParseString() {
        assertEquals("hello", Type.VARCHAR.parseValue("hello"));
        assertEquals("", Type.VARCHAR.parseValue(""));
    }

    @Test
    void testParseBoolean() {
        assertEquals(true, Type.BOOLEAN.parseValue("true"));
        assertEquals(false, Type.BOOLEAN.parseValue("false"));
        assertEquals(true, Type.BOOLEAN.parseValue("TRUE")); // case-insensitive
        assertEquals(false, Type.BOOLEAN.parseValue("FALSE"));
    }

    @Test
    void testParseLong() {
        assertEquals(1234567890123L, Type.LONG.parseValue("1234567890123"));
        assertEquals(0L, Type.LONG.parseValue("0"));
        assertEquals(-100L, Type.LONG.parseValue("-100"));
    }

    @Test
    void testParseDate() {
        Date date = (Date) Type.DATE.parseValue("2023-01-01T00:00:00Z");
        assertNotNull(date);
        assertEquals(1672531200000L, date.getTime());
    }

    @Test
    void testParseTimestamp() {
        Timestamp ts = (Timestamp) Type.TIMESTAMP.parseValue("2023-01-01 12:00:00.0");
        assertNotNull(ts);
        assertEquals(1672574400000L, ts.getTime());
    }

    @Test
    void testParseBinary() {
        byte[] data = (byte[]) Type.BINARY.parseValue("hello");
        assertArrayEquals("hello".getBytes(), data);
    }

    @Test
    void testParseNull() {
        for (Type type : Type.values()) {
            assertNull(type.parseValue(null));
            assertNull(type.parseValue("null"));
            assertNull(type.parseValue("NULL"));
        }
    }

    // ======================= toString() Tests =======================

    @Test
    void testIntToString() {
        assertEquals("42", Type.INT.toString(42));
    }

    @Test
    void testFloatToString() {
        assertEquals("3.14", Type.FLOAT.toString(3.14f));
    }

    @Test
    void testDoubleToString() {
        assertEquals("3.14159", Type.DOUBLE.toString(3.14159));
    }

    @Test
    void testStringToString() {
        assertEquals("hello", Type.VARCHAR.toString("hello"));
    }

    @Test
    void testBooleanToString() {
        assertEquals("true", Type.BOOLEAN.toString(true));
        assertEquals("false", Type.BOOLEAN.toString(false));
    }

    @Test
    void testLongToString() {
        assertEquals("1234567890123", Type.LONG.toString(1234567890123L));
    }

    @Test
    void testDateToString() {
        Date date = new Date(1672531200000L);
        assertEquals("2023-01-01", Type.DATE.toString(date));
    }

    @Test
    void testTimestampToString() {
        Timestamp ts = new Timestamp(1672574400000L);
        assertEquals("2023-01-01 12:00:00", Type.TIMESTAMP.toString(ts));
    }

    @Test
    void testBinaryToString() {
        byte[] data = { 104, 101, 108, 108, 111 }; // "hello"
        assertEquals("hello", Type.BINARY.toString(data));
    }

    @Test
    void testNullToString() {
        for (Type type : Type.values()) {
            assertEquals("null", type.toString(null));
        }
    }

    // ======================= Edge Case Tests =======================

    @Test
    void testMaxStringSize() {
        // Test max size validation
        String maxString = "a".repeat(10);
        Type.VARCHAR.validateValue(maxString, 10);

        // Test one char over limit
        String tooLong = "a".repeat(11);
        assertThrows(IllegalArgumentException.class,
                () -> Type.VARCHAR.validateValue(tooLong, 10));
    }

    @Test
    void testMaxBinarySize() {
        // Test max size validation
        byte[] maxData = new byte[10];
        Type.BINARY.validateValue(maxData, 10);

        // Test one byte over limit
        byte[] tooBig = new byte[11];
        assertThrows(IllegalArgumentException.class,
                () -> Type.BINARY.validateValue(tooBig, 10));
    }

    @Test
    void testZeroSizeString() {
        // Should allow empty string
        Type.VARCHAR.validateValue("", 0);

        // Should reject any non-empty string
        assertThrows(IllegalArgumentException.class,
                () -> Type.VARCHAR.validateValue("a", 0));
    }

    @Test
    void testZeroSizeBinary() {
        // Should allow empty byte array
        Type.BINARY.validateValue(new byte[0], 0);

        // Should reject any non-empty byte array
        assertThrows(IllegalArgumentException.class,
                () -> Type.BINARY.validateValue(new byte[1], 0));
    }

    @Test
    void testNumberFormatting() {
        // Test locale-independent formatting
        assertEquals("3.1415926535", Type.DOUBLE.toString(3.1415926535));
        assertEquals("3.14", Type.FLOAT.toString(3.14f));
    }

    @Test
    void testDatePrecision() {
        // Test millisecond precision
        Date date = new Date(1672531200123L);
        assertEquals("2023-01-01", Type.DATE.toString(date));
    }

    @Test
    void testTimestampNanos() {
        // Test nanosecond precision
        Timestamp ts = new Timestamp(1672574400000L);
        ts.setNanos(123456789);
        assertEquals("2023-01-01 12:00:00.123456789", Type.TIMESTAMP.toString(ts));
    }

    // ======================= toBytes() Tests =======================

    @Test
    void testIntToBytes() {
        byte[] bytes = Type.INT.toBytes(42);
        assertEquals(4, bytes.length);
        assertEquals(42, ByteBuffer.wrap(bytes).getInt());
    }

    @Test
    void testIntMinMaxToBytes() {
        byte[] minBytes = Type.INT.toBytes(Integer.MIN_VALUE);
        assertEquals(Integer.MIN_VALUE, ByteBuffer.wrap(minBytes).getInt());

        byte[] maxBytes = Type.INT.toBytes(Integer.MAX_VALUE);
        assertEquals(Integer.MAX_VALUE, ByteBuffer.wrap(maxBytes).getInt());
    }

    @Test
    void testFloatToBytes() {
        byte[] bytes = Type.FLOAT.toBytes(3.14f);
        assertEquals(4, bytes.length);
        assertEquals(3.14f, ByteBuffer.wrap(bytes).getFloat(), 0.001);
    }

    @Test
    void testDoubleToBytes() {
        byte[] bytes = Type.DOUBLE.toBytes(3.14159);
        assertEquals(8, bytes.length);
        assertEquals(3.14159, ByteBuffer.wrap(bytes).getDouble(), 0.000001);
    }

    @Test
    void testBooleanToBytes() {
        byte[] trueBytes = Type.BOOLEAN.toBytes(true);
        assertArrayEquals(new byte[] { 1 }, trueBytes);

        byte[] falseBytes = Type.BOOLEAN.toBytes(false);
        assertArrayEquals(new byte[] { 0 }, falseBytes);
    }

    @Test
    void testLongToBytes() {
        byte[] bytes = Type.LONG.toBytes(1234567890123L);
        assertEquals(8, bytes.length);
        assertEquals(1234567890123L, ByteBuffer.wrap(bytes).getLong());
    }

    @Test
    void testDateToBytes() {
        Date date = new Date(1672531200000L);
        byte[] bytes = Type.DATE.toBytes(date);
        assertEquals(8, bytes.length);
        assertEquals(1672531200000L, ByteBuffer.wrap(bytes).getLong());
    }

    @Test
    void testTimestampToBytes() {
        Timestamp ts = new Timestamp(1672574400000L);
        ts.setNanos(123456789);
        byte[] bytes = Type.TIMESTAMP.toBytes(ts);

        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        // Should expect the base millis without nanos
        assertEquals(1672574400000L, buffer.getLong());
        assertEquals(123456789, buffer.getInt());
    }

    @Test
    void testStringToBytes() {
        byte[] bytes = Type.VARCHAR.toBytes("hello");
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        assertEquals(5, buffer.getShort());
        byte[] strBytes = new byte[5];
        buffer.get(strBytes);
        assertArrayEquals("hello".getBytes(), strBytes);
    }

    @Test
    void testEmptyStringToBytes() {
        byte[] bytes = Type.VARCHAR.toBytes("");
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        assertEquals(0, buffer.getShort());
        assertEquals(0, buffer.remaining());
    }

    @Test
    void testBinaryToBytes() {
        byte[] data = { 1, 2, 3, 4, 5 };
        byte[] bytes = Type.BINARY.toBytes(data);
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        assertEquals(5, buffer.getShort());
        byte[] binData = new byte[5];
        buffer.get(binData);
        assertArrayEquals(data, binData);
    }

    @Test
    void testEmptyBinaryToBytes() {
        byte[] bytes = Type.BINARY.toBytes(new byte[0]);
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        assertEquals(0, buffer.getShort());
        assertEquals(0, buffer.remaining());
    }

    // ======================= fromBytes() Tests =======================
/* 
    @Test
    void testIntFromBytes() {
        byte[] bytes = ByteBuffer.allocate(4).putInt(42).array();
        Object result = Type.INT.fromBytes(bytes, 0);
        assertEquals(42, result.valueObject());
        assertEquals(4, result.nextIndex());
    }

    @Test
    void testFloatFromBytes() {
        byte[] bytes = ByteBuffer.allocate(4).putFloat(3.14f).array();
        Object result = Type.FLOAT.fromBytes(bytes, 0);
        assertEquals(3.14f, (float) result.valueObject(), 0.001);
        assertEquals(4, result.nextIndex());
    }

    @Test
    void testDoubleFromBytes() {
        byte[] bytes = ByteBuffer.allocate(8).putDouble(3.14159).array();
        Object result = Type.DOUBLE.fromBytes(bytes, 0);
        assertEquals(3.14159, (double) result.valueObject(), 0.000001);
        assertEquals(8, result.nextIndex());
    }

    @Test
    void testBooleanFromBytes() {
        Object trueResult = Type.BOOLEAN.fromBytes(new byte[] { 1 }, 0);
        assertEquals(true, trueResult.valueObject());
        assertEquals(1, trueResult.nextIndex());

        Object falseResult = Type.BOOLEAN.fromBytes(new byte[] { 0 }, 0);
        assertEquals(false, falseResult.valueObject());
        assertEquals(1, falseResult.nextIndex());
    }

    @Test
    void testLongFromBytes() {
        byte[] bytes = ByteBuffer.allocate(8).putLong(1234567890123L).array();
        Object result = Type.LONG.fromBytes(bytes, 0);
        assertEquals(1234567890123L, result.valueObject());
        assertEquals(8, result.nextIndex());
    }

    @Test
    void testDateFromBytes() {
        byte[] bytes = ByteBuffer.allocate(8).putLong(1672531200000L).array();
        Object result = Type.DATE.fromBytes(bytes, 0);
        assertEquals(1672531200000L, ((Date) result.valueObject()).getTime());
        assertEquals(8, result.nextIndex());
    }

    @Test
    void testTimestampFromBytes() {
        ByteBuffer buffer = ByteBuffer.allocate(12);
        buffer.putLong(1672574400000L).putInt(123456789);
        Object result = Type.TIMESTAMP.fromBytes(buffer.array(), 0);

        Timestamp ts = (Timestamp) result.valueObject();
        // Should expect the full timestamp with nanos incorporated
        assertEquals(1672574400000L + 123, ts.getTime());
        assertEquals(123456789, ts.getNanos());
        assertEquals(12, result.nextIndex());
    }

    @Test
    void testStringFromBytes() {
        ByteBuffer buffer = ByteBuffer.allocate(7);
        buffer.putShort((short) 5).put("hello".getBytes());
        Object result = Type.VARCHAR.fromBytes(buffer.array(), 0);
        assertEquals("hello", result.valueObject());
        assertEquals(7, result.nextIndex());
    }

    @Test
    void testEmptyStringFromBytes() {
        ByteBuffer buffer = ByteBuffer.allocate(2);
        buffer.putShort((short) 0);
        Object result = Type.VARCHAR.fromBytes(buffer.array(), 0);
        assertEquals("", result.valueObject());
        assertEquals(2, result.nextIndex());
    }

    @Test
    void testBinaryFromBytes() {
        byte[] data = { 1, 2, 3, 4, 5 };
        ByteBuffer buffer = ByteBuffer.allocate(7);
        buffer.putShort((short) 5).put(data);
        Object result = Type.BINARY.fromBytes(buffer.array(), 0);
        assertArrayEquals(data, (byte[]) result.valueObject());
        assertEquals(7, result.nextIndex());
    }

    @Test
    void testEmptyBinaryFromBytes() {
        ByteBuffer buffer = ByteBuffer.allocate(2);
        buffer.putShort((short) 0);
        Object result = Type.BINARY.fromBytes(buffer.array(), 0);
        assertArrayEquals(new byte[0], (byte[]) result.valueObject());
        assertEquals(2, result.nextIndex());
    }

    // ======================= Round-trip Tests =======================

    @ParameterizedTest
    @ValueSource(ints = { 0, 1, -1, Integer.MIN_VALUE, Integer.MAX_VALUE })
    void testIntRoundTrip(int value) {
        byte[] bytes = Type.INT.toBytes(value);
        Object result = Type.INT.fromBytes(bytes, 0);
        assertEquals(value, result.valueObject());
    }

    @ParameterizedTest
    @ValueSource(floats = { 0.0f, 1.5f, -3.14f, Float.MIN_VALUE, Float.MAX_VALUE })
    void testFloatRoundTrip(float value) {
        byte[] bytes = Type.FLOAT.toBytes(value);
        Object result = Type.FLOAT.fromBytes(bytes, 0);
        assertEquals(value, (float) result.valueObject(), 0.0001f);
    }

    @ParameterizedTest
    @ValueSource(doubles = { 0.0, 1.5, -3.14159, Double.MIN_VALUE, Double.MAX_VALUE })
    void testDoubleRoundTrip(double value) {
        byte[] bytes = Type.DOUBLE.toBytes(value);
        Object result = Type.DOUBLE.fromBytes(bytes, 0);
        assertEquals(value, (double) result.valueObject(), 0.0000001);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void testBooleanRoundTrip(boolean value) {
        byte[] bytes = Type.BOOLEAN.toBytes(value);
        Object result = Type.BOOLEAN.fromBytes(bytes, 0);
        assertEquals(value, result.valueObject());
    }

    @ParameterizedTest
    @ValueSource(longs = { 0L, 123456789L, -987654321L, Long.MIN_VALUE, Long.MAX_VALUE })
    void testLongRoundTrip(long value) {
        byte[] bytes = Type.LONG.toBytes(value);
        Object result = Type.LONG.fromBytes(bytes, 0);
        assertEquals(value, result.valueObject());
    }

    @Test
    void testDateRoundTrip() {
        Date date = new Date();
        byte[] bytes = Type.DATE.toBytes(date);
        Object result = Type.DATE.fromBytes(bytes, 0);
        assertEquals(date.getTime(), ((Date) result.valueObject()).getTime());
    }

    @Test
    void testTimestampRoundTrip() {
        Timestamp ts = new Timestamp(System.currentTimeMillis());
        ts.setNanos(123456789);
        byte[] bytes = Type.TIMESTAMP.toBytes(ts);
        Object result = Type.TIMESTAMP.fromBytes(bytes, 0);

        Timestamp resultTs = (Timestamp) result.valueObject();
        assertEquals(ts.getTime(), resultTs.getTime());
        assertEquals(ts.getNanos(), resultTs.getNanos());
    }

    @Test
    void testBinaryRoundTrip() {
        byte[] data = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        byte[] bytes = Type.BINARY.toBytes(data);
        Object result = Type.BINARY.fromBytes(bytes, 0);
        assertArrayEquals(data, (byte[]) result.valueObject());
    }

    // ======================= Error Handling Tests =======================

    @Test
    void testToBytesInvalidType() {
        assertThrows(IllegalArgumentException.class,
                () -> Type.INT.toBytes("not an integer"));

        assertThrows(IllegalArgumentException.class,
                () -> Type.VARCHAR.toBytes(42));
    }

    @Test
    void testFromBytesBufferTooShort() {
        // Test with buffer shorter than required
        byte[] shortBuffer = new byte[3];
        assertThrows(IllegalArgumentException.class,
                () -> Type.INT.fromBytes(shortBuffer, 0));

        byte[] stringBuffer = new byte[1]; // Not enough for even the length prefix
        assertThrows(IllegalArgumentException.class,
                () -> Type.VARCHAR.fromBytes(stringBuffer, 0));
    }

    @Test
    void testFromBytesWithOffsetBufferTooShort() {
        byte[] buffer = new byte[10];
        // Try to read INT at position 7 (only 3 bytes left)
        assertThrows(IllegalArgumentException.class,
                () -> Type.INT.fromBytes(buffer, 7));
    }

    // ======================= Null Handling Tests =======================

    @Test
    void testToBytesNullValue() {
        for (Type type : Type.values()) {
            byte[] bytes = type.toBytes(null);
            assertArrayEquals(new byte[0], bytes);
        }
    }

    @Test
    void testFromBytesNullBuffer() {
        for (Type type : Type.values()) {
            assertThrows(NullPointerException.class,
                    () -> type.fromBytes(null, 0));
        }
    }

    // ======================= Next Index Tests =======================

    @Test
    void testNextIndexWithOffset() {
        byte[] buffer = new byte[20];
        int startIndex = 5;

        // Write an integer at position 5
        ByteBuffer.wrap(buffer, startIndex, 4).putInt(42);

        Object result = Type.INT.fromBytes(buffer, startIndex);
        assertEquals(42, result.valueObject());
        assertEquals(startIndex + 4, result.nextIndex());
    }

    @Test
    void testNextIndexForString() {
        String value = "test";
        byte[] strBytes = value.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(10);
        buffer.putShort((short) strBytes.length).put(strBytes);

        Object result = Type.VARCHAR.fromBytes(buffer.array(), 0);
        assertEquals(value, result.valueObject());
        assertEquals(2 + strBytes.length, result.nextIndex());
    }

    // ======================= Special Case Tests =======================

    @Test
    void testBooleanEdgeCases() {
        // Any non-zero value should be considered true?
        // Our implementation only uses 0 for false, 1 for true
        Object result = Type.BOOLEAN.fromBytes(new byte[] { 2 }, 0);
        assertEquals(true, result.valueObject()); // 2 != 0 -> true

        result = Type.BOOLEAN.fromBytes(new byte[] { -1 }, 0);
        assertEquals(true, result.valueObject());
    }

    @Test
    void testBinaryWithOffset() {
        byte[] data = { 1, 2, 3, 4, 5 };
        byte[] buffer = new byte[20];
        int offset = 7;

        ByteBuffer.wrap(buffer, offset, 2).putShort((short) data.length);
        System.arraycopy(data, 0, buffer, offset + 2, data.length);

        Object result = Type.BINARY.fromBytes(buffer, offset);
        assertArrayEquals(data, (byte[]) result.valueObject());
        assertEquals(offset + 2 + data.length, result.nextIndex());
    }

    @Test
    void testMixedTypesInBuffer() {
        // Create buffer with: [INT(42), STRING("hello")]
        byte[] intBytes = Type.INT.toBytes(42);
        byte[] strBytes = Type.VARCHAR.toBytes("hello");

        ByteBuffer buffer = ByteBuffer.allocate(intBytes.length + strBytes.length);
        buffer.put(intBytes).put(strBytes);

        // Read both values
        Object intResult = Type.INT.fromBytes(buffer.array(), 0);
        assertEquals(42, intResult.valueObject());

        Object strResult = Type.VARCHAR.fromBytes(buffer.array(), intResult.nextIndex());
        assertEquals("hello", strResult.valueObject());
    }*/
}