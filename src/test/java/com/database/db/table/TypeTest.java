package com.database.db.table;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
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
            //"STRING, VARCHAR",
            //"string, VARCHAR",
            //"varchar, VARCHAR",
            "VARCHAR, VARCHAR",
            //"text, VARCHAR",
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
    void testFromStringValid(String input, DataType expected) {
        assertEquals(expected, DataType.fromString(input));
    }

    @ParameterizedTest
    @ValueSource(strings = { "", " ", "  ", "unknown", "array", "decimal" })
    void testFromStringInvalid(String input) {
        assertThrows(IllegalArgumentException.class, () -> DataType.fromString(input));
    }

    @Test
    void testFromStringNull() {
        assertThrows(IllegalArgumentException.class, () -> DataType.fromString(null));
    }

    // ======================= getFixedSize() Tests =======================

    @Test
    void testGetFixedSize() {
        assertEquals(4, DataType.INT.getSize());
        assertEquals(4, DataType.FLOAT.getSize());
        assertEquals(8, DataType.DOUBLE.getSize());
        assertEquals(1, DataType.BOOLEAN.getSize());
        assertEquals(8, DataType.LONG.getSize());
        assertEquals(8, DataType.DATE.getSize());
        assertEquals(16, DataType.TIMESTAMP.getSize());
        assertEquals(-1, DataType.VARCHAR.getSize());
        assertEquals(-1, DataType.BINARY.getSize());
    }

    // ======================= validateValue() Tests =======================

    @Test
    void testValidateIntValid() {
        DataType.INT.validateValue(42, 0);
        DataType.INT.validateValue(0, 0);
        DataType.INT.validateValue(-100, 0);
    }

    @Test
    void testValidateIntInvalid() {
        assertThrows(IllegalArgumentException.class,
                () -> DataType.INT.validateValue("not an int", 0));
        assertThrows(IllegalArgumentException.class,
                () -> DataType.INT.validateValue(3.14, 0));
    }

    @Test
    void testValidateFloatValid() {
        DataType.FLOAT.validateValue(3.14f, 0);
        DataType.FLOAT.validateValue(0.0f, 0);
        DataType.FLOAT.validateValue(-1.5f, 0);
    }

    @Test
    void testValidateFloatInvalid() {
        assertThrows(IllegalArgumentException.class,
                () -> DataType.FLOAT.validateValue("not a float", 0));
        assertThrows(IllegalArgumentException.class,
                () -> DataType.FLOAT.validateValue(42, 0));
    }

    @Test
    void testValidateDoubleValid() {
        DataType.DOUBLE.validateValue(3.14159, 0);
        DataType.DOUBLE.validateValue(0.0, 0);
        DataType.DOUBLE.validateValue(-1.5, 0);
    }

    @Test
    void testValidateDoubleInvalid() {
        assertThrows(IllegalArgumentException.class,
                () -> DataType.DOUBLE.validateValue("not a double", 0));
        assertThrows(IllegalArgumentException.class,
                () -> DataType.DOUBLE.validateValue(3.14f, 0));
    }

    @Test
    void testValidateStringValid() {
        DataType.VARCHAR.validateValue("hello", 10);
        DataType.VARCHAR.validateValue("", 10);
        DataType.VARCHAR.validateValue("short", 5);
    }

    @Test
    void testValidateStringInvalid() {
        // Wrong type
        assertThrows(IllegalArgumentException.class,
                () -> DataType.VARCHAR.validateValue(42, 10));

        // Size exceeded
        assertThrows(IllegalArgumentException.class,
                () -> DataType.VARCHAR.validateValue("this is too long", 10));
    }

    @Test
    void testValidateBooleanValid() {
        DataType.BOOLEAN.validateValue(true, 0);
        DataType.BOOLEAN.validateValue(false, 0);
    }

    @Test
    void testValidateBooleanInvalid() {
        assertThrows(IllegalArgumentException.class,
                () -> DataType.BOOLEAN.validateValue("true", 0));
        assertThrows(IllegalArgumentException.class,
                () -> DataType.BOOLEAN.validateValue(1, 0));
    }

    @Test
    void testValidateLongValid() {
        DataType.LONG.validateValue(1234567890123L, 0);
        DataType.LONG.validateValue(0L, 0);
        DataType.LONG.validateValue(-100L, 0);
    }

    @Test
    void testValidateLongInvalid() {
        assertThrows(IllegalArgumentException.class,
                () -> DataType.LONG.validateValue("not a long", 0));
        assertThrows(IllegalArgumentException.class,
                () -> DataType.LONG.validateValue(42, 0));
    }

    @Test
    void testValidateDateValid() {
        DataType.DATE.validateValue(new Date(), 0);
        DataType.DATE.validateValue(new Date(0), 0);
    }

    @Test
    void testValidateDateInvalid() {
        assertThrows(IllegalArgumentException.class,
                () -> DataType.DATE.validateValue("2023-01-01", 0));
        assertThrows(IllegalArgumentException.class,
                () -> DataType.DATE.validateValue(123456789, 0));
    }

    @Test
    void testValidateTimestampValid() {
        DataType.TIMESTAMP.validateValue(new Timestamp(System.currentTimeMillis()), 0);
        DataType.TIMESTAMP.validateValue(new Timestamp(0), 0);
    }

    @Test
    void testValidateTimestampInvalid() {
        assertThrows(IllegalArgumentException.class,
                () -> DataType.TIMESTAMP.validateValue(new Date(), 0));
        assertThrows(IllegalArgumentException.class,
                () -> DataType.TIMESTAMP.validateValue("2023-01-01 12:00:00", 0));
    }

    @Test
    void testValidateBinaryValid() {
        DataType.BINARY.validateValue(new byte[10], 10);
        DataType.BINARY.validateValue(new byte[0], 10);
        DataType.BINARY.validateValue(new byte[5], 5);
    }

    @Test
    void testValidateBinaryInvalid() {
        // Wrong type
        assertThrows(IllegalArgumentException.class,
                () -> DataType.BINARY.validateValue("binary data", 10));

        // Size exceeded
        byte[] largeData = new byte[11];
        assertThrows(IllegalArgumentException.class,
                () -> DataType.BINARY.validateValue(largeData, 10));
    }

    // ======================= parseValue() Tests =======================

    @Test
    void testParseInt() {
        assertEquals(42, DataType.INT.parseValue("42"));
        assertEquals(0, DataType.INT.parseValue("0"));
        assertEquals(-100, DataType.INT.parseValue("-100"));
    }

    @Test
    void testParseFloat() {
        assertEquals(3.14f, DataType.FLOAT.parseValue("3.14"));
        assertEquals(0.0f, DataType.FLOAT.parseValue("0.0"));
        assertEquals(-1.5f, DataType.FLOAT.parseValue("-1.5"));
    }

    @Test
    void testParseDouble() {
        assertEquals(3.14159, DataType.DOUBLE.parseValue("3.14159"));
        assertEquals(0.0, DataType.DOUBLE.parseValue("0.0"));
        assertEquals(-1.5, DataType.DOUBLE.parseValue("-1.5"));
    }

    @Test
    void testParseString() {
        assertEquals("hello", DataType.VARCHAR.parseValue("hello"));
        assertEquals("", DataType.VARCHAR.parseValue(""));
    }

    @Test
    void testParseBoolean() {
        assertEquals(true, DataType.BOOLEAN.parseValue("true"));
        assertEquals(false, DataType.BOOLEAN.parseValue("false"));
        assertEquals(true, DataType.BOOLEAN.parseValue("TRUE")); // case-insensitive
        assertEquals(false, DataType.BOOLEAN.parseValue("FALSE"));
    }

    @Test
    void testParseLong() {
        assertEquals(1234567890123L, DataType.LONG.parseValue("1234567890123"));
        assertEquals(0L, DataType.LONG.parseValue("0"));
        assertEquals(-100L, DataType.LONG.parseValue("-100"));
    }

    @Test
    void testParseDate() {
        Date date = (Date) DataType.DATE.parseValue("2023-01-01T00:00:00Z");
        assertNotNull(date);
        assertEquals(1672531200000L, date.getTime());
    }

    @Test
    void testParseTimestamp() {
        Timestamp ts = (Timestamp) DataType.TIMESTAMP.parseValue("2023-01-01 12:00:00.0");
        assertNotNull(ts);
        assertEquals(1672574400000L, ts.getTime());
    }

    @Test
    void testParseBinary() {
        byte[] data = (byte[]) DataType.BINARY.parseValue("hello");
        assertArrayEquals("hello".getBytes(), data);
    }

    @Test
    void testParseNull() {
        for (DataType type : DataType.values()) {
            assertNull(type.parseValue(null));
            assertNull(type.parseValue("null"));
            assertNull(type.parseValue("NULL"));
        }
    }

    // ======================= toString() Tests =======================

    @Test
    void testIntToString() {
        assertEquals("42", DataType.INT.toString(42));
    }

    @Test
    void testFloatToString() {
        assertEquals("3.14", DataType.FLOAT.toString(3.14f));
    }

    @Test
    void testDoubleToString() {
        assertEquals("3.14159", DataType.DOUBLE.toString(3.14159));
    }

    @Test
    void testStringToString() {
        assertEquals("hello", DataType.VARCHAR.toString("hello"));
    }

    @Test
    void testBooleanToString() {
        assertEquals("true", DataType.BOOLEAN.toString(true));
        assertEquals("false", DataType.BOOLEAN.toString(false));
    }

    @Test
    void testLongToString() {
        assertEquals("1234567890123", DataType.LONG.toString(1234567890123L));
    }

    @Test
    void testDateToString() {
        Date date = new Date(1672531200000L);
        assertEquals("2023-01-01", DataType.DATE.toString(date));
    }

    @Test
    void testTimestampToString() {
        Timestamp ts = new Timestamp(1672574400000L);
        assertEquals("2023-01-01 12:00:00", DataType.TIMESTAMP.toString(ts));
    }

    @Test
    void testBinaryToString() {
        byte[] data = { 104, 101, 108, 108, 111 }; // "hello"
        assertEquals("hello", DataType.BINARY.toString(data));
    }

    @Test
    void testNullToString() {
        for (DataType type : DataType.values()) {
            assertEquals("null", type.toString(null));
        }
    }

    // ======================= Edge Case Tests =======================

    @Test
    void testMaxStringSize() {
        // Test max size validation
        String maxString = "a".repeat(10);
        DataType.VARCHAR.validateValue(maxString, 10);

        // Test one char over limit
        String tooLong = "a".repeat(11);
        assertThrows(IllegalArgumentException.class,
                () -> DataType.VARCHAR.validateValue(tooLong, 10));
    }

    @Test
    void testMaxBinarySize() {
        // Test max size validation
        byte[] maxData = new byte[10];
        DataType.BINARY.validateValue(maxData, 10);

        // Test one byte over limit
        byte[] tooBig = new byte[11];
        assertThrows(IllegalArgumentException.class,
                () -> DataType.BINARY.validateValue(tooBig, 10));
    }

    @Test
    void testZeroSizeString() {
        // Should allow empty string
        DataType.VARCHAR.validateValue("", 0);

        // Should reject any non-empty string
        assertThrows(IllegalArgumentException.class,
                () -> DataType.VARCHAR.validateValue("a", 0));
    }

    @Test
    void testZeroSizeBinary() {
        // Should allow empty byte array
        DataType.BINARY.validateValue(new byte[0], 0);

        // Should reject any non-empty byte array
        assertThrows(IllegalArgumentException.class,
                () -> DataType.BINARY.validateValue(new byte[1], 0));
    }

    @Test
    void testNumberFormatting() {
        // Test locale-independent formatting
        assertEquals("3.1415926535", DataType.DOUBLE.toString(3.1415926535));
        assertEquals("3.14", DataType.FLOAT.toString(3.14f));
    }

    @Test
    void testDatePrecision() {
        // Test millisecond precision
        Date date = new Date(1672531200123L);
        assertEquals("2023-01-01", DataType.DATE.toString(date));
    }

    @Test
    void testTimestampNanos() {
        // Test nanosecond precision
        Timestamp ts = new Timestamp(1672574400000L);
        ts.setNanos(123456789);
        assertEquals("2023-01-01 12:00:00.123456789", DataType.TIMESTAMP.toString(ts));
    }

    // ======================= toBytes() Tests =======================

    @Test
    void testIntToBytes() {
        byte[] bytes = DataType.INT.toBytes(42);
        assertEquals(4, bytes.length);
        assertEquals(42, ByteBuffer.wrap(bytes).getInt());
    }

    @Test
    void testIntMinMaxToBytes() {
        byte[] minBytes = DataType.INT.toBytes(Integer.MIN_VALUE);
        assertEquals(Integer.MIN_VALUE, ByteBuffer.wrap(minBytes).getInt());

        byte[] maxBytes = DataType.INT.toBytes(Integer.MAX_VALUE);
        assertEquals(Integer.MAX_VALUE, ByteBuffer.wrap(maxBytes).getInt());
    }

    @Test
    void testFloatToBytes() {
        byte[] bytes = DataType.FLOAT.toBytes(3.14f);
        assertEquals(4, bytes.length);
        assertEquals(3.14f, ByteBuffer.wrap(bytes).getFloat(), 0.001);
    }

    @Test
    void testDoubleToBytes() {
        byte[] bytes = DataType.DOUBLE.toBytes(3.14159);
        assertEquals(8, bytes.length);
        assertEquals(3.14159, ByteBuffer.wrap(bytes).getDouble(), 0.000001);
    }

    @Test
    void testBooleanToBytes() {
        byte[] trueBytes = DataType.BOOLEAN.toBytes(true);
        assertArrayEquals(new byte[] { 1 }, trueBytes);

        byte[] falseBytes = DataType.BOOLEAN.toBytes(false);
        assertArrayEquals(new byte[] { 0 }, falseBytes);
    }

    @Test
    void testLongToBytes() {
        byte[] bytes = DataType.LONG.toBytes(1234567890123L);
        assertEquals(8, bytes.length);
        assertEquals(1234567890123L, ByteBuffer.wrap(bytes).getLong());
    }

    @Test
    void testDateToBytes() {
        Date date = new Date(1672531200000L);
        byte[] bytes = DataType.DATE.toBytes(date);
        assertEquals(8, bytes.length);
        assertEquals(1672531200000L, ByteBuffer.wrap(bytes).getLong());
    }

    @Test
    void testTimestampToBytes() {
        Timestamp ts = new Timestamp(1672574400000L);
        ts.setNanos(123456789);
        byte[] bytes = DataType.TIMESTAMP.toBytes(ts);

        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        // Should expect the base millis without nanos
        assertEquals(1672574400000L, buffer.getLong());
        assertEquals(123456789, buffer.getInt());
    }

    @Test
    void testStringToBytes() {
        byte[] bytes = DataType.VARCHAR.toBytes("hello");
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        assertEquals(5, buffer.getShort());
        byte[] strBytes = new byte[5];
        buffer.get(strBytes);
        assertArrayEquals("hello".getBytes(), strBytes);
    }

    @Test
    void testEmptyStringToBytes() {
        byte[] bytes = DataType.VARCHAR.toBytes("");
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        assertEquals(0, buffer.getShort());
        assertEquals(0, buffer.remaining());
    }

    @Test
    void testBinaryToBytes() {
        byte[] data = { 1, 2, 3, 4, 5 };
        byte[] bytes = DataType.BINARY.toBytes(data);
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        assertEquals(5, buffer.getShort());
        byte[] binData = new byte[5];
        buffer.get(binData);
        assertArrayEquals(data, binData);
    }

    @Test
    void testEmptyBinaryToBytes() {
        byte[] bytes = DataType.BINARY.toBytes(new byte[0]);
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