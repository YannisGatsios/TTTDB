package com.database.tttdb.table;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.database.tttdb.core.table.DataType;

class DataTypeTest {
    // Test constants
    private static final LocalDate TEST_DATE = LocalDate.of(2023, 1, 1);
    private static final LocalTime TEST_TIME = LocalTime.of(12, 30, 45, 123456789);
    private static final LocalDateTime TEST_TIMESTAMP = LocalDateTime.of(2023, 1, 1, 12, 30, 45, 123456789);
    private static final ZonedDateTime TEST_TIMESTAMPTZ = ZonedDateTime.of(
            LocalDateTime.of(2023, 1, 1, 12, 30, 45, 123456789),
            ZoneId.of("America/New_York")
    );
    private static final Duration TEST_INTERVAL = Duration.ofHours(48).plusMinutes(30).plusNanos(123456789);
    private static final UUID TEST_UUID = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
    private static final byte[] TEST_BINARY = {0x01, 0x02, 0x03, 0x04, 0x05};

    // ======================= fromString() Tests =======================
    @ParameterizedTest
    @CsvSource({
        "INT, INT",
        "int, INT",
        "SHORT, SHORT",
        "short, SHORT",
        "FLOAT, FLOAT",
        "float, FLOAT",
        "DOUBLE, DOUBLE",
        "double, DOUBLE",
        "CHAR, CHAR",
        "char, CHAR",
        "VARCHAR, VARCHAR",
        "varchar, VARCHAR",
        "BOOLEAN, BOOLEAN",
        "boolean, BOOLEAN",
        "bool, BOOLEAN",
        "LONG, LONG",
        "long, LONG",
        "DATE, DATE",
        "date, DATE",
        "TIME, TIME",
        "time, TIME",
        "TIMESTAMP, TIMESTAMP",
        "timestamp, TIMESTAMP",
        "datetime, TIMESTAMP",
        "TIMESTAMP_WITH_TIME_ZONE, TIMESTAMP_WITH_TIME_ZONE",
        "timestamptz, TIMESTAMP_WITH_TIME_ZONE",
        "INTERVAL, INTERVAL",
        "interval, INTERVAL",
        "duration, INTERVAL",
        "UUID, UUID",
        "uuid, UUID",
        "BYTE, BYTE",
        "byte, BYTE",
        "VARBYTE, VARBYTE",
        "varbyte, VARBYTE"
    })
    void testFromStringValid(String input, DataType expected) {
        assertEquals(expected, DataType.fromString(input));
    }

    // ======================= getSize() Tests =======================
    @Test
    void testGetFixedSize() {
        assertEquals(4, DataType.INT.getSize());
        assertEquals(2, DataType.SHORT.getSize());
        assertEquals(4, DataType.FLOAT.getSize());
        assertEquals(8, DataType.DOUBLE.getSize());
        assertEquals(1, DataType.BOOLEAN.getSize());
        assertEquals(8, DataType.LONG.getSize());
        assertEquals(8, DataType.DATE.getSize());
        assertEquals(8, DataType.TIME.getSize());
        assertEquals(16, DataType.TIMESTAMP.getSize());
        assertEquals(-1, DataType.TIMESTAMP_WITH_TIME_ZONE.getSize());
        assertEquals(16, DataType.INTERVAL.getSize());
        assertEquals(16, DataType.UUID.getSize());
        assertEquals(-1, DataType.CHAR.getSize());
        assertEquals(-1, DataType.BYTE.getSize());
    }

    // ======================= validateValue() Tests =======================
    @Test
    void testValidateDateValid() {
        DataType.DATE.validateValue(TEST_DATE, 0);
    }

    @Test
    void testValidateTimeValid() {
        DataType.TIME.validateValue(TEST_TIME, 0);
    }

    @Test
    void testValidateTimestampValid() {
        DataType.TIMESTAMP.validateValue(TEST_TIMESTAMP, 0);
    }

    @Test
    void testValidateTimestampWithTzValid() {
        DataType.TIMESTAMP_WITH_TIME_ZONE.validateValue(TEST_TIMESTAMPTZ, 0);
    }

    @Test
    void testValidateIntervalValid() {
        DataType.INTERVAL.validateValue(TEST_INTERVAL, 0);
    }

    @Test
    void testValidateUuidValid() {
        DataType.UUID.validateValue(TEST_UUID, 0);
    }

    @Test
    void testValidateDateInvalid() {
        assertThrows(IllegalArgumentException.class,
                () -> DataType.DATE.validateValue("2023-01-01", 0));
    }

    @Test
    void testValidateTimeInvalid() {
        assertThrows(IllegalArgumentException.class,
                () -> DataType.TIME.validateValue("12:30:45", 0));
    }

    // ======================= parseValue() Tests =======================
    @Test
    void testParseDate() {
        assertEquals(TEST_DATE, DataType.DATE.parseValue("2023-01-01"));
    }

    @Test
    void testParseTime() {
        assertEquals(TEST_TIME, DataType.TIME.parseValue("12:30:45.123456789"));
    }

    @Test
    void testParseTimestamp() {
        assertEquals(TEST_TIMESTAMP, DataType.TIMESTAMP.parseValue("2023-01-01T12:30:45.123456789"));
    }

    @Test
    void testParseTimestampWithTz() {
        assertEquals(TEST_TIMESTAMPTZ, 
            DataType.TIMESTAMP_WITH_TIME_ZONE.parseValue("2023-01-01T12:30:45.123456789-05:00[America/New_York]"));
    }

    @Test
    void testParseInterval() {
        assertEquals(TEST_INTERVAL, DataType.INTERVAL.parseValue("PT48H30M0.123456789S"));
    }

    @Test
    void testParseUuid() {
        assertEquals(TEST_UUID, DataType.UUID.parseValue("550e8400-e29b-41d4-a716-446655440000"));
    }

    @Test
    void testParseBinary() {
        String base64 = Base64.getEncoder().encodeToString(TEST_BINARY);
        assertArrayEquals(TEST_BINARY, (byte[]) DataType.BYTE.parseValue(base64));
    }

    @Test
    void testParseBooleanStrict() {
        assertThrows(IllegalArgumentException.class,
            () -> DataType.BOOLEAN.parseValue("yes"));
    }

    // ======================= toString() Tests =======================
    @Test
    void testDateToString() {
        assertEquals("2023-01-01", DataType.DATE.toString(TEST_DATE));
    }

    @Test
    void testTimeToString() {
        assertEquals("12:30:45.123456789", DataType.TIME.toString(TEST_TIME));
    }

    @Test
    void testTimestampToString() {
        assertEquals("2023-01-01T12:30:45.123456789", DataType.TIMESTAMP.toString(TEST_TIMESTAMP));
    }

    @Test
    void testTimestampWithTzToString() {
        String formatted = TEST_TIMESTAMPTZ.format(DateTimeFormatter.ISO_ZONED_DATE_TIME);
        assertEquals(formatted, DataType.TIMESTAMP_WITH_TIME_ZONE.toString(TEST_TIMESTAMPTZ));
    }

    @Test
    void testIntervalToString() {
        assertEquals("PT48H30M0.123456789S", DataType.INTERVAL.toString(TEST_INTERVAL));
    }

    @Test
    void testUuidToString() {
        assertEquals("550e8400-e29b-41d4-a716-446655440000", DataType.UUID.toString(TEST_UUID));
    }

    @Test
    void testBinaryToString() {
        String expected = Base64.getEncoder().encodeToString(TEST_BINARY);
        assertEquals(expected, DataType.BYTE.toString(TEST_BINARY));
    }

    // ======================= toBytes() Tests =======================
    @Test
    void testDateToBytes() {
        byte[] bytes = DataType.DATE.toBytes(TEST_DATE);
        assertEquals(8, bytes.length);
        long epochDay = ByteBuffer.wrap(bytes).getLong();
        assertEquals(TEST_DATE.toEpochDay(), epochDay);
    }

    @Test
    void testTimeToBytes() {
        byte[] bytes = DataType.TIME.toBytes(TEST_TIME);
        assertEquals(8, bytes.length);
        long nanos = ByteBuffer.wrap(bytes).getLong();
        assertEquals(TEST_TIME.toNanoOfDay(), nanos);
    }

    @Test
    void testTimestampToBytes() {
        byte[] bytes = DataType.TIMESTAMP.toBytes(TEST_TIMESTAMP);
        assertEquals(16, bytes.length);
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        long epochDay = buffer.getLong();
        long nanoOfDay = buffer.getLong();
        assertEquals(TEST_TIMESTAMP.toLocalDate().toEpochDay(), epochDay);
        assertEquals(TEST_TIMESTAMP.toLocalTime().toNanoOfDay(), nanoOfDay);
    }

    @Test
    void testTimestampWithTzToBytes() {
        byte[] bytes = DataType.TIMESTAMP_WITH_TIME_ZONE.toBytes(TEST_TIMESTAMPTZ);
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        
        // Verify components
        Instant instant = TEST_TIMESTAMPTZ.toInstant();
        String zoneId = TEST_TIMESTAMPTZ.getZone().getId();
        byte[] zoneBytes = zoneId.getBytes();
        
        assertEquals(instant.getEpochSecond(), buffer.getLong());
        assertEquals(instant.getNano(), buffer.getInt());
        assertEquals((short) zoneBytes.length, buffer.getShort());
        byte[] actualZoneBytes = new byte[zoneBytes.length];
        buffer.get(actualZoneBytes);
        assertArrayEquals(zoneBytes, actualZoneBytes);
    }

    @Test
    void testIntervalToBytes() {
        byte[] bytes = DataType.INTERVAL.toBytes(TEST_INTERVAL);
        assertEquals(16, bytes.length);
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        assertEquals(TEST_INTERVAL.getSeconds(), buffer.getLong());
        assertEquals(TEST_INTERVAL.getNano(), buffer.getInt());
        assertEquals(0, buffer.getInt()); // Padding
    }

    @Test
    void testUuidToBytes() {
        byte[] bytes = DataType.UUID.toBytes(TEST_UUID);
        assertEquals(16, bytes.length);
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        assertEquals(TEST_UUID.getMostSignificantBits(), buffer.getLong());
        assertEquals(TEST_UUID.getLeastSignificantBits(), buffer.getLong());
    }

    // ======================= fromBytes() Tests =======================
    @Test
    void testDateFromBytes() {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putLong(TEST_DATE.toEpochDay());
        buffer.flip();
        assertEquals(TEST_DATE, DataType.DATE.fromBytes(buffer));
    }

    @Test
    void testTimeFromBytes() {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putLong(TEST_TIME.toNanoOfDay());
        buffer.flip();
        assertEquals(TEST_TIME, DataType.TIME.fromBytes(buffer));
    }

    @Test
    void testTimestampFromBytes() {
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.putLong(TEST_TIMESTAMP.toLocalDate().toEpochDay());
        buffer.putLong(TEST_TIMESTAMP.toLocalTime().toNanoOfDay());
        buffer.flip();
        assertEquals(TEST_TIMESTAMP, DataType.TIMESTAMP.fromBytes(buffer));
    }

    @Test
    void testTimestampWithTzFromBytes() {
        // Prepare serialized data
        Instant instant = TEST_TIMESTAMPTZ.toInstant();
        String zoneId = TEST_TIMESTAMPTZ.getZone().getId();
        byte[] zoneBytes = zoneId.getBytes();
        ByteBuffer buffer = ByteBuffer.allocate(14 + zoneBytes.length);
        buffer.putLong(instant.getEpochSecond());
        buffer.putInt(instant.getNano());
        buffer.putShort((short) zoneBytes.length);
        buffer.put(zoneBytes);
        buffer.flip();

        // Test deserialization
        ZonedDateTime result = (ZonedDateTime) DataType.TIMESTAMP_WITH_TIME_ZONE.fromBytes(buffer);
        assertEquals(TEST_TIMESTAMPTZ, result);
    }

    @Test
    void testIntervalFromBytes() {
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.putLong(TEST_INTERVAL.getSeconds());
        buffer.putInt(TEST_INTERVAL.getNano());
        buffer.putInt(0); // Padding
        buffer.flip();
        assertEquals(TEST_INTERVAL, DataType.INTERVAL.fromBytes(buffer));
    }

    // ======================= detect() Tests =======================
    @Test
    void testDetectTypes() {
        assertEquals(DataType.INT, DataType.detectFixedSizeType(42));
        assertEquals(DataType.SHORT, DataType.detectFixedSizeType((short) 10));
        assertEquals(DataType.FLOAT, DataType.detectFixedSizeType(3.14f));
        assertEquals(DataType.DOUBLE, DataType.detectFixedSizeType(3.14));
        assertEquals(DataType.BOOLEAN, DataType.detectFixedSizeType(true));
        assertEquals(DataType.LONG, DataType.detectFixedSizeType(123L));
        assertEquals(DataType.DATE, DataType.detectFixedSizeType(LocalDate.now()));
        assertEquals(DataType.TIME, DataType.detectFixedSizeType(LocalTime.now()));
        assertEquals(DataType.TIMESTAMP, DataType.detectFixedSizeType(LocalDateTime.now()));
        assertEquals(DataType.TIMESTAMP_WITH_TIME_ZONE, DataType.detectFixedSizeType(ZonedDateTime.now()));
        assertEquals(DataType.INTERVAL, DataType.detectFixedSizeType(Duration.ofHours(2)));
        assertEquals(DataType.CHAR, DataType.detectFixedSizeType("test"));
        assertEquals(DataType.UUID, DataType.detectFixedSizeType(UUID.randomUUID()));
        assertEquals(DataType.BYTE, DataType.detectFixedSizeType(new byte[10]));
    }

    // ======================= Edge Cases =======================

    @Test
    void testBinaryEmpty() {
        byte[] empty = new byte[0];
        DataType.BYTE.validateValue(empty, 0);
        
        byte[] bytes = DataType.BYTE.toBytes(empty);
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        assertEquals(0, buffer.getShort());
    }

    @Test
    void testBinarySizeExceeded() {
        byte[] data = new byte[1025]; // Exceeds max size
        assertThrows(IllegalArgumentException.class,
            () -> DataType.BYTE.validateValue(data, 1024));
    }

    @Test
    void testNullHandling() {
        // Null value validation
        for (DataType type : DataType.values()) {
            assertThrows(NullPointerException.class,
                () -> type.validateValue(null, 0));
        }

        // Null serialization
        for (DataType type : DataType.values()) {
            assertEquals(0,type.toBytes(null).length);
        }
    }
    // Assuming a wrapper class TypedValue for fromBytes result with methods:
    // Object valueObject();
    // int nextIndex();

    // ======================= fromBytes() Tests =======================

    @Test
    void testIntFromBytes() {
        ByteBuffer buf = ByteBuffer.allocate(4).putInt(42);
        buf.flip();
        Object result = DataType.INT.fromBytes(buf);
        assertEquals(42, result);
    }

    @Test
    void testFloatFromBytes() {
        ByteBuffer buf = ByteBuffer.allocate(4).putFloat(3.14f);
        buf.flip();
        Object result = DataType.FLOAT.fromBytes(buf);
        assertEquals(3.14f, (float) result, 1e-6);
    }

    @Test
    void testDoubleFromBytes() {
        ByteBuffer buf = ByteBuffer.allocate(8).putDouble(3.14159);
        buf.flip();
        Object result = DataType.DOUBLE.fromBytes(buf);
        assertEquals(3.14159, (double) result, 1e-9);
    }

    @Test
    void testBooleanFromBytes() {
        ByteBuffer trueBuf = ByteBuffer.wrap(new byte[]{1});
        ByteBuffer falseBuf = ByteBuffer.wrap(new byte[]{0});
        assertEquals(true, DataType.BOOLEAN.fromBytes(trueBuf));
        assertEquals(false, DataType.BOOLEAN.fromBytes(falseBuf));
    }

    @Test
    void testLongFromBytes() {
        ByteBuffer buf = ByteBuffer.allocate(8).putLong(1234567890123L);
        buf.flip();
        Object result = DataType.LONG.fromBytes(buf);
        assertEquals(1234567890123L, result);
    }

    @Test
    void testDateFromBytes2() {
        LocalDate ld = LocalDate.ofEpochDay(19000);
        ByteBuffer buf = ByteBuffer.allocate(8).putLong(ld.toEpochDay());
        buf.flip();
        Object result = DataType.DATE.fromBytes(buf);
        assertEquals(ld, result);
    }

    @Test
    void testTimestampFromBytes2() {
        LocalDateTime ldt = LocalDateTime.of(2023,1,1,12,0,0,654321);
        // serialize as epochDay + nanoOfDay
        ByteBuffer buf = ByteBuffer.allocate(16)
                .putLong(ldt.toLocalDate().toEpochDay())
                .putLong(ldt.toLocalTime().toNanoOfDay());
        buf.flip();
        Object result = DataType.TIMESTAMP.fromBytes(buf);
        assertEquals(ldt, result);
    }

    @Test
    void testStringFromBytes() {
        String s = "hello";
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(2 + bytes.length)
                .putShort((short) bytes.length)
                .put(bytes);
        buf.flip();
        Object result = DataType.CHAR.fromBytes(buf);
        assertEquals(s, result);
    }

    @Test
    void testEmptyStringFromBytes() {
        ByteBuffer buf = ByteBuffer.allocate(2).putShort((short) 0);
        buf.flip();
        Object result = DataType.CHAR.fromBytes(buf);
        assertEquals("", result);
    }

    @Test
    void testBinaryFromBytes() {
        byte[] data = {1,2,3,4,5};
        ByteBuffer buf = ByteBuffer.allocate(2 + data.length)
                .putShort((short) data.length)
                .put(data);
        buf.flip();
        Object result = DataType.BYTE.fromBytes(buf);
        assertArrayEquals(data, (byte[]) result);
    }

    @Test
    void testEmptyBinaryFromBytes() {
        ByteBuffer buf = ByteBuffer.allocate(2).putShort((short) 0);
        buf.flip();
        Object result = DataType.BYTE.fromBytes(buf);
        assertArrayEquals(new byte[0], (byte[]) result);
    }


    // ======================= Round‑trip Tests =======================

    @ParameterizedTest @ValueSource(ints = {0,1,-1,Integer.MIN_VALUE,Integer.MAX_VALUE})
    void testIntRoundTrip(int v) {
        byte[] b = DataType.INT.toBytes(v);
        Object r = DataType.INT.fromBytes(ByteBuffer.wrap(b));
        assertEquals(v, r);
    }

    @ParameterizedTest @ValueSource(floats = {0.0f,1.5f,-3.14f,Float.MIN_VALUE,Float.MAX_VALUE})
    void testFloatRoundTrip(float v) {
        byte[] b = DataType.FLOAT.toBytes(v);
        Object r = DataType.FLOAT.fromBytes(ByteBuffer.wrap(b));
        assertEquals(v, (float) r, 1e-6f);
    }

    @ParameterizedTest @ValueSource(doubles = {0.0,1.5,-3.14159,Double.MIN_VALUE,Double.MAX_VALUE})
    void testDoubleRoundTrip(double v) {
        byte[] b = DataType.DOUBLE.toBytes(v);
        Object r = DataType.DOUBLE.fromBytes(ByteBuffer.wrap(b));
        assertEquals(v, (double) r, 1e-9);
    }

    @ParameterizedTest @ValueSource(booleans = {true,false})
    void testBooleanRoundTrip(boolean v) {
        byte[] b = DataType.BOOLEAN.toBytes(v);
        Object r = DataType.BOOLEAN.fromBytes(ByteBuffer.wrap(b));
        assertEquals(v, r);
    }

    @ParameterizedTest @ValueSource(longs = {0L,123456789L,-987654321L,Long.MIN_VALUE,Long.MAX_VALUE})
    void testLongRoundTrip(long v) {
        byte[] b = DataType.LONG.toBytes(v);
        Object r = DataType.LONG.fromBytes(ByteBuffer.wrap(b));
        assertEquals(v, r);
    }

    @Test
    void testDateRoundTrip() {
        LocalDate d = LocalDate.of(2025,7,23);
        byte[] b = DataType.DATE.toBytes(d);
        Object r = DataType.DATE.fromBytes(ByteBuffer.wrap(b));
        assertEquals(d, r);
    }

    @Test
    void testTimestampRoundTrip() {
        LocalDateTime ts = LocalDateTime.now().withNano(123456789);
        byte[] b = DataType.TIMESTAMP.toBytes(ts);
        Object r = DataType.TIMESTAMP.fromBytes(ByteBuffer.wrap(b));
        assertEquals(ts, r);
    }

    @Test
    void testBinaryRoundTrip() {
        byte[] data = {0,1,2,3,4,5,6,7,8,9};
        byte[] b = DataType.BYTE.toBytes(data);
        Object r = DataType.BYTE.fromBytes(ByteBuffer.wrap(b));
        assertArrayEquals(data, (byte[]) r);
    }


    // ======================= Offset‑position Tests =======================

    @Test
    void testFromBytesWithOffsetPosition() {
        byte[] buffer = new byte[16];
        int offset = 5;
        // put an int at offset
        ByteBuffer.wrap(buffer).position(offset).putInt(99);
        ByteBuffer bb = ByteBuffer.wrap(buffer);
        bb.position(offset);
        Object r = DataType.INT.fromBytes(bb);
        assertEquals(99, r);
    }

    @Test
    void testStringFromBytesWithOffset() {
        String v = "offset";
        byte[] sBytes = v.getBytes(StandardCharsets.UTF_8);
        byte[] buffer = new byte[20];
        int off = 3;
        ByteBuffer.wrap(buffer).position(off).putShort((short) sBytes.length).put(sBytes);
        ByteBuffer bb = ByteBuffer.wrap(buffer);
        bb.position(off);
        Object r = DataType.CHAR.fromBytes(bb);
        assertEquals(v, r);
    }

    // ======================= Error Conditions =======================

    @Test
    void testFromBytesBufferTooShort() {
        // INT needs 4 bytes
        ByteBuffer shortBuf = ByteBuffer.allocate(3);
        shortBuf.flip();
        assertThrows(IllegalArgumentException.class, () -> DataType.INT.fromBytes(shortBuf));

        // VARCHAR needs at least 2-byte length
        ByteBuffer tiny = ByteBuffer.allocate(1);
        tiny.flip();
        assertThrows(IllegalArgumentException.class, () -> DataType.CHAR.fromBytes(tiny));
    }

    @Test
    void testFromBytesNullBuffer() {
        assertThrows(NullPointerException.class, () -> DataType.INT.fromBytes(null));
    }

    @Test
    void testToBytesInvalid() {
        assertThrows(IllegalArgumentException.class, () -> DataType.INT.toBytes("not int"));
        assertThrows(IllegalArgumentException.class, () -> DataType.CHAR.toBytes(123));
    }
}