package com.database.db.table;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.Arrays;
import java.util.Date;
import java.util.Objects;

public enum Type {
    INT,        // 4-byte integer
    FLOAT,      // 4-byte floating point
    DOUBLE,     // 8-byte floating point
    STRING,     // Variable-length string
    BOOLEAN,    // 1-byte boolean
    LONG,       // 8-byte integer
    DATE,       // 8-byte date (millis since epoch)
    TIMESTAMP,  // 16-byte timestamp (nanosecond precision)
    BINARY;     // Binary data

    // UTC formatters for consistent date/time handling
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd")
            .withZone(ZoneOffset.UTC);

    private static final DateTimeFormatter TIMESTAMP_FORMATTER = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd HH:mm:ss")
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
            .toFormatter()
            .withZone(ZoneOffset.UTC);

    /**
     * Returns the fixed size in bytes for fixed-size types,
     * or -1 for variable-length types.
     */
    public int getFixedSize() {
        return switch (this) {
            case INT -> 4;
            case FLOAT -> 4;
            case DOUBLE -> 8;
            case BOOLEAN -> 1;
            case LONG -> 8;
            case DATE -> 8;
            case TIMESTAMP -> 16;
            case STRING, BINARY -> -1;  // Variable size
        };
    }
    /**
     * Converts a string representation to the corresponding DataType enum.
     * 
     * @param typeName The string representation of the data type (case-insensitive)
     * @return The matching DataType enum
     * @throws IllegalArgumentException if no matching type is found
     */
    public static Type fromString(String typeName) {
        if (typeName == null || typeName.isBlank()) {
            throw new IllegalArgumentException("Type name cannot be null or blank");
        }
        // Normalize input: trim and convert to uppercase
        String normalized = typeName.trim().toUpperCase();
        // Handle common aliases
        switch (normalized) {
            case "INTEGER":
            case "INT4":
            case "SIGNED INTEGER":
                return INT;
            case "CHAR":
            case "VARCHAR":
            case "TEXT":
                return STRING;
            case "BOOL":
                return BOOLEAN;
            case "BIGINT":
            case "INT8":
                return LONG;
            case "DATETIME":
                return DATE;
            case "BLOB":
            case "BYTE":
                return BINARY;
        }
        // Try exact enum name match
        try {
            return Type.valueOf(normalized);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unknown data type: " + typeName, e);
        }
    }

    /**
     * Validates a value against this data type.
     * @param value The value to validate
     * @param size The declared size for variable-length types (ignored for fixed types)
     * @throws IllegalArgumentException if validation fails
     */
    public void validateValue(Object value, int size) {
        switch (this) {
            case INT:
                if (!(value instanceof Integer)) 
                    throw new IllegalArgumentException("Expected Integer");
                break;
            case FLOAT:
                if (!(value instanceof Float)) 
                    throw new IllegalArgumentException("Expected Float");
                break;
            case DOUBLE:
                if (!(value instanceof Double)) 
                    throw new IllegalArgumentException("Expected Double");
                break;
            case STRING:
                if (!(value instanceof String))
                    throw new IllegalArgumentException("Expected String");
                if (size >= 0 && ((String) value).length() > size)
                    throw new IllegalArgumentException("String exceeds max length of " + size);
                break;
            case BOOLEAN:
                if (!(value instanceof Boolean))
                    throw new IllegalArgumentException("Expected Boolean");
                break;
            case LONG:
                if (!(value instanceof Long))
                    throw new IllegalArgumentException("Expected Long");
                break;
            case DATE:
                if (!(value instanceof java.util.Date))
                    throw new IllegalArgumentException("Expected Date");
                break;
            case TIMESTAMP:
                if (!(value instanceof java.sql.Timestamp))
                    throw new IllegalArgumentException("Expected Timestamp");
                break;
            case BINARY:
                if (!(value instanceof byte[]))
                    throw new IllegalArgumentException("Expected byte array");
                byte[] data = (byte[]) value;
                if (size == 0 && data.length > 0) {
                    throw new IllegalArgumentException("Non-empty binary not allowed when size is 0");
                }
                if (size > 0 && data.length > size) {
                    throw new IllegalArgumentException("Binary data exceeds max size of " + size);
                }
                break;
        }
    }

    /**
     * Converts a string representation to the appropriate type.
     * Useful for parsing input data.
     */
    public Object parseValue(String s) {
        if (s == null || s.equalsIgnoreCase("null")) return null;
        
        return switch (this) {
            case INT -> Integer.parseInt(s);
            case FLOAT -> Float.parseFloat(s);
            case DOUBLE -> Double.parseDouble(s);
            case STRING -> s;
            case BOOLEAN -> Boolean.parseBoolean(s);
            case LONG -> Long.parseLong(s);
            case DATE -> {
                try {
                    // First try ISO format parsing
                    Instant instant = Instant.parse(s);
                    yield Date.from(instant);
                } catch (DateTimeParseException e) {
                    try {
                        // Fall back to date-only format
                        LocalDate date = LocalDate.parse(s, DATE_FORMATTER);
                        yield Date.from(date.atStartOfDay(ZoneOffset.UTC).toInstant());
                    } catch (DateTimeParseException e2) {
                        throw new IllegalArgumentException("Invalid date format: " + s);
                    }
                }
            }
            case TIMESTAMP -> {
                try {
                    // First try ISO format parsing
                    Instant instant = Instant.parse(s);
                    yield Timestamp.from(instant);
                } catch (DateTimeParseException e) {
                    try {
                        // Fall back to custom format with UTC timezone
                        LocalDateTime dateTime = LocalDateTime.parse(s, TIMESTAMP_FORMATTER);
                        // Convert to UTC instant
                        Instant instant = dateTime.atZone(ZoneOffset.UTC).toInstant();
                        yield Timestamp.from(instant);
                    } catch (DateTimeParseException e2) {
                        throw new IllegalArgumentException("Invalid timestamp format: " + s);
                    }
                }
            }
            case BINARY -> s.getBytes(); // Simplified for demo
        };
    }
    /**
     * Represents the result of deserialization: the deserialized value and 
     * the next position in the buffer after reading the value.
     *
     * @param valueObject The deserialized Java object
     * @param nextIndex The next read position in the buffer
     */
    public record DeserializationResult(Object valueObject, int nextIndex) {};
    /**
     * Deserializes a value from a byte buffer starting at the specified position.
     * 
     * @param bufferData The byte buffer containing serialized data
     * @param startIndex The starting position in the buffer
     * @return DeserializationResult containing the value and next read position
     * @throws IllegalArgumentException If the type is unsupported or data is invalid
     */
    public DeserializationResult fromBytes(byte[] bufferData, int startIndex) {
        Objects.requireNonNull(bufferData, "Buffer cannot be null");
        if (startIndex < 0 || startIndex >= bufferData.length) {
            throw new IllegalArgumentException("Invalid start index: " + startIndex);
        }

        Object object;
        int nextIndex = startIndex;
         try {
            switch (this) {
                case STRING:
                    // Read length (2 bytes)
                    checkBuffer(bufferData, nextIndex, Short.BYTES);
                    short size = ByteBuffer.wrap(bufferData, nextIndex, Short.BYTES).getShort();
                    nextIndex += Short.BYTES;
                    
                    // Read string data
                    checkBuffer(bufferData, nextIndex, size);
                    object = new String(bufferData, nextIndex, size, StandardCharsets.UTF_8);
                    nextIndex += size;
                    break;
                    
                case BINARY:
                    // Read length (2 bytes)
                    checkBuffer(bufferData, nextIndex, Short.BYTES);
                    short binSize = ByteBuffer.wrap(bufferData, nextIndex, Short.BYTES).getShort();
                    nextIndex += Short.BYTES;
                    // Read binary data
                    checkBuffer(bufferData, nextIndex, binSize);
                    object = Arrays.copyOfRange(bufferData, nextIndex, nextIndex + binSize);
                    nextIndex += binSize;
                    break;
                case INT:
                    checkBuffer(bufferData, nextIndex, Integer.BYTES);
                    object = ByteBuffer.wrap(bufferData, nextIndex, Integer.BYTES).getInt();
                    nextIndex += Integer.BYTES;
                    break;
                case LONG:
                    checkBuffer(bufferData, nextIndex, Long.BYTES);
                    object = ByteBuffer.wrap(bufferData, nextIndex, Long.BYTES).getLong();
                    nextIndex += Long.BYTES;
                    break;
                case FLOAT:
                    checkBuffer(bufferData, nextIndex, Float.BYTES);
                    object = ByteBuffer.wrap(bufferData, nextIndex, Float.BYTES).getFloat();
                    nextIndex += Float.BYTES;
                    break;
                case DOUBLE:
                    checkBuffer(bufferData, nextIndex, Double.BYTES);
                    object = ByteBuffer.wrap(bufferData, nextIndex, Double.BYTES).getDouble();
                    nextIndex += Double.BYTES;
                    break;
                case BOOLEAN:
                    checkBuffer(bufferData, nextIndex, 1);
                    object = bufferData[nextIndex] != 0;
                    nextIndex += 1;
                    break;
                case DATE:
                    checkBuffer(bufferData, nextIndex, Long.BYTES);
                    long millis = ByteBuffer.wrap(bufferData, nextIndex, Long.BYTES).getLong();
                    object = new Date(millis);
                    nextIndex += Long.BYTES;
                    break;
                case TIMESTAMP:
                    // Read milliseconds (long) + nanoseconds (int)
                    checkBuffer(bufferData, nextIndex, Long.BYTES + Integer.BYTES);
                    long baseMillis = ByteBuffer.wrap(bufferData, nextIndex, Long.BYTES).getLong();
                    nextIndex += Long.BYTES;
                    int nanos = ByteBuffer.wrap(bufferData, nextIndex, Integer.BYTES).getInt();
                    Timestamp ts = new Timestamp(baseMillis);
                    ts.setNanos(nanos);
                    object = ts;
                    nextIndex += Integer.BYTES;
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported type: " + this);
            }
        } catch (IndexOutOfBoundsException e) {
            throw new IllegalArgumentException("Buffer too short for type: " + this, e);
        }
        return new DeserializationResult(object, nextIndex);
    }
    // Helper to check buffer length
    private void checkBuffer(byte[] buffer, int start, int length) {
        if (start + length > buffer.length) {
            throw new IllegalArgumentException(
                    String.format("Buffer too short. Required: %d, Available: %d",
                            length, buffer.length - start));
        }
    }
    /**
     * Serializes a Java object to its byte representation according to this type.
     *
     * @param value The Java object to serialize
     * @return Byte array representation of the value
     * @throws IllegalArgumentException If value type is incompatible or unsupported
     */
    public byte[] toBytes(Object value) {
        if (value == null) return new byte[0];
        validateValue(value, -1);
        switch (this) {
            case INT:
                return ByteBuffer.allocate(4)
                        .putInt((Integer) value)
                        .array();
            case LONG:
                return ByteBuffer.allocate(8)
                        .putLong((Long) value)
                        .array();
            case FLOAT:
                return ByteBuffer.allocate(4)
                        .putFloat((Float) value)
                        .array();
            case DOUBLE:
                return ByteBuffer.allocate(8)
                        .putDouble((Double) value)
                        .array();
            case BOOLEAN:
                return new byte[] { (byte) (((Boolean) value) ? 1 : 0) };
            case DATE:
                long epoch = ((Date) value).getTime();
                return ByteBuffer.allocate(8)
                        .putLong(epoch)
                        .array();
            case TIMESTAMP:
                Timestamp ts = (Timestamp) value;
                long milliseconds = ts.getTime();
                int nanos = ts.getNanos();
                // Adjust for nanos already included in milliseconds
                long baseMillis = milliseconds - (nanos / 1_000_000);
                ByteBuffer tsBuffer = ByteBuffer.allocate(12);
                tsBuffer.putLong(baseMillis);
                tsBuffer.putInt(nanos);
                return tsBuffer.array();
            case STRING:
                byte[] strBytes = ((String) value).getBytes(StandardCharsets.UTF_8);
                ByteBuffer strBuffer = ByteBuffer.allocate(2 + strBytes.length);
                strBuffer.putShort((short) strBytes.length);
                strBuffer.put(strBytes);
                return strBuffer.array();
            case BINARY:
                byte[] binData = (byte[]) value;
                ByteBuffer binBuffer = ByteBuffer.allocate(2 + binData.length);
                binBuffer.putShort((short) binData.length);
                binBuffer.put(binData);
                return binBuffer.array();
            default:
                throw new IllegalArgumentException("Unsupported type for serialization: " + this);
        }
    }

    /**
     * Converts a value to its string representation.
     * Useful for serialization.
     */
    public String toString(Object value) {
        if (value == null)
            return "null";

        return switch (this) {
            case DATE ->
                DATE_FORMATTER.format(((Date) value).toInstant());

            case TIMESTAMP -> {
                Timestamp ts = (Timestamp) value;
                yield TIMESTAMP_FORMATTER.format(ts.toInstant());
            }

            case BINARY ->
                new String((byte[]) value, StandardCharsets.UTF_8);

            default ->
                value.toString();
        };
    }
}