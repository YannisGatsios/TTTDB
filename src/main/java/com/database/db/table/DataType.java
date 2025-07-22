package com.database.db.table;

import java.nio.BufferUnderflowException;
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
import java.util.Date;
import java.util.Objects;
import java.util.UUID;//TODO

public enum DataType {
    INT(Integer.class),        // 4-byte integer
    SHORT(Short.class),        // 2-byte short
    FLOAT(Float.class),      // 4-byte floating point
    DOUBLE(Double.class),     // 8-byte floating point
    VARCHAR(String.class),     // Variable-length string
    BOOLEAN(Boolean.class),    // 1-byte boolean
    LONG(Long.class),       // 8-byte integer
    DATE(Date.class),       // 8-byte date (millis since epoch)
    TIMESTAMP(Timestamp.class),  // 16-byte timestamp (nanosecond precision)
    UUID(UUID.class),
    BINARY(Byte[].class);     // Binary data

    private final Class<?> javaClass;

    DataType(Class<?> javaClass) {
        this.javaClass = javaClass;
    }

    public Class<?> getJavaClass() {
        return javaClass;
    }

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
    public int getSize() {
        return switch (this) {
            case INT -> 4;
            case SHORT -> 2;
            case FLOAT -> 4;
            case DOUBLE -> 8;
            case BOOLEAN -> 1;
            case LONG -> 8;
            case DATE -> 8;
            case TIMESTAMP -> 16;
            case UUID -> 16;
            case VARCHAR, BINARY -> -1;  // Variable size
        };
    }
    /**
     * Converts a string representation to the corresponding DataType enum.
     * 
     * @param typeName The string representation of the data type (case-insensitive)
     * @return The matching DataType enum
     * @throws IllegalArgumentException if no matching type is found
     */
    public static DataType fromString(String typeName) {
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
            case "SHOER":
                return SHORT;
            case "CHAR":
            case "VARCHAR":
            case "TEXT":
                return VARCHAR;
            case "BOOL":
                return BOOLEAN;
            case "BIGINT":
            case "INT8":
                return LONG;
            case "DATETIME":
                return DATE;
            case "UUID":
                return UUID;
            case "BLOB":
            case "BYTE":
                return BINARY;
        }
        // Try exact enum name match
        try {
            return DataType.valueOf(normalized);
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
            case SHORT:
                if (!(value instanceof Short)) 
                    throw new IllegalArgumentException("Expected Short");
                break;
            case FLOAT:
                if (!(value instanceof Float)) 
                    throw new IllegalArgumentException("Expected Float");
                break;
            case DOUBLE:
                if (!(value instanceof Double)) 
                    throw new IllegalArgumentException("Expected Double");
                break;
            case VARCHAR:
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
            case UUID:
                if (!(value instanceof java.util.UUID))
                    throw new IllegalArgumentException("Expected UUID");
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
        if (s == null || s.equalsIgnoreCase("NULL")) return null;
        
        return switch (this) {
            case INT -> Integer.parseInt(s);
            case SHORT -> Short.parseShort(s);
            case FLOAT -> Float.parseFloat(s);
            case DOUBLE -> Double.parseDouble(s);
            case VARCHAR -> s;
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
            case UUID -> java.util.UUID.fromString(s);
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
    public Object fromBytes(ByteBuffer buffer) {
        Objects.requireNonNull(buffer, "Buffer cannot be null");
        try {
            switch (this) {
                case INT:
                    return buffer.getInt();
                case SHORT:
                    return buffer.getShort();
                case LONG:
                    return buffer.getLong();
                case FLOAT:
                    return buffer.getFloat();
                case DOUBLE:
                    return buffer.getDouble();
                case BOOLEAN:
                    return buffer.get() != 0;
                case DATE:
                    return new Date(buffer.getLong());
                case TIMESTAMP: {
                    long millis = buffer.getLong();
                    int nanos = buffer.getInt();
                    Timestamp ts = new Timestamp(millis);
                    ts.setNanos(nanos);
                    return ts;
                }
                case UUID: {
                    long msb = buffer.getLong();
                    long lsb = buffer.getLong();
                    return new UUID(msb, lsb);
                }
                case VARCHAR: {
                    short len = buffer.getShort();
                    if (buffer.remaining() < len)
                        throw new IllegalArgumentException("Buffer underflow: expected " + len + " bytes for VARCHAR");
                    byte[] bytes = new byte[len];
                    buffer.get(bytes);
                    return new String(bytes, StandardCharsets.UTF_8);
                }
                case BINARY: {
                    short len = buffer.getShort();
                    if (buffer.remaining() < len)
                        throw new IllegalArgumentException("Buffer underflow: expected " + len + " bytes for BINARY");
                    byte[] bytes = new byte[len];
                    buffer.get(bytes);
                    return bytes;
                }
                default:
                    throw new IllegalArgumentException("Unsupported type: " + this);
            }
        } catch (BufferUnderflowException | IndexOutOfBoundsException e) {
            throw new IllegalArgumentException("Buffer too short for type: " + this, e);
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
            case SHORT:
                return ByteBuffer.allocate(2)
                        .putShort((Short) value)
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
            case UUID:
                UUID uuid = (UUID) value;
                ByteBuffer uuidBuffer = ByteBuffer.allocate(16);
                uuidBuffer.putLong(uuid.getMostSignificantBits());
                uuidBuffer.putLong(uuid.getLeastSignificantBits());
                return uuidBuffer.array();
            case VARCHAR:
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

    public static DataType detect(Object value) {
        if (value instanceof Integer) return DataType.INT;
        if (value instanceof Short) return DataType.SHORT;
        if (value instanceof Float) return DataType.FLOAT;
        if (value instanceof Double) return DataType.DOUBLE;
        if (value instanceof Boolean) return DataType.BOOLEAN;
        if (value instanceof Long) return DataType.LONG;
        if (value instanceof java.util.Date) return DataType.DATE;
        if (value instanceof java.sql.Timestamp) return DataType.TIMESTAMP;
        if (value instanceof String) return DataType.VARCHAR;
        if (value instanceof java.util.UUID) return DataType.UUID;
        if (value instanceof byte[]) return DataType.BINARY;

        throw new IllegalArgumentException("Unsupported type: " + value.getClass());
    }
}