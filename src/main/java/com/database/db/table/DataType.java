package com.database.db.table;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.Objects;
import java.util.UUID;


public enum DataType {
    INT(Integer.class, false),        // 4-byte integer
    SHORT(Short.class, false),        // 2-byte short
    FLOAT(Float.class, false),      // 4-byte floating point
    DOUBLE(Double.class, false),     // 8-byte floating point
    CHAR(String.class, false),     // Fixed-length string
    VARCHAR(String.class, true),     // Variable-length string
    BOOLEAN(Boolean.class, false),    // 1-byte boolean
    LONG(Long.class, false),       // 8-byte integer
    DATE(LocalDate.class, false),       // Date without time
    TIME(LocalTime.class, false),      // Time without date
    TIMESTAMP(LocalDateTime.class, false),  // Date and time without timezone
    TIMESTAMP_WITH_TIME_ZONE(ZonedDateTime.class, false), // Date and time with timezone
    INTERVAL(Duration.class, false),    // Time interval
    UUID(UUID.class, false),
    BYTE(byte[].class, false),     // Binary data
    VARBYTE(byte[].class, true);     // Binary data

    private final Class<?> javaClass;
    private final boolean isVariable;

    DataType(Class<?> javaClass, boolean isVariable) {
        this.javaClass = javaClass;
        this.isVariable = isVariable;
    }

    public Class<?> getJavaClass() {
        return javaClass;
    }

    public boolean isVariable(){
        return this.isVariable;
    }

    // UTC formatters for consistent date/time handling
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE;
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ISO_LOCAL_TIME;
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE_TIME;
    private static final DateTimeFormatter TIMESTAMP_WITH_TZ_FORMATTER = DateTimeFormatter.ISO_ZONED_DATE_TIME;

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
            case TIME -> 8;
            case TIMESTAMP -> 16;
            case INTERVAL -> 16;
            case UUID -> 16;
            case CHAR, VARCHAR, BYTE, VARBYTE, TIMESTAMP_WITH_TIME_ZONE -> -1;  // Variable size
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
            case "INT":
                return INT;
            case "SHORT":
                return SHORT;
            case "FLOAT":
                return FLOAT;
            case "DOUBLE":
                return DOUBLE;
            case "BOOL":
                return BOOLEAN;
            case "LONG":
                return LONG;
            case "CHAR":
                return CHAR;
            case "VARCHAR":
                return VARCHAR;
            case "DATETIME":
                return TIMESTAMP;
            case "BYTE":
                return BYTE;
            case "VARBYTE":
                return VARBYTE;
            case "TIME":
                return TIME;
            case "DATE":
                return DATE;
            case "TIMESTAMPTZ":
                return TIMESTAMP_WITH_TIME_ZONE;
            case "DURATION":
                return INTERVAL;
            case "UUID":
                return UUID;
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
        if (value == null) {
            throw new NullPointerException("Value cannot be null");
        }
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
            case CHAR:
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
                if (!(value instanceof LocalDate))
                    throw new IllegalArgumentException("Expected LocalDate");
                break;
            case TIME:
                if (!(value instanceof LocalTime))
                    throw new IllegalArgumentException("Expected LocalTime");
                break;
            case TIMESTAMP:
                if (!(value instanceof LocalDateTime))
                    throw new IllegalArgumentException("Expected LocalDateTime");
                break;
            case TIMESTAMP_WITH_TIME_ZONE:
                if (!(value instanceof ZonedDateTime))
                    throw new IllegalArgumentException("Expected ZonedDateTime");
                break;
            case INTERVAL:
                if (!(value instanceof Duration))
                    throw new IllegalArgumentException("Expected Duration");
                break;
            case UUID:
                if (!(value instanceof java.util.UUID))
                    throw new IllegalArgumentException("Expected UUID");
                break;
            case VARBYTE:
            case BYTE:
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
            case CHAR,VARCHAR -> s;
            case BOOLEAN -> {
                if (!s.equalsIgnoreCase("true") && !s.equalsIgnoreCase("false")) {
                    throw new IllegalArgumentException("Invalid boolean value: " + s);
                }
                yield Boolean.parseBoolean(s);
            }
            case LONG -> Long.parseLong(s);
            case DATE -> LocalDate.parse(s, DATE_FORMATTER);
            case TIME -> LocalTime.parse(s, TIME_FORMATTER);
            case TIMESTAMP -> LocalDateTime.parse(s, TIMESTAMP_FORMATTER);
            case TIMESTAMP_WITH_TIME_ZONE -> ZonedDateTime.parse(s, TIMESTAMP_WITH_TZ_FORMATTER);
            case INTERVAL -> Duration.parse(s);
            case UUID -> java.util.UUID.fromString(s);
            case BYTE,VARBYTE -> Base64.getDecoder().decode(s);
        };
    }
    /**
     * Deserializes a value from a byte buffer starting at the specified position.
     * 
     * @param bufferData The byte buffer containing serialized data
     * @param startIndex The starting position in the buffer
     * @return Object containing the value
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
                case DATE: {
                    long epochDay = buffer.getLong();
                    return LocalDate.ofEpochDay(epochDay);
                }
                case TIME: {
                    long nanosOfDay = buffer.getLong();
                    return LocalTime.ofNanoOfDay(nanosOfDay);
                }
                case TIMESTAMP: {
                    long epochDay = buffer.getLong();
                    long nanosOfDay = buffer.getLong();
                    LocalDate date = LocalDate.ofEpochDay(epochDay);
                    LocalTime time = LocalTime.ofNanoOfDay(nanosOfDay);
                    return LocalDateTime.of(date, time);
                }
                case TIMESTAMP_WITH_TIME_ZONE: {
                    long epochSecond = buffer.getLong();
                    int nano = buffer.getInt();
                    short zoneIdLength = buffer.getShort();
                    byte[] zoneBytes = new byte[zoneIdLength];
                    buffer.get(zoneBytes);
                    String zoneId = new String(zoneBytes, StandardCharsets.UTF_8);
                    Instant instant = Instant.ofEpochSecond(epochSecond, nano);
                    return ZonedDateTime.ofInstant(instant, ZoneId.of(zoneId));
                }
                case INTERVAL: {
                    long seconds = buffer.getLong();
                    int nanos = buffer.getInt();
                    buffer.getInt(); // Skip padding
                    return Duration.ofSeconds(seconds, nanos);
                }
                case UUID: {
                    long msb = buffer.getLong();
                    long lsb = buffer.getLong();
                    return new UUID(msb, lsb);
                }
                case VARCHAR:
                case CHAR: {
                    short len = buffer.getShort();
                    if (buffer.remaining() < len)
                        throw new IllegalArgumentException("Buffer underflow: expected " + len + " bytes for VARCHAR");
                    byte[] bytes = new byte[len];
                    buffer.get(bytes);
                    return new String(bytes, StandardCharsets.UTF_8);
                }
                case VARBYTE:
                case BYTE: {
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
            case DATE: {
                LocalDate date = (LocalDate) value;
                return ByteBuffer.allocate(8)
                        .putLong(date.toEpochDay())
                        .array();
            }
            case TIME: {
                LocalTime time = (LocalTime) value;
                return ByteBuffer.allocate(8)
                        .putLong(time.toNanoOfDay())
                        .array();
            }
            case TIMESTAMP: {
                LocalDateTime ldt = (LocalDateTime) value;
                ByteBuffer buffer = ByteBuffer.allocate(16);
                buffer.putLong(ldt.toLocalDate().toEpochDay());
                buffer.putLong(ldt.toLocalTime().toNanoOfDay());
                return buffer.array();
            }
            case TIMESTAMP_WITH_TIME_ZONE: {
                ZonedDateTime zdt = (ZonedDateTime) value;
                Instant instant = zdt.toInstant();
                String zoneId = zdt.getZone().getId();
                byte[] zoneBytes = zoneId.getBytes(StandardCharsets.UTF_8);
                if (zoneBytes.length > 64) {
                    throw new IllegalArgumentException("Zone ID too long: " + zoneId);
                }
                ByteBuffer buffer = ByteBuffer.allocate(14 + zoneBytes.length);
                buffer.putLong(instant.getEpochSecond());
                buffer.putInt(instant.getNano());
                buffer.putShort((short) zoneBytes.length);
                buffer.put(zoneBytes);
                return buffer.array();
            }
            case INTERVAL: {
                Duration duration = (Duration) value;
                ByteBuffer buffer = ByteBuffer.allocate(16);
                buffer.putLong(duration.getSeconds());
                buffer.putInt(duration.getNano());
                buffer.putInt(0); // Padding
                return buffer.array();
            }
            case UUID: {
                UUID uuid = (UUID) value;
                ByteBuffer uuidBuffer = ByteBuffer.allocate(16);
                uuidBuffer.putLong(uuid.getMostSignificantBits());
                uuidBuffer.putLong(uuid.getLeastSignificantBits());
                return uuidBuffer.array();
            }
            case VARCHAR:
            case CHAR: {
                byte[] strBytes = ((String) value).getBytes(StandardCharsets.UTF_8);
                ByteBuffer strBuffer = ByteBuffer.allocate(2 + strBytes.length);
                strBuffer.putShort((short) strBytes.length);
                strBuffer.put(strBytes);
                return strBuffer.array();
            }
            case VARBYTE:
            case BYTE: {
                byte[] binData = (byte[]) value;
                ByteBuffer binBuffer = ByteBuffer.allocate(2 + binData.length);
                binBuffer.putShort((short) binData.length);
                binBuffer.put(binData);
                return binBuffer.array();
            }
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
            case DATE -> DATE_FORMATTER.format((LocalDate) value);
            case TIME -> TIME_FORMATTER.format((LocalTime) value);
            case TIMESTAMP -> TIMESTAMP_FORMATTER.format((LocalDateTime) value);
            case TIMESTAMP_WITH_TIME_ZONE -> TIMESTAMP_WITH_TZ_FORMATTER.format((ZonedDateTime) value);
            case INTERVAL -> value.toString();
            case BYTE,VARBYTE -> Base64.getEncoder().encodeToString((byte[]) value);
            default -> value.toString();
        };
    }

    public static DataType detectFixedSizeType(Object value) {
        if (value instanceof Integer) return DataType.INT;
        if (value instanceof Short) return DataType.SHORT;
        if (value instanceof Float) return DataType.FLOAT;
        if (value instanceof Double) return DataType.DOUBLE;
        if (value instanceof Boolean) return DataType.BOOLEAN;
        if (value instanceof Long) return DataType.LONG;
        if (value instanceof LocalDate) return DataType.DATE;
        if (value instanceof LocalTime) return DataType.TIME;
        if (value instanceof LocalDateTime) return DataType.TIMESTAMP;
        if (value instanceof ZonedDateTime) return DataType.TIMESTAMP_WITH_TIME_ZONE;
        if (value instanceof Duration) return DataType.INTERVAL;
        if (value instanceof String) return DataType.CHAR;
        if (value instanceof java.util.UUID) return DataType.UUID;
        if (value instanceof byte[]) return DataType.BYTE;

        throw new IllegalArgumentException("Unsupported Fixed size type: " + value.getClass());
    }

    public static DataType detectVariableSizeType(Object value){
        if (value instanceof String) return DataType.VARCHAR;
        if (value instanceof byte[]) return DataType.VARBYTE;

        throw new IllegalArgumentException("Unsupported Variable size type: " + value.getClass());
    }
}