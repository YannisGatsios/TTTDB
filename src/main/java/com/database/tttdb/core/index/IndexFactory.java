package com.database.tttdb.core.index;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.UUID;

import com.database.tttdb.core.table.DataType;
import com.database.tttdb.core.table.Table;

public final class IndexFactory {

    public static IndexInit<?> createIndex(IndexKind kind, Table table, int columnIndex, DataType type) {
        return switch (kind) {
            case PRIMARY -> switch (type) {
                case SHORT -> new PrimaryKey<Short>(table, columnIndex);
                case INT -> new PrimaryKey<Integer>(table, columnIndex);
                case LONG -> new PrimaryKey<Long>(table, columnIndex);
                case FLOAT -> new PrimaryKey<Float>(table, columnIndex);
                case DOUBLE -> new PrimaryKey<Double>(table, columnIndex);
                case CHAR -> new PrimaryKey<String>(table, columnIndex);
                case DATE -> new PrimaryKey<LocalDate>(table, columnIndex);
                case TIME -> new PrimaryKey<LocalTime>(table, columnIndex);
                case TIMESTAMP -> new PrimaryKey<Timestamp>(table, columnIndex);
                case UUID -> new PrimaryKey<UUID>(table, columnIndex);
                default -> throw new IllegalArgumentException("Unsupported primary key type: " + type.name());
            };
            case UNIQUE -> switch (type) {
                case SHORT -> new Unique<Short>(table, columnIndex);
                case INT -> new Unique<Integer>(table, columnIndex);
                case LONG -> new Unique<Long>(table, columnIndex);
                case FLOAT -> new Unique<Float>(table, columnIndex);
                case DOUBLE -> new Unique<Double>(table, columnIndex);
                case CHAR -> new Unique<String>(table, columnIndex);
                case DATE -> new Unique<LocalDate>(table, columnIndex);
                case TIME -> new Unique<LocalTime>(table, columnIndex);
                case TIMESTAMP -> new Unique<Timestamp>(table, columnIndex);
                case UUID -> new Unique<UUID>(table, columnIndex);
                default -> throw new IllegalArgumentException("Unsupported unique index type: " + type.name());
            };
            case SECONDARY -> switch (type) {
                case SHORT -> new SecondaryKey<Short>(table, columnIndex);
                case INT -> new SecondaryKey<Integer>(table, columnIndex);
                case LONG -> new SecondaryKey<Long>(table, columnIndex);
                case FLOAT -> new SecondaryKey<Float>(table, columnIndex);
                case DOUBLE -> new SecondaryKey<Double>(table, columnIndex);
                case CHAR -> new SecondaryKey<String>(table, columnIndex);
                case DATE -> new SecondaryKey<LocalDate>(table, columnIndex);
                case TIME -> new SecondaryKey<LocalTime>(table, columnIndex);
                case TIMESTAMP -> new SecondaryKey<Timestamp>(table, columnIndex);
                case UUID -> new SecondaryKey<UUID>(table, columnIndex);
                default -> throw new IllegalArgumentException("Unsupported type: " + type.name());
            };
        };
    }

    public enum IndexKind { PRIMARY, UNIQUE, SECONDARY }
}