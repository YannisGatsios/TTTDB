package com.database.db.core.index;

import java.sql.Date;
import java.sql.Timestamp;

import com.database.db.core.table.DataType;
import com.database.db.core.table.Table;

public final class IndexFactory {

    public static IndexInit<?> createIndex(IndexKind kind, Table table, int columnIndex, DataType type) {
        return switch (kind) {
            case PRIMARY -> switch (type) {
                case INT -> new PrimaryKey<Integer>(table, columnIndex);
                case LONG -> new PrimaryKey<Long>(table, columnIndex);
                case FLOAT -> new PrimaryKey<Float>(table, columnIndex);
                case DOUBLE -> new PrimaryKey<Double>(table, columnIndex);
                case CHAR -> new PrimaryKey<String>(table, columnIndex);
                case DATE -> new PrimaryKey<Date>(table, columnIndex);
                case TIMESTAMP -> new PrimaryKey<Timestamp>(table, columnIndex);
                default -> throw new IllegalArgumentException("Unsupported primary key type: " + type.name());
            };
            case UNIQUE -> switch (type) {
                case INT -> new Unique<Integer>(table, columnIndex);
                case LONG -> new Unique<Long>(table, columnIndex);
                case FLOAT -> new Unique<Float>(table, columnIndex);
                case DOUBLE -> new Unique<Double>(table, columnIndex);
                case CHAR -> new Unique<String>(table, columnIndex);
                case DATE -> new Unique<Date>(table, columnIndex);
                case TIMESTAMP -> new Unique<Timestamp>(table, columnIndex);
                default -> throw new IllegalArgumentException("Unsupported unique index type: " + type.name());
            };
            case SECONDARY -> switch (type) {
                case INT -> new SecondaryKey<Integer>(table, columnIndex);
                case LONG -> new SecondaryKey<Long>(table, columnIndex);
                case FLOAT -> new SecondaryKey<Float>(table, columnIndex);
                case DOUBLE -> new SecondaryKey<Double>(table, columnIndex);
                case CHAR -> new SecondaryKey<String>(table, columnIndex);
                case DATE -> new SecondaryKey<Date>(table, columnIndex);
                case TIMESTAMP -> new SecondaryKey<Timestamp>(table, columnIndex);
                default -> throw new IllegalArgumentException("Unsupported type: " + type.name());
            };
        };
    }

    public enum IndexKind { PRIMARY, UNIQUE, SECONDARY }
}