package com.database.db.api;

public class DatabaseException extends RuntimeException {
    public DatabaseException(String message) {
        super(message);
    }

    public DatabaseException(String message, Throwable cause) {
        super(message, cause);
    }
    public static class SchemaException extends DatabaseException {
        public SchemaException(String msg) { super(msg); }
    }

    public static class EntryValidationException extends DatabaseException {
        public EntryValidationException(String msg) { super(msg); }
    }

    public static class CheckConstraintException extends DatabaseException {
        private final String checkName;
        public CheckConstraintException(String checkName, String message) {
            super(message);
            this.checkName = checkName;
        }
        public String getCheckName() {
            return checkName;
        }
    }
    public static class ForeignKeyException extends DatabaseException {
        private final String foreignKeyName;
        public ForeignKeyException(String name, String msg) { super(msg); this.foreignKeyName = name; }
        public ForeignKeyException(String name, String msg, Throwable cause) { super(msg, cause); this.foreignKeyName = name; }
        public String getForeignKeyName() { return foreignKeyName; }
    }
}