package com.database.db.table;

public enum Constraint {
    
    PRIMARY_KEY("Unique + Not NULL identifier"),
    UNIQUE("Unique values (NULL allowed)"),
    INDEX("Non unique + Not NULL identifier"),
    NOT_NULL("No NULLs allowed"),
    CHECK("Values must satisfy a condition"),
    FOREIGN_KEY("Values must exist in another table"),
    AUTO_INCREMENT("Auto-generated sequence number"),
    ON_UPDATE_DELETE("How to propagate changes on update/delete"),
    DEFERRABLE("When to check constraints (deferred)"),
    NOT_ENFORCED("Constraint declared but not enforced"),
    GENERATED_ALWAYS("Computed column from expression"),
    NO_CONSTRAINT("No constraint was applied from the column");

    private final String description;

    Constraint(String description) {
        this.description = description;
    }

    /**
     * Returns a short description of what the constraint ensures.
     */
    public String description() {
        return description;
    }

    public static Constraint fromString(String constraint){
        return switch (constraint) {
            case "PRIMARY_KEY" -> PRIMARY_KEY;
            case "UNIQUE" -> UNIQUE;
            case "INDEX" -> INDEX;
            case "NOT_NULL" -> NOT_NULL;
            case "CHECK" -> CHECK;
            case "FOREIGN_KEY" -> FOREIGN_KEY;
            case "AUTO_INCREMENT" -> AUTO_INCREMENT;
            case "ON_UPDATE_DELETE" -> ON_UPDATE_DELETE;
            case "DEFERRABLE" -> DEFERRABLE;
            case "NOT_ENFORCED" -> NOT_ENFORCED;
            case "GENERATED_ALWAYS" -> GENERATED_ALWAYS;
            case "NO_CONSTRAINT" -> NO_CONSTRAINT;
            default -> null;
        };
    }

    @Override
    public String toString(){
        switch (this) {
            case PRIMARY_KEY: return "PRIMARY_KEY";
            case UNIQUE: return "UNIQUE";
            case INDEX: return "INDEX";
            case NOT_NULL: return "NOT_NULL";
            case CHECK: return "CHECK";
            case FOREIGN_KEY: return "FOREIGN_KEY";
            case AUTO_INCREMENT: return "AUTO_INCREMENT";
            case ON_UPDATE_DELETE: return "ON_UPDATE_DELETE";
            case DEFERRABLE: return "DEFERRABLE";
            case NOT_ENFORCED: return "NOT_ENFORCED";
            case GENERATED_ALWAYS: return "GENERATED_ALWAYS";
            case NO_CONSTRAINT: return "NO_CONSTRAINT";
            default: return "ERROR";
        }
    }
}
