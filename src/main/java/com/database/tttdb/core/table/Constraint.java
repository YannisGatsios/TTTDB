package com.database.tttdb.core.table;

public enum Constraint {
    
    PRIMARY_KEY("Unique + Not NULL identifier"),
    UNIQUE("Unique values (NULL allowed)"),
    SECONDARY_KEY("Non unique + Not NULL identifier"),
    NOT_NULL("No NULLs allowed"),
    CHECK("Values must satisfy a condition"),
    AUTO_INCREMENT("Auto-generated sequence number"),
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
            case "INDEX" -> SECONDARY_KEY;
            case "NOT_NULL" -> NOT_NULL;
            case "CHECK" -> CHECK;
            case "AUTO_INCREMENT" -> AUTO_INCREMENT;
            case "NO_CONSTRAINT" -> NO_CONSTRAINT;
            default -> null;
        };
    }

    @Override
    public String toString(){
        switch (this) {
            case PRIMARY_KEY: return "PRIMARY_KEY";
            case UNIQUE: return "UNIQUE";
            case SECONDARY_KEY: return "INDEX";
            case NOT_NULL: return "NOT_NULL";
            case CHECK: return "CHECK";
            case AUTO_INCREMENT: return "AUTO_INCREMENT";
            case NO_CONSTRAINT: return "NO_CONSTRAINT";
            default: return "ERROR";
        }
    }
}
