package com.database.db.api;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.database.db.Database;
import com.database.db.page.Entry;
import com.database.db.table.Constraint;
import com.database.db.table.DataType;
import com.database.db.table.Table;

public class Schema {

    private final List<ColumnInner> columns;
    private final List<Check> checkList;
    private final List<ForeignKey> foreignKeyList;

    public record ColumnInner(String name, DataType type, int size, List<Constraint> constraints, Object defaultValue) {}
    
    public Schema(){
        this.columns = new ArrayList<>();
        this.checkList = new ArrayList<>();
        this.foreignKeyList = new ArrayList<>();
    }
    public Column column(String name){
        return new Column(name, this);
    }
    public Check check(String checkName){
        return new Check(checkName, this);
    }
    public ForeignKey foreignKey(String name){
        return new ForeignKey(name, this);
    }
    public void add(ColumnInner column){
        this.columns.add(column);
    }
    public void add(Check check){
        this.checkList.add(check);
    }
    public void add(ForeignKey foreignKey){
        this.foreignKeyList.add(foreignKey);
    }
    public int getColumnIndex(String columnName) {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).name().equals(columnName)) {
                return i;
            }
        }
        return -1;
    }
    public boolean hasColumn(String name) {
        return getColumnIndex(name) >= 0;
    }
    /**
    * This method is used internally 
    */
    public ColumnInner[] get(Database database) {
        if (columns.isEmpty()) {
            return new ColumnInner[0]; // return empty array instead of null
        }
        this.isValidSchema(database);
        return columns.toArray(new ColumnInner[0]);
    }
    public ColumnInner[] getColumns(){
        return columns.toArray(new ColumnInner[0]);
    }
    /**
    * This method is used internally 
    */
    public List<ForeignKey> getForeignKeys() { return this.foreignKeyList; }
    /**
    * This method is used internally 
    */
    private boolean isValidSchema(Database database){
        Set<String> columnNames = new HashSet<>();
        boolean hasPrimaryKey = false;
        boolean hasAutoIncrement = false;

        for (ColumnInner column : columns) {
            // Unique column names
            if (!columnNames.add(column.name())) {
                throw new IllegalStateException("Duplicate column: " + column.name());
            }

            // Type + size
            if (column.type() == null) {
                throw new IllegalStateException("Column " + column.name() + " has no type.");
            }
            if (column.type().getSize() == -1 && column.size() <= 0) {
                throw new IllegalStateException("Column " + column.name() + " has invalid size.");
            }

            // Constraints
            if (column.constraints().contains(Constraint.PRIMARY_KEY)) {
                if (hasPrimaryKey) {
                    throw new IllegalStateException("Multiple primary keys not allowed.");
                }
                hasPrimaryKey = true;
                if (!column.constraints().contains(Constraint.NOT_NULL)) {
                    throw new IllegalStateException("Primary key column " + column.name() + " must be NOT NULL.");
                }
            }

            if (column.constraints().contains(Constraint.AUTO_INCREMENT)) {
                if (hasAutoIncrement) {
                    throw new IllegalStateException("Only one auto-increment column allowed.");
                }
                hasAutoIncrement = true;
                if (column.type() != DataType.LONG) {
                    throw new IllegalStateException("Auto-increment must be LONG, but got " + column.type());
                }
            }

            // Default value type check
            if (column.defaultValue() != null && !column.type().isValid(column.defaultValue())) {
                throw new IllegalStateException("Default value for " + column.name() + " is not valid for type " + column.type());
            }

            int indexCount = 0;
            if (column.constraints().contains(Constraint.PRIMARY_KEY)) indexCount++;
            if (column.constraints().contains(Constraint.UNIQUE)) indexCount++;
            if (column.constraints().contains(Constraint.INDEX)) indexCount++;

            if (indexCount > 1) {
                throw new IllegalStateException(
                    "Column " + column.name() + " can only be one of PRIMARY KEY, UNIQUE, or INDEX"
                );
            }
        }

        // Check constraints must reference valid columns
        for (Check check : checkList) {
            if (!check.isValid()) {
                throw new IllegalStateException("Check references unknown column for CHECK: " + check.name());
            }
        }

        for (ForeignKey foreignKey : foreignKeyList) {
            foreignKey.isValid(database);
        }

        return true;
    }
    public void isValidEntry(Entry entry, Table table) {
        ColumnInner[] cols = columns.toArray(new ColumnInner[0]);
        Object[] values = entry.getEntry();

        if (values.length != cols.length) {
            throw new IllegalArgumentException(
                "Entry column count (" + values.length + 
                ") does not match schema column count (" + cols.length + ")"
            );
        }

        for (int i = 0; i < cols.length; i++) {
            ColumnInner column = cols[i];
            Object value = values[i];

            // NOT NULL check
            if (column.constraints().contains(Constraint.NOT_NULL) && value == null) {
                throw new IllegalArgumentException("Column " + column.name() + " cannot be NULL.");
            }

            // Type check
            if (value != null && !column.type().isValid(value)) {
                throw new IllegalArgumentException(
                    "Invalid type for column " + column.name() + 
                    ". Expected " + column.type().getJavaClass().getSimpleName() + 
                    " but got " + value.getClass().getSimpleName()
                );
            }

            // Size check (CHAR, BYTE arrays, etc.)
            if (value != null && column.size() > 0) {
                if (value instanceof String s && s.length() > column.size()) {
                    throw new IllegalArgumentException(
                        "Value for column " + column.name() + 
                        " exceeds max size of " + column.size()
                    );
                }
                if (value instanceof byte[] b && b.length > column.size()) {
                    throw new IllegalArgumentException(
                        "Value for column " + column.name() + 
                        " exceeds max size of " + column.size()
                    );
                }
            }

            // UNIQUE check (needs table lookup)
            if ((column.constraints().contains(Constraint.PRIMARY_KEY) || column.constraints().contains(Constraint.UNIQUE))  && value != null) {
                int idx = getColumnIndex(column.name());
                if (table.isKeyFound(value, idx)) {
                    throw new IllegalArgumentException(
                        "Duplicate value for UNIQUE column " + column.name()
                    );
                }
            }
        }

        // Check constraints (WHERE-like conditions)
        for (Check check : checkList) {
            if (!check.isTrue(entry, table.getSchema())) {
                throw new IllegalArgumentException(
                    "Entry violates CHECK constraint on column " + check.name()
                );
            }
        }
    }
}