package com.database.db.api;

import java.util.ArrayList;
import java.util.List;

import com.database.db.Database;
import com.database.db.api.Schema.ColumnInner;
import com.database.db.table.Constraint;

public class ForeignKey {
    public enum ForeignKeyAction {
        RESTRICT,
        CASCADE,
        SET_NULL,
        SET_DEFAULT,
    }
    private final String name;
    private Schema schema;
    private List<String> childColumns = new ArrayList<>();
    private String referenceTable;
    private List<String> referenceColumns = new ArrayList<>();
    private ForeignKeyAction onDelete = ForeignKeyAction.RESTRICT;
    private ForeignKeyAction onUpdate = ForeignKeyAction.RESTRICT;

    public ForeignKey(String name, Schema schema){
        this.name = name;
        this.schema = schema;
    }
    public ForeignKey column(String column){
        this.childColumns.add(column);
        return this;
    }
    public Reference reference(){
        return new Reference(this);
    }
    public ForeignKey onDelete(ForeignKeyAction action) {
        this.onDelete = action;
        return this;
    }
    public ForeignKey onUpdate(ForeignKeyAction action) {
        this.onUpdate = action;
        return this;
    }
    private void add(Reference reference){
        this.referenceColumns = reference.getColumns();
        this.referenceTable = reference.getTable();
    }
    public Schema endForeignKey(){
        this.schema.add(this);
        return this.schema;
    }
    public class Reference {
        private ForeignKey foreignKey;
        private String referenceTable;
        private List<String> parentColumns = new ArrayList<>();
        public Reference(ForeignKey foreignKey){
            this.foreignKey = foreignKey;
        }
        public Reference table(String table){
            this.referenceTable = table;
            return this;
        }
        public Reference column(String column){
            this.parentColumns.add(column);
            return this;
        }
        public ForeignKey end(){
            this.foreignKey.add(this);
            return this.foreignKey;
        }
        private String getTable() { return this.referenceTable; }
        private List<String> getColumns() { return this.parentColumns; }
    }
    public String getName() { return this.name; }
    public List<String> getChildColumns() { return this.childColumns; }
    public String getReferenceTable() { return this.referenceTable; }
    public List<String> getReferenceColumns() { return this.referenceColumns; }
    public ForeignKeyAction getOnDelete() { return onDelete; }
    public ForeignKeyAction getOnUpdate() { return onUpdate; }
    public boolean isValid(Database database){
        if (childColumns.isEmpty()) {
            throw new IllegalStateException("Foreign key '" + name + "' must have at least one child column.");
        }
        if (referenceColumns.isEmpty()) {
            throw new IllegalStateException("Foreign key '" + name + "' must have at least one parent column.");
        }
        if (childColumns.size() != referenceColumns.size()) {
            throw new IllegalStateException(
                "Foreign key '" + name + "' has mismatched column counts: " +
                "child=" + childColumns.size() + ", parent=" + referenceColumns.size()
            );
        }
        // Validate child columns exist
        for (String column : childColumns) {
            if (!schema.hasColumn(column)) {
                throw new IllegalStateException(
                    "Foreign key '" + name + "' references unknown child column '" + column + "'."
                );
            }
        }
        // Validate parent table exists
        if (database.getTable(referenceTable) == null) {
            throw new IllegalStateException(
                "Foreign key '" + name + "' references missing table '" + referenceTable + "'."
            );
        }
        Schema parentSchema = database.getSchema(referenceTable);
        // Validate parent columns exist
        for (String column : referenceColumns) {
            if (!parentSchema.hasColumn(column)) {
                throw new IllegalStateException(
                    "Foreign key '" + name + "' references unknown parent column '" + column +
                    "' in table '" + referenceTable + "'."
                );
            }
        }
        // Validate parent columns are PK or UNIQUE
        ColumnInner[] parentCols = parentSchema.get(database);
        for (String referenceColumn : referenceColumns) {
            int columnIndex = parentSchema.getColumnIndex(referenceColumn);
            ColumnInner column = parentCols[columnIndex];
            if (!(column.constraints().contains(Constraint.PRIMARY_KEY) ||
                column.constraints().contains(Constraint.UNIQUE))) {
                throw new IllegalStateException(
                    "Foreign key '" + name + "' references column '" + column.name() +
                    "' in table '" + referenceTable +
                    "' which is not PRIMARY KEY or UNIQUE."
                );
            }
        }
        // Prevent duplicate FK names
        for (ForeignKey fk : schema.getForeignKeys()) {
            if (!fk.equals(this) && fk.getName().equalsIgnoreCase(this.name)) {
                throw new IllegalStateException("Duplicate foreign key name: '" + name + "'.");
            }
        }
        // Validate type compatibility
        ColumnInner[] childCols = schema.getColumns();
        for (int i = 0; i < childColumns.size(); i++) {
            ColumnInner childCol = childCols[schema.getColumnIndex(childColumns.get(i))];
            ColumnInner parentCol = parentCols[parentSchema.getColumnIndex(referenceColumns.get(i))];
            if (!childCol.type().equals(parentCol.type())) {
                throw new IllegalStateException(
                    "Type mismatch in foreign key '" + name + "': child column '" +
                    childCol.name() + "' (" + childCol.type() + 
                    ") does not match parent column '" + parentCol.name() +
                    "' (" + parentCol.type() + ")."
                );
            }
        }
        return true;
    }
}