package com.database.tttdb.api;

import java.util.ArrayList;
import java.util.List;

import com.database.tttdb.api.DatabaseException.ForeignKeyException;
import com.database.tttdb.api.Schema.ColumnInner;
import com.database.tttdb.core.Database;
import com.database.tttdb.core.table.Constraint;


/**
 * Represents a foreign key constraint in a table schema.
 * 
 * <p>Foreign keys link one or more columns in this table (child columns) to
 * columns in another table (parent/referenced columns), enforcing referential
 * integrity. You can specify actions to take when referenced rows are deleted
 * or updated, such as CASCADE, SET NULL, SET DEFAULT, or RESTRICT.</p>
 * 
 * <p>Example usage:</p>
 * <pre>
 * Schema postSchema = new Schema()
 *     .column("username").type(DataType.CHAR).size(15).endColumn()
 *     .foreignKey("fk_user_cascade")
 *         .column("username")
 *         .reference().table("users").column("username").end()
 *         .onDelete(ForeignKey.ForeignKeyAction.CASCADE)
 *         .onUpdate(ForeignKey.ForeignKeyAction.RESTRICT)
 *     .endForeignKey();
 * </pre>
 * 
 * <p>This corresponds to the SQL equivalent:</p>
 * <pre>
 * FOREIGN KEY (username) REFERENCES users(username)
 * ON DELETE CASCADE
 * ON UPDATE RESTRICT
 * </pre>
 */
public class ForeignKey {
    /**
     * Defines actions that can be taken on a foreign key when the referenced
     * row is deleted or updated.
     */
    public enum ForeignKeyAction {
        /** Prevent deletion/update if child rows exist */
        RESTRICT,
        /** Delete/update child rows automatically */
        CASCADE,
        /** Set child columns to NULL on deletion/update of parent */
        SET_NULL,
        /** Set child columns to their default values on deletion/update of parent */
        SET_DEFAULT,
    }
    private final String name;
    private final Schema schema;
    private final List<String> childColumns = new ArrayList<>();
    private String referenceTable;
    private List<String> referenceColumns = new ArrayList<>();
    private ForeignKeyAction onDelete = ForeignKeyAction.RESTRICT;
    private ForeignKeyAction onUpdate = ForeignKeyAction.RESTRICT;

    /**
     * Creates a new foreign key for the given schema with the specified name.
     * 
     * @param name the foreign key name
     * @param schema the schema this foreign key belongs to
     */
    public ForeignKey(String name, Schema schema){
        this.name = name;
        this.schema = schema;
    }
    /**
     * Adds a child column (column in the current table) to the foreign key.
     * 
     * @param column the child column name
     * @return the current {@code ForeignKey} instance
     */
    public ForeignKey column(String column){
        this.childColumns.add(column);
        return this;
    }
    /**
     * Starts defining the referenced table and columns (parent table/columns).
     * 
     * @return a {@link Reference} builder object to configure the parent table and columns
     */
    public Reference reference(){
        return new Reference(this);
    }
    /**
     * Sets the action to perform on DELETE of a referenced row.
     * 
     * @param action the {@link ForeignKeyAction} to perform
     * @return the current {@code ForeignKey} instance
     */
    public ForeignKey onDelete(ForeignKeyAction action) {
        this.onDelete = action;
        return this;
    }
    /**
     * Sets the action to perform on UPDATE of a referenced row.
     * 
     * @param action the {@link ForeignKeyAction} to perform
     * @return the current {@code ForeignKey} instance
     */
    public ForeignKey onUpdate(ForeignKeyAction action) {
        this.onUpdate = action;
        return this;
    }
    private void add(Reference reference){
        this.referenceColumns = reference.getColumns();
        this.referenceTable = reference.getTable();
    }
    /**
     * Finalizes the foreign key and adds it to the parent schema.
     * 
     * @return the parent {@link Schema} instance
     */
    public Schema endForeignKey(){
        this.schema.add(this);
        return this.schema;
    }
    /**
     * Builder class to define the referenced table and columns for a foreign key.
     */
    public static class Reference {
        private final ForeignKey foreignKey;
        private String referenceTable;
        private final List<String> parentColumns = new ArrayList<>();
        public Reference(ForeignKey foreignKey){
            this.foreignKey = foreignKey;
        }
        /**
         * Sets the referenced table for this foreign key.
         * 
         * @param table the referenced table name
         * @return the current {@link Reference} instance
         */
        public Reference table(String table){
            this.referenceTable = table;
            return this;
        }
        /**
         * Adds a referenced column from the parent table.
         * 
         * @param column the referenced column name
         * @return the current {@link Reference} instance
         */
        public Reference column(String column){
            this.parentColumns.add(column);
            return this;
        }
        /**
         * Finalizes the reference definition and updates the foreign key object.
         * 
         * @return the parent {@link ForeignKey} instance
         */
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
    /**
     * Validates this foreign key against the given database.
     * 
     * <p>This method checks that the foreign key is properly defined and enforces
     * referential integrity rules. Validation includes:</p>
     * <ul>
     *   <li>At least one child column is defined.</li>
     *   <li>At least one parent column is defined.</li>
     *   <li>Child and parent column counts match.</li>
     *   <li>Child columns exist in this schema.</li>
     *   <li>Parent table exists in the database.</li>
     *   <li>Parent columns exist in the parent table schema.</li>
     *   <li>Parent columns are either PRIMARY KEY or UNIQUE.</li>
     *   <li>No duplicate foreign key names exist in the schema.</li>
     *   <li>Data types of child and parent columns match.</li>
     * </ul>
     * 
     * <p>If any of these conditions fail, a {@link ForeignKeyException} is thrown
     * with a descriptive error message.</p>
     * 
     * @param database the database to validate the foreign key against
     * @return {@code true} if the foreign key is valid
     * @throws ForeignKeyException if the foreign key is invalid, e.g.,
     *         missing columns, type mismatches, non-unique name, or referencing
     *         non-PK/UNIQUE parent columns
     */
    public boolean isValid(Database database){
        if (childColumns.isEmpty()) {
            throw new ForeignKeyException(
                name,
                "Foreign key '" + name + "' must have at least one child column.");
        }
        if (referenceColumns.isEmpty()) {
            throw new ForeignKeyException(
                name,
                "Foreign key '" + name + "' must have at least one parent column.");
        }
        if (childColumns.size() != referenceColumns.size()) {
            throw new ForeignKeyException(
                name,
                "Foreign key '" + name + "' has mismatched column counts: " +
                "child=" + childColumns.size() + ", parent=" + referenceColumns.size()
            );
        }
        // Validate child columns exist
        for (String column : childColumns) {
            if (!schema.hasColumn(column)) {
                throw new ForeignKeyException(
                    name,
                    "Foreign key '" + name + "' references unknown child column '" + column + "'."
                );
            }
        }
        // Validate parent table exists
        if (database.getTable(referenceTable) == null) {
            throw new ForeignKeyException(
                name,
                "Foreign key '" + name + "' references missing table '" + referenceTable + "'."
            );
        }
        Schema parentSchema = database.getSchema(referenceTable);
        // Validate parent columns exist
        for (String column : referenceColumns) {
            if (!parentSchema.hasColumn(column)) {
                throw new ForeignKeyException(
                    name,
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
                throw new ForeignKeyException(
                    name,
                    "Foreign key '" + name + "' references column '" + column.name() +
                    "' in table '" + referenceTable +
                    "' which is not PRIMARY KEY or UNIQUE."
                );
            }
        }
        // Prevent duplicate FK names
        for (ForeignKey fk : schema.getForeignKeys()) {
            if (!fk.equals(this) && fk.getName().equalsIgnoreCase(this.name)) {
                throw new ForeignKeyException(name,"Duplicate foreign key name: '" + name + "'.");
            }
        }
        // Validate type compatibility
        ColumnInner[] childCols = schema.getColumns();
        for (int i = 0; i < childColumns.size(); i++) {
            ColumnInner childCol = childCols[schema.getColumnIndex(childColumns.get(i))];
            ColumnInner parentCol = parentCols[parentSchema.getColumnIndex(referenceColumns.get(i))];
            if (!childCol.type().equals(parentCol.type())) {
                throw new ForeignKeyException(
                    name,
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