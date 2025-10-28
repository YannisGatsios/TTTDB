package com.database.tttdb.api;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.database.tttdb.api.DatabaseException.CheckConstraintException;
import com.database.tttdb.api.DatabaseException.EntryValidationException;
import com.database.tttdb.api.DatabaseException.SchemaException;
import com.database.tttdb.core.Database;
import com.database.tttdb.core.page.Entry;
import com.database.tttdb.core.table.Constraint;
import com.database.tttdb.core.table.DataType;
import com.database.tttdb.core.table.Table;
/**
 * The {@code Schema} class represents the structure of a table in the database,
 * including its columns, constraints, checks, and foreign keys.
 * 
 * <p>It provides a fluent API to define columns, set constraints, and configure
 * checks and foreign keys.</p>
 * 
 * <p>Example usage:</p>
 * <pre>
 * Schema userSchema = new Schema()
 *     .column("username").type(DataType.CHAR).size(15).primaryKey().endColumn()
 *     .column("age").type(DataType.INT).defaultValue(18).endColumn();
 * 
 * Schema postSchema = new Schema()
 *     .column("id").type(DataType.LONG).autoIncrementing().primaryKey().endColumn()
 *     .column("username").type(DataType.CHAR).size(15).defaultValue("DefaultUser").endColumn()
 *     .foreignKey("fk_user_cascade")
 *         .column("username")
 *         .reference().table("users").column("username").end()
 *         .onDelete(ForeignKey.ForeignKeyAction.CASCADE)
 *     .endForeignKey();
 * </pre>
 */
public class Schema {

    private final List<ColumnInner> columns;
    private final List<Check> checkList;
    private final List<ForeignKey> foreignKeyList;
    /**
     * Represents a single column in a table schema.
     *
     * @param name the column name
     * @param type the data type of the column
     * @param size the size of the column (for types like CHAR or BYTE)
     * @param constraints the list of constraints applied to this column
     * @param defaultValue the default value for the column
     */
    public record ColumnInner(String name, DataType type, int size, List<Constraint> constraints, Object defaultValue) {}
    /**
     * Creates a new empty schema.
     */
    public Schema(){
        this.columns = new ArrayList<>();
        this.checkList = new ArrayList<>();
        this.foreignKeyList = new ArrayList<>();
    }
    /**
     * Starts defining a new column in this schema.
     * 
     * <p>Fluent API example:</p>
     * <pre>
     * schema.column("username").type(DataType.CHAR).size(10).primaryKey().endColumn();
     * </pre>
     *
     * @param name the column name
     * @return a {@link Column} builder object to configure the column
     */
    public Column column(String name){
        return new Column(name, this);
    }
    /**
     * Starts defining a new check constraint in this schema.
     * 
     * <p>Fluent API example:</p>
     * <pre>
     * schema.check("age_check")
     *       .open()
     *          .column("num").isBiggerOrEqual(18).end()
     *       .close()
     *       .AND()
     *       .column("num").isSmaller(130).end()
     *       .endCheck();
     * </pre>
    * <p>This corresponds to the SQL check constraint:</p>
    * <pre>
    * CHECK (num >= 18) AND num < 130
    * </pre>
     *
     * @param checkName the name of the check constraint
     * @return a {@link Check} builder object to configure the check
     */
    public Check check(String checkName){
        return new Check(checkName, this);
    }
    /**
     * Starts defining a foreign key constraint in this schema.
     * 
     * <p>Foreign keys link one or more columns in this table to columns in another table,
     * and allow you to define actions on delete such as CASCADE, SET NULL, SET DEFAULT, or RESTRICT.</p>
     * 
     * <p>Example usage:</p>
     * <pre>
     * Schema postSchema = new Schema()
     *     .column("id").type(DataType.LONG).autoIncrementing().primaryKey().endColumn()
     *     .column("username").type(DataType.CHAR).size(15).defaultValue("DefaultUser").endColumn()
     *     .foreignKey("fk_user_cascade")
     *         .column("username")
     *         .reference().table("users").column("username").end()
     *         .onDelete(ForeignKey.ForeignKeyAction.CASCADE)
     *     .endForeignKey();
     * </pre>
     * 
     * @param name the foreign key name
     * @return a {@link ForeignKey} builder object to configure the foreign key, including
     *         referenced table/column and actions on delete
     */
    public ForeignKey foreignKey(String name){
        return new ForeignKey(name, this);
    }
    /**
    * This method is used internally 
    */
    public void add(ColumnInner column){
        this.columns.add(column);
    }
    /**
    * This method is used internally 
    */
    public void add(Check check){
        this.checkList.add(check);
    }
    /**
    * This method is used internally 
    */
    public void add(ForeignKey foreignKey){
        this.foreignKeyList.add(foreignKey);
    }
    /**
    * This method is used internally 
    */
    public int getColumnIndex(String columnName) {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).name().equals(columnName)) {
                return i;
            }
        }
        return -1;
    }
    /**
    * This method is used internally 
    */
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
    /**
    * This method is used internally 
    */
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
                throw new SchemaException("Duplicate column: " + column.name());
            }

            // Type + size
            if (column.type() == null) {
                throw new SchemaException("Column " + column.name() + " has no type.");
            }
            if (column.type().getSize() == -1 && column.size() <= 0) {
                throw new SchemaException("Column " + column.name() + " has invalid size.");
            }

            // Constraints
            if (column.constraints().contains(Constraint.PRIMARY_KEY)) {
                if (hasPrimaryKey) {
                    throw new SchemaException("Multiple primary keys not allowed.");
                }
                hasPrimaryKey = true;
                if (!column.constraints().contains(Constraint.NOT_NULL)) {
                    throw new SchemaException("Primary key column " + column.name() + " must be NOT NULL.");
                }
            }

            if (column.constraints().contains(Constraint.AUTO_INCREMENT)) {
                if (hasAutoIncrement) {
                    throw new SchemaException("Only one auto-increment column allowed.");
                }
                hasAutoIncrement = true;
                if (column.type() != DataType.LONG) {
                    throw new SchemaException("Auto-increment must be LONG, but got " + column.type());
                }
            }

            // Default value type check
            if (column.defaultValue() != null && !column.type().isValid(column.defaultValue())) {
                throw new SchemaException("Default value for " + column.name() + " is not valid for type " + column.type());
            }

            int indexCount = 0;
            if (column.constraints().contains(Constraint.PRIMARY_KEY)) indexCount++;
            if (column.constraints().contains(Constraint.UNIQUE)) indexCount++;
            if (column.constraints().contains(Constraint.SECONDARY_KEY)) indexCount++;

            if (indexCount > 1) {
                throw new SchemaException(
                    "Column " + column.name() + " can only be one of PRIMARY KEY, UNIQUE, or INDEX"
                );
            }
        }

        // Check constraints must reference valid columns
        for (Check check : checkList) {
            if (!check.isValid()) {
                throw new SchemaException("Check references unknown column for CHECK: " + check.name());
            }
        }

        for (ForeignKey foreignKey : foreignKeyList) {
            foreignKey.isValid(database);
        }

        return true;
    }
    public void isValidEntry(Entry entry, Table table) {
        ColumnInner[] cols = columns.toArray(new ColumnInner[0]);
        Object[] values = entry.getValues();

        if (values.length != cols.length) {
            throw new EntryValidationException(
                "Entry column count (" + values.length + 
                ") does not match schema column count (" + cols.length + ")"
            );
        }

        for (int i = 0; i < cols.length; i++) {
            ColumnInner column = cols[i];
            Object value = values[i];

            // NOT NULL check
            if (column.constraints().contains(Constraint.NOT_NULL) && value == null) {
                throw new EntryValidationException("Column " + column.name() + " cannot be NULL.");
            }

            // Type check
            if (value != null && !column.type().isValid(value)) {
                throw new EntryValidationException(
                    "Invalid type for column " + column.name() +
                    ". Expected " + column.type().getJavaClass().getSimpleName() +
                    " but got " + value.getClass().getSimpleName()
                );
            }

            // Size check (CHAR, BYTE arrays, etc.)
            if (value != null && column.size() > 0) {
                String msg = "Value for column " + column.name() +
                        " exceeds max size of " + column.size();
                if (value instanceof String s && s.length() > column.size()) {
                    throw new EntryValidationException(msg);
                }
                if (value instanceof byte[] b && b.length > column.size()) {
                    throw new EntryValidationException(msg);
                }
            }

            // UNIQUE check (needs table lookup)
            if ((column.constraints().contains(Constraint.PRIMARY_KEY) || column.constraints().contains(Constraint.UNIQUE))  && value != null) {
                int idx = getColumnIndex(column.name());
                if (table.containsKey(value, idx)) {
                    throw new EntryValidationException(
                        "Duplicate value for UNIQUE column " + column.name()
                    );
                }
            }
        }

        // Check constraints (WHERE-like conditions)
        for (Check check : checkList) {
            if (!check.isTrue(entry, table.getSchema())) {
                throw new CheckConstraintException(
                    check.name(),
                    "Entry violates CHECK constraint on column " + check.name()
                );
            }
        }
    }
}