package com.database.tttdb.api;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Set;

import com.database.tttdb.api.Schema.ColumnInner;
import com.database.tttdb.core.table.Constraint;
import com.database.tttdb.core.table.DataType;

/**
 * Represents a column in a table schema and provides a fluent API to define
 * its properties, constraints, and default values.
 * 
 * <p>Used together with {@link Schema} to build table definitions programmatically.</p>
 */
public class Column {
    private Schema schema;

    private String name;
    private DataType type;
    private int size;
    private final Set<Constraint> constraints = EnumSet.noneOf(Constraint.class);
    private Object defaultValue;
    /**
     * Creates an empty column.
     * Use the fluent API to configure it.
     */
    public Column(){}
    /**
     * Creates a column for a specific schema with a given name.
     * 
     * @param name the column name
     * @param schema the schema this column belongs to
     */
    public Column(String name, Schema schema){
        this.schema = schema;
        this.name = name;
    }
    /**
     * Sets the column name.
     * 
     * @param name the column name
     * @return the current {@code Column} instance
     */
    public Column name(String name){
        this.name = name;
        return this;
    }
    /**
     * Sets the column data type.
     * Automatically sets the column size if the data type has a fixed size.
     * 
     * @param type the data type
     * @return the current {@code Column} instance
     */
    public Column type(DataType type){
        this.type = type;
        if(this.type.getSize() != -1) this.size = this.type.getSize();
        return this;
    }
    /**
     * Sets the column size.
     * Respects the fixed size of the data type if applicable.
     * 
     * @param size the column size
     * @return the current {@code Column} instance
     */
    public Column size(int size){
        if(this.type.getSize() == -1) this.size = size;
        else this.size = this.type.getSize();
        return this;
    }
    /**
     * Marks this column as a primary key. Automatically adds NOT NULL constraint.
     * 
     * @return the current {@code Column} instance
     */
    public Column primaryKey(){
        this.constraints.add(Constraint.PRIMARY_KEY);
        this.constraints.add(Constraint.NOT_NULL);
        return this;
    }
    /**
     * Marks this column as UNIQUE.
     * 
     * @return the current {@code Column} instance
     */
    public Column unique(){
        this.constraints.add(Constraint.UNIQUE);
        return this;
    }
    /**
     * Adds an index to this column.
     * 
     * @return the current {@code Column} instance
     */
    public Column index(){
        this.constraints.add(Constraint.SECONDARY_KEY);
        return this;
    }
    /**
     * Marks this column as auto-incrementing (uses LONG type internally).
     * 
     * @return the current {@code Column} instance
     */
    public Column autoIncrementing(){
        this.type(DataType.LONG);
        this.constraints.add(Constraint.AUTO_INCREMENT);
        return this;
    }
    /**
     * Adds a NOT NULL constraint to this column.
     * 
     * @return the current {@code Column} instance
     */
    public Column notNull(){
        this.constraints.add(Constraint.NOT_NULL);
        return this;
    }
    /**
     * Sets a default value for this column.
     * Ensures the value matches the column data type.
     * 
     * @param value the default value
     * @return the current {@code Column} instance
     * @throws IllegalStateException if column type is not set
     * @throws IllegalArgumentException if value type is invalid
     */
    public Column defaultValue(Object value) {
        if (this.type == null) 
            throw new IllegalStateException("Column type must be set before assigning a default value.");
        if (!this.type.isValid(value)) 
            throw new IllegalArgumentException(
                "Invalid type for default value. Expected " 
                + type.getJavaClass().getSimpleName() 
                + " but got " + value.getClass().getSimpleName()
            );
        this.defaultValue = value;
        return this;
    }
    /**
     * Ends the column definition and adds it to the parent schema.
     * 
     * @return the parent {@link Schema} instance
     */
    public Schema endColumn(){
        this.schema.add(get());
        return this.schema;
    }
    /**
     * Builds a {@link ColumnInner} object representing this column.
     * Adds NO_CONSTRAINT if no constraints were set.
     * 
     * @return a {@link ColumnInner} representing this column
     */
    public ColumnInner get(){
        if(this.constraints.isEmpty()) this.constraints.add(Constraint.NO_CONSTRAINT);
        return new ColumnInner(
            name, 
            type, 
            size, 
            new ArrayList<>(constraints), 
            defaultValue
        );
    }
}