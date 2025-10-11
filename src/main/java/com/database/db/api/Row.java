package com.database.db.api;

/**
 * Represents a database-like record with column names and their associated values.
 * 
 * <p>This class allows storing and retrieving values by column name or index. 
 * It ensures that the number of columns matches the number of values.</p>
 */
public class Row {
    private final String[] columnNames;
    private final Object[] values;
    /**
     * Constructs a DBRecord by splitting a comma-separated list of column names
     * and associating them with the given values.
     *
     * @param columns a comma-separated string of column names
     * @param values the values corresponding to the columns
     * @throws IllegalArgumentException if the number of columns does not match the number of values
     */
    public Row(String columns, Object[] values){
        this(columns.split(","), values);
    }
    /**
     * Constructs a DBRecord with explicit column names and values.
     *
     * @param columnNames an array of column names
     * @param values an array of values corresponding to the column names
     * @throws IllegalArgumentException if the number of columns does not match the number of values
     */
    public Row(String[] columnNames, Object[] values){
        if(columnNames.length != values.length) {
            throw new IllegalArgumentException(
                "Number of columns (" + columnNames.length + 
                ") does not match number of values (" + values.length + ")."
            );
        }
        this.columnNames = columnNames;
        this.values = values;
    }
    /**
     * Constructs a DBRecord with empty values initialized to {@code null}.
     *
     * @param columns a comma-separated string of column names
     */
    public Row(String columns){
        this(columns.split(","));
    }
    /**
     * Constructs a DBRecord with empty values initialized to {@code null}.
     *
     * @param columnNames an array of column names
     */
    public Row(String[] columnNames){
        this.columnNames = columnNames;
        this.values = new Object[columnNames.length];
    }
    /**
     * Updates the value in the specified column.
     *
     * @param columnName name of the column to update
     * @param value new value to set for the column
     * @return this Row instance for method chaining
     * @throws IllegalArgumentException if no column with the given name exists
     */
    public Row set(String columnName, Object value){
        int columnIndex = this.columnIndex(columnName);
        if(columnIndex < 0) throw new IllegalArgumentException("Column not found: " + columnName);
        values[columnIndex] = value;
        return this;
    }
    /**
     * Gets the value of the specified column by name.
     * @param columnName the column name
     * @return the value of the column
     * @throws IllegalArgumentException if the column name does not exist
     */
    public Object get(String columnName){
        for (int i =0;i<columnNames.length;i++) {
            if(columnName.equals(columnNames[i])) return values[i];
        }
        throw new IllegalArgumentException("Column not found: " + columnName);
    }
    /**
     * Gets the value of the specified column by index.
     * @param index the column index
     * @return the value at the specified index
     */
    public Object get(int index) { return values[index]; }
    /**
     * Returns all values in this record.
     * @return an array of all column values
     */
    public Object[] getValues() { return values; }
    public String[] getColumns() { return columnNames; }
    /**
     * Returns a string representation of the record in the format:
     * <pre>
     * {column1=value1, column2=value2, ...}
     * </pre>
     *
     * @return a string representation of the record
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < columnNames.length; i++) {
            sb.append(columnNames[i]).append("=").append(values[i]);
            if (i < columnNames.length - 1) sb.append(", ");
        }
        sb.append("}");
        return sb.toString();
    }
    /**
     * Finds the index of a column name.
     *
     * @param columnName the column name
     * @return the index of the column, or -1 if not found
     */
    private int columnIndex(String columnsName){
        int result = -1;
        for (String column : columnNames) {
            result++;
            if(column.equals(columnsName)) return result;
        }
        return -1;
    }
}