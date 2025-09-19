package com.database.db.api;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.database.db.Database;
import com.database.db.api.Condition.WhereClause;
import com.database.db.manager.EntryManager;
import com.database.db.manager.ForeignKeyManager;
import com.database.db.page.Entry;
import com.database.db.table.Table;
/**
 * The {@code DBMS} class provides a simple database management system.
 * It supports multiple databases, tables, and basic SQL-like operations including
 * SELECT, INSERT, UPDATE, DELETE, and transaction management.
 * 
 * <p>Each {@code DBMS} instance manages a set of {@link Database} objects and provides
 * methods to manipulate data via {@link EntryManager}.</p>
 */
public class DBMS {
    static {
        Logger rootLogger = Logger.getLogger("");
        rootLogger.setLevel(Level.ALL);
        for (Handler handler : rootLogger.getHandlers()) {
            handler.setLevel(Level.ALL);
        }
    }
    private final Map<String,Database> databases;
    private Database selected;
    private String path = "";
    private final EntryManager entryManager;
    /**
     * Represents the configuration of a table, including its name and schema.
     */
    public record TableConfig(String tableName, Schema schema){}
    /**
     * Represents a database record with column names and their corresponding values.
     */
    public record Record(String[] columnNames, Object[] values){
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
            throw new IllegalArgumentException("Invalid column name "+columnName);
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
        public Object[] getAll() { return values; }
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
    }
    /**
     * Represents a SELECT query.
     */
    public record SelectQuery(String tableName, String resultColumns, WhereClause whereClause, int begin, int limit){
        /**
         * Returns the columns to select for the given table.
         * @param table the table
         * @return array of column names
         */
        public String[] getColumns(Table table){
            if(resultColumns == null) return table.getSchema().getNames();
            if(resultColumns.isBlank()) return new String[0];
            return resultColumns.split("\\s*,\\s*");
        }
    }
    /**
     * Represents an INSERT query.
     */
    public record InsertQuery(String tableName, String columns, Object[] values){
        /**
         * Returns the columns to insert into for the given table.
         * @param table the table
         * @return array of column names
         */
        public String[] getColumns(Table table){
            if(columns == null) return table.getSchema().getNames();
            if(columns.isBlank()) return new String[0];
            return columns.split("\\s*,\\s*");
        }
    }
    /**
     * Represents a DELETE query.
     */
    public record DeleteQuery(String tableName, WhereClause whereClause, int limit){}
    /**
     * Represents an UPDATE query.
     */
    public record UpdateQuery(String tableName, WhereClause whereClause, int limit, UpdateFields updateFields){}
    /**
     * Creates a new empty DBMS instance.
     */
    public DBMS(){
        this.databases = new HashMap<>();
        this.entryManager = new EntryManager();
    }
    /**
     * Creates a new DBMS instance with a database initialized.
     * @param databaseName the name of the database
     * @param cacheCapacity the cache capacity for the database
     */
    public DBMS(String databaseName, int cacheCapacity){
        this();
        this.setPath(path);
        this.addDatabase(databaseName, cacheCapacity);
    }
    /**
     * Sets the path where database files will be stored.
     * @param path the storage path
     * @return the current DBMS instance
     */
    public DBMS setPath(String path){
        this.path = path;
        return this;
    }
    /**
     * Adds a database to this DBMS.
     * @param databaseName the name of the database
     * @param cacheCapacity the cache capacity for the database
     * @return the current DBMS instance
     */
    public DBMS addDatabase(String databaseName, int cacheCapacity){
        Database database = new Database(databaseName, this, cacheCapacity);
        database.setPath(this.path);
        this.databases.put(databaseName, database);
        this.selected = database;
        return this;
    }
    /**
     * Creates all databases in this DBMS.
     * @return the current DBMS instance
     */
    public DBMS create(){
        Set<String> databaseNames = new HashSet<>(this.databases.keySet());
        for (String databaseName : databaseNames) {
            Database database = databases.get(databaseName);
            database.create();
        }
        return this;
    }
    /**
     * Drops the specified database.
     * @param databaseName the name of the database
     */
    public void dropDatabase(String databaseName){
        this.selected = this.databases.get(databaseName);
        this.dropDatabase();
    }
    /**
     * Drops the currently selected database.
     */
    public void dropDatabase(){
        if(this.selected == null) throw new IllegalArgumentException("Trying to drop Database but not Database selected.");
        this.selected.dropDatabase();
    }
    /**
     * Selects a database to operate on.
     * @param database the database name
     * @return the current DBMS instance
     */
    public DBMS selectDatabase(String database){
        if(this.selected != null) this.selected.commit();
        this.selected = this.databases.get(database);
        this.entryManager.selectDatabase(selected);
        return this;
    }
    /**
     * Adds a table to the selected database.
     * @param config the table configuration
     * @return the current DBMS instance
     */
    public DBMS addTable(TableConfig config){
        if(this.selected == null) throw new IllegalArgumentException("Trying to create Table with not Database selected.");
        this.selected.createTable(config);
        return this;
    }
    /**
     * Drops a table from the selected database.
     * @param tableName the table name
     * @return the current DBMS instance
     */
    public DBMS dropTable(String tableName){
        if(this.selected == null) throw new IllegalArgumentException("Trying to drop Table with not Database selected.");
        this.selected.dropTable(tableName);
        return this;
    }
    /**
     * Performs a SELECT query.
     * @param query the select query
     * @return list of records matching the query
     */
    public List<Record> select(SelectQuery query){
        if(this.selected == null) throw new IllegalArgumentException("Can not perform select statement when no Database selected.");
        this.entryManager.selectDatabase(selected);
        this.entryManager.selectTable(query.tableName);
        List<Entry> result = this.entryManager.selectEntriesAscending(query.whereClause, query.begin, query.limit);
        return this.prepareSelectResult(query.getColumns(entryManager.getTable()), result);
    }
    /**
     * Performs a SELECT query in descending order.
     * @param query the select query
     * @return list of records matching the query in descending order
     */
    public List<Record> selectDescending(SelectQuery query){
        if(this.selected == null) throw new IllegalArgumentException("Can not perform select statement when no Database selected.");
        this.entryManager.selectDatabase(selected);
        this.entryManager.selectTable(query.tableName);
        List<Entry> result = this.entryManager.selectEntriesDescending(query.whereClause, query.begin, query.limit);
        return this.prepareSelectResult(query.getColumns(entryManager.getTable()), result);
    }
    /**
     * Performs an INSERT query.
     * @param query the insert query
     */
    public void insert(InsertQuery query){
        if(this.selected == null) throw new IllegalArgumentException("Can not perform insert statement when no Database selected.");
        this.entryManager.selectDatabase(selected);
        this.entryManager.selectTable(query.tableName);
        Entry entry = Entry.prepareEntry(query.getColumns(entryManager.getTable()), query.values, this.selected.getTable(query.tableName));
        this.selected.getSchema(query.tableName).isValidEntry(entry, this.entryManager.getTable());
        ForeignKeyManager.foreignKeyCheck(this, selected, query.tableName, entry);
        this.entryManager.insertEntry(entry);
    }
    /**
     * Performs a DELETE query.
     * @param query the delete query
     * @return the number of deleted entries
     */
    public int delete(DeleteQuery query){
        if(this.selected == null) throw new IllegalArgumentException("Can not perform delete statement when no Database selected.");
        this.entryManager.selectDatabase(selected);
        this.entryManager.selectTable(query.tableName);
        return this.entryManager.deleteEntry(query.whereClause, query.limit);
    }
    /**
     * Performs an UPDATE query.
     * @param query the update query
     * @return the number of updated entries
     */
    public int update(UpdateQuery query){
        if(this.selected == null) throw new IllegalArgumentException("Can not perform update statement when no Database selected.");
        this.entryManager.selectDatabase(selected);
        this.entryManager.selectTable(query.tableName);
        return this.entryManager.updateEntry(query.whereClause, query.limit, query.updateFields);
    }
    /**
     * Starts a transaction in the selected database.
     * @param name the transaction name
     */
    public void startTransaction(String name){
        if(this.selected == null) throw new IllegalArgumentException("Can not start transaction when no Database selected.");
        this.selected.startTransaction(name);
    }
    /**
     * Rolls back the current transaction.
     */
    public void rollBack(){
        if(this.selected == null) throw new IllegalArgumentException("Can not roll back when no Database selected.");
        this.selected.rollBack();
    }
    /**
     * Commits the current transaction.
     */
    public void commit(){
        if(this.selected == null) throw new IllegalArgumentException("Can not commit when no Database selected.");
        this.selected.commit();
    }
    /**
     * Closes all databases and commits any pending changes.
     */
    public void close(){
        Set<String> databaseNames  = new HashSet<>(this.databases.keySet());
        for (String databaseName : databaseNames) {
            this.databases.get(databaseName).commit().close();
        }
    }
    /**
     * Checks if a value exists in the given table column.
     * @param tableName the table name
     * @param columnName the column name
     * @param value the value to check
     * @return true if the value exists, false otherwise
     */
    public boolean containsValue(String tableName, String columnName, Object value){
        if(this.selected == null) throw new IllegalArgumentException("Can not use containsValue when no Database selected.");
        Table table = this.selected.getTable(tableName);
        int index = table.getSchema().getColumnIndex(columnName);
        return table.isKeyFound(value, index);
    }
    /**
     * Returns the sizes of all columns in the given table.
     * @param tableName the table name
     * @return array of column sizes
     */
    public int[] getColumnSizes(String tableName){
        return this.selected.getTable(tableName).getSchema().getSizes();
    }
    /**
     * Prepares the result of a SELECT query as a list of {@link Record}.
     * @param resultColumns the columns to include in the result
     * @param selectResult the raw entries
     * @return list of records
     */
    private List<Record> prepareSelectResult(String[] resultColumns,List<Entry> selectResult){
        List<Record> result = new ArrayList<>();
        com.database.db.table.SchemaInner schema = this.entryManager.getTable().getSchema();
        for (Entry entry : selectResult) {
            Object[] values = new Object[resultColumns.length];
            for(int i = 0;i<resultColumns.length;i++){
                String name = resultColumns[i];
                int index = schema.getColumnIndex(name);
                if(index == -1) throw new IllegalArgumentException("Invalid result column");
                values[i] = entry.get(index);
            }
            result.add(new Record(resultColumns, values));
        }
        return result;
    }
}