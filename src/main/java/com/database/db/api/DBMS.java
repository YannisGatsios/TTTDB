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
import com.database.db.api.Query.Delete;
import com.database.db.api.Query.Select;
import com.database.db.api.Query.SelectType;
import com.database.db.api.Query.SelectionType;
import com.database.db.api.Query.Update;
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
    /**
     * Represents the configuration of a table, including its name and schema.
     */
    public record TableConfig(String tableName, Schema schema){}
    /**
     * Represents a SELECT query.
     */
    public record SelectQuery(String tableName, String resultColumns, WhereClause whereClause, int begin, int limit, SelectType type){
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
     * Creates all databases or Loads them if they exist in this DBMS
     * @return the current DBMS instance
     */
    public DBMS start(){
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
     * Performs a SELECT query against the currently selected database.
     * <p>
     * This method executes a fluent {@link Select} query, applying all configured
     * options such as:
     * <ul>
     *   <li>Target table name</li>
     *   <li>{@link WhereClause} filtering conditions</li>
     *   <li>{@code begin} offset (number of rows to skip)</li>
     *   <li>{@code limit} maximum number of rows to return</li>
     *   <li>Optional ordering by a specific column in ascending or descending order,
     *       as defined by {@link SelectionType}</li>
     * </ul>
     *
     * The underlying rows are retrieved as {@link Entry} objects from the table,
     * then converted into higher-level {@link Row} objects for the result set.
     *
     * <p><b>Usage example:</b></p>
     * <pre>{@code
     * List<Row> users = db.select(
     *     new Select("*")
     *         .from("users")
     *         .where().eq("active", true)
     *         .ASC("username")
     *         .limit(20)
     * );
     * }</pre>
     *
     * @param select the fluent {@link Select} builder defining the query
     * @return a list of {@link Row} objects matching the query constraints
     * @throws IllegalArgumentException if no database is currently selected
     */
    public List<Row> select(Select select){
        SelectQuery query = select.get();
        if(this.selected == null) throw new IllegalArgumentException("Can not perform select statement when no Database selected.");
        Table table = selected.getTable(query.tableName);
        List<Entry> result = table.select(query.whereClause, query.begin, query.limit, query.type);
        return this.prepareSelectResult(table, query, result);
    }
    /**
     * Inserts a single row into the specified table.
     *
     * <p>This is a convenience overload of
     * {@link #insert(String, List)} which wraps the given row
     * into a list and delegates to it.</p>
     *
     * @param tableName the target table name
     * @param newRow    the row to insert
     * @throws IllegalArgumentException if no database is currently selected
     */
    public void insert(String tableName, Row newRow){
        ArrayList<Row> row = new ArrayList<>();
        row.add(newRow);
        insert(tableName, row);
    }
    /**
     * Inserts multiple rows into the specified table in a single call.
     *
     * <p>This method uses the engine's normal insertion mechanism and ensures
     * that all rows and related indexes are updated consistently. If no
     * database has been selected, an {@link IllegalArgumentException} is thrown.</p>
     *
     * @param tableName the target table name
     * @param rows      the rows to insert
     * @throws IllegalArgumentException if no database is currently selected
     */
    public void insert(String tableName, List<Row> rows){
        if(this.selected == null) throw new IllegalArgumentException("Can not perform insert statement when no Database selected.");
        Table table = selected.getTable(tableName);
        table.insert(rows);
    }
    /**
     * Performs an "unsafe" insert of a single row into the specified table.
     *
     * <p><b> WARNING:</b> This method bypasses the engine's internal
     * transactional safety. If the process crashes during the insert,
     * the table's data and indexes may become inconsistent (for example,
     * a row may be written but its index entry not recorded).</p>
     *
     * <p>This method is intended for advanced usage where the caller manages
     * their own transaction boundaries, or when performance is prioritized
     * over durability and consistency guarantees.</p>
     *
     * <p>Normal users should prefer {@link #insert(String, Row)} unless
     * they fully understand the risks.</p>
     *
     * @param tableName the target table name
     * @param newRow    the row to insert
     * @throws IllegalArgumentException if no database is currently selected
     * @see #insert(String, Row)
     * @see #insert(String, List)
     */
    public void insertUnsafe(String tableName, Row newRow){
        if(this.selected == null) throw new IllegalArgumentException("Can not perform insert statement when no Database selected.");
        Table table = selected.getTable(tableName);
        Entry entry = Entry.prepareEntry(newRow.getColumns(), newRow.getValues(), table);
        this.selected.getSchema(tableName).isValidEntry(entry, table);
        ForeignKeyManager.foreignKeyCheck(this, selected, tableName, entry);
        table.insertUnsafe(entry);
    }
    /**
     * Performs a DELETE query.
     * @param query the delete query
     * @return the number of deleted entries
     */
    public int delete(Delete delete){
        DeleteQuery query = delete.get();
        if(this.selected == null) throw new IllegalArgumentException("Can not perform delete statement when no Database selected.");
        Table table = selected.getTable(query.tableName);
        return table.delete(query.whereClause, query.limit);
    }
    /**
     * Performs an UPDATE query.
     * @param query the update query
     * @return the number of updated entries
     */
    public int update(Update update){
        UpdateQuery query = update.get();
        if(this.selected == null) throw new IllegalArgumentException("Can not perform update statement when no Database selected.");
        Table table = selected.getTable(query.tableName);
        return table.update(query.whereClause, query.limit, query.updateFields);
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
        return table.containsKey(value, index);
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
     * Prepares the result of a SELECT query as a list of {@link DBRecord}.
     * @param resultColumns the columns to include in the result
     * @param selectResult the raw entries
     * @return list of records
     */
    private List<Row> prepareSelectResult(Table table, SelectQuery query,List<Entry> selectResult){
        List<Row> result = new ArrayList<>();
        String[] resultColumns = query.getColumns(table);
        com.database.db.table.SchemaInner schema = table.getSchema();
        for (Entry entry : selectResult) {
            Object[] values = new Object[resultColumns.length];
            for(int i = 0;i<resultColumns.length;i++){
                String name = resultColumns[i];
                int index = schema.getColumnIndex(name);
                if(index == -1) throw new IllegalArgumentException("Invalid result column");
                values[i] = entry.get(index);
            }
            result.add(new Row(resultColumns, values));
        }
        return result;
    }
}