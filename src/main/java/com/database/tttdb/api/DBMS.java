package com.database.tttdb.api;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import com.database.tttdb.api.Condition.WhereClause;
import com.database.tttdb.api.Query.Delete;
import com.database.tttdb.api.Query.Select;
import com.database.tttdb.api.Query.SelectType;
import com.database.tttdb.api.Query.Update;
import com.database.tttdb.core.Database;
import com.database.tttdb.core.manager.EntryManager;
import com.database.tttdb.core.manager.ForeignKeyManager;
import com.database.tttdb.core.page.Entry;
import com.database.tttdb.core.table.Table;
/**
 * The {@code DBMS} class provides a simple database management system.
 * It supports multiple databases, tables, and basic SQL-like operations including
 * SELECT, INSERT, UPDATE, DELETE, and transaction management.
 *
 * <p>Each {@code DBMS} instance manages a set of {@link Database} objects and provides
 * methods to manipulate data via {@link EntryManager}.</p>
 *
 * <h3>Threading model</h3>
 * <p>
 * All API methods are expected to be invoked from a single application thread.
 * Each {@link Database} maintains its own internal file I/O thread for persistence.
 * The DBMS itself performs no synchronization or concurrency control.
 * </p>
 */
public class DBMS {
    private final Map<String,Database> databases;
    private Database selected;
    private String path = "";
    private boolean isStarted = false;
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
            if(resultColumns == "*") return table.getSchema().getNames();
            if(resultColumns.isBlank() || resultColumns == null) return new String[0];
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
     * Sets the directory path where database files are stored.
     * <p>
     * If the directory does not exist, it will be created automatically.
     * If the path exists but is not a directory, a {@link DatabaseException} is thrown.
     * </p>
     *
     * @param path the filesystem path for database storage
     * @return this {@code DBMS} instance for method chaining
     * @throws DatabaseException if the path cannot be created or is not a directory
     */
    public DBMS setPath(String path){
        this.path = path;
        Path dir = Path.of(path);
        if (!Files.exists(dir)) {
            try {
                Files.createDirectories(dir);
            } catch (IOException e) {
                throw new DatabaseException("Failed to create directory: " + path, e);
            }
        } else if (!Files.isDirectory(dir)) {
            throw new DatabaseException("Path exists but is not a directory: " + path);
        }
        for (Database database : databases.values()) {
            database.setPath(path);
        }
        return this;
    }
    /**
     * Creates all databases or Loads them if they exist in this DBMS
     * @return the current DBMS instance
     */
    public DBMS start(){
        if(isStarted) throw new DatabaseException("can not start already started DBMS.");
        try {
            Files.createDirectories(Path.of(this.path));
            LogManager.getLogManager().reset();
            FileHandler fileHandler = new FileHandler(this.path + "/tttdb.log", 5_000_000, 3, true);
            fileHandler.setFormatter(new SimpleFormatter());
            fileHandler.setLevel(Level.FINE);

            Logger rootLogger = Logger.getLogger("");
            rootLogger.addHandler(fileHandler);
            rootLogger.setLevel(Level.INFO);
        } catch (IOException e) {
            System.err.println("Failed to initialize logging in " + this.path + ": " + e.getMessage());
        }
        Set<String> databaseNames = new HashSet<>(this.databases.keySet());
        for (String databaseName : databaseNames) {
            Database database = databases.get(databaseName);
            database.start();
        }
        isStarted = true;
        return this;
    }
    /**
     * Closes all databases and commits any pending changes.
     */
    public void close(){
        Set<String> databaseNames  = new HashSet<>(this.databases.keySet());
        for (String databaseName : databaseNames) {
            this.databases.get(databaseName).commit().close();
        }
        isStarted = false;
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
    public DBMS addTable(String tableName, Schema tableSchema){
        if(this.selected == null) throw new IllegalArgumentException("Trying to create Table with not Database selected.");
        this.selected.createTable(tableName, tableSchema);
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
     * Initiates a fluent SELECT query against the currently selected database.
     * <p>
     * This method creates a {@link Select} builder configured with the provided
     * column list. The builder can then be extended with methods such as
     * {@code from()}, {@code where()}, {@code ASC()}, {@code DESC()}, {@code limit()},
     * and finally executed with {@code exec()}.
     * </p>
     *
     * <p><b>Example:</b></p>
     * <pre>{@code
     * List<Row> result = db.select("id, username, date")
     *                      .from("users")
     *                      .where().eq("active", true)
     *                      .ASC("username")
     *                      .limit(20)
     *                      .fetch();
     * }</pre>
     *
     * @param selectColumns comma-separated column names to retrieve; use "*" for all columns
     * @return a {@link Select} builder for further query configuration
     * @throws IllegalArgumentException if no database is currently selected at execution time
     */
    public Select select(String selectColumns){
        return new Select(this, selectColumns);
    }
    public List<Row> select(SelectQuery query){
        if(this.selected == null) throw new IllegalArgumentException("Can not perform select statement when no Database selected.");
        Table table = selected.getTable(query.tableName);
        List<Entry> result = table.select(query.whereClause, query.begin, query.limit, query.type);
        return Row.prepareSelectResult(table, query, result);
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
     * @return the number of successfully inserted rows
     * @throws IllegalArgumentException if no database is currently selected
     */
    public int insert(String tableName, List<Row> rows){
        if(this.selected == null) throw new IllegalArgumentException("Can not perform insert statement when no Database selected.");
        Table table = selected.getTable(tableName);
        return table.insert(rows);
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
     * Initiates a fluent DELETE query against the currently selected database.
     * <p>
     * Returns a {@link Delete} builder for constructing a full {@link DeleteQuery}.
     * The builder supports chaining methods such as {@code from()}, {@code where()},
     * {@code limit()}, and final execution with {@code exec()}.
     * </p>
     *
     * <p><b>Example:</b></p>
     * <pre>{@code
     * int deleted = db.delete()
     *                 .from("users")
     *                 .where().column("username").isEqual("admin").end()
     *                 .limit(1)
     *                 .execute();
     * }</pre>
     *
     * @return a {@link Delete} builder for fluent query construction
     * @throws IllegalArgumentException if no database is selected at execution time
     */
    public Delete delete(){
        return new Delete(this);
    }
    public int delete(DeleteQuery query){
        if(this.selected == null) throw new IllegalArgumentException("Can not perform delete statement when no Database selected.");
        Table table = selected.getTable(query.tableName);
        return table.delete(query.whereClause, query.limit);
    }
    /**
     * Initiates a fluent UPDATE query on the specified table in the currently selected database.
     * <p>
     * Returns an {@link Update} builder used to define update operations through chained methods
     * such as {@code set()}, {@code where()}, {@code limit()}, and final execution with {@code execute()}.
     * </p>
     *
     * <p><b>Example:</b></p>
     * <pre>{@code
     * int affected = db.update("users")
     *                  .set("active", true)
     *                  .where().column("last_login").lt("2025-01-01").end()
     *                  .limit(100)
     *                  .execute();
     * }</pre>
     *
     * @param tableName the name of the table to update
     * @return an {@link Update} builder for constructing the update query
     * @throws IllegalArgumentException if no database is selected at execution time
     */
    public Update update(String tableName){
        return new Update(this, tableName);
    }
    public int update(UpdateQuery query){
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
    public void rollBack(String reason){
        if(this.selected == null) throw new IllegalArgumentException("Can not roll back when no Database selected.");
        this.selected.rollBack(reason);
    }
    /**
     * Commits the current transaction.
     */
    public void commit(){
        if(this.selected == null) throw new IllegalArgumentException("Can not commit when no Database selected.");
        this.selected.commit();
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
     * Prints the schema of a specific table in the selected database.
     *
     * @param tableName the table name to print
     */
    public void printTable(String tableName) {
        if (selected == null) {
            System.out.println("No database selected.");
            return;
        }
        Table table = selected.getTable(tableName);
        if (table == null) {
            System.out.printf("Table '%s' does not exist in database '%s'.%n", tableName, selected.getName());
            return;
        }
        System.out.println(table.getSchema().toString());
    }
    /**
     * Prints the structure of the currently selected database,
     * including all tables and their schemas.
     */
    public void printDatabase() {
        if (selected == null) {
            System.out.println("No database selected.");
            return;
        }
        System.out.println(selected.toString());
        System.out.println("-".repeat(80));
    }
    /**
     * Prints all databases managed by this DBMS, including their tables and schemas.
     */
    public void printAllDatabases() {
        if (databases.isEmpty()) {
            System.out.println("No databases loaded.");
            return;
        }
        for (Database db : databases.values()) {
            System.out.println(db.toString());
            System.out.println("-".repeat(80));
            System.out.println();
        }
    }
}