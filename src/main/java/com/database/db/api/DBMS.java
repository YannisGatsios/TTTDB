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

    public record TableConfig(String tableName, Schema schema, CacheCapacity cacheCapacity){}
    public record CacheCapacity(int tableCapacity, int indexCapacity){}
    public record Record(String[] columnNames, Object[] values){
        public Object get(String columnName){
            for (int i =0;i<columnNames.length;i++) {
                if(columnName.equals(columnNames[i])) return values[i];
            }
            throw new IllegalArgumentException("Invalid column name "+columnName);
        }
        public Object get(int index) { return values[index]; }
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

    public record SelectQuery(String tableName, String resultColumns, WhereClause whereClause, int begin, int limit){
        public String[] getColumns(Table table){
            if(resultColumns == null) return table.getSchema().getNames();
            if(resultColumns.isBlank()) return new String[0];
            return resultColumns.split("\\s*,\\s*");
        }
    }
    public record InsertQuery(String tableName, String columns, Object[] values){
        public String[] getColumns(Table table){
            if(columns == null) return table.getSchema().getNames();
            if(columns.isBlank()) return new String[0];
            return columns.split("\\s*,\\s*");
        }
    }
    public record DeleteQuery(String tableName, WhereClause whereClause, int limit){}
    public record UpdateQuery(String tableName, WhereClause whereClause, int limit, UpdateFields updateFields){}

    public DBMS(){
        this.databases = new HashMap<>();
        this.entryManager = new EntryManager();
    }
    public DBMS(String databaseName, String path){
        this();
        this.setPath(path);
        this.addDatabase(databaseName);
        this.selectDatabase(databaseName);
    }
    public DBMS setPath(String path){
        this.path = path;
        return this;
    }
    public DBMS addDatabase(String databaseName){
        Database database = new Database(databaseName, this);
        database.setPath(this.path);
        this.databases.put(databaseName, database);
        this.selected = database;
        return this;
    }
    public DBMS create(){
        Set<String> databaseNames = new HashSet<>(this.databases.keySet());
        for (String databaseName : databaseNames) {
            Database database = databases.get(databaseName);
            database.create();
        }
        return this;
    }
    public void dropDatabase(String databaseName){
        this.selected = this.databases.get(databaseName);
        this.dropDatabase();
    }
    public void dropDatabase(){
        if(this.selected == null) throw new IllegalArgumentException("Trying to drop Database but not Database selected.");
        this.selected.dropDatabase();
    }
    public DBMS selectDatabase(String database){
        if(this.selected != null) this.selected.commit();
        this.selected = this.databases.get(database);
        this.entryManager.selectDatabase(selected);
        return this;
    }
    public DBMS addTable(TableConfig config){
        if(this.selected == null) throw new IllegalArgumentException("Trying to create Table with not Database selected.");
        this.selected.createTable(config);
        return this;
    }
    public DBMS dropTable(String tableName){
        if(this.selected == null) throw new IllegalArgumentException("Trying to drop Table with not Database selected.");
        this.selected.dropTable(tableName);
        return this;
    }
    public List<Record> select(SelectQuery query){
        if(this.selected == null) throw new IllegalArgumentException("Can not perform select statement when no Database selected.");
        this.entryManager.selectDatabase(selected);
        this.entryManager.selectTable(query.tableName);
        List<Entry> result = this.entryManager.selectEntriesAscending(query.whereClause, query.begin, query.limit);
        return this.prepareSelectResult(query.getColumns(entryManager.getTable()), result);
    }
    public List<Record> selectDescending(SelectQuery query){
        if(this.selected == null) throw new IllegalArgumentException("Can not perform select statement when no Database selected.");
        this.entryManager.selectDatabase(selected);
        this.entryManager.selectTable(query.tableName);
        List<Entry> result = this.entryManager.selectEntriesDescending(query.whereClause, query.begin, query.limit);
        return this.prepareSelectResult(query.getColumns(entryManager.getTable()), result);
    }
    public void insert(InsertQuery query){
        if(this.selected == null) throw new IllegalArgumentException("Can not perform insert statement when no Database selected.");
        this.entryManager.selectDatabase(selected);
        this.entryManager.selectTable(query.tableName);
        Entry entry = Entry.prepareEntry(query.getColumns(entryManager.getTable()), query.values, this.selected.getTable(query.tableName));
        this.selected.getSchema(query.tableName).isValidEntry(entry, this.entryManager.getTable());
        ForeignKeyManager.foreignKeyCheck(this, selected, query.tableName, entry);
        this.entryManager.insertEntry(entry);
    }
    public int delete(DeleteQuery query){
        if(this.selected == null) throw new IllegalArgumentException("Can not perform delete statement when no Database selected.");
        this.entryManager.selectDatabase(selected);
        this.entryManager.selectTable(query.tableName);
        return this.entryManager.deleteEntry(query.whereClause, query.limit);
    }
    public int update(UpdateQuery query){
        if(this.selected == null) throw new IllegalArgumentException("Can not perform update statement when no Database selected.");
        this.entryManager.selectDatabase(selected);
        this.entryManager.selectTable(query.tableName);
        return this.entryManager.updateEntry(query.whereClause, query.limit, query.updateFields);
    }
    public void startTransaction(){
        if(this.selected == null) throw new IllegalArgumentException("Can not start transaction when no Database selected.");
        this.selected.startTransaction();
    }
    public void rollBack(){
        if(this.selected == null) throw new IllegalArgumentException("Can not roll back when no Database selected.");
        this.selected.rollBack();
    }
    public void commit(){
        if(this.selected == null) throw new IllegalArgumentException("Can not commit when no Database selected.");
        this.selected.commit();
    }
    public void close(){
        Set<String> databaseNames  = new HashSet<>(this.databases.keySet());
        for (String databaseName : databaseNames) {
            this.databases.get(databaseName).commit().close();
        }
    }
    public boolean containsValue(String tableName, String columnName, Object value){
        if(this.selected == null) throw new IllegalArgumentException("Can not use containsValue when no Database selected.");
        Table table = this.selected.getTable(tableName);
        int index = table.getSchema().getNameIndex(columnName);
        return table.isKeyFound(value, index);
    }
    public int[] getColumnSizes(String tableName){
        return this.selected.getTable(tableName).getSchema().getSizes();
    }
    private List<Record> prepareSelectResult(String[] resultColumns,List<Entry> selectResult){
        List<Record> result = new ArrayList<>();
        com.database.db.table.SchemaInner schema = this.entryManager.getTable().getSchema();
        for (Entry entry : selectResult) {
            Object[] values = new Object[resultColumns.length];
            for(int i = 0;i<resultColumns.length;i++){
                String name = resultColumns[i];
                int index = schema.getNameIndex(name);
                if(index == -1) throw new IllegalArgumentException("Invalid result column");
                values[i] = entry.get(index);
            }
            result.add(new Record(resultColumns, values));
        }
        return result;
    }
}