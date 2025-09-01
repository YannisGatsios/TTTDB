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
import com.database.db.page.Entry;
import com.database.db.table.Schema;
import com.database.db.table.Table;

public class DBMS {
    static {
        Logger rootLogger = Logger.getLogger("");
        rootLogger.setLevel(Level.ALL);
        for (Handler handler : rootLogger.getHandlers()) {
            handler.setLevel(Level.ALL);
        }
    }
    private Map<String,Database> databases;
    private Database selected;
    private String path = "";
    private EntryManager entryManager;

    public record TableConfig(String tableName, String schemaConfig, CacheCapacity cacheCapacity){}
    public record CacheCapacity(int tableCapacity, int indexCapacity){}

    public record Record(String[] columnNames, Object[] values){
        public Object get(String columnName){
            for (int i =0;i<columnNames.length;i++) {
                if(columnName.equals(columnNames[i])) return values[i];
            }
            throw new IllegalArgumentException("Invalid column name "+columnName);
        }
        public Object get(int index){
            return values[index];
        }
        public Object[] getAll(){
            return values;
        }
    }

    public record SelectQuery(String tableName, String[] resultColumns, WhereClause whereClause, int begin, int limit){}
    public record InsertQuery(String tableName, String[] columns, Object[] values){}
    public record DeleteQuery(String tableName, WhereClause whereClause, int limit){}
    public record UpdateQuery(String tableName, WhereClause whereClause, int limit, UpdateFields updateFields){}

    public DBMS(){
        this.databases = new HashMap<>();
        this.entryManager = new EntryManager();
    }
    public DBMS(String databaseName, String path){
        this();
        this.setPath(path);
        this.createDatabase(databaseName);
        this.selectDatabase(databaseName);
    }
    public DBMS setPath(String path){
        this.path = path;
        return this;
    }
    public DBMS createDatabase(String databaseName){
        Database database = new Database(databaseName);
        database.setPath(this.path);
        this.databases.put(databaseName, database);
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
    public DBMS createTable(TableConfig config){
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
        return this.prepareSelectResult(query.resultColumns, result);
    }
    public List<Record> selectDescending(SelectQuery query){
        if(this.selected == null) throw new IllegalArgumentException("Can not perform select statement when no Database selected.");
        this.entryManager.selectDatabase(selected);
        this.entryManager.selectTable(query.tableName);
        List<Entry> result = this.entryManager.selectEntriesDescending(query.whereClause, query.begin, query.limit);
        return this.prepareSelectResult(query.resultColumns, result);
    }
    public void insert(InsertQuery query){
        if(this.selected == null) throw new IllegalArgumentException("Can not perform insert statement when no Database selected.");
        this.entryManager.selectDatabase(selected);
        this.entryManager.selectTable(query.tableName);
        Entry entry = Entry.prepareEntry(query.columns, query.values, this.selected.getTable(query.tableName));
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
        if(this.selected == null) throw new IllegalArgumentException("Can not commit when no Database selected.");
        this.selected.startTransaction();
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
        Schema schema = this.entryManager.getTable().getSchema();
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
