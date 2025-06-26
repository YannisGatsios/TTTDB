package com.database.db;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import com.database.db.table.Schema;
import com.database.db.table.Table;

public class Database {
    private String name;
    private Map<String,Table<?>> tables;
    private FileIOThread fileIOThread;

    public Database(String name){
        this.name = name;
        this.tables = new HashMap<>();
    }

    public void addTable(String tableName, Schema schema) throws ExecutionException, InterruptedException, IOException {//TODO handle exceptions
        String type = schema.getColumnTypes()[schema.getPrimaryKeyIndex()];
        Table<?> table;
        switch (type) {
            case "String" -> table = new Table<String>(this.name, tableName, schema, this.fileIOThread);
            case "Integer" -> table = new Table<Integer>(this.name, tableName, schema, this.fileIOThread);
            default -> throw new IllegalArgumentException("Unsupported column type: " + type);
        }
        this.tables.put(tableName, table);
    }
    public void removeTable(String tableName){
        this.tables.remove(tableName);
    }
    public Table<?> getTable(String tableName) {
        return tables.get(tableName);
    }

    public void setFileIOThread(FileIOThread fileIOThread){ this.fileIOThread = fileIOThread; }
    public String getName(){ return this.name; }
    public void setName(String newName){ this.name = newName; }
    public Map<String,Table<?>> getTables(){ return this.tables; }
}
