package com.database.db;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import com.database.db.App.TableConfig;
import com.database.db.table.Table;

public class Database {
    private String name;
    private String path = "storage/";
    private HashMap<String,Table> tables;

    public Database(String name){
        this.name = name;
        this.tables = new HashMap<>();
    }

    public void addTable(TableConfig tableConfig) throws ExecutionException, InterruptedException, IOException, Exception {// TODO handle exceptions
        FileIOThread fileIOThread = new FileIOThread();
        fileIOThread.start();//Starting File IO Operations Thread
        this.tables.put(tableConfig.tableName(), 
            new Table(this.name,
                tableConfig.tableName(),
                tableConfig.schema(),
                fileIOThread,
                tableConfig.cacheCapacity(),
                this.path
                )
            );
    }
    public void removeTable(String tableName) throws InterruptedException{
        Table table = this.getTable(tableName);
        table.getFileIOThread().shutdown();
        this.tables.remove(tableName);
    }
    public Table getTable(String tableName) {
        return tables.get(tableName);
    }
    public void close() throws InterruptedException{
        for (Table table : this.tables.values()) {
            table.getFileIOThread().shutdown();
        }
    }

    public String getName(){ return this.name; }
    public Map<String,Table> getTables(){ return this.tables; }
}
