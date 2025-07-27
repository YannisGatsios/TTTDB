package com.database.db;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import com.database.db.App.TableConfig;
import com.database.db.manager.SchemaManager;
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
        SchemaManager.createTable(tableConfig.schema(), this.path, this.name, tableConfig.tableName());
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
        SchemaManager.dropTable(table);
        this.tables.remove(tableName);
    }
    public void removeAllTables(){
        Set<String> tableNames = new HashSet<>(this.tables.keySet());
        for (String string : tableNames) {
            try {
                this.removeTable(string);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                System.out.println("Failed internally to drop table: "+string+" from Database: "+this.name);
                e.printStackTrace();
            }
        }
    }
    public void close() throws InterruptedException{
        for (Table table : this.tables.values()) {
            table.getFileIOThread().shutdown();
        }
    }
    public Table getTable(String tableName) {
        return tables.get(tableName);
    }
    public void setPath(String path){
        this.path = path;
    }

    public String getName(){ return this.name; }
    public Map<String,Table> getTables(){ return this.tables; }
}
