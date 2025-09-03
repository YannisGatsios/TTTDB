package com.database.db;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.database.db.api.DBMS.TableConfig;
import com.database.db.manager.SchemaManager;
import com.database.db.table.Schema;
import com.database.db.table.Table;

public class Database {
    private static final Logger logger = Logger.getLogger(Database.class.getName());

    private String name;
    private String path = "";
    private Map<String,Table> tables;

    public Database(String name){
        this.name = name;
        this.tables = new HashMap<>();
    }

    public void createTable(TableConfig tableConfig) {
        if (this.tables.containsKey(tableConfig.tableName())) {
            logger.info(String.format("Table '%s' already exists in database '%s'. Skipping creation.", tableConfig.tableName(), this.name));
            return;
        }
        Schema schema = new Schema(tableConfig.schema().get());
        SchemaManager.createTable(schema, this.path, this.name, tableConfig.tableName());
        this.addTable(tableConfig);
        logger.info(
            String.format("Table '%s' created successfully in database '%s'.", tableConfig.tableName(), this.name));
    }
    private void addTable(TableConfig tableConfig){
        FileIOThread fileIOThread = new FileIOThread();
        fileIOThread.start();
        try {
            this.tables.put(tableConfig.tableName(), 
                new Table(this.name, this.path, fileIOThread, tableConfig));
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE,
                String.format("ExecutionException: Failed to create table '%s' in database '%s'.",tableConfig.tableName(), this.name),e);
        } catch (IOException e) {
            logger.log(Level.SEVERE,
                String.format("IOException: File operation failed while creating table '%s' in database '%s'.",tableConfig.tableName(), this.name),e);
        } catch (Exception e) {
            logger.log(Level.SEVERE, 
                String.format("Unexpected error while creating table '%s' in database '%s'.", tableConfig.tableName(), this.name), e);
        }
    }
    
    public void dropTable(String tableName){
        Table table = this.getTable(tableName);
        if (table == null) {
            logger.warning(String.format("Warning: Tried to remove non-existent table '%s' from database '%s'.", tableName, this.name));
            return;
        }
        try {
            table.getFileIOThread().shutdown();
            SchemaManager.dropTable(table);
            this.tables.remove(tableName);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.log(Level.WARNING,
                String.format("InterruptedException: Shutdown interrupted while removing table '%s' from database '%s'.", tableName, this.name), e);
        } catch (Exception e) {
            logger.log(Level.SEVERE,
                String.format("Error removing table '%s' from database '%s'.",tableName, name),e);
        }
        logger.info(String.format("Table '%s' removed from database '%s'.", tableName, this.name));
    }

    public void close(){
        for (Table table : this.tables.values()) {
            try {
                table.getFileIOThread().shutdown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.log(Level.WARNING,
                    String.format("InterruptedException: Shutdown interrupted while closing table '%s' in database '%s'.", table.getName(), this.name), e);
            }
        }
        logger.info(String.format("All tables closed for database '%s'.", this.name));
    }

    public void dropDatabase(){
        this.removeAllTables();
    }

    public void removeAllTables(){
        Set<String> tableNames = new HashSet<>(this.tables.keySet());
        for (String string : tableNames) {
            this.dropTable(string);
        }
        logger.info(String.format("Removed %d table(s) from database '%s'.", tableNames.size(), this.name));
    }

    public void startTransaction(){
        Set<String> tableNames = new HashSet<>(this.tables.keySet());
        for (String tableName : tableNames) {
            this.tables.get(tableName).startTransaction();
        }
    }

    public Database commit(){
        Set<String> tableNames = new HashSet<>(this.tables.keySet());
        for (String tableName : tableNames) {
            this.tables.get(tableName).commit();
        }
        return this;
    }

    public Table getTable(String tableName) {return tables.get(tableName);}
    public void setPath(String path){this.path = path;}
    public String getName(){ return this.name; }
    public Map<String,Table> getTables(){ return this.tables; }
}
