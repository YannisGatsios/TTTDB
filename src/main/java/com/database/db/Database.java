package com.database.db;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.database.db.App.TableConfig;
import com.database.db.manager.SchemaManager;
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

    public void create(){
        Path path = Paths.get(this.path+this.name+".xml");
        try {
            if (Files.exists(path)) {
                logger.info(String.format("Database config file '%s' already exists.", path));
            } else {
                Files.createDirectories(path.getParent());
                Files.createFile(path);
                logger.info(String.format("Database config file '%s' created.", path));
            }
        } catch (IOException e) {
            logger.log(Level.SEVERE, String.format("IOException: Could not create config file for database '%s'.", this.name), e);
        }
    }

    public void createTable(TableConfig tableConfig) {
        if (this.tables.containsKey(tableConfig.tableName())) {
            logger.info(String.format("Table '%s' already exists in database '%s'. Skipping creation.", tableConfig.tableName(), this.name));
            return;
        }
        try {
            SchemaManager.createTable(tableConfig.schema(), this.path, this.name, tableConfig.tableName());
            FileIOThread fileIOThread = new FileIOThread();
            fileIOThread.start();
            this.tables.put(tableConfig.tableName(), 
                new Table(this.name,
                    tableConfig.tableName(),
                    tableConfig.schema(),
                    fileIOThread,
                    tableConfig.cacheCapacity(),
                    this.path
                    )
                );
                logger.info(String.format("Table '%s' created successfully in database '%s'.", tableConfig.tableName(), this.name));
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, 
                String.format("ExecutionException: Failed to create table '%s' in database '%s'.", tableConfig.tableName(), this.name), e);
        } catch (IOException e) {
            logger.log(Level.SEVERE, 
                String.format("IOException: File operation failed while creating table '%s' in database '%s'.", tableConfig.tableName(), this.name), e);
        } catch (Exception e) {
            logger.log(Level.SEVERE, 
                String.format("Unexpected error while creating table '%s' in database '%s'.", tableConfig.tableName(), this.name), e);
        }
    }
    public void removeTable(String tableName){
        Table table = this.getTable(tableName);
        if (table == null) {
            logger.warning(String.format("Warning: Tried to remove non-existent table '%s' from database '%s'.", tableName, this.name));
            return;
        }
        try {
            table.getFileIOThread().shutdown();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.log(Level.WARNING,
                String.format("InterruptedException: Shutdown interrupted while removing table '%s' from database '%s'.", tableName, this.name), e);
        }
        SchemaManager.dropTable(table);
        this.tables.remove(tableName);
        logger.info(String.format("Table '%s' removed from database '%s'.", tableName, this.name));
    }
    public void removeAllTables(){
        Set<String> tableNames = new HashSet<>(this.tables.keySet());
        for (String string : tableNames) {
            this.removeTable(string);
        }
        logger.info(String.format("Removed %d table(s) from database '%s'.", tableNames.size(), this.name));
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
    public Table getTable(String tableName) {
        return tables.get(tableName);
    }
    public void setPath(String path){
        this.path = path;
    }

    public String getName(){ return this.name; }
    public Map<String,Table> getTables(){ return this.tables; }
}
