package com.database.tttdb.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.database.tttdb.api.DBMS;
import com.database.tttdb.api.DatabaseException;
import com.database.tttdb.api.ForeignKey.ForeignKeyAction;
import com.database.tttdb.core.cache.Cache;
import com.database.tttdb.core.cache.TransactionCache;
import com.database.tttdb.core.manager.SchemaManager;
import com.database.tttdb.core.table.Table;
import com.database.tttdb.api.ForeignKey;
import com.database.tttdb.api.Schema;

public class Database {
    private static final Logger logger = Logger.getLogger(Database.class.getName());

    private final String name;
    private final DBMS dbms;
    private String path = "";
    private final Map<String,Table> tables;
    private final Map<String,Schema> schema;

    private final Cache mainCache;
    private TransactionCache currentCache;
    private FileIOThread fileIOThread;

    public Database(String name, DBMS dbms, int cacheCapacity){
        this.name = name;
        this.dbms = dbms;
        this.tables = new HashMap<>();
        this.schema = new HashMap<>();
        this.fileIOThread = new FileIOThread(name);
        this.mainCache = new Cache(this, cacheCapacity);
    }

    public void start() {
        if (fileIOThread != null && fileIOThread.isAlive()) return;
        fileIOThread = new FileIOThread(name); // new instance
        fileIOThread.start();
        mainCache.setFileIOThread(fileIOThread);
        for (String t : new HashSet<>(tables.keySet())) {
            Table table = tables.get(t);
            SchemaManager.createTable(table.getSchema(), path, name, t);
            table.start();
        }
    }

    public void close(){
        try {
            this.fileIOThread.shutdown();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            String message = String.format("InterruptedException: Shutdown interrupted while closing Database '%s'.", this.name);
            logger.log(Level.WARNING, message, e);
            throw new DatabaseException(message, e);
        }
        logger.info(String.format("All tables closed for database '%s'.", this.name));
    }

    public void createTable(String tableName, Schema tableSchema) {
        if (this.tables.containsKey(tableName)) {
            String message = String.format("Table '%s' already exists in database '%s'. Skipping creation.", tableName, this.name);
            logger.fine(message);
            throw new DatabaseException(message);
        }
        this.addTable(tableName, tableSchema);
        logger.fine(
            String.format("Table '%s' created successfully in database '%s'.", tableName, this.name));
    }
    private void addTable(String tableName, Schema tableSchema){
        Schema newSchema = tableSchema;
        Table newTable = new Table(this, tableName, tableSchema);
        this.prepareForeignKey(newTable, newSchema);
        this.schema.put(tableName, newSchema);
        this.tables.put(tableName, newTable);
    }
    public record TableReference(
        String referenceName,
        String childTable, 
        String parentTable,
        List<String> childColumns, 
        List<String> parentColumns,
        ForeignKeyAction onUpdate, 
        ForeignKeyAction onDelete
    ){}
    private void prepareForeignKey(Table childTable, Schema schema){
        if(schema.getForeignKeys().isEmpty()) return;
        List<ForeignKey> foreignKeyList = schema.getForeignKeys();
        for (ForeignKey foreignKey : foreignKeyList) {
            Table parentTable = tables.get(foreignKey.getReferenceTable());
            TableReference tableReference = new TableReference(
                foreignKey.getName(),
                childTable.getName(), 
                parentTable.getName(), 
                foreignKey.getChildColumns(), 
                foreignKey.getReferenceColumns(), 
                foreignKey.getOnUpdate(), 
                foreignKey.getOnDelete());
            parentTable.addChild(tableReference);
            childTable.addParent(tableReference);
        }
    }

    public void dropDatabase(){
        this.commit();
        this.removeAllTables();
    }
    public void removeAllTables(){
        Set<String> tableNames = new HashSet<>(this.tables.keySet());
        for (String string : tableNames) {
            this.dropTable(string);
        }
        logger.info(String.format("Removed %d table(s) from database '%s'.", tableNames.size(), this.name));
    }
    public void dropTable(String tableName){
        Table table = this.getTable(tableName);
        if (table == null) {
            logger.warning(String.format("Warning: Tried to remove non-existent table '%s' from database '%s'.", tableName, this.name));
            return;
        }
        try {
            SchemaManager.dropTable(table);
            this.tables.remove(tableName);
        } catch (Exception e) {
            String message = String.format("Error removing table '%s' from database '%s'.",tableName, name); 
            logger.log(Level.SEVERE, message,e);
            throw new DatabaseException(message,e);
        }
        logger.info(String.format("Table '%s' removed from database '%s'.", tableName, this.name));
    }

    public void startTransaction(String name){
        if(this.currentCache == null) this.currentCache = new TransactionCache(this, this.mainCache, name);
        else this.currentCache = new TransactionCache(this, this.currentCache, name);
        for(Table table : tables.values()){
            table.beginTransaction();
        }
    }
    public void rollBack(String reason){
        if(this.currentCache == null){
            this.mainCache.rollback(reason);
            return;
        }
        this.currentCache.rollback(reason);
        Cache parent = this.currentCache.getParent();
        if(parent instanceof TransactionCache) this.currentCache = (TransactionCache)parent;
        else this.currentCache = null;
    }
    public Database commit(){
        if(this.currentCache == null) {
            this.mainCache.commit();
            return this;
        }
        this.currentCache.commit();
        Cache parent = this.currentCache.getParent();
        if(parent instanceof TransactionCache) this.currentCache = (TransactionCache)parent;
        else this.currentCache = null;
        return this;
    }

    public Cache getCache(){
        if(this.currentCache == null) return this.mainCache;
        return this.currentCache;
    }
    public DBMS getDBMS() {return this.dbms;}
    public Table getTable(String tableName) {return tables.get(tableName);}
    public Schema getSchema(String tableName) {return schema.get(tableName);}
    public void setPath(String path){this.path = path;}
    public String getPath() { return this.path; }
    public String getName(){ return this.name; }
    public FileIOThread getFileIOThread() { return this.fileIOThread; }
    public List<Table> getAllTablesList() {
        return new ArrayList<>(tables.values());
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("DATABASE: ").append(name.toUpperCase()).append("\n");
        sb.append("Path: ").append(path.isEmpty() ? "<not set>" : path).append("\n");
        sb.append("Tables: ").append(tables.size()).append("\n\n");
        if (tables.isEmpty()) {
            sb.append("No tables defined.\n");
            return sb.toString();
        }
        for (Table table : tables.values()) {
            sb.append(table.getSchema().toString()).append("\n\n");
        }
        return sb.toString().trim();
    }
}